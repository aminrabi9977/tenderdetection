
# src/scrapers/world_bank_scraper.py

from playwright.async_api import async_playwright, TimeoutError
import asyncio
from datetime import datetime, timedelta
import pandas as pd
import logging
from typing import List, Dict, Tuple, Optional
from src.scrapers.base_scraper import BaseScraper
from src.utils.date_utils import normalize_date

logger = logging.getLogger(__name__)

# Number of concurrent detail page processing
MAX_CONCURRENT_PAGES = 5

class WorldBankScraper(BaseScraper):
    def __init__(self, base_url: str):
        super().__init__(base_url)
        # Format today's date and date from a week ago
        self.today = datetime.now()
        self.today_str = self.today.strftime("%B %d, %Y")  # Format: "April 30, 2025"
        
        # Calculate the date from a week ago
        self.week_ago = self.today - timedelta(days=7)
        self.week_ago_str = self.week_ago.strftime("%B %d, %Y")
        
        self.results = []
        self.semaphore = None  # Will be initialized in scrape_data
        
        logger.info(f"World Bank scraper initialized")
        logger.info(f"Date range: {self.week_ago_str} to {self.today_str}")

    async def init_browser(self):
        """Initialize browser instance"""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=False,
            args=['--disable-http2']  # This can help with connection issues
        )
        self.context = await self.browser.new_context(
            viewport={'width': 1280, 'height': 800},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        )
        self.context.set_default_timeout(180000)  # 3 minutes timeout
        self.page = await self.context.new_page()
        # Create a semaphore to limit concurrent pages
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)

    async def close_browser(self):
        """Close browser instance"""
        await self.context.close()
        await self.browser.close()
        await self.playwright.stop()

    async def is_date_in_range(self, date_text: str) -> bool:
        """Check if a date is within our specified range (week_ago to today)"""
        try:
            # World Bank format is like "April 28, 2025"
            date_obj = datetime.strptime(date_text.strip(), "%B %d, %Y")
            return self.week_ago <= date_obj <= self.today
        except ValueError:
            logger.warning(f"Could not parse date: {date_text}")
            return False
            
    async def is_date_older_than_range(self, date_text: str) -> bool:
        """Check if a date is older than our range"""
        try:
            date_obj = datetime.strptime(date_text.strip(), "%B %d, %Y")
            return date_obj < self.week_ago
        except ValueError:
            return False

    async def extract_project_details(self, project_url: str) -> Optional[Dict]:
        """Extract additional project details from the project page"""
        detail_page = None
        try:
            # Use semaphore to limit concurrent pages
            async with self.semaphore:
                # Create a new page for the project detail
                detail_page = await self.context.new_page()
                
                # Try to load the page with multiple retry options
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        await detail_page.goto(project_url, timeout=60000)  # 60 seconds
                        break
                    except TimeoutError:
                        if attempt < max_retries - 1:
                            logger.warning(f"Attempt {attempt+1} timed out, retrying...")
                            await asyncio.sleep(2)  # Wait 2 seconds before retrying
                        else:
                            logger.error(f"Failed to load project details after {max_retries} attempts")
                            return None
                
                # Wait for page to load with more flexible options
                try:
                    await detail_page.wait_for_load_state("domcontentloaded", timeout=30000)
                    
                    # Wait for the project details container
                    await detail_page.wait_for_selector(".detail-download-section", 
                                                       state="visible", 
                                                       timeout=30000)
                except TimeoutError:
                    logger.warning("Timeout waiting for project details, proceeding with partial content")
                
                # Initialize project details dictionary
                project_details = {}
                
                # Extract Project ID (with timeout handling)
                try:
                    project_id_elem = await detail_page.query_selector("label:text('Project ID') + p.document-info")
                    if project_id_elem:
                        project_details['project_id'] = await project_id_elem.inner_text()
                except Exception as e:
                    logger.warning(f"Error extracting Project ID: {str(e)}")
                
                # Extract key fields with error handling
                fields_to_extract = [
                    ('Status', 'status'),
                    ('Team Leader', 'team_leader'),
                    ('Borrower', 'borrower'),
                    ('Disclosure Date', 'disclosure_date'),
                    ('Approval Date', 'approval_date'),
                    ('Effective Date', 'effective_date'),
                    ('Total Project Cost', 'total_project_cost'),
                    ('Implementing Agency', 'implementing_agency'),
                    ('Region', 'region'),
                    ('Fiscal Year', 'fiscal_year'),
                    ('Commitment Amount', 'commitment_amount'),
                    ('Environmental Category', 'environmental_category'),
                    ('Environmental and Social Risk', 'environmental_social_risk'),
                    ('Closing Date', 'closing_date'),
                    ('Last Update Date', 'last_update_date')
                ]
                
                for label, key in fields_to_extract:
                    try:
                        elem = await detail_page.query_selector(f"label:text('{label}') + p.document-info")
                        if elem:
                            project_details[key] = await elem.inner_text()
                    except Exception:
                        continue
                
                return project_details
                
        except Exception as e:
            logger.error(f"Error extracting project details from {project_url}: {str(e)}")
            return None
        finally:
            if detail_page:
                await detail_page.close()

    async def process_row(self, row) -> Optional[Dict]:
        """Process a single row from the table"""
        try:
            cells = await row.query_selector_all("td")
            if not cells or len(cells) < 6:
                return None
                
            # Extract published date from the last column
            date_text = await cells[5].inner_text()
            date_text = date_text.strip()
            
            # Check if date is older than our range (signal to stop searching further pages)
            if await self.is_date_older_than_range(date_text):
                return "STOP_SEARCH"
                
            # Check if date is in our range
            if not await self.is_date_in_range(date_text):
                return None
            
            # If we're here, the date is in our range
            row_data = {}
            
            # Extract description and its link
            desc_cell = cells[0]
            desc_link = await desc_cell.query_selector("a")
            if desc_link:
                row_data['description'] = await desc_link.inner_text()
                row_data['description_link'] = await desc_link.get_attribute('href')
            else:
                row_data['description'] = await desc_cell.inner_text()

            # Get basic info from the row
            row_data.update({
                'country': await cells[1].inner_text(),
                'project_title': await cells[2].inner_text(),
                'notice_type': await cells[3].inner_text(),
                'language': await cells[4].inner_text(),
                'publish_date': date_text
            })

            # Get project link if exists
            proj_link = await cells[2].query_selector("a")
            if proj_link:
                project_url = await proj_link.get_attribute('href')
                row_data['project_link'] = project_url
                
                # Extract additional project details
                project_details = await self.extract_project_details(project_url)
                if project_details:
                    row_data.update(project_details)

            logger.info(f"Found matching row: {row_data['description'][:50]}...")
            return row_data
            
        except Exception as e:
            logger.error(f"Error processing row: {str(e)}")
            return None

    async def process_rows_batch(self, rows) -> Tuple[List[Dict], bool]:
        """Process a batch of table rows in parallel"""
        tasks = []
        for row in rows:
            tasks.append(self.process_row(row))
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        
        # Check if any result is the stop signal
        should_stop = False
        filtered_results = []
        
        for result in results:
            if result == "STOP_SEARCH":
                should_stop = True
            elif result is not None:
                filtered_results.append(result)
        
        return filtered_results, should_stop

    async def check_page_for_date_range(self) -> Tuple[bool, bool]:
        """
        Check if current page has any rows with dates in our range.
        Also checks if we should stop searching because we've found older dates.
        
        Returns:
            Tuple[bool, bool]: (has_matches, should_stop_searching)
        """
        try:
            rows = await self.page.query_selector_all("table.project-opt-table tbody tr")
            has_matches = False
            should_stop_searching = False
            all_dates_newer = True  # Track if all dates on page are newer than range
            
            for row in rows:
                cells = await row.query_selector_all("td")
                if cells and len(cells) >= 6:
                    date_text = await cells[5].inner_text()
                    date_text = date_text.strip()
                    
                    # Check if date is in our range
                    if await self.is_date_in_range(date_text):
                        has_matches = True
                        all_dates_newer = False
                    
                    # Check if date is older than our range
                    if await self.is_date_older_than_range(date_text):
                        should_stop_searching = True
                        all_dates_newer = False
            
            # If all dates are newer and none are in range, we should keep going
            if all_dates_newer and not has_matches:
                logger.info("All dates on this page are newer than our range, continuing to next page")
            
            return (has_matches, should_stop_searching)
        except Exception as e:
            logger.error(f"Error checking page for date range: {str(e)}")
            return (False, False)

    async def extract_table_data(self) -> Tuple[List[Dict], bool]:
        """
        Extract matching rows from the current page
        
        Returns:
            Tuple[List[Dict], bool]: (results, should_stop)
        """
        try:
            # Get all rows from the table
            rows = await self.page.query_selector_all("table.project-opt-table tbody tr")
            logger.info(f"Found {len(rows)} rows in the table")
            
            if not rows:
                return [], False
            
            # Process rows in parallel batches
            batch_size = MAX_CONCURRENT_PAGES
            all_results = []
            should_stop = False
            
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i+batch_size]
                logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} rows)")
                
                batch_results, batch_should_stop = await self.process_rows_batch(batch)
                all_results.extend(batch_results)
                
                if batch_should_stop:
                    should_stop = True
                    break
                
                logger.info(f"Found {len(batch_results)} matching rows in this batch")
            
            return all_results, should_stop
            
        except Exception as e:
            logger.error(f"Error extracting table data: {str(e)}")
            return [], False

    async def check_next_page(self) -> bool:
        """Check if there's a next page and navigate to it if exists"""
        try:
            # Check and navigate to next page if exists
            next_button = await self.page.query_selector("li:not(.disabled) a i.fa.fa-angle-right:not(.fa-angle-right + i)")
            if next_button:
                # Find the parent <a> element to get the href
                next_link = await next_button.evaluate("el => el.closest('a')")
                logger.info("Found next page link")
                
                # Click the next button
                await next_button.evaluate("el => el.closest('a').click()")
                
                # Wait for navigation with more flexible timeout
                try:
                    await self.page.wait_for_load_state("domcontentloaded", timeout=60000)
                    # Wait for table to appear on the next page
                    await self.page.wait_for_selector("table.project-opt-table", 
                                                   state="visible", 
                                                   timeout=60000)
                    logger.info("Successfully navigated to next page")
                    
                    # Add a short sleep to let the page stabilize
                    await asyncio.sleep(2)
                    return True
                except TimeoutError:
                    logger.warning("Timeout navigating to next page, trying to continue anyway")
                    # Check if the table is present despite the timeout
                    table_present = await self.page.query_selector("table.project-opt-table")
                    if table_present:
                        logger.info("Table found despite timeout, continuing")
                        return True
                    else:
                        logger.error("Table not found after navigation - cannot continue")
                        return False
            else:
                logger.info("No next page button found - reached the end of pagination")
                return False
                
        except Exception as e:
            logger.error(f"Error checking next page: {str(e)}")
            # Try to verify current page number to see if we successfully moved forward
            try:
                active_page_elem = await self.page.query_selector("li.active a")
                if active_page_elem:
                    active_page_text = await active_page_elem.inner_text()
                    active_page_num = int(active_page_text.strip())
                    logger.info(f"Current active page indicator: {active_page_num}")
                    # Return true indicating we did move to a new page
                    return True
                else:
                    logger.error("Navigation failed, cannot determine current page")
                    return False
            except Exception:
                logger.error("Failed to verify current page, stopping pagination")
                return False

    async def scrape_data(self):
        """Main scraping function"""
        try:
            await self.init_browser()
            
            # Go to the page with more robust handling
            logger.info(f"Navigating to {self.base_url}...")
            
            # Try with multiple loading states and retry
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    await self.page.goto(self.base_url, timeout=120000, wait_until="domcontentloaded")
                    logger.info("Page loaded (DOM content)")
                    
                    # Try to wait for the table to appear
                    try:
                        await self.page.wait_for_selector("table.project-opt-table", 
                                                        state="visible", 
                                                        timeout=60000)
                        logger.info("Table found")
                        break
                    except TimeoutError:
                        logger.warning("Table not found after timeout, will try alternative approach")
                        
                        # Alternative: wait for any table
                        await self.page.wait_for_selector("table", state="visible", timeout=30000)
                        logger.info("Found a table element, proceeding")
                        break
                except TimeoutError:
                    if attempt < max_retries - 1:
                        logger.warning(f"Attempt {attempt+1} timed out, retrying...")
                        await asyncio.sleep(5)  # Wait 5 seconds before retrying
                    else:
                        logger.error(f"Failed to load page after {max_retries} attempts")
                        # Try one last approach - just wait for any content
                        await self.page.goto(self.base_url, timeout=120000, wait_until="commit")
                        await asyncio.sleep(10)  # Wait 10 seconds for any content to load
            
            # If we get here, check if we have the table
            table_exists = await self.page.query_selector("table.project-opt-table")
            if not table_exists:
                logger.error("The World Bank table is not visible. Cannot proceed.")
                return pd.DataFrame()  # Return empty DataFrame
            
            current_page = 1
            MAX_PAGES = 100  # Very high limit - effectively unlimited
            found_older_date = False
            no_more_pages = False
            
            while current_page <= MAX_PAGES and not no_more_pages:
                logger.info(f"Processing page {current_page}")
                
                # First check if this page has any matching dates in our range
                has_matches, should_stop_searching = await self.check_page_for_date_range()
                
                if has_matches:
                    logger.info(f"Found dates in our range on page {current_page}, extracting data")
                    # Extract data from current page
                    page_data, should_stop = await self.extract_table_data()
                    self.results.extend(page_data)
                    
                    logger.info(f"Found {len(page_data)} matching rows on page {current_page}")
                    
                    # If we should stop searching further pages, break here
                    if should_stop or should_stop_searching:
                        logger.info("Found dates older than our range, stopping search")
                        found_older_date = True
                        break
                elif should_stop_searching:
                    logger.info(f"Found dates older than our range on page {current_page}, stopping search")
                    found_older_date = True
                    break
                else:
                    logger.info(f"No matching dates found on page {current_page}, checking next page")
                
                # Check and navigate to next page if exists
                has_next = await self.check_next_page()
                if not has_next:
                    logger.info("No more pages to process")
                    break
                    
                current_page += 1
                
            if current_page >= MAX_PAGES:
                logger.info(f"Reached maximum page limit ({MAX_PAGES}). Stopping search.")
            
            # Convert results to DataFrame
            df = pd.DataFrame(self.results)
            
            if not df.empty:
                logger.info(f"Total rows collected: {len(df)}")
            else:
                logger.info("No matching rows found")
                
            return df
            
        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}")
            raise
        finally:
            await self.close_browser()