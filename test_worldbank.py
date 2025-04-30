# # test_worldbank.py

# import asyncio
# import logging
# from datetime import datetime
# from playwright.async_api import async_playwright
# import pandas as pd

# # Set up basic logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# class WorldBankScraper:
#     def __init__(self, url: str, target_date: str):
#         self.url = url
#         self.target_date = target_date
#         self.results = []

#     async def check_page_for_date(self, page) -> bool:
#         """Check if current page has any rows with target date"""
#         rows = await page.query_selector_all("table.project-opt-table tbody tr")
        
#         for row in rows:
#             cells = await row.query_selector_all("td")
#             if cells and len(cells) >= 6:
#                 date_text = await cells[5].inner_text()
#                 if date_text.strip() == self.target_date:
#                     return True
#         return False

#     async def scrape_data(self):
#         async with async_playwright() as p:
#             browser = await p.chromium.launch(headless=False)
#             context = await browser.new_context()
#             page = await context.new_page()
            
#             try:
#                 page.set_default_timeout(120000)
#                 await page.goto(self.url, wait_until="networkidle")
#                 logger.info("Page loaded successfully")

#                 await page.wait_for_selector("table.project-opt-table", state="visible")
#                 logger.info("Table found")
                
#                 current_page = 1
#                 while True:
#                     logger.info(f"Processing page {current_page}")
#                     await page.wait_for_selector("table.project-opt-table tbody tr", state="visible")
                    
#                     # First check if this page has any matching dates
#                     has_matching_dates = await self.check_page_for_date(page)
#                     if not has_matching_dates:
#                         logger.info(f"No matching dates found on page {current_page}, stopping search")
#                         break

#                     # If we found matching dates, process the rows
#                     rows = await page.query_selector_all("table.project-opt-table tbody tr")
#                     found_on_page = 0
                    
#                     for row in rows:
#                         cells = await row.query_selector_all("td")
#                         if not cells or len(cells) < 6:
#                             continue

#                         date_text = await cells[5].inner_text()
#                         date_text = date_text.strip()
                        
#                         if date_text == self.target_date:
#                             row_data = {}
                            
#                             desc_cell = cells[0]
#                             desc_link = await desc_cell.query_selector("a")
#                             if desc_link:
#                                 row_data['description'] = await desc_link.inner_text()
#                                 row_data['description_link'] = await desc_link.get_attribute('href')
#                             else:
#                                 row_data['description'] = await desc_cell.inner_text()

#                             row_data.update({
#                                 'country': await cells[1].inner_text(),
#                                 'project_title': await cells[2].inner_text(),
#                                 'notice_type': await cells[3].inner_text(),
#                                 'language': await cells[4].inner_text(),
#                                 'publish_date': date_text
#                             })

#                             proj_link = await cells[2].query_selector("a")
#                             if proj_link:
#                                 row_data['project_link'] = await proj_link.get_attribute('href')

#                             self.results.append(row_data)
#                             found_on_page += 1
#                             logger.info(f"Found matching row: {row_data['description'][:50]}...")

#                     logger.info(f"Found {found_on_page} matching rows on page {current_page}")

#                     # Try to go to next page
#                     next_button = await page.query_selector("li:not(.disabled) a i.fa.fa-angle-right:not(.fa-angle-right + i)")
#                     if next_button:
#                         await next_button.evaluate("el => el.closest('a').click()")
#                         await page.wait_for_load_state("networkidle")
#                         current_page += 1
#                         logger.info(f"Moving to page {current_page}")
#                     else:
#                         logger.info("No more pages available")
#                         break

#                 df = pd.DataFrame(self.results)
#                 return df

#             except Exception as e:
#                 logger.error(f"Error during scraping: {str(e)}")
#                 raise
#             finally:
#                 await browser.close()

# async def test_scraper():
#     url = "https://projects.worldbank.org/en/projects-operations/procurement?srce=both"
    
#     # Your target date (format: "February 22, 2025")
#     target_date = "February 23, 2025"
    
#     try:
#         scraper = WorldBankScraper(url, target_date)
#         df = await scraper.scrape_data()
        
#         if not df.empty:
#             output_file = f"world_bank_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
#             df.to_csv(output_file, index=False)
#             logger.info(f"Data saved to {output_file}")
#             logger.info(f"Total rows found: {len(df)}")
#         else:
#             logger.info(f"No data found for date: {target_date}")
            
#     except Exception as e:
#         logger.error(f"Error during scraping: {str(e)}")

# if __name__ == "__main__":
#     asyncio.run(test_scraper())
# ----------------------------------------------------------------------------------------------------------

# test_worldbank.py

# test_worldbank.py

import asyncio
import logging
from datetime import datetime, timedelta
from playwright.async_api import async_playwright, TimeoutError
import pandas as pd
from typing import List, Dict, Optional, Tuple

# Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Number of concurrent detail page processing
MAX_CONCURRENT_PAGES = 5

class WorldBankScraperTest:
    def __init__(self, url: str, start_date: str, end_date: str):
        self.url = url
        self.start_date_str = start_date
        self.end_date_str = end_date
        
        # Convert to datetime objects for comparison
        self.start_date = datetime.strptime(start_date, "%B %d, %Y")
        self.end_date = datetime.strptime(end_date, "%B %d, %Y")
        
        self.results = []
        self.semaphore = None  # Will be initialized in scrape_data
        
        logger.info(f"World Bank scraper test initialized")
        logger.info(f"Date range: {start_date} to {end_date}")

    async def is_date_in_range(self, date_text: str) -> bool:
        """Check if a date is within our specified range"""
        try:
            # World Bank format is like "April 28, 2025"
            date_obj = datetime.strptime(date_text.strip(), "%B %d, %Y")
            return self.start_date <= date_obj <= self.end_date
        except ValueError as e:
            logger.warning(f"Could not parse date: {date_text}. Error: {str(e)}")
            return False
            
    async def is_date_older_than_range(self, date_text: str) -> bool:
        """Check if a date is older than our range"""
        try:
            date_obj = datetime.strptime(date_text.strip(), "%B %d, %Y")
            return date_obj < self.start_date
        except ValueError:
            return False

    async def extract_project_details(self, context, project_url: str) -> Optional[Dict]:
        """Extract additional project details from the project page"""
        detail_page = None
        try:
            # Use semaphore to limit concurrent pages
            async with self.semaphore:
                # Create a new page for the project detail
                detail_page = await context.new_page()
                
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
                
                # Extract Status
                try:
                    status_elem = await detail_page.query_selector("label:text('Status') + p.document-info")
                    if status_elem:
                        project_details['status'] = await status_elem.inner_text()
                except Exception:
                    pass
                
                # Extract Team Leader
                try:
                    team_leader_elem = await detail_page.query_selector("label:text('Team Leader') + p.document-info")
                    if team_leader_elem:
                        project_details['team_leader'] = await team_leader_elem.inner_text()
                except Exception:
                    pass
                
                # Extract Borrower
                try:
                    borrower_elem = await detail_page.query_selector("label:text('Borrower') + p.document-info")
                    if borrower_elem:
                        project_details['borrower'] = await borrower_elem.inner_text()
                except Exception:
                    pass
                
                # Only extract a few key fields to reduce load
                fields_to_extract = [
                    ('Implementing Agency', 'implementing_agency'),
                    ('Total Project Cost', 'total_project_cost'),
                    ('Commitment Amount', 'commitment_amount'),
                    ('Closing Date', 'closing_date')
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

    async def process_row(self, context, row) -> Optional[Dict]:
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
                project_details = await self.extract_project_details(context, project_url)
                if project_details:
                    row_data.update(project_details)

            logger.info(f"Found matching row: {row_data['description'][:50]}...")
            return row_data
            
        except Exception as e:
            logger.error(f"Error processing row: {str(e)}")
            return None

    async def process_rows_batch(self, context, rows) -> Tuple[List[Dict], bool]:
        """Process a batch of table rows in parallel"""
        tasks = []
        for row in rows:
            tasks.append(self.process_row(context, row))
        
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

    async def check_page_for_date_range(self, page) -> Tuple[bool, bool]:
        """
        Check if current page has any rows with dates in our range.
        Also checks if we should stop searching because we've found older dates.
        
        Returns:
            Tuple[bool, bool]: (has_matches, should_stop_searching)
        """
        try:
            rows = await page.query_selector_all("table.project-opt-table tbody tr")
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

    async def scrape_data(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=False,
                args=['--disable-http2']  # This can help with some connection issues
            )
            context = await browser.new_context(
                viewport={'width': 1280, 'height': 800},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            )
            page = await context.new_page()
            
            # Create a semaphore to limit concurrent pages
            self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
            
            try:
                # Set longer timeout but use "domcontentloaded" instead of "networkidle"
                page.set_default_timeout(180000)  # 3 minutes timeout
                
                # Go to the page with more robust handling
                logger.info(f"Navigating to {self.url}...")
                
                # Try with multiple loading states and retry
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        await page.goto(self.url, timeout=120000, wait_until="domcontentloaded")
                        logger.info("Page loaded (DOM content)")
                        
                        # Try to wait for the table to appear
                        try:
                            await page.wait_for_selector("table.project-opt-table", 
                                                       state="visible", 
                                                       timeout=60000)
                            logger.info("Table found")
                            break
                        except TimeoutError:
                            logger.warning("Table not found after timeout, will try alternative approach")
                            
                            # Alternative: wait for any table
                            await page.wait_for_selector("table", state="visible", timeout=30000)
                            logger.info("Found a table element, proceeding")
                            break
                    except TimeoutError:
                        if attempt < max_retries - 1:
                            logger.warning(f"Attempt {attempt+1} timed out, retrying...")
                            await asyncio.sleep(5)  # Wait 5 seconds before retrying
                        else:
                            logger.error(f"Failed to load page after {max_retries} attempts")
                            # Try one last approach - just wait for any content
                            await page.goto(self.url, timeout=120000, wait_until="commit")
                            await asyncio.sleep(10)  # Wait 10 seconds for any content to load
                
                # If we get here, check if we have the table
                table_exists = await page.query_selector("table.project-opt-table")
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
                    has_matches, should_stop_searching = await self.check_page_for_date_range(page)
                    
                    if has_matches:
                        logger.info(f"Found dates in our range on page {current_page}, extracting data")
                        
                        # Get all rows from the table
                        rows = await page.query_selector_all("table.project-opt-table tbody tr")
                        logger.info(f"Found {len(rows)} rows on page {current_page}")
                        
                        # Process rows in parallel batches
                        batch_size = MAX_CONCURRENT_PAGES
                        for i in range(0, len(rows), batch_size):
                            batch = rows[i:i+batch_size]
                            logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} rows)")
                            
                            batch_results, should_stop = await self.process_rows_batch(context, batch)
                            self.results.extend(batch_results)
                            
                            logger.info(f"Found {len(batch_results)} matching rows in this batch")
                            
                            if should_stop:
                                logger.info("Found dates older than our range, stopping search")
                                found_older_date = True
                                break
                        
                        # If we should stop searching further pages, break here
                        if should_stop_searching or found_older_date:
                            logger.info("Found dates older than our range, stopping search")
                            break
                    elif should_stop_searching:
                        logger.info(f"Found dates older than our range on page {current_page}, stopping search")
                        found_older_date = True
                        break
                    else:
                        logger.info(f"No matching dates found on page {current_page}, checking next page")
                    
                    # Check and navigate to next page if exists
                    try:
                        next_button = await page.query_selector("li:not(.disabled) a i.fa.fa-angle-right:not(.fa-angle-right + i)")
                        if next_button:
                            # Find the parent <a> element to get the href
                            next_link = await next_button.evaluate("el => el.closest('a')")
                            logger.info(f"Found next page link (page {current_page+1})")
                            
                            # Click the next button
                            await next_button.evaluate("el => el.closest('a').click()")
                            
                            # Wait for navigation with more flexible timeout
                            try:
                                await page.wait_for_load_state("domcontentloaded", timeout=60000)
                                # Wait for table to appear on the next page
                                await page.wait_for_selector("table.project-opt-table", 
                                                           state="visible", 
                                                           timeout=60000)
                                logger.info(f"Successfully navigated to page {current_page+1}")
                                current_page += 1
                                
                                # Add a short sleep to let the page stabilize
                                await asyncio.sleep(2)
                            except TimeoutError:
                                logger.warning(f"Timeout navigating to page {current_page+1}, trying to continue anyway")
                                # Check if the table is present despite the timeout
                                table_present = await page.query_selector("table.project-opt-table")
                                if table_present:
                                    logger.info("Table found despite timeout, continuing")
                                    current_page += 1
                                else:
                                    logger.error("Table not found after navigation - cannot continue")
                                    no_more_pages = True
                        else:
                            logger.info("No next page button found - reached the end of pagination")
                            no_more_pages = True
                    except Exception as e:
                        logger.error(f"Error navigating to next page: {str(e)}")
                        # Try to verify current page number to see if we successfully moved forward
                        try:
                            active_page_elem = await page.query_selector("li.active a")
                            if active_page_elem:
                                active_page_text = await active_page_elem.inner_text()
                                active_page_num = int(active_page_text.strip())
                                logger.info(f"Current active page indicator: {active_page_num}")
                                if active_page_num > current_page:
                                    logger.info(f"Navigation succeeded despite error, now on page {active_page_num}")
                                    current_page = active_page_num
                                else:
                                    logger.error("Navigation failed, still on same page")
                                    no_more_pages = True
                            else:
                                logger.error("Cannot determine current page number, stopping pagination")
                                no_more_pages = True
                        except Exception:
                            logger.error("Failed to verify current page, stopping pagination")
                            no_more_pages = True
                
                if current_page >= MAX_PAGES:
                    logger.info(f"Reached maximum page limit ({MAX_PAGES}). Stopping search.")
                
                # Convert results to DataFrame
                df = pd.DataFrame(self.results)
                
                if not df.empty:
                    logger.info(f"Total rows collected: {len(df)}")
                else:
                    logger.info(f"No matching rows found for date range: {self.start_date_str} to {self.end_date_str}")
                    
                return df

            except Exception as e:
                logger.error(f"Error during scraping: {str(e)}")
                raise
            finally:
                await browser.close()

async def test_scraper():
    url = "https://projects.worldbank.org/en/projects-operations/procurement?srce=both"
    
    # Define date range (format: "Month DD, YYYY") - World Bank format
    # Change these dates to test different ranges
    start_date = "April 20, 2025"
    end_date = "April 28, 2025"
    
    try:
        scraper = WorldBankScraperTest(url, start_date, end_date)
        df = await scraper.scrape_data()
        
        if not df.empty:
            output_file = f"world_bank_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(output_file, index=False)
            logger.info(f"Data saved to {output_file}")
            logger.info(f"Total rows found: {len(df)}")
            print(df.head())  # Print first few rows to preview the data
        else:
            logger.info(f"No data found for date range: {start_date} to {end_date}")
            
    except Exception as e:
        logger.error(f"Error during scraping: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_scraper())