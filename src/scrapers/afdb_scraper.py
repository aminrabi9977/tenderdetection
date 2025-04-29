# # src/scrapers/afdb_scraper.py

# from playwright.async_api import async_playwright
# import asyncio
# from datetime import datetime
# import pandas as pd
# import logging
# from typing import List, Dict, Optional
# from src.scrapers.base_scraper import BaseScraper
# from src.utils.date_utils import normalize_date, format_date_for_site

# logger = logging.getLogger(__name__)

# # Number of concurrent detail page processing
# MAX_CONCURRENT_PAGES = 5

# class AfDBScraper(BaseScraper):
#     def __init__(self, base_url: str):
#         super().__init__(base_url)
#         # Format today's date as "28-Feb-2025" (AfDB format)
#         self.today = datetime.now().strftime("%d-%b-%Y")
#         self.results = []
#         # Extract the base domain from the URL
#         self.domain = '/'.join(base_url.split('/')[:3])  # Get "https://www.afdb.org"
#         self.semaphore = None  # Will be initialized in scrape_data
#         logger.info(f"AfDB scraper initialized with base domain: {self.domain}")
#         logger.info(f"Today's date in AfDB format: {self.today}")

#     async def init_browser(self):
#         """Initialize browser instance"""
#         self.playwright = await async_playwright().start()
#         self.browser = await self.playwright.chromium.launch(headless=False)
#         self.context = await self.browser.new_context()
#         self.context.set_default_timeout(60000)  # 60 seconds timeout
#         self.page = await self.context.new_page()
#         # Create a semaphore to limit concurrent pages
#         self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)

#     async def close_browser(self):
#         """Close browser instance"""
#         await self.context.close()
#         await self.browser.close()
#         await self.playwright.stop()

#     async def extract_sector_info(self, detail_url: str) -> str:
#         """Extract sector information from the detail page"""
#         detail_page = None
#         try:
#             # Use semaphore to limit concurrent pages
#             async with self.semaphore:
#                 # Create a new page for the detail view
#                 detail_page = await self.context.new_page()
#                 await detail_page.goto(detail_url)
#                 await detail_page.wait_for_load_state("networkidle")
                
#                 # Find the related sections block
#                 related_sections = await detail_page.query_selector('#block-views-keywords-block')
                
#                 if not related_sections:
#                     logger.warning(f"Related sections block not found on {detail_url}")
#                     return "N/A"
                
#                 # Extract all li elements within the related sections
#                 sector_items = await related_sections.query_selector_all("ul li a")
                
#                 sectors = []
#                 for item in sector_items:
#                     sector_text = await item.inner_text()
#                     sectors.append(sector_text.strip())
                
#                 # Join sectors with dash
#                 result = " - ".join(sectors)
#                 logger.debug(f"Extracted sectors: {result}")
#                 return result
                
#         except Exception as e:
#             logger.error(f"Error extracting sector info from {detail_url}: {str(e)}")
#             return "N/A"
#         finally:
#             if detail_page:
#                 await detail_page.close()

#     async def process_row_batch(self, rows):
#         """Process a batch of table rows in parallel"""
#         tasks = []
#         for row in rows:
#             tasks.append(self.process_row(row))
        
#         # Wait for all tasks to complete
#         results = await asyncio.gather(*tasks)
        
#         # Filter out None results
#         return [r for r in results if r]

#     async def process_row(self, row) -> Optional[Dict]:
#         """Process a single row from the table"""
#         try:
#             # Extract title cell and link
#             title_cell = await row.query_selector("td.views-field-title a")
#             if not title_cell:
#                 return None
                
#             title_text = await title_cell.inner_text()
#             title_link = await title_cell.get_attribute('href')
            
#             # Get full URL for the detail page
#             if title_link.startswith('/'):
#                 detail_url = self.domain + title_link
#             else:
#                 detail_url = title_link
            
#             # Extract date cell - we already know it matches the target date from pre-filtering
#             date_cell = await row.query_selector("td.views-field-field-publication-date span")
#             publish_date = await date_cell.inner_text() if date_cell else self.today
            
#             # Parse the title to extract country (text after first dash)
#             title_parts = title_text.split('-')
            
#             country = title_parts[1].strip() if len(title_parts) > 1 else "N/A"
            
#             # Extract sector information from detail page
#             sector = await self.extract_sector_info(detail_url)
            
#             logger.info(f"Processed: {title_text[:50]}...")
            
#             return {
#                 'publish_date': publish_date.strip(),
#                 'country': country,
#                 'title': title_text,
#                 'sector': sector,
#                 'url': detail_url
#             }
            
#         except Exception as e:
#             logger.error(f"Error processing row: {str(e)}")
#             return None

#     async def extract_table_data(self) -> List[Dict]:
#         """Extract data from the current page's table"""
#         try:
#             # Wait for the table to be visible
#             await self.page.wait_for_selector("table.views-table tbody tr", state="visible")
            
#             # Get all rows from the table
#             rows = await self.page.query_selector_all("table.views-table tbody tr")
#             logger.info(f"Found {len(rows)} rows in the table")
            
#             # Pre-filter rows that match today's date to avoid unnecessary detail page visits
#             matching_rows = []
#             for row in rows:
#                 date_cell = await row.query_selector("td.views-field-field-publication-date span")
#                 if date_cell:
#                     publish_date = await date_cell.inner_text()
#                     publish_date = publish_date.strip()
#                     if publish_date == self.today:
#                         matching_rows.append(row)
            
#             logger.info(f"Found {len(matching_rows)} rows matching today's date")
            
#             if not matching_rows:
#                 return []
            
#             # Process matching rows in parallel batches
#             batch_size = MAX_CONCURRENT_PAGES
#             all_results = []
            
#             for i in range(0, len(matching_rows), batch_size):
#                 batch = matching_rows[i:i+batch_size]
#                 logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} rows)")
                
#                 batch_results = await self.process_row_batch(batch)
#                 all_results.extend(batch_results)
                
#                 logger.info(f"Found {len(batch_results)} matching rows in this batch")
            
#             return all_results
            
#         except Exception as e:
#             logger.error(f"Error extracting table data: {str(e)}")
#             return []

#     async def check_next_page(self) -> bool:
#         """Check if there's a next page and navigate to it if it exists"""
#         try:
#             # Look for "next" pagination link
#             next_link = await self.page.query_selector("li.pager-next a")
            
#             if next_link:
#                 # Click to navigate to next page
#                 await next_link.click()
#                 await self.page.wait_for_load_state("networkidle")
#                 # Wait for the table to appear on the next page
#                 await self.page.wait_for_selector("table.views-table", state="visible")
#                 logger.info("Navigated to next page")
#                 return True
#             else:
#                 logger.info("No next page found")
#                 return False
                
#         except Exception as e:
#             logger.error(f"Error checking next page: {str(e)}")
#             return False

#     async def check_page_for_today(self) -> bool:
#         """Check if current page has any rows with today's date"""
#         date_cells = await self.page.query_selector_all("td.views-field-field-publication-date span")
        
#         for cell in date_cells:
#             date_text = await cell.inner_text()
#             date_text = date_text.strip()
            
#             if date_text == self.today:
#                 return True
        
#         return False

#     async def scrape_data(self):
#         """Main scraping function"""
#         try:
#             await self.init_browser()
#             await self.page.goto(self.base_url)
#             await self.page.wait_for_load_state("networkidle")
            
#             current_page = 1
            
#             while True:
#                 logger.info(f"Processing page {current_page}")
                
#                 # First check if this page has any matching dates
#                 has_matching_dates = await self.check_page_for_today()
#                 if not has_matching_dates:
#                     logger.info(f"No matching dates found on page {current_page}, stopping search")
#                     break

#                 # Extract data from current page
#                 page_data = await self.extract_table_data()
#                 self.results.extend(page_data)
                
#                 logger.info(f"Found {len(page_data)} matching rows on page {current_page}")
                
#                 # Check and navigate to next page if exists
#                 has_next = await self.check_next_page()
#                 if not has_next:
#                     logger.info("No more pages to process")
#                     break
                    
#                 current_page += 1
            
#             # Convert results to DataFrame
#             df = pd.DataFrame(self.results)
            
#             if not df.empty:
#                 logger.info(f"Total rows collected: {len(df)}")
#             else:
#                 logger.info("No matching rows found")
                
#             return df
            
#         except Exception as e:
#             logger.error(f"Error during scraping: {str(e)}")
#             raise
#         finally:
#             await self.close_browser()

# --------------------------------------------------------------------------------------------------------

# src/scrapers/afdb_scraper.py

from playwright.async_api import async_playwright
import asyncio
from datetime import datetime, timedelta
import pandas as pd
import logging
from typing import List, Dict, Optional, Tuple
from src.scrapers.base_scraper import BaseScraper
from src.utils.date_utils import normalize_date, format_date_for_site

logger = logging.getLogger(__name__)

# Number of concurrent detail page processing
MAX_CONCURRENT_PAGES = 5

class AfDBScraper(BaseScraper):
    def __init__(self, base_url: str):
        super().__init__(base_url)
        # Format today's date and date from a week ago
        self.today = datetime.now()
        # AfDB format is like "18-Apr-2025"
        self.today_str = self.today.strftime("%d-%b-%Y")
        
        # Calculate the date from a week ago
        self.week_ago = self.today - timedelta(days=7)
        self.week_ago_str = self.week_ago.strftime("%d-%b-%Y")
        
        self.results = []
        # Extract the base domain from the URL
        self.domain = '/'.join(base_url.split('/')[:3])  # Get "https://www.afdb.org"
        self.semaphore = None  # Will be initialized in scrape_data
        
        logger.info(f"AfDB scraper initialized with base domain: {self.domain}")
        logger.info(f"Date range: {self.week_ago_str} to {self.today_str}")

    async def init_browser(self):
        """Initialize browser instance"""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=False)
        self.context = await self.browser.new_context()
        self.context.set_default_timeout(60000)  # 60 seconds timeout
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
            # AfDB format is like "18-Apr-2025"
            date_obj = datetime.strptime(date_text.strip(), "%d-%b-%Y")
            return self.week_ago <= date_obj <= self.today
        except ValueError as e:
            logger.warning(f"Could not parse date: {date_text}. Error: {str(e)}")
            return False
            
    async def is_date_older_than_range(self, date_text: str) -> bool:
        """Check if a date is older than our range"""
        try:
            date_obj = datetime.strptime(date_text.strip(), "%d-%b-%Y")
            return date_obj < self.week_ago
        except ValueError:
            return False

    async def extract_sector_info(self, detail_url: str) -> str:
        """Extract sector information from the detail page"""
        detail_page = None
        try:
            # Use semaphore to limit concurrent pages
            async with self.semaphore:
                # Create a new page for the detail view
                detail_page = await self.context.new_page()
                await detail_page.goto(detail_url)
                await detail_page.wait_for_load_state("networkidle")
                
                # Find the related sections block
                related_sections = await detail_page.query_selector('#block-views-keywords-block')
                
                if not related_sections:
                    logger.warning(f"Related sections block not found on {detail_url}")
                    return "N/A"
                
                # Extract all li elements within the related sections
                sector_items = await related_sections.query_selector_all("ul li a")
                
                sectors = []
                for item in sector_items:
                    sector_text = await item.inner_text()
                    sectors.append(sector_text.strip())
                
                # Join sectors with dash
                result = " - ".join(sectors)
                logger.debug(f"Extracted sectors: {result}")
                return result
                
        except Exception as e:
            logger.error(f"Error extracting sector info from {detail_url}: {str(e)}")
            return "N/A"
        finally:
            if detail_page:
                await detail_page.close()

    async def process_row_batch(self, rows):
        """Process a batch of table rows in parallel"""
        tasks = []
        for row in rows:
            tasks.append(self.process_row(row))
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        
        # Filter out None results and "STOP_SEARCH" signals
        filtered_results = []
        should_stop = False
        
        for result in results:
            if result == "STOP_SEARCH":
                should_stop = True
            elif result is not None:
                filtered_results.append(result)
        
        return filtered_results, should_stop

    async def process_row(self, row) -> Optional[Dict]:
        """Process a single row from the table"""
        try:
            # Extract date from the row
            date_cell = await row.query_selector("div.field-content span.date-display-single")
            if not date_cell:
                return None
                
            publish_date = await date_cell.inner_text()
            publish_date = publish_date.strip()
            
            # Check if date is older than our range (signal to stop searching further pages)
            if await self.is_date_older_than_range(publish_date):
                return "STOP_SEARCH"
                
            # Check if date is in our range
            if not await self.is_date_in_range(publish_date):
                return None
            
            # Extract title cell and link
            title_cell = await row.query_selector("span.field-content a")
            if not title_cell:
                return None
                
            title_text = await title_cell.inner_text()
            title_link = await title_cell.get_attribute('href')
            
            # Get full URL for the detail page
            if title_link.startswith('/'):
                detail_url = self.domain + title_link
            else:
                detail_url = title_link
            
            # Parse the title to extract country (text after first dash)
            title_parts = title_text.split('-')
            
            country = title_parts[1].strip() if len(title_parts) > 1 else "N/A"
            
            # Extract sector information from detail page
            sector = await self.extract_sector_info(detail_url)
            
            logger.info(f"Processed: {title_text[:50]}...")
            
            return {
                'publish_date': publish_date,
                'country': country,
                'title': title_text,
                'sector': sector,
                'url': detail_url
            }
            
        except Exception as e:
            logger.error(f"Error processing row: {str(e)}")
            return None

    async def extract_table_data(self) -> Tuple[List[Dict], bool]:
        """Extract data from the current page's grid items"""
        try:
            # Wait for the grid to be visible (based on code1.txt structure)
            await self.page.wait_for_selector(".views-bootstrap-grid-plugin-style .row", state="visible")
            
            # Get all grid items (column divs) from the grid
            grid_items = await self.page.query_selector_all(".views-bootstrap-grid-plugin-style .row > div")
            logger.info(f"Found {len(grid_items)} grid items")
            
            if not grid_items:
                return [], False
            
            # Process grid items in parallel batches
            batch_size = MAX_CONCURRENT_PAGES
            all_results = []
            
            for i in range(0, len(grid_items), batch_size):
                batch = grid_items[i:i+batch_size]
                logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} rows)")
                
                batch_results, should_stop = await self.process_row_batch(batch)
                all_results.extend(batch_results)
                
                logger.info(f"Found {len(batch_results)} matching rows in this batch")
                
                if should_stop:
                    return all_results, True
            
            return all_results, False
            
        except Exception as e:
            logger.error(f"Error extracting grid data: {str(e)}")
            return [], False

    async def check_page_for_date_range(self) -> Tuple[bool, bool]:
        """
        Check if the current page has dates in our range and if we should stop searching
        
        Returns:
            Tuple[bool, bool]: (has_matches, should_stop_searching)
        """
        try:
            # Get all date elements on the page
            date_elems = await self.page.query_selector_all("span.date-display-single")
            
            has_matches = False
            should_stop_searching = False
            
            for elem in date_elems:
                date_text = await elem.inner_text()
                date_text = date_text.strip()
                
                # Check if date is in our range
                if await self.is_date_in_range(date_text):
                    has_matches = True
                
                # Check if date is older than our range
                if await self.is_date_older_than_range(date_text):
                    should_stop_searching = True
            
            return (has_matches, should_stop_searching)
        except Exception as e:
            logger.error(f"Error checking for date range: {str(e)}")
            return (False, False)

    async def check_next_page(self) -> bool:
        """Check if there's a next page and navigate to it if it exists"""
        try:
            # Look for "next" pagination link based on code4.txt structure
            next_link = await self.page.query_selector("li.next a[title='Go to next page']")
            
            if next_link:
                # Get the href attribute
                href = await next_link.get_attribute('href')
                logger.info(f"Found next page link: {href}")
                
                # Click to navigate to next page
                await next_link.click()
                await self.page.wait_for_load_state("networkidle")
                # Wait for grid to appear on the next page
                await self.page.wait_for_selector(".views-bootstrap-grid-plugin-style", state="visible")
                logger.info("Navigated to next page")
                return True
            else:
                logger.info("No next page found")
                return False
                
        except Exception as e:
            logger.error(f"Error checking next page: {str(e)}")
            return False

    async def scrape_data(self):
        """Main scraping function"""
        try:
            await self.init_browser()
            await self.page.goto(self.base_url)
            await self.page.wait_for_load_state("networkidle")
            
            current_page = 1
            max_pages = 10  # Process up to 10 pages as specified
            found_older_date = False
            
            while current_page <= max_pages:
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
                
            if not found_older_date and current_page > max_pages:
                logger.info(f"Reached maximum page limit ({max_pages}). Stopping search.")
            
            # Log final results
            if self.results:
                logger.info(f"Total rows collected: {len(self.results)}")
            else:
                logger.info("No matching rows found within date range")
            
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