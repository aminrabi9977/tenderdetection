# # src/scrapers/afd_scraper.py

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

# class AFDScraper(BaseScraper):
#     def __init__(self, base_url: str):
#         super().__init__(base_url)
#         # Format today's date as "Mar 4, 2025" (format used by AFD/dgMarket)
#         self.today = datetime.now().strftime("%b %d, %Y")
#         self.results = []
#         # Extract the base domain from the URL
#         self.domain = '/'.join(base_url.split('/')[:3])  # Get "https://tenders-afd.dgmarket.com"
#         self.semaphore = None  # Will be initialized in scrape_data
#         logger.info(f"AFD scraper initialized with base domain: {self.domain}")
#         logger.info(f"Today's date in AFD format: {self.today}")

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

#     async def extract_tender_details(self, tender_url: str) -> Optional[Dict]:
#         """Extract details from a specific tender detail page"""
#         detail_page = None
#         try:
#             # Use semaphore to limit concurrent pages
#             async with self.semaphore:
#                 # Create a new page for the detail view
#                 detail_page = await self.context.new_page()
#                 await detail_page.goto(tender_url)
#                 await detail_page.wait_for_load_state("networkidle")
                
#                 # Extract additional details from the tender detail page
#                 # Assuming the tender detail page has more information
                
#                 # Extract tender description/details
#                 description_elem = await detail_page.query_selector("div.content")
#                 description = await description_elem.inner_text() if description_elem else "N/A"
                
#                 # Extract funding agency
#                 funding_elem = await detail_page.query_selector("span.label:has-text('Funding Agency') + span")
#                 funding_agency = await funding_elem.inner_text() if funding_elem else "N/A"
                
#                 # Extract reference number
#                 ref_elem = await detail_page.query_selector("span.label:has-text('Reference') + span")
#                 reference_number = await ref_elem.inner_text() if ref_elem else "N/A"
                
#                 # Get document links if available
#                 document_links = []
#                 doc_elems = await detail_page.query_selector_all("a[href*='download']")
#                 for doc in doc_elems:
#                     doc_url = await doc.get_attribute('href')
#                     if doc_url.startswith('/'):
#                         doc_url = self.domain + doc_url
#                     document_links.append(doc_url)
                
#                 return {
#                     'description': description.strip(),
#                     'funding_agency': funding_agency.strip(),
#                     'reference_number': reference_number.strip(),
#                     'document_links': document_links
#                 }
                
#         except Exception as e:
#             logger.error(f"Error extracting tender details from {tender_url}: {str(e)}")
#             return None
#         finally:
#             if detail_page:
#                 await detail_page.close()

#     async def process_row(self, row) -> Optional[Dict]:
#         """Process a single row from the table"""
#         try:
#             # Extract published date
#             date_elem = await row.query_selector("td.published")
#             if not date_elem:
#                 return None
                
#             published_date = await date_elem.inner_text()
#             published_date = published_date.strip()
            
#             # Check if date matches today's date
#             logger.debug(f"Row date: {published_date}, Today: {self.today}")
#             if published_date != self.today:
#                 return None
            
#             # Extract country
#             country_elem = await row.query_selector("td.country")
#             country = await country_elem.inner_text() if country_elem else "N/A"
            
#             # Extract notice title and link
#             title_elem = await row.query_selector("td a")
#             title = await title_elem.inner_text() if title_elem else "N/A"
#             notice_link = await title_elem.get_attribute('href') if title_elem else None
            
#             # Make sure we have an absolute URL
#             if notice_link and notice_link.startswith('/'):
#                 notice_link = self.domain + notice_link
            
#             # Extract deadline
#             deadline_elem = await row.query_selector("td.deadline")
#             deadline = await deadline_elem.inner_text() if deadline_elem else "N/A"
#             deadline = deadline.strip()
            
#             # Create basic result 
#             result = {
#                 'published_date': published_date,
#                 'country': country.strip(),
#                 'title': title.strip(),
#                 'deadline': deadline,
#                 'url': notice_link if notice_link else "N/A"
#             }
            
#             logger.info(f"Found matching tender: {title}")
#             return result
            
#         except Exception as e:
#             logger.error(f"Error processing row: {str(e)}")
#             return None

#     async def process_row_with_details(self, row) -> Optional[Dict]:
#         """Process a row and get additional details from tender page"""
#         basic_info = await self.process_row(row)
        
#         if not basic_info:
#             return None
            
#         # Get the tender URL
#         tender_url = basic_info['url']
#         if tender_url != "N/A":
#             # Extract additional details from the tender page
#             additional_details = await self.extract_tender_details(tender_url)
            
#             if additional_details:
#                 # Merge the basic info with additional details
#                 basic_info.update(additional_details)
                
#         return basic_info

#     async def process_rows_batch(self, rows):
#         """Process a batch of table rows in parallel"""
#         tasks = []
#         for row in rows:
#             tasks.append(self.process_row_with_details(row))
        
#         # Wait for all tasks to complete
#         results = await asyncio.gather(*tasks)
        
#         # Filter out None results
#         return [r for r in results if r]

#     async def extract_table_data(self) -> List[Dict]:
#         """Extract data from the current page's table"""
#         try:
#             # Wait for the table to be loaded
#             await self.page.wait_for_selector("table#notice", state="visible")
            
#             # Get all rows from the table (skip header row)
#             rows = await self.page.query_selector_all("table#notice tbody tr")
#             logger.info(f"Found {len(rows)} rows in the table")
            
#             # Process rows in parallel batches
#             batch_size = MAX_CONCURRENT_PAGES
#             all_results = []
            
#             for i in range(0, len(rows), batch_size):
#                 batch = rows[i:i+batch_size]
#                 logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} rows)")
                
#                 batch_results = await self.process_rows_batch(batch)
#                 all_results.extend(batch_results)
                
#                 logger.info(f"Found {len(batch_results)} matching rows in this batch")
            
#             return all_results
            
#         except Exception as e:
#             logger.error(f"Error extracting table data: {str(e)}")
#             return []

#     async def check_page_for_today_with_date_cutoff(self) -> bool:
#         """
#         Check if current page has any rows with today's date.
#         Also returns False if we detect dates older than today,
#         meaning we can stop searching further pages.
        
#         Returns:
#             tuple: (has_matches, should_stop_searching)
#         """
#         try:
#             date_elems = await self.page.query_selector_all("td.published")
#             has_matches = False
#             should_stop_searching = False
            
#             # Convert today's date to a datetime object for comparison
#             today_date = datetime.strptime(self.today, "%b %d, %Y")
            
#             for elem in date_elems:
#                 date_text = await elem.inner_text()
#                 date_text = date_text.strip()
                
#                 if date_text == self.today:
#                     has_matches = True
                
#                 # Try to parse the date for comparison
#                 try:
#                     elem_date = datetime.strptime(date_text, "%b %d, %Y")
#                     # If we found a date older than today, we can stop searching
#                     if elem_date < today_date:
#                         should_stop_searching = True
#                 except ValueError:
#                     # If we can't parse the date, just continue
#                     continue
            
#             return (has_matches, should_stop_searching)
#         except Exception as e:
#             logger.error(f"Error checking for today's date: {str(e)}")
#             return (False, False)

#     async def check_next_page(self) -> bool:
#         """Check if there's a next page and navigate to it if it exists"""
#         try:
#             # Based on the code1 example, the pagination links are after the table
#             # Look for the "Next" link in pagination
#             pagination_text = await self.page.evaluate("""
#                 () => {
#                     const tables = document.querySelectorAll('table#notice');
#                     if (tables.length === 0) return null;
                    
#                     // Get the last table (should be the main one)
#                     const table = tables[tables.length - 1];
                    
#                     // Get the parent of the table
#                     const parent = table.parentNode;
                    
#                     // The pagination text is after the table in the same parent
#                     const text = parent.innerHTML;
#                     return text;
#                 }
#             """)
            
#             if not pagination_text:
#                 logger.info("No pagination text found")
#                 return False
            
#             # Check if there's a "Next" link that's not the current page
#             next_link = await self.page.query_selector("a:has-text('Next')")
            
#             if next_link:
#                 # Click to navigate to next page
#                 await next_link.click()
#                 await self.page.wait_for_load_state("networkidle")
#                 # Wait for the table to load on the next page
#                 await self.page.wait_for_selector("table#notice", state="visible")
#                 logger.info("Navigated to next page")
#                 return True
#             else:
#                 # Check if we're on the last page (no Next link)
#                 logger.info("No 'Next' link found or we're on the last page")
#                 return False
                
#         except Exception as e:
#             logger.error(f"Error checking next page: {str(e)}")
#             return False

#     async def scrape_data(self):
#         """Main scraping function"""
#         try:
#             await self.init_browser()
#             await self.page.goto(self.base_url)
#             await self.page.wait_for_load_state("networkidle")
            
#             current_page = 1
            
#             while True:
#                 logger.info(f"Processing page {current_page}")
                
#                 # Check if this page has any matching dates and if we should stop searching
#                 has_matches, should_stop_searching = await self.check_page_for_today_with_date_cutoff()
                
#                 if has_matches:
#                     logger.info(f"Found matching dates on page {current_page}, extracting data")
#                     # Extract data from current page
#                     page_data = await self.extract_table_data()
#                     self.results.extend(page_data)
                    
#                     logger.info(f"Found {len(page_data)} matching rows on page {current_page}")
#                     # We found our target date, so we don't need to check more pages
#                     break
#                 elif should_stop_searching:
#                     logger.info(f"Found dates older than target on page {current_page}, stopping search")
#                     break
#                 else:
#                     logger.info(f"No matching dates found on page {current_page}, checking next page")
                    
#                     # Check and navigate to next page if exists
#                     has_next = await self.check_next_page()
#                     if not has_next:
#                         logger.info("No more pages to process")
#                         break
                    
#                     current_page += 1
            
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
#------------------------------------------------------------------------------------------

# src/scrapers/afd_scraper.py

from playwright.async_api import async_playwright
import asyncio
from datetime import datetime, timedelta
import pandas as pd
import logging
from typing import List, Dict, Optional
from src.scrapers.base_scraper import BaseScraper
from src.utils.date_utils import normalize_date, format_date_for_site

logger = logging.getLogger(__name__)

# Number of concurrent detail page processing
MAX_CONCURRENT_PAGES = 5

class AFDScraper(BaseScraper):
    def __init__(self, base_url: str):
        super().__init__(base_url)
        # Format today's date and date from a week ago
        self.today = datetime.now()
        self.today_str = self.today.strftime("%b %d, %Y")
        
        # Calculate the date from a week ago
        self.week_ago = self.today - timedelta(days=7)
        self.week_ago_str = self.week_ago.strftime("%b %d, %Y")
        
        self.results = []
        # Extract the base domain from the URL
        self.domain = '/'.join(base_url.split('/')[:3])  # Get "https://tenders-afd.dgmarket.com"
        self.semaphore = None  # Will be initialized in scrape_data
        
        logger.info(f"AFD scraper initialized with base domain: {self.domain}")
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

    async def extract_tender_details(self, tender_url: str) -> Optional[Dict]:
        """Extract details from a specific tender detail page"""
        detail_page = None
        try:
            # Use semaphore to limit concurrent pages
            async with self.semaphore:
                # Create a new page for the detail view
                detail_page = await self.context.new_page()
                await detail_page.goto(tender_url)
                await detail_page.wait_for_load_state("networkidle")
                
                # Extract additional details from the tender detail page
                # Assuming the tender detail page has more information
                
                # Extract tender description/details
                description_elem = await detail_page.query_selector("div.content")
                description = await description_elem.inner_text() if description_elem else "N/A"
                
                # Extract funding agency
                funding_elem = await detail_page.query_selector("span.label:has-text('Funding Agency') + span")
                funding_agency = await funding_elem.inner_text() if funding_elem else "N/A"
                
                # Extract reference number
                ref_elem = await detail_page.query_selector("span.label:has-text('Reference') + span")
                reference_number = await ref_elem.inner_text() if ref_elem else "N/A"
                
                # Get document links if available
                document_links = []
                doc_elems = await detail_page.query_selector_all("a[href*='download']")
                for doc in doc_elems:
                    doc_url = await doc.get_attribute('href')
                    if doc_url.startswith('/'):
                        doc_url = self.domain + doc_url
                    document_links.append(doc_url)
                
                return {
                    'description': description.strip(),
                    'funding_agency': funding_agency.strip(),
                    'reference_number': reference_number.strip(),
                    'document_links': document_links
                }
                
        except Exception as e:
            logger.error(f"Error extracting tender details from {tender_url}: {str(e)}")
            return None
        finally:
            if detail_page:
                await detail_page.close()

    async def process_row(self, row) -> Optional[Dict]:
        """Process a single row from the table"""
        try:
            # Extract published date
            date_elem = await row.query_selector("td.published")
            if not date_elem:
                return None
                
            published_date = await date_elem.inner_text()
            published_date = published_date.strip()
            
            # Check if date is in our range (week_ago <= date <= today)
            try:
                published_date_obj = datetime.strptime(published_date, "%b %d, %Y")
                
                # Skip if date is not in our range
                if not (self.week_ago <= published_date_obj <= self.today):
                    return None
            except ValueError:
                # If we can't parse the date, skip this row
                return None
            
            # Extract country
            country_elem = await row.query_selector("td.country")
            country = await country_elem.inner_text() if country_elem else "N/A"
            
            # Extract notice title and link
            title_elem = await row.query_selector("td a")
            title = await title_elem.inner_text() if title_elem else "N/A"
            notice_link = await title_elem.get_attribute('href') if title_elem else None
            
            # Make sure we have an absolute URL
            if notice_link and notice_link.startswith('/'):
                notice_link = self.domain + notice_link
            
            # Extract deadline
            deadline_elem = await row.query_selector("td.deadline")
            deadline = await deadline_elem.inner_text() if deadline_elem else "N/A"
            deadline = deadline.strip()
            
            # Create basic result 
            result = {
                'published_date': published_date,
                'country': country.strip(),
                'title': title.strip(),
                'deadline': deadline,
                'url': notice_link if notice_link else "N/A"
            }
            
            logger.info(f"Found matching tender: {title}")
            return result
            
        except Exception as e:
            logger.error(f"Error processing row: {str(e)}")
            return None

    async def process_row_with_details(self, row) -> Optional[Dict]:
        """Process a row and get additional details from tender page"""
        basic_info = await self.process_row(row)
        
        if not basic_info:
            return None
            
        # Get the tender URL
        tender_url = basic_info['url']
        if tender_url != "N/A":
            # Extract additional details from the tender page
            additional_details = await self.extract_tender_details(tender_url)
            
            if additional_details:
                # Merge the basic info with additional details
                basic_info.update(additional_details)
                
        return basic_info

    async def process_rows_batch(self, rows):
        """Process a batch of table rows in parallel"""
        tasks = []
        for row in rows:
            tasks.append(self.process_row_with_details(row))
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        
        # Filter out None results
        return [r for r in results if r]

    async def check_page_for_date_range_with_cutoff(self) -> tuple:
        """
        Check if current page has any rows with dates in our range (week_ago to today).
        Also returns signal if we should stop searching because we've found dates older than our range.
        
        Returns:
            tuple: (has_matches, should_stop_searching)
        """
        try:
            date_elems = await self.page.query_selector_all("td.published")
            has_matches = False
            should_stop_searching = False
            
            for elem in date_elems:
                date_text = await elem.inner_text()
                date_text = date_text.strip()
                
                # Try to parse the date for comparison
                try:
                    elem_date = datetime.strptime(date_text, "%b %d, %Y")
                    
                    # Check if date is in our range (week_ago <= date <= today)
                    if self.week_ago <= elem_date <= self.today:
                        has_matches = True
                    
                    # If we found a date older than week_ago, we can stop searching
                    if elem_date < self.week_ago:
                        should_stop_searching = True
                except ValueError:
                    # If we can't parse the date, just continue
                    continue
            
            return (has_matches, should_stop_searching)
        except Exception as e:
            logger.error(f"Error checking for date range: {str(e)}")
            return (False, False)

    async def extract_table_data(self) -> List[Dict]:
        """Extract data from the current page's table"""
        try:
            # Wait for the table to be loaded
            await self.page.wait_for_selector("table#notice", state="visible")
            
            # Get all rows from the table (skip header row)
            rows = await self.page.query_selector_all("table#notice tbody tr")
            logger.info(f"Found {len(rows)} rows in the table")
            
            # Process rows in parallel batches
            batch_size = MAX_CONCURRENT_PAGES
            all_results = []
            
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i+batch_size]
                logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} rows)")
                
                batch_results = await self.process_rows_batch(batch)
                all_results.extend(batch_results)
                
                logger.info(f"Found {len(batch_results)} matching rows in this batch")
            
            return all_results
            
        except Exception as e:
            logger.error(f"Error extracting table data: {str(e)}")
            return []

    async def check_next_page(self) -> bool:
        """Check if there's a next page and navigate to it if it exists"""
        try:
            # Based on the code1 example, the pagination links are after the table
            # Look for the "Next" link in pagination
            pagination_text = await self.page.evaluate("""
                () => {
                    const tables = document.querySelectorAll('table#notice');
                    if (tables.length === 0) return null;
                    
                    // Get the last table (should be the main one)
                    const table = tables[tables.length - 1];
                    
                    // Get the parent of the table
                    const parent = table.parentNode;
                    
                    // The pagination text is after the table in the same parent
                    const text = parent.innerHTML;
                    return text;
                }
            """)
            
            if not pagination_text:
                logger.info("No pagination text found")
                return False
            
            # Check if there's a "Next" link that's not the current page
            next_link = await self.page.query_selector("a:has-text('Next')")
            
            if next_link:
                # Click to navigate to next page
                await next_link.click()
                await self.page.wait_for_load_state("networkidle")
                # Wait for the table to load on the next page
                await self.page.wait_for_selector("table#notice", state="visible")
                logger.info("Navigated to next page")
                return True
            else:
                # Check if we're on the last page (no Next link)
                logger.info("No 'Next' link found or we're on the last page")
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
            
            while True:
                logger.info(f"Processing page {current_page}")
                
                # Check if this page has any matching dates in our range and if we should stop searching
                has_matches, should_stop_searching = await self.check_page_for_date_range_with_cutoff()
                
                if has_matches:
                    logger.info(f"Found dates in our range on page {current_page}, extracting data")
                    # Extract data from current page
                    page_data = await self.extract_table_data()
                    self.results.extend(page_data)
                    
                    logger.info(f"Found {len(page_data)} matching rows on page {current_page}")
                    
                    # If we should stop searching further pages, break here
                    if should_stop_searching:
                        logger.info("Found dates older than our range, stopping search")
                        break
                elif should_stop_searching:
                    logger.info(f"Found dates older than our range on page {current_page}, stopping search")
                    break
                else:
                    logger.info(f"No matching dates found on page {current_page}, checking next page")
                    
                    # Check and navigate to next page if exists
                    has_next = await self.check_next_page()
                    if not has_next:
                        logger.info("No more pages to process")
                        break
                    
                    current_page += 1
            
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