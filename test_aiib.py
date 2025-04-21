# # test_aiib.py

# import asyncio
# import logging
# from datetime import datetime
# from playwright.async_api import async_playwright
# import pandas as pd

# # Set up basic logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# class AIIBScraperTest:
#     def __init__(self, url: str, target_date: str):
#         self.url = url
#         self.target_date = target_date
#         self.results = []
#         # Extract the base domain from the URL
#         self.domain = '/'.join(url.split('/')[:3])  # Get "https://www.aiib.org"
#         logger.info(f"AIIB scraper initialized with base domain: {self.domain}")
#         logger.info(f"Target date: {self.target_date}")

#     async def process_row(self, row):
#         """Process a single row from the table"""
#         try:
#             # Extract date cell
#             date_elem = await row.query_selector(".table-col.table-date .s2")
#             if not date_elem:
#                 return None
                
#             issue_date = await date_elem.inner_text()
#             issue_date = issue_date.strip()
            
#             # Check if date matches target date
#             logger.debug(f"Row date: {issue_date}, Target: {self.target_date}")
#             if issue_date != self.target_date:
#                 return None
            
#             # Extract country
#             country_elem = await row.query_selector(".table-col.table-country .country-value")
#             country = await country_elem.inner_text() if country_elem else "N/A"
            
#             # Extract project name
#             project_elem = await row.query_selector(".table-col.table-project .title-value")
#             project_name = await project_elem.inner_text() if project_elem else "N/A"
            
#             # Extract download link
#             download_link_elem = await row.query_selector(".table-col.table-project a")
#             download_link = await download_link_elem.get_attribute('href') if download_link_elem else "N/A"
#             if download_link != "N/A" and download_link.startswith('/'):
#                 download_link = self.domain + download_link
            
#             # Extract sector
#             sector_elem = await row.query_selector(".table-col.table-energy .sector-value")
#             sector = await sector_elem.inner_text() if sector_elem else "N/A"
            
#             # Extract notice type
#             type_elem = await row.query_selector(".table-col.table-type .type-value")
#             notice_type = await type_elem.inner_text() if type_elem else "N/A"
            
#             # Create result
#             result = {
#                 'date': issue_date,
#                 'country': country.strip(),
#                 'project_name': project_name.strip(),
#                 'sector': sector.strip(),
#                 'notice_type': notice_type.strip(),
#                 'download_link': download_link,
#                 'url': self.url  # Base URL since we don't have detail pages
#             }
            
#             logger.info(f"Found matching procurement: {project_name}")
#             return result
            
#         except Exception as e:
#             logger.error(f"Error processing row: {str(e)}")
#             return None

#     async def scrape_data(self):
#         async with async_playwright() as p:
#             browser = await p.chromium.launch(headless=False)
#             context = await browser.new_context()
#             page = await context.new_page()
            
#             try:
#                 # Set longer timeout
#                 page.set_default_timeout(120000)  # 2 minutes timeout
                
#                 # Go to the page
#                 await page.goto(self.url, wait_until="networkidle")
#                 logger.info("Page loaded successfully")

#                 # Wait for the table to be visible
#                 await page.wait_for_selector(".table-body", state="visible")
#                 logger.info("Table found")
                
#                 current_page = 1
                
#                 while True:
#                     logger.info(f"Processing page {current_page}")
                    
#                     # Get all rows from the table
#                     rows = await page.query_selector_all(".table-row")
#                     logger.info(f"Found {len(rows)} rows on page {current_page}")
                    
#                     # Process each row
#                     page_results = []
#                     for row in rows:
#                         result = await self.process_row(row)
#                         if result:
#                             page_results.append(result)
                    
#                     self.results.extend(page_results)
#                     logger.info(f"Found {len(page_results)} matching rows on page {current_page}")
                    
#                     # Look for pagination navigation
#                     next_button = await page.query_selector("a.pagenav-next")
                    
#                     if not next_button:
#                         logger.info("No next page found")
#                         break
                        
#                     # Check if it's not disabled
#                     is_disabled = await next_button.get_attribute("class")
#                     if is_disabled and "disabled" in is_disabled:
#                         logger.info("Next button is disabled, no more pages")
#                         break
                        
#                     logger.info(f"Moving to page {current_page + 1}")
                    
#                     # Click to navigate to next page
#                     await next_button.click()
#                     await page.wait_for_load_state("networkidle")
#                     await page.wait_for_selector(".table-body", state="visible")
                    
#                     current_page += 1
                
#                 # Convert results to DataFrame
#                 df = pd.DataFrame(self.results)
#                 return df

#             except Exception as e:
#                 logger.error(f"Error during scraping: {str(e)}")
#                 raise
#             finally:
#                 await browser.close()

# async def test_scraper():
#     url = "https://www.aiib.org/en/opportunities/business/project-procurement/list.html"
    
#     # Your target date in AIIB format (format like "Feb 28, 2025")
#     target_date = "Nov 25, 2024"  # Adjust to a date that exists in the table
    
#     try:
#         scraper = AIIBScraperTest(url, target_date)
#         df = await scraper.scrape_data()
        
#         if not df.empty:
#             output_file = f"aiib_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
#             df.to_csv(output_file, index=False)
#             logger.info(f"Data saved to {output_file}")
#             logger.info(f"Total rows found: {len(df)}")
#             print(df)  # Print the DataFrame to see the results
#         else:
#             logger.info(f"No data found for date: {target_date}")
            
#     except Exception as e:
#         logger.error(f"Error during scraping: {str(e)}")

# if __name__ == "__main__":
#     asyncio.run(test_scraper())

# ---------------------------------------------------------------------------------------------------

# test_aiib.py

# test_aiib.py

import asyncio
import logging
from datetime import datetime
from playwright.async_api import async_playwright
import pandas as pd

# Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Increase concurrent page processing for AIIB
MAX_CONCURRENT_PAGES = 10

class AIIBScraperTest:
    def __init__(self, url: str, start_date: str, end_date: str):
        self.url = url
        self.start_date_str = start_date
        self.end_date_str = end_date
        
        # Convert to datetime objects for comparison
        self.start_date = datetime.strptime(start_date, "%b %d, %Y")
        self.end_date = datetime.strptime(end_date, "%b %d, %Y")
        
        self.results = []
        # Extract the base domain from the URL
        self.domain = '/'.join(url.split('/')[:3])  # Get "https://www.aiib.org"
        self.semaphore = None  # Will be initialized in scrape_data
        logger.info(f"AIIB scraper initialized with base domain: {self.domain}")
        logger.info(f"Date range: {start_date} to {end_date}")

    async def is_date_in_range(self, date_text: str) -> bool:
        """Check if a date is within our specified range"""
        try:
            # AIIB format can be like "Mar 27, 2025" or "April 18, 2025"
            # Handle both abbreviated and full month names
            try:
                date_obj = datetime.strptime(date_text.strip(), "%b %d, %Y")
            except ValueError:
                date_obj = datetime.strptime(date_text.strip(), "%B %d, %Y")
            
            return self.start_date <= date_obj <= self.end_date
        except ValueError as e:
            logger.warning(f"Could not parse date: {date_text}. Error: {str(e)}")
            return False

    async def is_date_older_than_range(self, date_text: str) -> bool:
        """Check if a date is older than our range"""
        try:
            # Try to parse with both abbreviated and full month names
            try:
                date_obj = datetime.strptime(date_text.strip(), "%b %d, %Y")
            except ValueError:
                date_obj = datetime.strptime(date_text.strip(), "%B %d, %Y")
            
            return date_obj < self.start_date
        except ValueError:
            return False

    async def process_row(self, row):
        """Process a single row from the table"""
        try:
            # Extract the issue date from the row
            date_elem = await row.query_selector(".table-col.table-date .s2")
            if not date_elem:
                return None
                
            issue_date = await date_elem.inner_text()
            issue_date = issue_date.strip()
            
            # Check if date is older than our range
            if await self.is_date_older_than_range(issue_date):
                logger.info(f"Found date {issue_date} older than our range, stopping search")
                return "STOP_SEARCH"
            
            # Check if date is in our range
            if not await self.is_date_in_range(issue_date):
                logger.info(f"Skipping row with issue date {issue_date} (outside range)")
                return None
            
            # Extract the member/country
            country_elem = await row.query_selector(".table-col.table-country .country-value")
            country = await country_elem.inner_text() if country_elem else "N/A"
            
            # Extract project/notice title
            title_elem = await row.query_selector(".table-col.table-project .title-value")
            title = await title_elem.inner_text() if title_elem else "N/A"
            
            # Extract the download link
            download_link_elem = await row.query_selector(".table-col.table-project a")
            download_link = await download_link_elem.get_attribute('href') if download_link_elem else None
            
            # Make the download link absolute if it's relative
            if download_link and download_link.startswith('/'):
                download_link = self.domain + download_link
            
            # Extract sector
            sector_elem = await row.query_selector(".table-col.table-energy .sector-value")
            sector = await sector_elem.inner_text() if sector_elem else "N/A"
            
            # Extract notice type
            type_elem = await row.query_selector(".table-col.table-type .type-value")
            notice_type = await type_elem.inner_text() if type_elem else "N/A"
            
            # Create result
            result = {
                'issue_date': issue_date,
                'country': country.strip(),
                'title': title.strip(),
                'sector': sector.strip(),
                'notice_type': notice_type.strip(),
                'download_link': download_link if download_link else "N/A"
            }
            
            logger.info(f"Found matching row: {title}")
            return result
            
        except Exception as e:
            logger.error(f"Error processing row: {str(e)}")
            return None

    async def process_rows_batch(self, rows):
        """Process a batch of rows in parallel"""
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

    async def scrape_data(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()
            page = await context.new_page()
            
            # Create a semaphore to limit concurrent pages
            self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
            
            try:
                # Set longer timeout
                page.set_default_timeout(120000)  # 2 minutes timeout
                
                # Go to the page
                await page.goto(self.url, wait_until="networkidle")
                logger.info("Page loaded successfully")

                # Wait for the table to be visible
                await page.wait_for_selector(".table-body", state="visible")
                logger.info("Table found")
                
                current_page = 1
                should_stop = False
                
                while not should_stop:
                    logger.info(f"Processing page {current_page}")
                    
                    # Get all rows from the table
                    rows = await page.query_selector_all(".table-row")
                    logger.info(f"Found {len(rows)} rows on page {current_page}")
                    
                    if not rows:
                        logger.info("No rows found on page, stopping search")
                        break
                    
                    # Process rows in parallel batches
                    batch_size = MAX_CONCURRENT_PAGES
                    for i in range(0, len(rows), batch_size):
                        batch = rows[i:i+batch_size]
                        logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} rows)")
                        
                        batch_results, should_stop_search = await self.process_rows_batch(batch)
                        self.results.extend(batch_results)
                        
                        logger.info(f"Found {len(batch_results)} matching rows in this batch")
                        
                        if should_stop_search:
                            logger.info("Found dates older than our range, stopping search")
                            should_stop = True
                            break
                    
                    # Check for next page button/link if we haven't found a reason to stop
                    if not should_stop:
                        next_page_elem = await page.query_selector("a.next")
                        if next_page_elem:
                            logger.info(f"Moving to page {current_page + 1}")
                            await next_page_elem.click()
                            await page.wait_for_load_state("networkidle")
                            # Wait for the table to load on the next page
                            await page.wait_for_selector(".table-body", state="visible")
                            current_page += 1
                        else:
                            logger.info("No next page found")
                            break
                
                # Convert results to DataFrame
                df = pd.DataFrame(self.results)
                return df

            except Exception as e:
                logger.error(f"Error during scraping: {str(e)}")
                raise
            finally:
                await browser.close()

async def test_scraper():
    url = "https://www.aiib.org/en/opportunities/business/project-procurement/list.html"
    
    # Define fixed date range for testing
    start_date = "Jun 13, 2024"  # Format: "MMM DD, YYYY"
    end_date = "Apr 20, 2025"    # Format: "MMM DD, YYYY"
    
    try:
        scraper = AIIBScraperTest(url, start_date, end_date)
        df = await scraper.scrape_data()
        
        if not df.empty:
            output_file = f"aiib_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(output_file, index=False)
            logger.info(f"Data saved to {output_file}")
            logger.info(f"Total items found: {len(df)}")
            print(df)  # Print the DataFrame to see the results
        else:
            logger.info(f"No data found for date range: {start_date} to {end_date}")
            
    except Exception as e:
        logger.error(f"Error during scraping: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_scraper())