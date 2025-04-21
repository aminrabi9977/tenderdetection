# # test_isdb.py

# import asyncio
# import logging
# from datetime import datetime
# from playwright.async_api import async_playwright
# import pandas as pd

# # Set up basic logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # Number of concurrent detail page processing
# MAX_CONCURRENT_PAGES = 5

# class ISDBScraperTest:
#     def __init__(self, url: str, target_date: str):
#         self.url = url
#         self.target_date = target_date
#         self.results = []
#         self.semaphore = None  # Will be initialized in scrape_data

#     async def extract_tender_details(self, context, detail_url: str):
#         """Extract details from a specific tender page"""
#         detail_page = None
#         try:
#             # Use semaphore to limit concurrent pages
#             async with self.semaphore:
#                 # Create a new page for the detail view
#                 detail_page = await context.new_page()
#                 await detail_page.goto(detail_url)
#                 await detail_page.wait_for_load_state("networkidle")
                
#                 # Wait for the details to load
#                 await detail_page.wait_for_selector(".details", state="visible")
                
#                 # Extract all required fields
#                 tender_data = {}
                
#                 # Extract Notice Type
#                 notice_type_elem = await detail_page.query_selector(".field--name-field-notice-type .field--item")
#                 if notice_type_elem:
#                     tender_data['notice_type'] = await notice_type_elem.inner_text()
                
#                 # Extract Issue Date
#                 issue_date_elem = await detail_page.query_selector(".field--name-field-issue-date .field--item")
#                 if issue_date_elem:
#                     tender_data['issue_date'] = await issue_date_elem.inner_text()
                
#                 # Extract Last date of submission
#                 submission_date_elem = await detail_page.query_selector(".field--name-field-close-date .field--item")
#                 if submission_date_elem:
#                     tender_data['submission_date'] = await submission_date_elem.inner_text()
                
#                 # Extract Tender Type
#                 tender_type_elem = await detail_page.query_selector(".field--name-field-tender-type .field--item")
#                 if tender_type_elem:
#                     tender_data['tender_type'] = await tender_type_elem.inner_text()
                
#                 # Extract Project code
#                 project_code_elem = await detail_page.query_selector(".field--name-field-project-code .field--item")
#                 if project_code_elem:
#                     tender_data['project_code'] = await project_code_elem.inner_text()
                
#                 # Extract Project title
#                 project_title_elem = await detail_page.query_selector(".field--name-field-project-title .field--item")
#                 if project_title_elem:
#                     tender_data['project_title'] = await project_title_elem.inner_text()
                
#                 # Extract Email
#                 email_elem = await detail_page.query_selector(".field--name-field-email .field--item")
#                 if email_elem:
#                     tender_data['email'] = await email_elem.inner_text()
                
#                 # Extract Document link (if any)
#                 document_link_elem = await detail_page.query_selector(".field--name-field-documents .file-link a")
#                 if document_link_elem:
#                     tender_data['document_link'] = await document_link_elem.get_attribute('href')
                
#                 # Add original URL
#                 tender_data['url'] = detail_url
                
#                 # Check if the tender matches our target date
#                 if 'issue_date' in tender_data:
#                     issue_date = tender_data['issue_date'].strip()
#                     logger.info(f"Issue date: {issue_date}, Target: {self.target_date}")
                    
#                     if issue_date == self.target_date:
#                         logger.info(f"Found matching tender with issue date: {issue_date}")
#                         return tender_data
                
#                 return None
                
#         except Exception as e:
#             logger.error(f"Error extracting tender details from {detail_url}: {str(e)}")
#             return None
#         finally:
#             if detail_page:
#                 await detail_page.close()

#     async def process_tender_batch(self, context, urls):
#         """Process a batch of tender URLs in parallel"""
#         tasks = []
#         for url in urls:
#             # Make sure we have a full URL
#             if url.startswith('/'):
#                 # Convert relative URL to absolute
#                 base_url = self.url.split('/project-procurement')[0] 
#                 url = base_url + url
            
#             tasks.append(self.extract_tender_details(context, url))
        
#         # Wait for all tasks to complete
#         results = await asyncio.gather(*tasks)
        
#         # Filter out None results
#         return [r for r in results if r]

#     async def scrape_data(self):
#         async with async_playwright() as p:
#             browser = await p.chromium.launch(headless=False)
#             context = await browser.new_context()
            
#             # Create a semaphore to limit concurrent pages
#             self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
            
#             try:
#                 # Set longer timeout
#                 context.set_default_timeout(120000)  # 2 minutes timeout
                
#                 # Create a page for navigating the main site
#                 page = await context.new_page()
                
#                 # Go to the page
#                 await page.goto(self.url, wait_until="networkidle")
#                 logger.info("Page loaded successfully")

#                 # Wait for the tender container to be visible based on actual HTML
#                 await page.wait_for_selector("[data-index-view='tenders_listing']", state="visible")
#                 logger.info("Tenders listing found")
                
#                 # Create a list to store all tender URLs across all pages
#                 all_tender_urls = []
#                 current_page = 1
                
#                 while True:
#                     # Get all article elements within the tenders listing
#                     tender_articles = await page.query_selector_all("[data-index-view='tenders_listing'] article")
#                     logger.info(f"Found {len(tender_articles)} tender articles on page {current_page}")
                    
#                     # Extract tender links from each article
#                     page_urls = []
#                     for article in tender_articles:
#                         # Find the title link in each article
#                         link = await article.query_selector(".field-title a")
#                         if link:
#                             href = await link.get_attribute('href')
#                             if href and href not in all_tender_urls and href not in page_urls:
#                                 page_urls.append(href)
                    
#                     logger.info(f"Found {len(page_urls)} unique tender URLs on page {current_page}")
                    
#                     # Add to master list
#                     all_tender_urls.extend(page_urls)
                    
#                     # Look for a "next page" link or button
#                     next_button = await page.query_selector("li.pager__item--next a")
                    
#                     if not next_button:
#                         logger.info("No next page found")
#                         break
                        
#                     logger.info(f"Moving to page {current_page + 1}")
                    
#                     # Click to navigate to next page
#                     await next_button.click()
#                     await page.wait_for_load_state("networkidle")
#                     await page.wait_for_selector("[data-index-view='tenders_listing']", state="visible")
                    
#                     current_page += 1
                
#                 logger.info(f"Total unique tender URLs found: {len(all_tender_urls)}")
                
#                 # Process tenders in parallel batches
#                 batch_size = MAX_CONCURRENT_PAGES * 2  # Process in larger batches
#                 for i in range(0, len(all_tender_urls), batch_size):
#                     batch = all_tender_urls[i:i+batch_size]
#                     logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} tenders)")
                    
#                     batch_results = await self.process_tender_batch(context, batch)
#                     self.results.extend(batch_results)
                    
#                     logger.info(f"Found {len(batch_results)} matching tenders in this batch")
                
#                 # Convert results to DataFrame
#                 df = pd.DataFrame(self.results)
#                 return df

#             except Exception as e:
#                 logger.error(f"Error during scraping: {str(e)}")
#                 raise
#             finally:
#                 await browser.close()

# async def test_scraper():
#     url = "https://www.isdb.org/project-procurement/tenders"
    
#     # Target date format: "28 December 2022" - ISDB format
#     target_date = "28 December 2022"
    
#     try:
#         scraper = ISDBScraperTest(url, target_date)
#         df = await scraper.scrape_data()
        
#         if not df.empty:
#             output_file = f"isdb_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
#             df.to_csv(output_file, index=False)
#             logger.info(f"Data saved to {output_file}")
#             logger.info(f"Total tenders found: {len(df)}")
#         else:
#             logger.info(f"No data found for date: {target_date}")
            
#     except Exception as e:
#         logger.error(f"Error during scraping: {str(e)}")

# if __name__ == "__main__":
#     asyncio.run(test_scraper())
#--------------------------------------------------------------------


# test_isdb.py

import asyncio
import logging
from datetime import datetime
from playwright.async_api import async_playwright
import pandas as pd

# Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Number of concurrent detail page processing
MAX_CONCURRENT_PAGES = 5

class ISDBScraperTest:
    def __init__(self, url: str, start_date: str, end_date: str):
        self.url = url
        self.start_date = start_date
        self.end_date = end_date
        self.results = []
        self.semaphore = None  # Will be initialized in scrape_data

    async def extract_tender_details(self, context, detail_url: str):
        """Extract details from a specific tender page"""
        detail_page = None
        try:
            # Use semaphore to limit concurrent pages
            async with self.semaphore:
                # Create a new page for the detail view
                detail_page = await context.new_page()
                await detail_page.goto(detail_url)
                await detail_page.wait_for_load_state("networkidle")
                
                # Wait for the details to load
                await detail_page.wait_for_selector(".details", state="visible")
                
                # Extract all required fields
                tender_data = {}
                
                # Extract Notice Type
                notice_type_elem = await detail_page.query_selector(".field--name-field-notice-type .field--item")
                if notice_type_elem:
                    tender_data['notice_type'] = await notice_type_elem.inner_text()
                
                # Extract Issue Date
                issue_date_elem = await detail_page.query_selector(".field--name-field-issue-date .field--item")
                if issue_date_elem:
                    tender_data['issue_date'] = await issue_date_elem.inner_text()
                
                # Extract Last date of submission
                submission_date_elem = await detail_page.query_selector(".field--name-field-close-date .field--item")
                if submission_date_elem:
                    tender_data['submission_date'] = await submission_date_elem.inner_text()
                
                # Extract Tender Type
                tender_type_elem = await detail_page.query_selector(".field--name-field-tender-type .field--item")
                if tender_type_elem:
                    tender_data['tender_type'] = await tender_type_elem.inner_text()
                
                # Extract Project code
                project_code_elem = await detail_page.query_selector(".field--name-field-project-code .field--item")
                if project_code_elem:
                    tender_data['project_code'] = await project_code_elem.inner_text()
                
                # Extract Project title
                project_title_elem = await detail_page.query_selector(".field--name-field-project-title .field--item")
                if project_title_elem:
                    tender_data['project_title'] = await project_title_elem.inner_text()
                
                # Extract Email
                email_elem = await detail_page.query_selector(".field--name-field-email .field--item")
                if email_elem:
                    tender_data['email'] = await email_elem.inner_text()
                
                # Extract Document link (if any)
                document_link_elem = await detail_page.query_selector(".field--name-field-documents .file-link a")
                if document_link_elem:
                    tender_data['document_link'] = await document_link_elem.get_attribute('href')
                
                # Add original URL
                tender_data['url'] = detail_url
                
                # Check if the tender is within our date range
                if 'issue_date' in tender_data:
                    issue_date = tender_data['issue_date'].strip()
                    logger.info(f"Issue date: {issue_date}, Range: {self.start_date} to {self.end_date}")
                    
                    # Convert to datetime for comparison
                    try:
                        issue_date_obj = datetime.strptime(issue_date, "%d %B %Y")
                        start_date_obj = datetime.strptime(self.start_date, "%d %B %Y")
                        end_date_obj = datetime.strptime(self.end_date, "%d %B %Y")
                        
                        if start_date_obj <= issue_date_obj <= end_date_obj:
                            logger.info(f"Found matching tender with issue date: {issue_date}")
                            return tender_data
                    except ValueError:
                        logger.warning(f"Could not parse date: {issue_date}")
                
                return None
                
        except Exception as e:
            logger.error(f"Error extracting tender details from {detail_url}: {str(e)}")
            return None
        finally:
            if detail_page:
                await detail_page.close()

    async def process_tender_batch(self, context, urls):
        """Process a batch of tender URLs in parallel"""
        tasks = []
        for url in urls:
            # Make sure we have a full URL
            if url.startswith('/'):
                # Convert relative URL to absolute
                base_url = self.url.split('/project-procurement')[0] 
                url = base_url + url
            
            tasks.append(self.extract_tender_details(context, url))
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        
        # Filter out None results
        return [r for r in results if r]

    async def scrape_data(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()
            
            # Create a semaphore to limit concurrent pages
            self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
            
            try:
                # Set longer timeout
                context.set_default_timeout(120000)  # 2 minutes timeout
                
                # Create a page for navigating the main site
                page = await context.new_page()
                
                # Go to the page
                await page.goto(self.url, wait_until="networkidle")
                logger.info("Page loaded successfully")

                # Wait for the tender container to be visible based on actual HTML
                await page.wait_for_selector("[data-index-view='tenders_listing']", state="visible")
                logger.info("Tenders listing found")
                
                # Create a list to store all tender URLs across all pages
                all_tender_urls = []
                current_page = 1
                
                while True:
                    # Get all article elements within the tenders listing
                    tender_articles = await page.query_selector_all("[data-index-view='tenders_listing'] article")
                    logger.info(f"Found {len(tender_articles)} tender articles on page {current_page}")
                    
                    # Extract tender links from each article
                    page_urls = []
                    for article in tender_articles:
                        # Find the title link in each article
                        link = await article.query_selector(".field-title a")
                        if link:
                            href = await link.get_attribute('href')
                            if href and href not in all_tender_urls and href not in page_urls:
                                page_urls.append(href)
                    
                    logger.info(f"Found {len(page_urls)} unique tender URLs on page {current_page}")
                    
                    # Add to master list
                    all_tender_urls.extend(page_urls)
                    
                    # Look for a "next page" link or button
                    next_button = await page.query_selector("li.pager__item--next a")
                    
                    if not next_button:
                        logger.info("No next page found")
                        break
                        
                    logger.info(f"Moving to page {current_page + 1}")
                    
                    # Click to navigate to next page
                    await next_button.click()
                    await page.wait_for_load_state("networkidle")
                    await page.wait_for_selector("[data-index-view='tenders_listing']", state="visible")
                    
                    current_page += 1
                
                logger.info(f"Total unique tender URLs found: {len(all_tender_urls)}")
                
                # Process tenders in parallel batches
                batch_size = MAX_CONCURRENT_PAGES * 2  # Process in larger batches
                for i in range(0, len(all_tender_urls), batch_size):
                    batch = all_tender_urls[i:i+batch_size]
                    logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} tenders)")
                    
                    batch_results = await self.process_tender_batch(context, batch)
                    self.results.extend(batch_results)
                    
                    logger.info(f"Found {len(batch_results)} matching tenders in this batch")
                
                # Convert results to DataFrame
                df = pd.DataFrame(self.results)
                return df

            except Exception as e:
                logger.error(f"Error during scraping: {str(e)}")
                raise
            finally:
                await browser.close()

async def test_scraper():
    url = "https://www.isdb.org/project-procurement/tenders"
    
    # Date range (format: "DD Month YYYY" - ISDB format)
    start_date = "19 February 2025"
    end_date = "11 March 2025"
    
    try:
        scraper = ISDBScraperTest(url, start_date, end_date)
        df = await scraper.scrape_data()
        
        if not df.empty:
            output_file = f"isdb_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(output_file, index=False)
            logger.info(f"Data saved to {output_file}")
            logger.info(f"Total tenders found: {len(df)}")
        else:
            logger.info(f"No data found for date range: {start_date} to {end_date}")
            
    except Exception as e:
        logger.error(f"Error during scraping: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_scraper())