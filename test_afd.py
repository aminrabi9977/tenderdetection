# # test_afd.py

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

# class AFDScraperTest:
#     def __init__(self, url: str, target_date: str):
#         self.url = url
#         self.target_date = target_date
#         self.results = []
#         # Extract the base domain from the URL
#         self.domain = '/'.join(url.split('/')[:3])  # Get "https://tenders-afd.dgmarket.com"
#         self.semaphore = None  # Will be initialized in scrape_data
#         logger.info(f"AFD scraper initialized with base domain: {self.domain}")
#         logger.info(f"Target date: {self.target_date}")

#     async def extract_tender_details(self, context, tender_url: str):
#         """Extract details from a specific tender detail page"""
#         detail_page = None
#         try:
#             # Use semaphore to limit concurrent pages
#             async with self.semaphore:
#                 # Create a new page for the detail view
#                 detail_page = await context.new_page()
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

#     async def process_row(self, context, row):
#         """Process a single row from the table"""
#         try:
#             # Extract published date
#             date_elem = await row.query_selector("td.published")
#             if not date_elem:
#                 return None
                
#             published_date = await date_elem.inner_text()
#             published_date = published_date.strip()
            
#             # Check if date matches target date
#             logger.debug(f"Row date: {published_date}, Target: {self.target_date}")
#             if published_date != self.target_date:
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
            
#             # If we have a tender URL, get additional details
#             if notice_link and notice_link != "N/A":
#                 additional_details = await self.extract_tender_details(context, notice_link)
#                 if additional_details:
#                     result.update(additional_details)
            
#             logger.info(f"Found matching tender: {title}")
#             return result
            
#         except Exception as e:
#             logger.error(f"Error processing row: {str(e)}")
#             return None

#     async def process_rows_batch(self, context, rows):
#         """Process a batch of table rows in parallel"""
#         tasks = []
#         for row in rows:
#             tasks.append(self.process_row(context, row))
        
#         # Wait for all tasks to complete
#         results = await asyncio.gather(*tasks)
        
#         # Filter out None results
#         return [r for r in results if r]

#     async def scrape_data(self):
#         async with async_playwright() as p:
#             browser = await p.chromium.launch(headless=False)
#             context = await browser.new_context()
#             page = await context.new_page()
            
#             # Create a semaphore to limit concurrent pages
#             self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
            
#             try:
#                 # Set longer timeout
#                 page.set_default_timeout(120000)  # 2 minutes timeout
                
#                 # Go to the page
#                 await page.goto(self.url, wait_until="networkidle")
#                 logger.info("Page loaded successfully")

#                 # Wait for the table to be visible
#                 await page.wait_for_selector("table#notice", state="visible")
#                 logger.info("Table found")
                
#                 current_page = 1
                
#                 while True:
#                     logger.info(f"Processing page {current_page}")
                    
#                     # Check if this page has any matching dates
#                     date_elems = await page.query_selector_all("td.published")
#                     has_matching_dates = False
                    
#                     for elem in date_elems:
#                         date_text = await elem.inner_text()
#                         date_text = date_text.strip()
                        
#                         if date_text == self.target_date:
#                             has_matching_dates = True
#                             break
                    
#                     if has_matching_dates:
#                         logger.info(f"Found matching dates on page {current_page}, extracting data")
                        
#                         # Get all rows from the table (skip header row)
#                         rows = await page.query_selector_all("table#notice tbody tr")
#                         logger.info(f"Found {len(rows)} rows on page {current_page}")
                        
#                         # Process rows in parallel batches
#                         batch_size = MAX_CONCURRENT_PAGES
                        
#                         for i in range(0, len(rows), batch_size):
#                             batch = rows[i:i+batch_size]
#                             logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} rows)")
                            
#                             batch_results = await self.process_rows_batch(context, batch)
#                             self.results.extend(batch_results)
                            
#                             logger.info(f"Found {len(batch_results)} matching rows in this batch")
                        
#                         # We found our target date, so we don't need to check more pages
#                         break
#                     else:
#                         logger.info(f"No matching dates found on page {current_page}, checking next page")
                        
#                         # Look for the "Next" link in pagination
#                         next_link = await page.query_selector("a:has-text('Next')")
                        
#                         if not next_link:
#                             logger.info("No 'Next' link found or we're on the last page")
#                             break
                            
#                         logger.info(f"Moving to page {current_page + 1}")
                        
#                         # Click to navigate to next page
#                         await next_link.click()
#                         await page.wait_for_load_state("networkidle")
#                         await page.wait_for_selector("table#notice", state="visible")
                        
#                         current_page += 1
                
#                 # Convert results to DataFrame
#                 df = pd.DataFrame(self.results)
#                 return df

#             except Exception as e:
#                 logger.error(f"Error during scraping: {str(e)}")
#                 raise
#             finally:
#                 await browser.close()

# async def test_scraper():
#     url = "https://tenders-afd.dgmarket.com/tenders/brandedNoticeList.do"
    
#     # Your target date in AFD format (format like "Mar 4, 2025")
#     target_date = "Jan 22, 2025"  # Adjust to a date that exists in the table
    
#     try:
#         scraper = AFDScraperTest(url, target_date)
#         df = await scraper.scrape_data()
        
#         if not df.empty:
#             output_file = f"afd_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
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

# test_afd.py

import asyncio
import logging
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
import pandas as pd

# Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Number of concurrent detail page processing
MAX_CONCURRENT_PAGES = 5

class AFDScraperTest:
    def __init__(self, url: str, start_date: str, end_date: str):
        self.url = url
        self.start_date_str = start_date
        self.end_date_str = end_date
        
        # Convert to datetime objects for comparison
        self.start_date = datetime.strptime(start_date, "%b %d, %Y")
        self.end_date = datetime.strptime(end_date, "%b %d, %Y")
        
        self.results = []
        # Extract the base domain from the URL
        self.domain = '/'.join(url.split('/')[:3])  # Get "https://tenders-afd.dgmarket.com"
        self.semaphore = None  # Will be initialized in scrape_data
        logger.info(f"AFD scraper initialized with base domain: {self.domain}")
        logger.info(f"Date range: {start_date} to {end_date}")

    async def extract_tender_details(self, context, tender_url: str):
        """Extract details from a specific tender detail page"""
        detail_page = None
        try:
            # Use semaphore to limit concurrent pages
            async with self.semaphore:
                # Create a new page for the detail view
                detail_page = await context.new_page()
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

    async def process_row(self, context, row):
        """Process a single row from the table"""
        try:
            # Extract published date
            date_elem = await row.query_selector("td.published")
            if not date_elem:
                return None
                
            published_date = await date_elem.inner_text()
            published_date = published_date.strip()
            
            # Check if date is in our range (start_date <= date <= end_date)
            try:
                published_date_obj = datetime.strptime(published_date, "%b %d, %Y")
                
                # Skip if date is not in our range
                if not (self.start_date <= published_date_obj <= self.end_date):
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
            
            # If we have a tender URL, get additional details
            if notice_link and notice_link != "N/A":
                additional_details = await self.extract_tender_details(context, notice_link)
                if additional_details:
                    result.update(additional_details)
            
            logger.info(f"Found matching tender: {title}")
            return result
            
        except Exception as e:
            logger.error(f"Error processing row: {str(e)}")
            return None

    async def process_rows_batch(self, context, rows):
        """Process a batch of table rows in parallel"""
        tasks = []
        for row in rows:
            tasks.append(self.process_row(context, row))
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        
        # Filter out None results
        return [r for r in results if r]

    async def check_page_for_date_range_with_cutoff(self, page):
        """
        Check if current page has any rows with dates in our range.
        Also checks if we should stop searching because we've found older dates.
        
        Returns:
            tuple: (has_matches, should_stop_searching)
        """
        date_elems = await page.query_selector_all("td.published")
        has_matches = False
        should_stop_searching = False
        
        for elem in date_elems:
            date_text = await elem.inner_text()
            date_text = date_text.strip()
            
            # Try to parse the date for comparison
            try:
                elem_date = datetime.strptime(date_text, "%b %d, %Y")
                
                # Check if date is in our range (start_date <= date <= end_date)
                if self.start_date <= elem_date <= self.end_date:
                    has_matches = True
                
                # If we found a date older than start_date, we can stop searching
                if elem_date < self.start_date:
                    should_stop_searching = True
            except ValueError:
                # If we can't parse the date, just continue
                continue
        
        return (has_matches, should_stop_searching)

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
                await page.wait_for_selector("table#notice", state="visible")
                logger.info("Table found")
                
                current_page = 1
                
                while True:
                    logger.info(f"Processing page {current_page}")
                    
                    # Check if this page has any matching dates in our range and if we should stop searching
                    has_matches, should_stop_searching = await self.check_page_for_date_range_with_cutoff(page)
                    
                    if has_matches:
                        logger.info(f"Found dates in our range on page {current_page}, extracting data")
                        
                        # Get all rows from the table (skip header row)
                        rows = await page.query_selector_all("table#notice tbody tr")
                        logger.info(f"Found {len(rows)} rows on page {current_page}")
                        
                        # Process rows in parallel batches
                        batch_size = MAX_CONCURRENT_PAGES
                        
                        for i in range(0, len(rows), batch_size):
                            batch = rows[i:i+batch_size]
                            logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} rows)")
                            
                            batch_results = await self.process_rows_batch(context, batch)
                            self.results.extend(batch_results)
                            
                            logger.info(f"Found {len(batch_results)} matching rows in this batch")
                        
                        # If we should stop searching further pages, break here
                        if should_stop_searching:
                            logger.info("Found dates older than our range, stopping search")
                            break
                    elif should_stop_searching:
                        logger.info(f"Found dates older than our range on page {current_page}, stopping search")
                        break
                    else:
                        logger.info(f"No matching dates found on page {current_page}, checking next page")
                        
                        # Look for the "Next" link in pagination
                        next_link = await page.query_selector("a:has-text('Next')")
                        
                        if not next_link:
                            logger.info("No 'Next' link found or we're on the last page")
                            break
                            
                        logger.info(f"Moving to page {current_page + 1}")
                        
                        # Click to navigate to next page
                        await next_link.click()
                        await page.wait_for_load_state("networkidle")
                        await page.wait_for_selector("table#notice", state="visible")
                        
                        current_page += 1
                
                # Convert results to DataFrame
                df = pd.DataFrame(self.results)
                return df

            except Exception as e:
                logger.error(f"Error during scraping: {str(e)}")
                raise
            finally:
                await browser.close()

async def test_scraper():
    url = "https://tenders-afd.dgmarket.com/tenders/brandedNoticeList.do"
    
    # Define a date range (format like "Feb 20, 2025" to "Feb 28, 2025")
    start_date = "Feb 20, 2025"  # One week ago
    end_date = "Feb 28, 2025"    # Today
    
    try:
        scraper = AFDScraperTest(url, start_date, end_date)
        df = await scraper.scrape_data()
        
        if not df.empty:
            output_file = f"afd_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(output_file, index=False)
            logger.info(f"Data saved to {output_file}")
            logger.info(f"Total rows found: {len(df)}")
            print(df)  # Print the DataFrame to see the results
        else:
            logger.info(f"No data found for date range: {start_date} to {end_date}")
            
    except Exception as e:
        logger.error(f"Error during scraping: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_scraper())
                        