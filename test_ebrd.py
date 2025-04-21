# # test_ebrd.py

# import asyncio
# import logging
# from datetime import datetime
# from playwright.async_api import async_playwright
# import pandas as pd
# import re
# from urllib.parse import urljoin

# # Set up basic logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# class EBRDScraperTest:
#     def __init__(self, url: str, target_date: str):
#         self.url = url
#         self.target_date = target_date
#         self.results = []
#         # Extract the base domain from the URL
#         self.domain = url.split('/')[0] + '//' + url.split('/')[2]
#         logger.info(f"Base domain: {self.domain}")

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

#                 # Wait for the posts to be visible
#                 await page.wait_for_selector("tbody#posts", state="visible")
#                 logger.info("Posts table found")
                
#                 current_page = 1
#                 current_url = self.url
                
#                 while True:
#                     logger.info(f"Processing page {current_page}")
                    
#                     # Make sure we're on the right page (only for pages after the first)
#                     if current_page > 1:
#                         await page.goto(current_url, wait_until="networkidle")
#                         await page.wait_for_selector("tbody#posts", state="visible")
                    
#                     # Make sure the rows are loaded
#                     await page.wait_for_selector("tbody#posts tr.post", state="visible")
                    
#                     # Check if this page has any matching dates
#                     rows = await page.query_selector_all("tbody#posts tr.post")
#                     logger.info(f"Found {len(rows)} rows on page {current_page}")
                    
#                     has_matching_dates = False
                    
#                     for row in rows:
#                         date_cell = await row.query_selector("td:first-child dt")
#                         if date_cell:
#                             date_text = await date_cell.inner_text()
#                             date_text = date_text.strip()
#                             logger.info(f"Found date: {date_text}")
#                             if date_text == self.target_date:
#                                 has_matching_dates = True
#                                 break
                    
#                     if not has_matching_dates:
#                         logger.info(f"No matching dates found on page {current_page}, stopping search")
#                         break
                    
#                     # Extract data from matching rows
#                     found_on_page = 0
                    
#                     for row in rows:
#                         try:
#                             # Get all cells in the row
#                             cells = await row.query_selector_all("td")
#                             if not cells or len(cells) < 7:
#                                 continue

#                             # Get issue date from the first column
#                             issue_date_cell = await cells[0].query_selector("dt")
#                             if not issue_date_cell:
#                                 continue
                                
#                             issue_date = await issue_date_cell.inner_text()
#                             issue_date = issue_date.strip()
                            
#                             if issue_date == self.target_date:
#                                 # Get closing date
#                                 closing_date_cell = await cells[1].query_selector("dt")
#                                 closing_date = await closing_date_cell.inner_text() if closing_date_cell else ""
                                
#                                 # Get location
#                                 location = await cells[2].inner_text()
                                
#                                 # Get project name and link
#                                 project_name_cell = await cells[3].query_selector("a")
#                                 project_name = await project_name_cell.inner_text() if project_name_cell else ""
#                                 project_link = await project_name_cell.get_attribute('href') if project_name_cell else ""
                                
#                                 # Get other details
#                                 sector = await cells[4].inner_text()
#                                 contract = await cells[5].inner_text()
#                                 notice_type = await cells[6].inner_text()
                                
#                                 row_data = {
#                                     'issue_date': issue_date,
#                                     'closing_date': closing_date.strip(),
#                                     'location': location.strip(),
#                                     'project_name': project_name.strip(),
#                                     'project_link': project_link,
#                                     'sector': sector.strip(),
#                                     'contract': contract.strip(),
#                                     'notice_type': notice_type.strip()
#                                 }
                                
#                                 self.results.append(row_data)
#                                 found_on_page += 1
#                                 logger.info(f"Found matching row: {project_name}")
#                         except Exception as e:
#                             logger.error(f"Error processing row: {str(e)}")
#                             continue
                    
#                     logger.info(f"Found {found_on_page} matching rows on page {current_page}")
                    
#                     # Try a simpler approach - just try to navigate to next page by number
#                     current_page += 1
                    
#                     # Try to extract the pattern from the current URL
#                     next_url = None
#                     if current_page == 2:
#                         # Use JavaScript to extract all pagination links and their texts
#                         pagination_data = await page.evaluate('''
#                             () => {
#                                 const links = Array.from(document.querySelectorAll('.saf-paging a'));
#                                 return links.map(link => ({
#                                     text: link.textContent.trim(),
#                                     href: link.href
#                                 }));
#                             }
#                         ''')
                        
#                         logger.info(f"Found {len(pagination_data)} pagination links via JS")
                        
#                         # Find link with text "2"
#                         for link_data in pagination_data:
#                             if link_data['text'] == '2':
#                                 next_url = link_data['href']
#                                 logger.info(f"Found full URL for page 2: {next_url}")
#                                 break
                    
#                     if not next_url:
#                         # If we couldn't find page 2 link, we'll stop
#                         logger.info("Could not determine URL for next page, stopping")
#                         break
                    
#                     current_url = next_url
                
#                 # Convert results to DataFrame
#                 df = pd.DataFrame(self.results)
#                 return df

#             except Exception as e:
#                 logger.error(f"Error during scraping: {str(e)}")
#                 raise
#             finally:
#                 await browser.close()

# async def test_scraper():
#     url = "https://www.ebrd.com/work-with-us/procurement/notices.html"
    
#     # Your target date (format: "20 Feb 2025") - EBRD format
#     target_date = "20 Feb 2025"
    
#     try:
#         scraper = EBRDScraperTest(url, target_date)
#         df = await scraper.scrape_data()
        
#         if not df.empty:
#             output_file = f"ebrd_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
#             df.to_csv(output_file, index=False)
#             logger.info(f"Data saved to {output_file}")
#             logger.info(f"Total rows found: {len(df)}")
#         else:
#             logger.info(f"No data found for date: {target_date}")
            
#     except Exception as e:
#         logger.error(f"Error during scraping: {str(e)}")

# if __name__ == "__main__":
#     asyncio.run(test_scraper())

# ----------------------------------------------------------------------
# test_ebrd.py

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

class EBRDScraperTest:
    def __init__(self, url: str, start_date: str, end_date: str):
        self.url = url
        self.start_date_str = start_date
        self.end_date_str = end_date
        
        # Convert to datetime objects for comparison
        self.start_date = datetime.strptime(start_date, "%d %b %Y")
        self.end_date = datetime.strptime(end_date, "%d %b %Y")
        
        self.results = []
        # Extract the base domain from the URL
        self.domain = '/'.join(url.split('/')[:3])  # Get "https://www.ebrd.com"
        self.semaphore = None  # Will be initialized in scrape_data
        logger.info(f"EBRD scraper initialized with base domain: {self.domain}")
        logger.info(f"Date range: {start_date} to {end_date}")

    async def extract_tender_details(self, context, detail_url: str):
        """Extract details from a specific tender detail page"""
        detail_page = None
        try:
            # Use semaphore to limit concurrent pages
            async with self.semaphore:
                # Create a new page for the detail view
                detail_page = await context.new_page()
                await detail_page.goto(detail_url)
                await detail_page.wait_for_load_state("networkidle")
                
                # Initialize data dictionary
                tender_data = {}
                
                # Extract all required fields based on new HTML structure
                
                # Project ID
                project_id_elem = await detail_page.query_selector(".project-overview__projectID")
                if project_id_elem:
                    tender_data['project_id'] = await project_id_elem.inner_text()
                
                # Procurement Ref No
                proc_ref_elem = await detail_page.query_selector(".project-overview__main-card:has(.project-overview__card-title:text-is('Procurement Ref No.')) .project-overview__card-description")
                if proc_ref_elem:
                    tender_data['procurement_ref_no'] = await proc_ref_elem.inner_text()
                
                # Location
                location_elem = await detail_page.query_selector(".project-overview__main-card:has(.project-overview__card-title:text-is('Location')) .project-overview__card-description")
                if location_elem:
                    tender_data['location'] = await location_elem.inner_text()
                
                # City Name
                city_elem = await detail_page.query_selector(".project-overview__main-card:has(.project-overview__card-title:text-is('City Name')) .project-overview__card-description")
                if city_elem:
                    tender_data['city_name'] = await city_elem.inner_text()
                
                # Business Sector
                sector_elem = await detail_page.query_selector(".project-overview__main-card:has(.project-overview__card-title:text-is('Business Sector')) .project-overview__card-description")
                if sector_elem:
                    tender_data['business_sector'] = await sector_elem.inner_text()
                
                # Funding Source
                funding_elem = await detail_page.query_selector(".project-overview__main-card:has(.project-overview__card-title:text-is('Funding Source')) .project-overview__card-description")
                if funding_elem:
                    tender_data['funding_source'] = await funding_elem.inner_text()
                
                # Notice Type
                notice_type_elem = await detail_page.query_selector(".project-overview__main-card:has(.project-overview__card-title:text-is('Notice Type')) .project-overview__card-description")
                if notice_type_elem:
                    tender_data['notice_type'] = await notice_type_elem.inner_text()
                
                # Contract Type
                contract_type_elem = await detail_page.query_selector(".project-overview__main-card:has(.project-overview__card-title:text-is('Contract Type')) .project-overview__card-description")
                if contract_type_elem:
                    tender_data['contract_type'] = await contract_type_elem.inner_text()
                
                # Issue Date
                issue_date_elem = await detail_page.query_selector(".project-overview__main-card:has(.project-overview__card-title:text-is('Issue Date')) .project-overview__card-description")
                if issue_date_elem:
                    tender_data['issue_date'] = await issue_date_elem.inner_text()
                
                # Closing Date
                closing_date_elem = await detail_page.query_selector(".project-overview__main-card:has(.project-overview__card-title:text-is('Closing Date')) .project-overview__card-description")
                if closing_date_elem:
                    tender_data['closing_date'] = await closing_date_elem.inner_text()
                
                # Add original URL
                tender_data['url'] = detail_url
                
                return tender_data
                
        except Exception as e:
            logger.error(f"Error extracting tender details from {detail_url}: {str(e)}")
            return None
        finally:
            if detail_page:
                await detail_page.close()

    async def is_date_in_range(self, date_text: str) -> bool:
        """Check if a date is within our specified range"""
        try:
            # EBRD format is like "19 Nov 2024" or "20 Mar 2025"
            date_obj = datetime.strptime(date_text.strip(), "%d %b %Y")
            return self.start_date <= date_obj <= self.end_date
        except ValueError:
            logger.warning(f"Could not parse date: {date_text}")
            return False

    async def process_tender_card(self, context, card):
        """Process a single tender card from the search results"""
        try:
            # Extract the issue date from the card
            issue_date_elem = await card.query_selector(".search-result__project-details.date-block div:first-child p:last-child span:last-child")
            if not issue_date_elem:
                return None
                
            issue_date = await issue_date_elem.inner_text()
            issue_date = issue_date.strip()
            
            # Check if date is in our range
            if not await self.is_date_in_range(issue_date):
                logger.info(f"Skipping tender with issue date {issue_date} (outside range)")
                return None
            
            # Extract the tender URL
            link_elem = await card.query_selector("h4.project-details a")
            if not link_elem:
                return None
                
            href = await link_elem.get_attribute('href')
            if not href:
                return None
                
            # Make the URL absolute if it's relative
            if href.startswith('/'):
                href = self.domain + href
            
            logger.info(f"Found matching tender with issue date {issue_date}, getting details")
            
            # Extract details from the tender page
            return await self.extract_tender_details(context, href)
            
        except Exception as e:
            logger.error(f"Error processing tender card: {str(e)}")
            return None

    async def process_tender_batch(self, context, cards):
        """Process a batch of tender cards in parallel"""
        tasks = []
        for card in cards:
            tasks.append(self.process_tender_card(context, card))
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        
        # Filter out None results
        return [r for r in results if r]

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

                current_page = 1
                has_more_pages = True
                
                while has_more_pages:
                    logger.info(f"Processing page {current_page}")
                    
                    # Wait for cards to load
                    await page.wait_for_selector(".search-result__result-card", state="visible")
                    
                    # Get all tender cards on the current page
                    tender_cards = await page.query_selector_all(".search-result__result-card")
                    logger.info(f"Found {len(tender_cards)} tender cards on page {current_page}")
                    
                    if not tender_cards:
                        logger.info("No tender cards found on page, stopping search")
                        break
                    
                    # Process tender cards in parallel batches
                    batch_size = MAX_CONCURRENT_PAGES
                    for i in range(0, len(tender_cards), batch_size):
                        batch = tender_cards[i:i+batch_size]
                        logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} tenders)")
                        
                        batch_results = await self.process_tender_batch(context, batch)
                        self.results.extend(batch_results)
                        
                        logger.info(f"Found {len(batch_results)} matching tenders in this batch")
                    
                    # Check for next page button
                    next_page_button = await page.query_selector("a.pagination__button--next:not(.disabled)")
                    if next_page_button:
                        logger.info(f"Moving to page {current_page + 1}")
                        await next_page_button.click()
                        await page.wait_for_load_state("networkidle")
                        current_page += 1
                    else:
                        logger.info("No next page found")
                        has_more_pages = False
                
                # Convert results to DataFrame
                df = pd.DataFrame(self.results)
                return df

            except Exception as e:
                logger.error(f"Error during scraping: {str(e)}")
                raise
            finally:
                await browser.close()

async def test_scraper():
    url = "https://www.ebrd.com/work-with-us/procurement/notices.html"
    
    # Define fixed date range for testing
    start_date = "01 Nov 2024"  # Format: "DD MMM YYYY"
    end_date = "31 Mar 2025"    # Format: "DD MMM YYYY"
    
    try:
        scraper = EBRDScraperTest(url, start_date, end_date)
        df = await scraper.scrape_data()
        
        if not df.empty:
            output_file = f"ebrd_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(output_file, index=False)
            logger.info(f"Data saved to {output_file}")
            logger.info(f"Total tenders found: {len(df)}")
            print(df)  # Print the DataFrame to see the results
        else:
            logger.info(f"No data found for date range: {start_date} to {end_date}")
            
    except Exception as e:
        logger.error(f"Error during scraping: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_scraper())