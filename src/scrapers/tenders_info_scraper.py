# # src/scrapers/tenders_info_scraper.py

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

# class TendersInfoScraper(BaseScraper):
#     def __init__(self, base_url: str):
#         super().__init__(base_url)
#         # Format today's date as "28 Feb 2025" (format used by TendersInfo)
#         self.today = datetime.now().strftime("%d %b %Y")
#         self.results = []
#         self.semaphore = None  # Will be initialized in scrape_data

#     async def init_browser(self):
#         """Initialize browser instance"""
#         self.playwright = await async_playwright().start()
#         self.browser = await self.playwright.chromium.launch(headless=False)
#         self.context = await self.browser.new_context()
#         self.context.set_default_timeout(60000)
#         self.page = await self.context.new_page()
#         # Create a semaphore to limit concurrent pages
#         self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)

#     async def close_browser(self):
#         """Close browser instance"""
#         await self.context.close()
#         await self.browser.close()
#         await self.playwright.stop()
        
#     async def extract_table_data(self) -> List[Dict]:
#         """
#         Required by BaseScraper - we'll implement it to extract global tenders
#         Note: This is essentially a wrapper around extract_global_tenders
#         """
#         return await self.extract_global_tenders()

#     async def extract_tender_details(self, detail_url: str) -> Optional[Dict]:
#         """Extract details from a specific tender page"""
#         detail_page = None
#         try:
#             # Use semaphore to limit concurrent pages
#             async with self.semaphore:
#                 # Create a new page for the detail view to avoid navigation issues
#                 detail_page = await self.context.new_page()
#                 await detail_page.goto(detail_url)
#                 await detail_page.wait_for_load_state("networkidle")
                
#                 # Wait for the form to load
#                 await detail_page.wait_for_selector(".form-horizontal", state="visible")
                
#                 # Extract all required fields
#                 tender_data = {}
                
#                 # Extract Tender TI Ref No
#                 ref_no_elem = await detail_page.query_selector("label:text('Tender TI Ref No') + div p")
#                 if ref_no_elem:
#                     tender_data['ref_no'] = await ref_no_elem.inner_text()
                
#                 # Extract Tender Date
#                 date_elem = await detail_page.query_selector("label:text('Tender Date') + div p")
#                 if date_elem:
#                     tender_data['date'] = await date_elem.inner_text()
                
#                 # Extract Tender Description
#                 desc_elem = await detail_page.query_selector("label:text('Tender Description') + div p")
#                 if desc_elem:
#                     tender_data['description'] = await desc_elem.inner_text()
                
#                 # Extract Tender Deadline
#                 deadline_elem = await detail_page.query_selector("label:text('Tender Deadline') + div p")
#                 if deadline_elem:
#                     tender_data['deadline'] = await deadline_elem.inner_text()
                
#                 # Extract Tender Project Location
#                 location_elem = await detail_page.query_selector("label:text('Tender Project Location') + div p")
#                 if location_elem:
#                     tender_data['location'] = await location_elem.inner_text()
                
#                 # Extract Tender Sector
#                 sector_elem = await detail_page.query_selector("label:text('Tender Sector') + div p")
#                 if sector_elem:
#                     tender_data['sector'] = await sector_elem.inner_text()
                
#                 # Extract Tender CPV
#                 cpv_elem = await detail_page.query_selector("label:text('Tender CPV') + div p")
#                 if cpv_elem:
#                     tender_data['cpv'] = await cpv_elem.inner_text()
                
#                 # Extract Tender Estimated Cost
#                 cost_elem = await detail_page.query_selector("label:text('Tender Estimated Cost') + div p")
#                 if cost_elem:
#                     tender_data['estimated_cost'] = await cost_elem.inner_text()
                
#                 # Extract Tender Document Type
#                 doc_type_elem = await detail_page.query_selector("label:text('Tender Document Type') + div p")
#                 if doc_type_elem:
#                     tender_data['document_type'] = await doc_type_elem.inner_text()
                
#                 # Add original URL
#                 tender_data['url'] = detail_url
                
#                 return tender_data
                
#         except Exception as e:
#             logger.error(f"Error extracting tender details from {detail_url}: {str(e)}")
#             return None
#         finally:
#             if detail_page:
#                 await detail_page.close()

#     async def process_tender_batch(self, tender_links):
#         """Process a batch of tender links in parallel"""
#         tasks = []
#         for link_data in tender_links:
#             title = link_data['title']
#             href = link_data['href']
            
#             if not href:
#                 continue
                
#             tasks.append(self._process_tender(title, href))
        
#         # Wait for all tasks to complete
#         results = await asyncio.gather(*tasks)
        
#         # Filter out None results
#         return [r for r in results if r]

#     async def _process_tender(self, title, href):
#         """Process a single tender, encapsulated for parallel processing"""
#         try:
#             logger.info(f"Processing tender: {title[:50]}...")
            
#             # Get the detail page
#             tender_data = await self.extract_tender_details(href)
            
#             if tender_data and 'date' in tender_data:
#                 # Check if the tender date matches today's date
#                 tender_date = tender_data['date'].strip()
#                 logger.debug(f"Tender date: {tender_date}, Today: {self.today}")
                
#                 if tender_date == self.today:
#                     logger.info(f"Found matching tender: {title}")
#                     return tender_data
            
#             return None
        
#         except Exception as e:
#             logger.error(f"Error processing tender {title[:30]}: {str(e)}")
#             return None

#     async def extract_global_tenders(self) -> List[Dict]:
#         """Extract data specifically from the global tenders section"""
#         try:
#             # Wait for the global tenders panel to load - use standard Playwright selector
#             await self.page.wait_for_selector(".panel-heading:has-text('Global Tenders')")
#             logger.info("Global tenders section found")
            
#             # Get only the global tenders panel's tender links using proper JS approach
#             tender_links = await self.page.evaluate("""
#                 () => {
#                     // Find Global Tenders panel using text content
#                     const headings = Array.from(document.querySelectorAll('.panel-heading'));
#                     const globalHeading = headings.find(el => el.textContent.includes('Global Tenders'));
                    
#                     if (!globalHeading) return [];
                    
#                     const globalPanel = globalHeading.closest('.panel');
#                     if (!globalPanel) return [];
                    
#                     // Find all tender links within this panel
#                     const links = globalPanel.querySelectorAll('a.tenderBrief');
                    
#                     return Array.from(links).map(link => {
#                         return {
#                             title: link.textContent.trim(),
#                             href: link.href
#                         };
#                     });
#                 }
#             """)
            
#             logger.info(f"Found {len(tender_links)} global tender links")
            
#             if not tender_links:
#                 return []

#             # Process tender links in parallel batches
#             batch_size = MAX_CONCURRENT_PAGES
#             all_results = []
            
#             for i in range(0, len(tender_links), batch_size):
#                 batch = tender_links[i:i+batch_size]
#                 logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} tenders)")
                
#                 batch_results = await self.process_tender_batch(batch)
#                 all_results.extend(batch_results)
                
#                 logger.info(f"Found {len(batch_results)} matching tenders in this batch")
            
#             return all_results
            
#         except Exception as e:
#             logger.error(f"Error extracting global tenders: {str(e)}")
#             return []

#     async def navigate_to_next_page(self) -> bool:
#         """Navigate to the next page of global tenders"""
#         try:
#             # Check for "Load More" button for global tenders
#             load_more_button = await self.page.query_selector(".load-more-tenders")
            
#             if load_more_button:
#                 # Check if the button is visible and enabled
#                 is_visible = await load_more_button.is_visible()
#                 is_disabled = await load_more_button.get_attribute("disabled") == "true"
                
#                 if is_visible and not is_disabled:
#                     logger.info("Found 'Load More' button, clicking it")
#                     await load_more_button.click()
#                     await self.page.wait_for_load_state("networkidle")
#                     # Wait for new tenders to be added to the DOM
#                     await self.page.wait_for_timeout(2000)  # Short delay to ensure new content is loaded
#                     return True
            
#             # Alternative: Check for numbered pagination
#             pagination = await self.page.query_selector("ul.pagination")
#             if pagination:
#                 # Find the active page
#                 active_li = await pagination.query_selector("li.active")
#                 if active_li:
#                     # Find the next page link
#                     next_page = await active_li.evaluate("""
#                         (node) => {
#                             const nextSibling = node.nextElementSibling;
#                             if (nextSibling && !nextSibling.classList.contains('disabled')) {
#                                 const link = nextSibling.querySelector('a');
#                                 return link ? link.getAttribute('href') : null;
#                             }
#                             return null;
#                         }
#                     """)
                    
#                     if next_page:
#                         logger.info(f"Found next page link: {next_page}")
#                         await self.page.goto(next_page)
#                         await self.page.wait_for_load_state("networkidle")
#                         return True
            
#             logger.info("No next page found")
#             return False
                
#         except Exception as e:
#             logger.error(f"Error navigating to next page: {str(e)}")
#             return False

#     async def check_next_page(self) -> bool:
#         """Required by BaseScraper - implemented through navigate_to_next_page"""
#         return await self.navigate_to_next_page()

#     async def scrape_data(self):
#         """Main scraping function"""
#         try:
#             await self.init_browser()
#             await self.page.goto(self.base_url)
#             await self.page.wait_for_load_state("networkidle")
            
#             current_page = 1
            
#             while True:
#                 logger.info(f"Processing page {current_page}")
                
#                 # Extract global tenders matching today's date from current page
#                 page_tenders = await self.extract_table_data()
#                 self.results.extend(page_tenders)
                
#                 logger.info(f"Found {len(page_tenders)} matching tenders on page {current_page}")
                
#                 # Check if there are more pages to process
#                 has_next = await self.check_next_page()
#                 if not has_next:
#                     logger.info("No more pages to process")
#                     break
                
#                 current_page += 1
            
#             # Convert results to DataFrame
#             df = pd.DataFrame(self.results)
            
#             if not df.empty:
#                 logger.info(f"Total tenders collected: {len(df)}")
#             else:
#                 logger.info("No matching tenders found")
                
#             return df
            
#         except Exception as e:
#             logger.error(f"Error during scraping: {str(e)}")
#             raise
#         finally:
#             await self.close_browser()
# -------------------------------------------------------------------------------------------
# src/scrapers/tenders_info_scraper.py

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

class TendersInfoScraper(BaseScraper):
    def __init__(self, base_url: str):
        super().__init__(base_url)
        # Format today's date and date from a week ago
        self.today = datetime.now()
        self.today_str = self.today.strftime("%d %b %Y")  # Format used by TendersInfo: "28 Feb 2025"
        
        # Calculate the date from a week ago
        self.week_ago = self.today - timedelta(days=7)
        self.week_ago_str = self.week_ago.strftime("%d %b %Y")
        
        self.results = []
        self.semaphore = None  # Will be initialized in scrape_data
        
        logger.info(f"TendersInfo scraper initialized")
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
            # TendersInfo format is like "28 Feb 2024"
            date_obj = datetime.strptime(date_text.strip(), "%d %b %Y")
            return self.week_ago <= date_obj <= self.today
        except ValueError as e:
            logger.warning(f"Could not parse date: {date_text}. Error: {str(e)}")
            return False
            
    async def is_date_older_than_range(self, date_text: str) -> bool:
        """Check if a date is older than our range"""
        try:
            date_obj = datetime.strptime(date_text.strip(), "%d %b %Y")
            return date_obj < self.week_ago
        except ValueError:
            return False
    
    async def extract_table_data(self) -> Tuple[List[Dict], bool]:
        """
        Required by BaseScraper - we'll implement it to extract global tenders
        and table with tenders from other countries
        
        Returns:
            Tuple[List[Dict], bool]: (results, should_stop)
        """
        tender_links = await self.extract_tender_links()
        
        if not tender_links:
            return [], False
            
        # Process tender links in parallel batches
        batch_size = MAX_CONCURRENT_PAGES
        all_results = []
        should_stop = False
        
        for i in range(0, len(tender_links), batch_size):
            batch = tender_links[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} tenders)")
            
            batch_results, batch_should_stop = await self.process_tender_batch(batch)
            all_results.extend(batch_results)
            
            if batch_should_stop:
                should_stop = True
            
            logger.info(f"Found {len(batch_results)} matching tenders in this batch")
            
        return all_results, should_stop

    async def extract_tender_details(self, detail_url: str) -> Optional[Dict]:
        """Extract details from a specific tender page"""
        detail_page = None
        try:
            # Use semaphore to limit concurrent pages
            async with self.semaphore:
                # Create a new page for the detail view to avoid navigation issues
                detail_page = await self.context.new_page()
                await detail_page.goto(detail_url)
                await detail_page.wait_for_load_state("networkidle")
                
                # Wait for the form to load
                await detail_page.wait_for_selector(".form-horizontal", state="visible")
                
                # Extract all required fields
                tender_data = {}
                
                # Extract Tender TI Ref No
                ref_no_elem = await detail_page.query_selector("label:text('Tender TI Ref No') + div p")
                if ref_no_elem:
                    tender_data['ref_no'] = await ref_no_elem.inner_text()
                
                # Extract Tender Date
                date_elem = await detail_page.query_selector("label:text('Tender Date') + div p")
                if date_elem:
                    tender_data['date'] = await date_elem.inner_text()
                
                # Extract Tender Description
                desc_elem = await detail_page.query_selector("label:text('Tender Description') + div p")
                if desc_elem:
                    tender_data['description'] = await desc_elem.inner_text()
                
                # Extract Tender Deadline
                deadline_elem = await detail_page.query_selector("label:text('Tender Deadline') + div p")
                if deadline_elem:
                    tender_data['deadline'] = await deadline_elem.inner_text()
                
                # Extract Tender Project Location
                location_elem = await detail_page.query_selector("label:text('Tender Project Location') + div p")
                if location_elem:
                    tender_data['location'] = await location_elem.inner_text()
                
                # Extract Tender Sector
                sector_elem = await detail_page.query_selector("label:text('Tender Sector') + div p")
                if sector_elem:
                    tender_data['sector'] = await sector_elem.inner_text()
                
                # Extract Tender CPV
                cpv_elem = await detail_page.query_selector("label:text('Tender CPV') + div p")
                if cpv_elem:
                    tender_data['cpv'] = await cpv_elem.inner_text()
                
                # Extract Tender Document Type
                doc_type_elem = await detail_page.query_selector("label:text('Tender Document Type') + div p")
                if doc_type_elem:
                    tender_data['document_type'] = await doc_type_elem.inner_text()
                
                # Add original URL
                tender_data['url'] = detail_url
                
                # Check if date is in our range
                if 'date' in tender_data:
                    date_text = tender_data['date'].strip()
                    
                    # Check if date is older than our range
                    if await self.is_date_older_than_range(date_text):
                        return "STOP_SEARCH"
                        
                    # Check if date is in our range
                    if await self.is_date_in_range(date_text):
                        return tender_data
                
                return None
                
        except Exception as e:
            logger.error(f"Error extracting tender details from {detail_url}: {str(e)}")
            return None
        finally:
            if detail_page:
                await detail_page.close()

    async def extract_tender_links(self) -> List[Dict]:
        """Extract all tender links from Global Tenders and the country-specific tenders table"""
        try:
            all_links = []
            
            # Extract from Global Tenders section
            global_tender_links = await self.page.evaluate("""
                () => {
                    // Find Global Tenders panel using text content
                    const headings = Array.from(document.querySelectorAll('.panel-heading'));
                    const globalHeading = headings.find(el => el.textContent.includes('Global Tenders'));
                    
                    if (!globalHeading) return [];
                    
                    const globalPanel = globalHeading.closest('.panel');
                    if (!globalPanel) return [];
                    
                    // Find all tender links within this panel
                    const links = globalPanel.querySelectorAll('a.tenderBrief');
                    
                    return Array.from(links).map(link => {
                        return {
                            title: link.textContent.trim(),
                            href: link.href
                        };
                    });
                }
            """)
            
            # Add extracted links to the result
            all_links.extend(global_tender_links)
            logger.info(f"Found {len(global_tender_links)} global tender links")
            
            # Extract from the second country-specific table (the one below Global Tenders)
            country_tender_links = await self.page.evaluate("""
                () => {
                    // Find the headings for all tables
                    const headings = Array.from(document.querySelectorAll('.panel-heading'));
                    
                    // Skip the Global Tenders and Free Tenders, find the country-specific one
                    // This will be the second table that's not Global Tenders or Free Tenders
                    let countryHeading = null;
                    for (const heading of headings) {
                        if (!heading.textContent.includes('Global Tenders') && 
                            !heading.textContent.includes('Free Tenders')) {
                            countryHeading = heading;
                            break;
                        }
                    }
                    
                    if (!countryHeading) return [];
                    
                    const countryPanel = countryHeading.closest('.panel');
                    if (!countryPanel) return [];
                    
                    // Find all tender links within this panel
                    const links = countryPanel.querySelectorAll('a.tenderBrief');
                    
                    return Array.from(links).map(link => {
                        return {
                            title: link.textContent.trim(),
                            href: link.href
                        };
                    });
                }
            """)
            
            # Add country-specific links to the result
            all_links.extend(country_tender_links)
            logger.info(f"Found {len(country_tender_links)} country-specific tender links")
            
            return all_links
            
        except Exception as e:
            logger.error(f"Error extracting tender links: {str(e)}")
            return []

    async def process_tender_batch(self, tender_links) -> Tuple[List[Dict], bool]:
        """Process a batch of tender links in parallel"""
        tasks = []
        for link_data in tender_links:
            title = link_data['title']
            href = link_data['href']
            
            if not href:
                continue
                
            tasks.append(self.extract_tender_details(href))
        
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

    async def check_next_page(self) -> bool:
        """Required by BaseScraper - check if there's a next page and navigate to it"""
        try:
            # Check for "Load More" button for global tenders
            load_more_button = await self.page.query_selector(".load-more-tenders")
            
            if load_more_button:
                # Check if the button is visible and enabled
                is_visible = await load_more_button.is_visible()
                is_disabled = await load_more_button.get_attribute("disabled") == "true"
                
                if is_visible and not is_disabled:
                    logger.info("Found 'Load More' button, clicking it")
                    await load_more_button.click()
                    await self.page.wait_for_load_state("networkidle")
                    # Wait for new tenders to be added to the DOM
                    await self.page.wait_for_timeout(2000)  # Short delay to ensure new content is loaded
                    return True
            
            # Alternative: Check for numbered pagination
            pagination = await self.page.query_selector("ul.pagination")
            if pagination:
                # Find the active page
                active_li = await pagination.query_selector("li.active")
                if active_li:
                    # Find the next page link
                    next_page = await active_li.evaluate("""
                        (node) => {
                            const nextSibling = node.nextElementSibling;
                            if (nextSibling && !nextSibling.classList.contains('disabled')) {
                                const link = nextSibling.querySelector('a');
                                return link ? link.getAttribute('href') : null;
                            }
                            return null;
                        }
                    """)
                    
                    if next_page:
                        logger.info(f"Found next page link: {next_page}")
                        await self.page.goto(next_page)
                        await self.page.wait_for_load_state("networkidle")
                        return True
            
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
            stop_search = False
            
            while not stop_search:
                logger.info(f"Processing page {current_page}")
                
                # Extract data from current page
                page_data, should_stop = await self.extract_table_data()
                self.results.extend(page_data)
                
                logger.info(f"Found {len(page_data)} matching tenders on page {current_page}")
                
                # If we should stop searching further pages, break here
                if should_stop:
                    logger.info("Found dates older than our range, stopping search")
                    break
                
                # Check and navigate to next page if exists
                has_next = await self.check_next_page()
                if not has_next:
                    logger.info("No more pages to process")
                    break
                    
                current_page += 1
            
            # Convert results to DataFrame
            df = pd.DataFrame(self.results)
            
            if not df.empty:
                logger.info(f"Total tenders collected: {len(df)}")
            else:
                logger.info("No matching tenders found")
                
            return df
            
        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}")
            raise
        finally:
            await self.close_browser()