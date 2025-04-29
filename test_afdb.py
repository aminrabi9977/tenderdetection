# test_afdb_date_range.py

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

class AfDBScraperTest:
    def __init__(self, url: str, start_date: str, end_date: str):
        self.url = url
        self.start_date_str = start_date
        self.end_date_str = end_date
        
        # Convert to datetime objects for comparison
        self.start_date = datetime.strptime(start_date, "%d-%b-%Y")
        self.end_date = datetime.strptime(end_date, "%d-%b-%Y")
        
        self.results = []
        # Extract the base domain from the URL
        self.domain = '/'.join(url.split('/')[:3])  # Get "https://www.afdb.org"
        self.semaphore = None  # Will be initialized in scrape_data
        
        logger.info(f"AfDB scraper test initialized with base domain: {self.domain}")
        logger.info(f"Date range: {start_date} to {end_date}")

    async def is_date_in_range(self, date_text: str) -> bool:
        """Check if a date is within our specified range"""
        try:
            # AfDB format is like "18-Apr-2025"
            date_obj = datetime.strptime(date_text.strip(), "%d-%b-%Y")
            return self.start_date <= date_obj <= self.end_date
        except ValueError as e:
            logger.warning(f"Could not parse date: {date_text}. Error: {str(e)}")
            return False
            
    async def is_date_older_than_range(self, date_text: str) -> bool:
        """Check if a date is older than our range"""
        try:
            date_obj = datetime.strptime(date_text.strip(), "%d-%b-%Y")
            return date_obj < self.start_date
        except ValueError:
            return False

    async def extract_sector_info(self, context, detail_url: str) -> str:
        """Extract sector information from the detail page"""
        detail_page = None
        try:
            # Use semaphore to limit concurrent pages
            async with self.semaphore:
                # Create a new page for the detail view
                detail_page = await context.new_page()
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

    async def process_row_batch(self, context, rows):
        """Process a batch of table rows in parallel"""
        tasks = []
        for row in rows:
            tasks.append(self.process_row(context, row))
        
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

    async def process_row(self, context, row):
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
            sector = await self.extract_sector_info(context, detail_url)
            
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

    async def check_page_for_date_range(self, page):
        """
        Check if the current page has dates in our range and if we should stop searching
        
        Returns:
            Tuple[bool, bool]: (has_matches, should_stop_searching)
        """
        try:
            # Get all date elements on the page
            date_elems = await page.query_selector_all("span.date-display-single")
            
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

    async def scrape_data(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()
            page = await context.new_page()
            
            # Create a semaphore to limit concurrent pages
            self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
            
            try:
                # Set longer timeout
                context.set_default_timeout(120000)  # 2 minutes timeout
                
                # Go to the page
                await page.goto(self.url, wait_until="networkidle")
                logger.info("Page loaded successfully")

                # Wait for the grid to be visible
                await page.wait_for_selector(".views-bootstrap-grid-plugin-style .row", state="visible")
                logger.info("Grid found")
                
                current_page = 1
                max_pages = 10  # Process up to 10 pages as specified
                found_older_date = False
                
                while current_page <= max_pages:
                    logger.info(f"Processing page {current_page}")
                    
                    # First check if this page has any matching dates in our range
                    has_matches, should_stop_searching = await self.check_page_for_date_range(page)
                    
                    if has_matches:
                        logger.info(f"Found dates in our range on page {current_page}, extracting data")
                        
                        # Get all grid items (column divs) from the grid
                        grid_items = await page.query_selector_all(".views-bootstrap-grid-plugin-style .row > div")
                        logger.info(f"Found {len(grid_items)} grid items on page {current_page}")
                        
                        # Process grid items in parallel batches
                        batch_size = MAX_CONCURRENT_PAGES
                        for i in range(0, len(grid_items), batch_size):
                            batch = grid_items[i:i+batch_size]
                            logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} rows)")
                            
                            batch_results, should_stop = await self.process_row_batch(context, batch)
                            self.results.extend(batch_results)
                            
                            logger.info(f"Found {len(batch_results)} matching rows in this batch")
                            
                            if should_stop:
                                logger.info("Found dates older than our range, stopping search")
                                should_stop_searching = True
                                found_older_date = True
                                break
                        
                        # If we should stop searching further pages, break here
                        if should_stop_searching:
                            logger.info("Found dates older than our range, stopping search")
                            found_older_date = True
                            break
                    elif should_stop_searching:
                        logger.info(f"Found dates older than our range on page {current_page}, stopping search")
                        found_older_date = True
                        break
                    else:
                        logger.info(f"No matching dates found on page {current_page}, checking next page")
                    
                    # Look for "next" pagination link based on code4.txt structure
                    next_link = await page.query_selector("li.next a[title='Go to next page']")
                    
                    if not next_link:
                        logger.info("No next page found")
                        break
                        
                    # Get the href attribute
                    href = await next_link.get_attribute('href')
                    logger.info(f"Found next page link: {href}")
                    
                    logger.info(f"Moving to page {current_page + 1}")
                    
                    # Click to navigate to next page
                    await next_link.click()
                    await page.wait_for_load_state("networkidle")
                    # Wait for grid to appear on the next page
                    await page.wait_for_selector(".views-bootstrap-grid-plugin-style", state="visible")
                    
                    current_page += 1
                
                if not found_older_date and current_page > max_pages:
                    logger.info(f"Reached maximum page limit ({max_pages}). Stopping search.")
                
                # Convert results to DataFrame
                df = pd.DataFrame(self.results)
                return df

            except Exception as e:
                logger.error(f"Error during scraping: {str(e)}")
                raise
            finally:
                await browser.close()

async def test_scraper():
    url = "https://www.afdb.org/en/projects-and-operations/procurement"
    
    # Define a custom date range (format: "DD-MMM-YYYY")
    # This date range should be passed as parameters when running the test
    # Default: one month range to get more comprehensive results
    end_date = "18-Apr-2025"
    start_date = "15-Apr-2025"
    
    try:
        scraper = AfDBScraperTest(url, start_date, end_date)
        df = await scraper.scrape_data()
        
        if not df.empty:
            output_file = f"afdb_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
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