# test_afdb.py

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

class AfDBScraperTest:
    def __init__(self, url: str, target_date: str):
        self.url = url
        self.target_date = target_date
        self.results = []
        # Extract the base domain from the URL
        self.domain = '/'.join(url.split('/')[:3])  # Get "https://www.afdb.org"
        self.semaphore = None  # Will be initialized in scrape_data
        logger.info(f"Base domain: {self.domain}")

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
                logger.info(f"Extracted sectors: {result}")
                return result
                
        except Exception as e:
            logger.error(f"Error extracting sector info from {detail_url}: {str(e)}")
            return "N/A"
        finally:
            if detail_page:
                await detail_page.close()

    async def process_row(self, context, row) -> dict:
        """Process a single row from the table"""
        try:
            # Extract date cell
            date_cell = await row.query_selector("td.views-field-field-publication-date span")
            if not date_cell:
                return None
                
            publish_date = await date_cell.inner_text()
            publish_date = publish_date.strip()
            
            # Check if date matches target date
            if publish_date != self.target_date:
                return None
            
            # Extract title cell and link
            title_cell = await row.query_selector("td.views-field-title a")
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
            
            logger.info(f"Found matching procurement: {title_text[:50]}...")
            
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

    async def process_row_batch(self, context, rows):
        """Process a batch of table rows in parallel"""
        tasks = []
        for row in rows:
            tasks.append(self.process_row(context, row))
        
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

                # Wait for the table to be visible
                await page.wait_for_selector("table.views-table", state="visible")
                logger.info("Table found")
                
                current_page = 1
                
                while True:
                    logger.info(f"Processing page {current_page}")
                    
                    # Get all rows from the table
                    rows = await page.query_selector_all("table.views-table tbody tr")
                    logger.info(f"Found {len(rows)} rows on page {current_page}")
                    
                    # Pre-filter rows that match target date
                    matching_rows = []
                    for row in rows:
                        date_cell = await row.query_selector("td.views-field-field-publication-date span")
                        if date_cell:
                            publish_date = await date_cell.inner_text()
                            publish_date = publish_date.strip()
                            if publish_date == self.target_date:
                                matching_rows.append(row)
                    
                    logger.info(f"Found {len(matching_rows)} rows matching target date")
                    
                    if matching_rows:
                        # Process rows in parallel batches
                        batch_size = MAX_CONCURRENT_PAGES
                        for i in range(0, len(matching_rows), batch_size):
                            batch = matching_rows[i:i+batch_size]
                            logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} rows)")
                            
                            batch_results = await self.process_row_batch(context, batch)
                            self.results.extend(batch_results)
                            
                            logger.info(f"Found {len(batch_results)} matching rows in this batch")
                    
                    # Look for "next" pagination link
                    next_link = await page.query_selector("li.pager-next a")
                    
                    if not next_link:
                        logger.info("No next page found")
                        break
                        
                    logger.info(f"Moving to page {current_page + 1}")
                    
                    # Click to navigate to next page
                    await next_link.click()
                    await page.wait_for_load_state("networkidle")
                    
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
    url = "https://www.afdb.org/en/projects-and-operations/procurement"
    
    # Your target date in AfDB format (DD-MMM-YYYY format like 28-Feb-2025)
    target_date = "28-Feb-2025"  # Adjust to a date that exists in the table
    
    try:
        scraper = AfDBScraperTest(url, target_date)
        df = await scraper.scrape_data()
        
        if not df.empty:
            output_file = f"afdb_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(output_file, index=False)
            logger.info(f"Data saved to {output_file}")
            logger.info(f"Total rows found: {len(df)}")
        else:
            logger.info(f"No data found for date: {target_date}")
            
    except Exception as e:
        logger.error(f"Error during scraping: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_scraper())