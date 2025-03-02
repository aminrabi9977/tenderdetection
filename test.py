# test.py

import asyncio
import logging
from datetime import datetime
from playwright.async_api import async_playwright
import pandas as pd

# Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WorldBankScraper:
    def __init__(self, url: str, target_date: str):
        self.url = url
        self.target_date = target_date
        self.results = []

    async def check_page_for_date(self, page) -> bool:
        """Check if current page has any rows with target date"""
        rows = await page.query_selector_all("table.project-opt-table tbody tr")
        
        for row in rows:
            cells = await row.query_selector_all("td")
            if cells and len(cells) >= 6:
                date_text = await cells[5].inner_text()
                if date_text.strip() == self.target_date:
                    return True
        return False

    async def scrape_data(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                page.set_default_timeout(120000)
                await page.goto(self.url, wait_until="networkidle")
                logger.info("Page loaded successfully")

                await page.wait_for_selector("table.project-opt-table", state="visible")
                logger.info("Table found")
                
                current_page = 1
                while True:
                    logger.info(f"Processing page {current_page}")
                    await page.wait_for_selector("table.project-opt-table tbody tr", state="visible")
                    
                    # First check if this page has any matching dates
                    has_matching_dates = await self.check_page_for_date(page)
                    if not has_matching_dates:
                        logger.info(f"No matching dates found on page {current_page}, stopping search")
                        break

                    # If we found matching dates, process the rows
                    rows = await page.query_selector_all("table.project-opt-table tbody tr")
                    found_on_page = 0
                    
                    for row in rows:
                        cells = await row.query_selector_all("td")
                        if not cells or len(cells) < 6:
                            continue

                        date_text = await cells[5].inner_text()
                        date_text = date_text.strip()
                        
                        if date_text == self.target_date:
                            row_data = {}
                            
                            desc_cell = cells[0]
                            desc_link = await desc_cell.query_selector("a")
                            if desc_link:
                                row_data['description'] = await desc_link.inner_text()
                                row_data['description_link'] = await desc_link.get_attribute('href')
                            else:
                                row_data['description'] = await desc_cell.inner_text()

                            row_data.update({
                                'country': await cells[1].inner_text(),
                                'project_title': await cells[2].inner_text(),
                                'notice_type': await cells[3].inner_text(),
                                'language': await cells[4].inner_text(),
                                'publish_date': date_text
                            })

                            proj_link = await cells[2].query_selector("a")
                            if proj_link:
                                row_data['project_link'] = await proj_link.get_attribute('href')

                            self.results.append(row_data)
                            found_on_page += 1
                            logger.info(f"Found matching row: {row_data['description'][:50]}...")

                    logger.info(f"Found {found_on_page} matching rows on page {current_page}")

                    # Try to go to next page
                    next_button = await page.query_selector("li:not(.disabled) a i.fa.fa-angle-right:not(.fa-angle-right + i)")
                    if next_button:
                        await next_button.evaluate("el => el.closest('a').click()")
                        await page.wait_for_load_state("networkidle")
                        current_page += 1
                        logger.info(f"Moving to page {current_page}")
                    else:
                        logger.info("No more pages available")
                        break

                df = pd.DataFrame(self.results)
                return df

            except Exception as e:
                logger.error(f"Error during scraping: {str(e)}")
                raise
            finally:
                await browser.close()

async def test_scraper():
    url = "https://projects.worldbank.org/en/projects-operations/procurement?srce=both"
    
    # Your target date (format: "February 22, 2025")
    target_date = "February 23, 2025"
    
    try:
        scraper = WorldBankScraper(url, target_date)
        df = await scraper.scrape_data()
        
        if not df.empty:
            output_file = f"world_bank_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(output_file, index=False)
            logger.info(f"Data saved to {output_file}")
            logger.info(f"Total rows found: {len(df)}")
        else:
            logger.info(f"No data found for date: {target_date}")
            
    except Exception as e:
        logger.error(f"Error during scraping: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_scraper())