# test_ebrd.py

import asyncio
import logging
from datetime import datetime
from playwright.async_api import async_playwright
import pandas as pd
import re
from urllib.parse import urljoin

# Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EBRDScraperTest:
    def __init__(self, url: str, target_date: str):
        self.url = url
        self.target_date = target_date
        self.results = []
        # Extract the base domain from the URL
        self.domain = url.split('/')[0] + '//' + url.split('/')[2]
        logger.info(f"Base domain: {self.domain}")

    async def scrape_data(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                # Set longer timeout
                page.set_default_timeout(120000)  # 2 minutes timeout
                
                # Go to the page
                await page.goto(self.url, wait_until="networkidle")
                logger.info("Page loaded successfully")

                # Wait for the posts to be visible
                await page.wait_for_selector("tbody#posts", state="visible")
                logger.info("Posts table found")
                
                current_page = 1
                current_url = self.url
                
                while True:
                    logger.info(f"Processing page {current_page}")
                    
                    # Make sure we're on the right page (only for pages after the first)
                    if current_page > 1:
                        await page.goto(current_url, wait_until="networkidle")
                        await page.wait_for_selector("tbody#posts", state="visible")
                    
                    # Make sure the rows are loaded
                    await page.wait_for_selector("tbody#posts tr.post", state="visible")
                    
                    # Check if this page has any matching dates
                    rows = await page.query_selector_all("tbody#posts tr.post")
                    logger.info(f"Found {len(rows)} rows on page {current_page}")
                    
                    has_matching_dates = False
                    
                    for row in rows:
                        date_cell = await row.query_selector("td:first-child dt")
                        if date_cell:
                            date_text = await date_cell.inner_text()
                            date_text = date_text.strip()
                            logger.info(f"Found date: {date_text}")
                            if date_text == self.target_date:
                                has_matching_dates = True
                                break
                    
                    if not has_matching_dates:
                        logger.info(f"No matching dates found on page {current_page}, stopping search")
                        break
                    
                    # Extract data from matching rows
                    found_on_page = 0
                    
                    for row in rows:
                        try:
                            # Get all cells in the row
                            cells = await row.query_selector_all("td")
                            if not cells or len(cells) < 7:
                                continue

                            # Get issue date from the first column
                            issue_date_cell = await cells[0].query_selector("dt")
                            if not issue_date_cell:
                                continue
                                
                            issue_date = await issue_date_cell.inner_text()
                            issue_date = issue_date.strip()
                            
                            if issue_date == self.target_date:
                                # Get closing date
                                closing_date_cell = await cells[1].query_selector("dt")
                                closing_date = await closing_date_cell.inner_text() if closing_date_cell else ""
                                
                                # Get location
                                location = await cells[2].inner_text()
                                
                                # Get project name and link
                                project_name_cell = await cells[3].query_selector("a")
                                project_name = await project_name_cell.inner_text() if project_name_cell else ""
                                project_link = await project_name_cell.get_attribute('href') if project_name_cell else ""
                                
                                # Get other details
                                sector = await cells[4].inner_text()
                                contract = await cells[5].inner_text()
                                notice_type = await cells[6].inner_text()
                                
                                row_data = {
                                    'issue_date': issue_date,
                                    'closing_date': closing_date.strip(),
                                    'location': location.strip(),
                                    'project_name': project_name.strip(),
                                    'project_link': project_link,
                                    'sector': sector.strip(),
                                    'contract': contract.strip(),
                                    'notice_type': notice_type.strip()
                                }
                                
                                self.results.append(row_data)
                                found_on_page += 1
                                logger.info(f"Found matching row: {project_name}")
                        except Exception as e:
                            logger.error(f"Error processing row: {str(e)}")
                            continue
                    
                    logger.info(f"Found {found_on_page} matching rows on page {current_page}")
                    
                    # Try a simpler approach - just try to navigate to next page by number
                    current_page += 1
                    
                    # Try to extract the pattern from the current URL
                    next_url = None
                    if current_page == 2:
                        # Use JavaScript to extract all pagination links and their texts
                        pagination_data = await page.evaluate('''
                            () => {
                                const links = Array.from(document.querySelectorAll('.saf-paging a'));
                                return links.map(link => ({
                                    text: link.textContent.trim(),
                                    href: link.href
                                }));
                            }
                        ''')
                        
                        logger.info(f"Found {len(pagination_data)} pagination links via JS")
                        
                        # Find link with text "2"
                        for link_data in pagination_data:
                            if link_data['text'] == '2':
                                next_url = link_data['href']
                                logger.info(f"Found full URL for page 2: {next_url}")
                                break
                    
                    if not next_url:
                        # If we couldn't find page 2 link, we'll stop
                        logger.info("Could not determine URL for next page, stopping")
                        break
                    
                    current_url = next_url
                
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
    
    # Your target date (format: "20 Feb 2025") - EBRD format
    target_date = "20 Feb 2025"
    
    try:
        scraper = EBRDScraperTest(url, target_date)
        df = await scraper.scrape_data()
        
        if not df.empty:
            output_file = f"ebrd_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(output_file, index=False)
            logger.info(f"Data saved to {output_file}")
            logger.info(f"Total rows found: {len(df)}")
        else:
            logger.info(f"No data found for date: {target_date}")
            
    except Exception as e:
        logger.error(f"Error during scraping: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_scraper())