# src/scrapers/ebrd_scraper.py

from playwright.async_api import async_playwright
import asyncio
from datetime import datetime
import pandas as pd
import logging
from typing import List, Dict
from src.scrapers.base_scraper import BaseScraper
from src.utils.date_utils import normalize_date

logger = logging.getLogger(__name__)

class EBRDScraper(BaseScraper):
    def __init__(self, base_url: str):
        super().__init__(base_url)
        # Format today's date as "20 Feb 2025" (format used by EBRD)
        self.today = datetime.now().strftime("%d %b %Y")
        self.results = []
        # Extract the base domain from the URL
        self.domain = base_url.split('/')[0] + '//' + base_url.split('/')[2]
        logger.info(f"Base domain: {self.domain}")

    async def init_browser(self):
        """Initialize browser instance"""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=False)
        self.context = await self.browser.new_context()
        self.context.set_default_timeout(60000)
        self.page = await self.context.new_page()

    async def close_browser(self):
        """Close browser instance"""
        await self.context.close()
        await self.browser.close()
        await self.playwright.stop()

    async def check_page_for_today(self) -> bool:
        """Check if current page has any rows with today's date"""
        # Wait for rows to be visible
        try:
            await self.page.wait_for_selector("tbody#posts tr.post", state="visible")
            rows = await self.page.query_selector_all("tbody#posts tr.post")
            logger.info(f"Found {len(rows)} rows to check for today's date")
            
            for row in rows:
                # Get the Issue Date cell (first column)
                date_cell = await row.query_selector("td:first-child dt")
                if date_cell:
                    date_text = await date_cell.inner_text()
                    # Clean and normalize the date
                    date_text = date_text.strip()
                    logger.debug(f"Found date: {date_text}, comparing with today: {self.today}")
                    if date_text == self.today:
                        return True
            return False
        except Exception as e:
            logger.error(f"Error checking for today's date: {str(e)}")
            return False

    async def extract_table_data(self) -> List[Dict]:
        """Extract matching rows from the current page"""
        rows = await self.page.query_selector_all("tbody#posts tr.post")
        table_data = []
        
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
                
                # Only process rows matching today's date
                if issue_date == self.today:
                    # Get closing date
                    closing_date_cell = await cells[1].query_selector("dt")
                    closing_date = await closing_date_cell.inner_text() if closing_date_cell else ""
                    
                    # Extract other data
                    location = await cells[2].inner_text()
                    
                    # Get project name and link
                    project_name_cell = await cells[3].query_selector("a")
                    project_name = await project_name_cell.inner_text() if project_name_cell else ""
                    project_link = await project_name_cell.get_attribute('href') if project_name_cell else ""
                    
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
                    
                    table_data.append(row_data)
                    logger.info(f"Found matching row: {project_name[:50]}...")
            except Exception as e:
                logger.error(f"Error processing row: {str(e)}")
                continue
        
        return table_data

    async def get_next_page_url(self, current_page: int) -> str:
        """Get the URL for the next page using JavaScript evaluation"""
        try:
            # Use JavaScript to extract all pagination links and their texts
            pagination_data = await self.page.evaluate('''
                () => {
                    const links = Array.from(document.querySelectorAll('.saf-paging a'));
                    return links.map(link => ({
                        text: link.textContent.trim(),
                        href: link.href
                    }));
                }
            ''')
            
            logger.info(f"Found {len(pagination_data)} pagination links via JS")
            
            # Find link with text matching the next page number
            next_page_num = str(current_page + 1)
            for link_data in pagination_data:
                if link_data['text'] == next_page_num:
                    next_url = link_data['href']
                    logger.info(f"Found full URL for page {next_page_num}: {next_url}")
                    return next_url
                    
            logger.info(f"No link found for page {next_page_num}")
            return None
        except Exception as e:
            logger.error(f"Error getting next page URL: {str(e)}")
            return None

    async def check_next_page(self) -> bool:
        """
        Required by BaseScraper, but we're using get_next_page_url instead.
        This is just a placeholder to satisfy the abstract base class.
        """
        return False

    async def scrape_data(self):
        """Main scraping function"""
        try:
            await self.init_browser()
            await self.page.goto(self.base_url)
            await self.page.wait_for_load_state("networkidle")
            
            # Wait for the specific posts table to be visible
            await self.page.wait_for_selector("tbody#posts", state="visible")
            logger.info("Posts table found")
            
            current_page = 1
            current_url = self.base_url
            
            while True:
                logger.info(f"Processing page {current_page}")
                
                # Make sure we're on the right page (only for pages after the first)
                if current_page > 1:
                    await self.page.goto(current_url, wait_until="networkidle")
                    await self.page.wait_for_selector("tbody#posts", state="visible")
                
                # First check if this page has any matching dates
                has_matching_dates = await self.check_page_for_today()
                if not has_matching_dates:
                    logger.info(f"No matching dates found on page {current_page}, stopping search")
                    break

                # Extract data from current page
                page_data = await self.extract_table_data()
                self.results.extend(page_data)
                
                logger.info(f"Found {len(page_data)} matching rows on page {current_page}")
                
                # Get URL for the next page
                next_url = await self.get_next_page_url(current_page)
                if not next_url:
                    logger.info("No more pages to process")
                    break
                
                current_url = next_url
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