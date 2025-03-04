# src/scrapers/aiib_scraper.py

from playwright.async_api import async_playwright
import asyncio
from datetime import datetime
import pandas as pd
import logging
from typing import List, Dict, Optional
from src.scrapers.base_scraper import BaseScraper
from src.utils.date_utils import normalize_date, format_date_for_site

logger = logging.getLogger(__name__)

class AIIBScraper(BaseScraper):
    def __init__(self, base_url: str):
        super().__init__(base_url)
        # Format today's date as "Feb 28, 2025" (format used by AIIB)
        self.today = datetime.now().strftime("%b %d, %Y")
        self.results = []
        # Extract the base domain from the URL
        self.domain = '/'.join(base_url.split('/')[:3])  # Get "https://www.aiib.org"
        logger.info(f"AIIB scraper initialized with base domain: {self.domain}")
        logger.info(f"Today's date in AIIB format: {self.today}")

    async def init_browser(self):
        """Initialize browser instance"""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=False)
        self.context = await self.browser.new_context()
        self.context.set_default_timeout(60000)  # 60 seconds timeout
        self.page = await self.context.new_page()

    async def close_browser(self):
        """Close browser instance"""
        await self.context.close()
        await self.browser.close()
        await self.playwright.stop()

    async def process_row(self, row) -> Optional[Dict]:
        """Process a single row from the table"""
        try:
            # Extract date cell
            date_elem = await row.query_selector(".table-col.table-date .s2")
            if not date_elem:
                return None
                
            issue_date = await date_elem.inner_text()
            issue_date = issue_date.strip()
            
            # Check if date matches today's date
            logger.debug(f"Row date: {issue_date}, Today: {self.today}")
            if issue_date != self.today:
                return None
            
            # Extract country
            country_elem = await row.query_selector(".table-col.table-country .country-value")
            country = await country_elem.inner_text() if country_elem else "N/A"
            
            # Extract project name
            project_elem = await row.query_selector(".table-col.table-project .title-value")
            project_name = await project_elem.inner_text() if project_elem else "N/A"
            
            # Extract download link
            download_link_elem = await row.query_selector(".table-col.table-project a")
            download_link = await download_link_elem.get_attribute('href') if download_link_elem else "N/A"
            if download_link != "N/A" and download_link.startswith('/'):
                download_link = self.domain + download_link
            
            # Extract sector
            sector_elem = await row.query_selector(".table-col.table-energy .sector-value")
            sector = await sector_elem.inner_text() if sector_elem else "N/A"
            
            # Extract notice type
            type_elem = await row.query_selector(".table-col.table-type .type-value")
            notice_type = await type_elem.inner_text() if type_elem else "N/A"
            
            # Create result
            result = {
                'date': issue_date,
                'country': country.strip(),
                'project_name': project_name.strip(),
                'sector': sector.strip(),
                'notice_type': notice_type.strip(),
                'download_link': download_link,
                'url': self.base_url  # Base URL since we don't have detail pages
            }
            
            logger.info(f"Found matching procurement: {project_name}")
            return result
            
        except Exception as e:
            logger.error(f"Error processing row: {str(e)}")
            return None

    async def extract_table_data(self) -> List[Dict]:
        """Extract data from the current page's table"""
        try:
            # Wait for the table to be loaded
            await self.page.wait_for_selector(".table-body", state="visible")
            
            # Get all rows from the table
            rows = await self.page.query_selector_all(".table-row")
            logger.info(f"Found {len(rows)} rows in the table")
            
            # Process each row
            results = []
            for row in rows:
                result = await self.process_row(row)
                if result:
                    results.append(result)
            
            logger.info(f"Found {len(results)} matching rows")
            return results
            
        except Exception as e:
            logger.error(f"Error extracting table data: {str(e)}")
            return []

    async def check_page_for_today(self) -> bool:
        """Check if current page has any rows with today's date"""
        try:
            date_elems = await self.page.query_selector_all(".table-col.table-date .s2")
            
            for elem in date_elems:
                date_text = await elem.inner_text()
                date_text = date_text.strip()
                
                if date_text == self.today:
                    return True
            
            return False
        except Exception as e:
            logger.error(f"Error checking for today's date: {str(e)}")
            return False

    async def check_next_page(self) -> bool:
        """Check if there's a next page and navigate to it if it exists"""
        try:
            # Look for pagination navigation
            next_button = await self.page.query_selector("a.pagenav-next")
            
            if next_button:
                # Check if it's not disabled
                is_disabled = await next_button.get_attribute("class")
                if is_disabled and "disabled" in is_disabled:
                    logger.info("Next button is disabled, no more pages")
                    return False
                
                # Click to navigate to next page
                await next_button.click()
                await self.page.wait_for_load_state("networkidle")
                # Wait for the table to load on the next page
                await self.page.wait_for_selector(".table-body", state="visible")
                logger.info("Navigated to next page")
                return True
            else:
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
            
            while True:
                logger.info(f"Processing page {current_page}")
                
                # First check if this page has any matching dates
                has_matching_dates = await self.check_page_for_today()
                if not has_matching_dates:
                    logger.info(f"No matching dates found on page {current_page}, stopping search")
                    break

                # Extract data from current page
                page_data = await self.extract_table_data()
                self.results.extend(page_data)
                
                logger.info(f"Found {len(page_data)} matching rows on page {current_page}")
                
                # Check and navigate to next page if exists
                has_next = await self.check_next_page()
                if not has_next:
                    logger.info("No more pages to process")
                    break
                    
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