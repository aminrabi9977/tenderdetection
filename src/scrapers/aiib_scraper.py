

# src/scrapers/aiib_scraper.py

from playwright.async_api import async_playwright
import asyncio
from datetime import datetime, timedelta
import pandas as pd
import logging
from typing import List, Dict, Optional, Tuple
from src.scrapers.base_scraper import BaseScraper
from src.utils.date_utils import normalize_date, format_date_for_site

logger = logging.getLogger(__name__)

# Increase concurrent page processing for AIIB
MAX_CONCURRENT_PAGES = 10

class AIIBScraper(BaseScraper):
    def __init__(self, base_url: str):
        super().__init__(base_url)
        # Format today's date and date from a week ago
        self.today = datetime.now()
        self.today_str = self.today.strftime("%b %d, %Y")  # Format used by AIIB: "Mar 27, 2025"
        
        # Calculate the date from a week ago
        self.week_ago = self.today - timedelta(days=7)
        self.week_ago_str = self.week_ago.strftime("%b %d, %Y")
        
        self.results = []
        # Extract the base domain from the URL
        self.domain = '/'.join(base_url.split('/')[:3])  # Get "https://www.aiib.org"
        
        logger.info(f"AIIB scraper initialized with base domain: {self.domain}")
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
            # AIIB format can be like "Mar 27, 2025" or "April 18, 2025"
            # Handle both abbreviated and full month names
            try:
                date_obj = datetime.strptime(date_text.strip(), "%b %d, %Y")
            except ValueError:
                date_obj = datetime.strptime(date_text.strip(), "%B %d, %Y")
            
            return self.week_ago <= date_obj <= self.today
        except ValueError as e:
            logger.warning(f"Could not parse date: {date_text}. Error: {str(e)}")
            return False

    async def is_date_older_than_range(self, date_text: str) -> bool:
        """Check if a date is older than our range"""
        try:
            # Try to parse with both abbreviated and full month names
            try:
                date_obj = datetime.strptime(date_text.strip(), "%b %d, %Y")
            except ValueError:
                date_obj = datetime.strptime(date_text.strip(), "%B %d, %Y")
            
            return date_obj < self.week_ago
        except ValueError:
            return False

    async def process_row(self, row):
        """Process a single row from the table"""
        try:
            # Extract the issue date from the row
            date_elem = await row.query_selector(".table-col.table-date .s2")
            if not date_elem:
                return None
                
            issue_date = await date_elem.inner_text()
            issue_date = issue_date.strip()
            
            # Check if date is older than our range
            if await self.is_date_older_than_range(issue_date):
                logger.info(f"Found date {issue_date} older than our range, stopping search")
                return "STOP_SEARCH"
            
            # Check if date is in our range
            if not await self.is_date_in_range(issue_date):
                logger.info(f"Skipping row with issue date {issue_date} (outside range)")
                return None
            
            # Extract the member/country
            country_elem = await row.query_selector(".table-col.table-country .country-value")
            country = await country_elem.inner_text() if country_elem else "N/A"
            
            # Extract project/notice title
            title_elem = await row.query_selector(".table-col.table-project .title-value")
            title = await title_elem.inner_text() if title_elem else "N/A"
            
            # Extract the download link
            download_link_elem = await row.query_selector(".table-col.table-project a")
            download_link = await download_link_elem.get_attribute('href') if download_link_elem else None
            
            # Make the download link absolute if it's relative
            if download_link and download_link.startswith('/'):
                download_link = self.domain + download_link
            
            # Extract sector
            sector_elem = await row.query_selector(".table-col.table-energy .sector-value")
            sector = await sector_elem.inner_text() if sector_elem else "N/A"
            
            # Extract notice type
            type_elem = await row.query_selector(".table-col.table-type .type-value")
            notice_type = await type_elem.inner_text() if type_elem else "N/A"
            
            # Create result
            result = {
                'issue_date': issue_date,
                'country': country.strip(),
                'title': title.strip(),
                'sector': sector.strip(),
                'notice_type': notice_type.strip(),
                'download_link': download_link if download_link else "N/A"
            }
            
            logger.info(f"Found matching row: {title}")
            return result
            
        except Exception as e:
            logger.error(f"Error processing row: {str(e)}")
            return None

    async def process_rows_batch(self, rows) -> Tuple[List[Dict], bool]:
        """Process a batch of rows in parallel"""
        tasks = []
        for row in rows:
            tasks.append(self.process_row(row))
        
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

    async def extract_table_data(self) -> Tuple[List[Dict], bool]:
        """Extract opportunities from current page that match the date range"""
        try:
            # Get all rows from the table
            rows = await self.page.query_selector_all(".table-row")
            logger.info(f"Found {len(rows)} rows in the table")
            
            if not rows:
                return [], False
            
            # Process rows in parallel batches
            batch_size = MAX_CONCURRENT_PAGES
            all_results = []
            
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i+batch_size]
                logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} rows)")
                
                batch_results, should_stop = await self.process_rows_batch(batch)
                all_results.extend(batch_results)
                
                logger.info(f"Found {len(batch_results)} matching rows in this batch")
                
                if should_stop:
                    return all_results, True
            
            return all_results, False
            
        except Exception as e:
            logger.error(f"Error extracting table data: {str(e)}")
            return [], False

    async def check_next_page(self) -> bool:
        """Check if there's a next page and navigate to it if it exists"""
        try:
            # Check for next page link
            next_page_elem = await self.page.query_selector("a.next")
            if next_page_elem:
                await next_page_elem.click()
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
            
            # Wait for the table to be visible
            await self.page.wait_for_selector(".table-body", state="visible")
            logger.info("Table found")
            
            current_page = 1
            should_stop = False
            
            while not should_stop:
                logger.info(f"Processing page {current_page}")
                
                # Extract data from current page
                page_data, should_stop_search = await self.extract_table_data()
                self.results.extend(page_data)
                
                logger.info(f"Found {len(page_data)} matching rows on page {current_page}")
                
                # If we should stop searching (found dates older than our range), break
                if should_stop_search:
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
                logger.info(f"Total rows collected: {len(df)}")
            else:
                logger.info("No matching rows found")
                
            return df
            
        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}")
            raise
        finally:
            await self.close_browser()