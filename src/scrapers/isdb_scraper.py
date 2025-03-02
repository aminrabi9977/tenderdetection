# src/scrapers/isdb_scraper.py

from playwright.async_api import async_playwright
import asyncio
from datetime import datetime
import pandas as pd
import logging
from typing import List, Dict, Optional
from src.scrapers.base_scraper import BaseScraper
from src.utils.date_utils import normalize_date, format_date_for_site

logger = logging.getLogger(__name__)

# Number of concurrent detail page processing
MAX_CONCURRENT_PAGES = 5

class ISDBScraper(BaseScraper):
    def __init__(self, base_url: str):
        super().__init__(base_url)
        # Format today's date as "28 December 2022" (format used by ISDB)
        self.today = datetime.now().strftime("%d %B %Y")
        self.results = []
        self.semaphore = None  # Will be initialized in scrape_data

    async def init_browser(self):
        """Initialize browser instance"""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=False)
        self.context = await self.browser.new_context()
        self.context.set_default_timeout(60000)
        self.page = await self.context.new_page()
        # Create a semaphore to limit concurrent pages
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)

    async def close_browser(self):
        """Close browser instance"""
        await self.context.close()
        await self.browser.close()
        await self.playwright.stop()

    async def extract_tender_details(self, detail_url: str) -> Optional[Dict]:
        """Extract details from a specific tender page"""
        detail_page = None
        try:
            # Use semaphore to limit concurrent pages
            async with self.semaphore:
                # Create a new page for the detail view
                detail_page = await self.context.new_page()
                await detail_page.goto(detail_url)
                await detail_page.wait_for_load_state("networkidle")
                
                # Wait for the details to load
                await detail_page.wait_for_selector(".details", state="visible")
                
                # Extract all required fields
                tender_data = {}
                
                # Extract Notice Type
                notice_type_elem = await detail_page.query_selector(".field--name-field-notice-type .field--item")
                if notice_type_elem:
                    tender_data['notice_type'] = await notice_type_elem.inner_text()
                
                # Extract Issue Date
                issue_date_elem = await detail_page.query_selector(".field--name-field-issue-date .field--item")
                if issue_date_elem:
                    tender_data['issue_date'] = await issue_date_elem.inner_text()
                
                # Extract Last date of submission
                submission_date_elem = await detail_page.query_selector(".field--name-field-close-date .field--item")
                if submission_date_elem:
                    tender_data['submission_date'] = await submission_date_elem.inner_text()
                
                # Extract Tender Type
                tender_type_elem = await detail_page.query_selector(".field--name-field-tender-type .field--item")
                if tender_type_elem:
                    tender_data['tender_type'] = await tender_type_elem.inner_text()
                
                # Extract Project code
                project_code_elem = await detail_page.query_selector(".field--name-field-project-code .field--item")
                if project_code_elem:
                    tender_data['project_code'] = await project_code_elem.inner_text()
                
                # Extract Project title
                project_title_elem = await detail_page.query_selector(".field--name-field-project-title .field--item")
                if project_title_elem:
                    tender_data['project_title'] = await project_title_elem.inner_text()
                
                # Extract Email
                email_elem = await detail_page.query_selector(".field--name-field-email .field--item")
                if email_elem:
                    tender_data['email'] = await email_elem.inner_text()
                
                # Extract Document link (if any)
                document_link_elem = await detail_page.query_selector(".field--name-field-documents .file-link a")
                if document_link_elem:
                    tender_data['document_link'] = await document_link_elem.get_attribute('href')
                
                # Add original URL
                tender_data['url'] = detail_url
                
                # Check if the tender matches our target date
                if 'issue_date' in tender_data:
                    issue_date = tender_data['issue_date'].strip()
                    logger.debug(f"Issue date: {issue_date}, Today: {self.today}")
                    
                    if issue_date == self.today:
                        logger.info(f"Found matching tender with issue date: {issue_date}")
                        return tender_data
                
                return None
                
        except Exception as e:
            logger.error(f"Error extracting tender details from {detail_url}: {str(e)}")
            return None
        finally:
            if detail_page:
                await detail_page.close()

    async def process_tender_batch(self, urls):
        """Process a batch of tender URLs in parallel"""
        tasks = []
        for url in urls:
            # Make sure we have a full URL
            if url.startswith('/'):
                # Convert relative URL to absolute
                base_url = self.base_url.split('/project-procurement')[0] 
                url = base_url + url
            
            tasks.append(self.extract_tender_details(url))
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        
        # Filter out None results
        return [r for r in results if r]

    async def extract_table_data(self) -> List[Dict]:
        """Required by BaseScraper - collect all tender URLs from current page"""
        try:
            # Wait for the tenders container to be visible
            await self.page.wait_for_selector("[data-index-view='tenders_listing']", state="visible")
            
            # Get all article elements
            tender_articles = await self.page.query_selector_all("[data-index-view='tenders_listing'] article")
            
            # Extract tender URLs
            urls = []
            for article in tender_articles:
                link = await article.query_selector(".field-title a")
                if link:
                    href = await link.get_attribute('href')
                    if href:
                        urls.append(href)
            
            logger.info(f"Found {len(urls)} tender URLs on current page")
            return urls
            
        except Exception as e:
            logger.error(f"Error extracting tender URLs: {str(e)}")
            return []

    async def check_next_page(self) -> bool:
        """Check if there's a next page and navigate to it if it exists"""
        try:
            # Look for a "next page" link or button
            next_button = await self.page.query_selector("li.pager__item--next a")
            
            if next_button:
                # Click to navigate to next page
                await next_button.click()
                await self.page.wait_for_load_state("networkidle")
                await self.page.wait_for_selector("[data-index-view='tenders_listing']", state="visible")
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
            
            # Collect all tender URLs across all pages
            all_tender_urls = []
            current_page = 1
            
            while True:
                logger.info(f"Collecting URLs from page {current_page}")
                
                # Extract URLs from current page
                page_urls = await self.extract_table_data()
                all_tender_urls.extend(page_urls)
                
                # Check and navigate to next page if exists
                has_next = await self.check_next_page()
                if not has_next:
                    logger.info("No more pages to process")
                    break
                    
                current_page += 1
            
            logger.info(f"Total tender URLs collected: {len(all_tender_urls)}")
            
            # Process all tenders in parallel batches
            batch_size = MAX_CONCURRENT_PAGES * 2  # Process in larger batches
            for i in range(0, len(all_tender_urls), batch_size):
                batch = all_tender_urls[i:i+batch_size]
                logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} tenders)")
                
                batch_results = await self.process_tender_batch(batch)
                self.results.extend(batch_results)
                
                logger.info(f"Found {len(batch_results)} matching tenders in this batch")
            
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