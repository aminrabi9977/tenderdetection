
# src/scrapers/ebrd_scraper.py

from playwright.async_api import async_playwright
import asyncio
from datetime import datetime, timedelta
import pandas as pd
import logging
from typing import List, Dict, Optional
from src.scrapers.base_scraper import BaseScraper
from src.utils.date_utils import normalize_date, format_date_for_site

logger = logging.getLogger(__name__)

# Number of concurrent detail page processing
MAX_CONCURRENT_PAGES = 5

class EBRDScraper(BaseScraper):
    def __init__(self, base_url: str):
        super().__init__(base_url)
        # Format today's date and date from a week ago
        self.today = datetime.now()
        self.today_str = self.today.strftime("%d %b %Y")  # Format used by EBRD: "20 Mar 2025"
        
        # Calculate the date from a week ago
        self.week_ago = self.today - timedelta(days=7)
        self.week_ago_str = self.week_ago.strftime("%d %b %Y")
        
        self.results = []
        # Extract the base domain from the URL
        self.domain = '/'.join(base_url.split('/')[:3])  # Get "https://www.ebrd.com"
        self.semaphore = None  # Will be initialized in scrape_data
        
        logger.info(f"EBRD scraper initialized with base domain: {self.domain}")
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
            # EBRD format is like "19 Nov 2024" or "20 Mar 2025"
            date_obj = datetime.strptime(date_text.strip(), "%d %b %Y")
            return self.week_ago <= date_obj <= self.today
        except ValueError:
            logger.warning(f"Could not parse date: {date_text}")
            return False

    async def extract_tender_details(self, detail_url: str) -> Optional[Dict]:
        """Extract details from a specific tender detail page"""
        detail_page = None
        try:
            # Use semaphore to limit concurrent pages
            async with self.semaphore:
                # Create a new page for the detail view
                detail_page = await self.context.new_page()
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

    async def process_tender_card(self, card):
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
            return await self.extract_tender_details(href)
            
        except Exception as e:
            logger.error(f"Error processing tender card: {str(e)}")
            return None

    async def process_tender_batch(self, cards):
        """Process a batch of tender cards in parallel"""
        tasks = []
        for card in cards:
            tasks.append(self.process_tender_card(card))
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        
        # Filter out None results
        return [r for r in results if r]

    async def extract_table_data(self) -> List[Dict]:
        """Extract matching tender data from the current page"""
        try:
            # Wait for cards to load
            await self.page.wait_for_selector(".search-result__result-card", state="visible")
            
            # Get all tender cards on the current page
            tender_cards = await self.page.query_selector_all(".search-result__result-card")
            logger.info(f"Found {len(tender_cards)} tender cards")
            
            if not tender_cards:
                return []
            
            # Process tender cards in parallel batches
            batch_size = MAX_CONCURRENT_PAGES
            all_results = []
            
            for i in range(0, len(tender_cards), batch_size):
                batch = tender_cards[i:i+batch_size]
                logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} tenders)")
                
                batch_results = await self.process_tender_batch(batch)
                all_results.extend(batch_results)
                
                logger.info(f"Found {len(batch_results)} matching tenders in this batch")
            
            return all_results
            
        except Exception as e:
            logger.error(f"Error extracting table data: {str(e)}")
            return []

    async def check_next_page(self) -> bool:
        """Check if there's a next page and navigate to it if it exists"""
        try:
            # Check for next page button
            next_page_button = await self.page.query_selector("a.pagination__button--next:not(.disabled)")
            if next_page_button:
                await next_page_button.click()
                await self.page.wait_for_load_state("networkidle")
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
                
                # Extract data from current page
                page_data = await self.extract_table_data()
                self.results.extend(page_data)
                
                logger.info(f"Found {len(page_data)} matching tenders on page {current_page}")
                
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