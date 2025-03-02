# # src/scrapers/world_bank_scraper.py

# from playwright.async_api import async_playwright
# import asyncio
# from datetime import datetime
# import pandas as pd
# import logging
# from typing import List, Dict
# from bs4 import BeautifulSoup
# from src.scrapers.base_scraper import BaseScraper
# from src.utils.date_utils import normalize_date
# from pathlib import Path

# logger = logging.getLogger(__name__)

# class WorldBankScraper(BaseScraper):
#     def __init__(self, base_url: str):
#         super().__init__(base_url)
#         # Format today's date as "February 24, 2025"
#         self.today = datetime.now().strftime("%B %d, %Y")
#         self.results = []

#     async def init_browser(self):
#         """Initialize browser instance"""
#         self.playwright = await async_playwright().start()
#         self.browser = await self.playwright.chromium.launch(headless=False)  # Set to False to see what's happening
#         self.context = await self.browser.new_context()
#         self.context.set_default_timeout(60000)  # Set timeout to 60 seconds
#         self.page = await self.context.new_page()

#     async def close_browser(self):
#         """Close browser instance"""
#         await self.context.close()
#         await self.browser.close()
#         await self.playwright.stop()

#     async def extract_table_data(self) -> List[Dict]:
#         """Extract data from the current page's table using Beautiful Soup"""
#         html_content = await self.page.content()
#         soup = BeautifulSoup(html_content, 'html.parser')
        
#         table_data = []
#         tbody = soup.find('tbody')
#         if not tbody:
#             return table_data
            
#         rows = tbody.find_all('tr')
        
#         for row in rows:
#             cells = row.find_all('td')
#             if not cells:
#                 continue

#             # Extract publish date from the appropriate column (Published Date)
#             try:
#                 publish_date_cell = cells[5].get_text().strip()  # Index 5 for Published Date
#                 publish_date = normalize_date(publish_date_cell)
                
#                 # Only process rows matching today's date
#                 if publish_date == self.today:
#                     description = cells[0].get_text().strip()
#                     country = cells[1].get_text().strip()
#                     project_title = cells[2].get_text().strip()
#                     notice_type = cells[3].get_text().strip()
#                     language = cells[4].get_text().strip()

#                     row_data = {
#                         'description': description,
#                         'country': country,
#                         'project_title': project_title,
#                         'notice_type': notice_type,
#                         'language': language,
#                         'publish_date': publish_date
#                     }
                    
#                     # Extract links if present
#                     description_link = cells[0].find('a')
#                     project_link = cells[2].find('a')
                    
#                     if description_link:
#                         row_data['description_link'] = description_link.get('href', '')
#                     if project_link:
#                         row_data['project_link'] = project_link.get('href', '')
                    
#                     table_data.append(row_data)
#                     logger.info(f"Found matching row: {description[:50]}...")
                
#             except Exception as e:
#                 logger.error(f"Error processing row: {str(e)}")
#                 continue
        
#         return table_data

#     async def has_today_dates(self) -> bool:
#         """Check if current page has any rows with today's date"""
#         html_content = await self.page.content()
#         soup = BeautifulSoup(html_content, 'html.parser')
        
#         tbody = soup.find('tbody')
#         if not tbody:
#             return False
            
#         rows = tbody.find_all('tr')
#         for row in rows:
#             cells = row.find_all('td')
#             if cells:
#                 try:
#                     publish_date = normalize_date(cells[5].get_text().strip())  # Index 5 for Published Date
#                     if publish_date == self.today:
#                         return True
#                 except Exception:
#                     continue
#         return False

#     async def check_next_page(self) -> bool:
#         """Check if there's a next page and click if exists"""
#         try:
#             # Find the next page button (single right arrow)
#             next_button = await self.page.query_selector("li:not(.disabled) a i.fa.fa-angle-right:not(.fa-angle-right + i)")
            
#             if next_button:
#                 # Click the parent <a> tag
#                 await next_button.evaluate("el => el.closest('a').click()")
#                 await self.page.wait_for_load_state("networkidle")
                
#                 # Check if new page has relevant dates before continuing
#                 if await self.has_today_dates():
#                     logger.info("Found matching dates on next page")
#                     return True
#                 else:
#                     logger.info("No matching dates found on next page")
                
#             return False
            
#         except Exception as e:
#             logger.error(f"Error checking next page: {str(e)}")
#             return False

#     async def scrape_data(self):
#         """Main scraping function"""
#         try:
#             await self.init_browser()
#             await self.page.goto(self.base_url)
#             await self.page.wait_for_load_state("networkidle")
            
#             page_number = 1
#             while True:
#                 logger.info(f"Processing page {page_number}")
                
#                 # Extract data from current page
#                 page_data = await self.extract_table_data()
#                 self.results.extend(page_data)
                
#                 logger.info(f"Found {len(page_data)} matching rows on page {page_number}")
                
#                 # Check and navigate to next page if exists
#                 has_next = await self.check_next_page()
#                 if not has_next:
#                     logger.info("No more pages to process")
#                     break
                    
#                 page_number += 1
            
#             # Convert results to DataFrame
#             df = pd.DataFrame(self.results)
            
#             if not df.empty:
#                 logger.info(f"Total rows collected: {len(df)}")
#             else:
#                 logger.info("No matching rows found")
                
#             return df
            
#         except Exception as e:
#             logger.error(f"Error during scraping: {str(e)}")
#             raise
#         finally:
#             await self.close_browser()

# ------------------------------------------------------------------------------------------
# src/scrapers/world_bank_scraper.py

# from playwright.async_api import async_playwright
# import asyncio
# from datetime import datetime
# import pandas as pd
# import logging
# from typing import List, Dict, Optional
# from bs4 import BeautifulSoup
# from src.scrapers.base_scraper import BaseScraper
# from src.utils.date_utils import normalize_date

# logger = logging.getLogger(__name__)

# class WorldBankScraper(BaseScraper):
#     def __init__(self, base_url: str, target_date: Optional[str] = None):
#         super().__init__(base_url)
#         # If target_date is provided, use it; otherwise use today's date
#         if target_date:
#             self.target_date = normalize_date(target_date)
#         else:
#             self.target_date = datetime.now().strftime("%B %d, %Y")
#         self.results = []

#     async def init_browser(self):
#         """Initialize browser instance"""
#         self.playwright = await async_playwright().start()
#         self.browser = await self.playwright.chromium.launch(headless=False)
#         self.context = await self.browser.new_context()
#         self.context.set_default_timeout(60000)
#         self.page = await self.context.new_page()

#     async def close_browser(self):
#         """Close browser instance"""
#         await self.context.close()
#         await self.browser.close()
#         await self.playwright.stop()

#     async def extract_table_data(self) -> List[Dict]:
#         """Extract data from the current page's table using Beautiful Soup"""
#         html_content = await self.page.content()
#         soup = BeautifulSoup(html_content, 'html.parser')
        
#         table_data = []
#         tbody = soup.find('tbody')
#         if not tbody:
#             return table_data
            
#         rows = tbody.find_all('tr')
        
#         for row in rows:
#             cells = row.find_all('td')
#             if not cells:
#                 continue

#             try:
#                 publish_date_cell = cells[5].get_text().strip()
#                 publish_date = normalize_date(publish_date_cell)
                
#                 # Compare with target_date instead of today
#                 if publish_date == self.target_date:
#                     description = cells[0].get_text().strip()
#                     country = cells[1].get_text().strip()
#                     project_title = cells[2].get_text().strip()
#                     notice_type = cells[3].get_text().strip()
#                     language = cells[4].get_text().strip()

#                     row_data = {
#                         'description': description,
#                         'country': country,
#                         'project_title': project_title,
#                         'notice_type': notice_type,
#                         'language': language,
#                         'publish_date': publish_date
#                     }
                    
#                     description_link = cells[0].find('a')
#                     project_link = cells[2].find('a')
                    
#                     if description_link:
#                         row_data['description_link'] = description_link.get('href', '')
#                     if project_link:
#                         row_data['project_link'] = project_link.get('href', '')
                    
#                     table_data.append(row_data)
#                     logger.info(f"Found matching row: {description[:50]}...")
                
#             except Exception as e:
#                 logger.error(f"Error processing row: {str(e)}")
#                 continue
        
#         return table_data

#     async def has_target_dates(self) -> bool:
#         """Check if current page has any rows with target date"""
#         html_content = await self.page.content()
#         soup = BeautifulSoup(html_content, 'html.parser')
        
#         tbody = soup.find('tbody')
#         if not tbody:
#             return False
            
#         rows = tbody.find_all('tr')
#         for row in rows:
#             cells = row.find_all('td')
#             if cells:
#                 try:
#                     publish_date = normalize_date(cells[5].get_text().strip())
#                     if publish_date == self.target_date:
#                         return True
#                 except Exception:
#                     continue
#         return False

#     async def check_next_page(self) -> bool:
#         """Check if there's a next page and click if exists"""
#         try:
#             next_button = await self.page.query_selector("li:not(.disabled) a i.fa.fa-angle-right:not(.fa-angle-right + i)")
            
#             if next_button:
#                 await next_button.evaluate("el => el.closest('a').click()")
#                 await self.page.wait_for_load_state("networkidle")
                
#                 if await self.has_target_dates():
#                     logger.info("Found matching dates on next page")
#                     return True
#                 else:
#                     logger.info("No matching dates found on next page")
                
#             return False
            
#         except Exception as e:
#             logger.error(f"Error checking next page: {str(e)}")
#             return False

#     async def scrape_data(self):
#         """Main scraping function"""
#         try:
#             await self.init_browser()
#             await self.page.goto(self.base_url)
#             await self.page.wait_for_load_state("networkidle")
            
#             page_number = 1
#             while True:
#                 logger.info(f"Processing page {page_number}")
                
#                 page_data = await self.extract_table_data()
#                 self.results.extend(page_data)
                
#                 logger.info(f"Found {len(page_data)} matching rows on page {page_number}")
                
#                 has_next = await self.check_next_page()
#                 if not has_next:
#                     logger.info("No more pages to process")
#                     break
                    
#                 page_number += 1
            
#             df = pd.DataFrame(self.results)
            
#             if not df.empty:
#                 logger.info(f"Total rows collected: {len(df)}")
#             else:
#                 logger.info("No matching rows found")
                
#             return df
            
#         except Exception as e:
#             logger.error(f"Error during scraping: {str(e)}")
#             raise
#         finally:
#             await self.close_browser()
# ---------------------------------------------------------------------------------------------------

# src/scrapers/world_bank_scraper.py

from playwright.async_api import async_playwright
import asyncio
from datetime import datetime
import pandas as pd
import logging
from typing import List, Dict
from bs4 import BeautifulSoup
from src.scrapers.base_scraper import BaseScraper
from src.utils.date_utils import normalize_date

logger = logging.getLogger(__name__)

class WorldBankScraper(BaseScraper):
    def __init__(self, base_url: str):
        super().__init__(base_url)
        # Format today's date as "February 24, 2025"
        self.today = datetime.now().strftime("%B %d, %Y")
        self.results = []

    async def init_browser(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=False)
        self.context = await self.browser.new_context()
        self.context.set_default_timeout(60000)
        self.page = await self.context.new_page()

    async def close_browser(self):
        await self.context.close()
        await self.browser.close()
        await self.playwright.stop()

    async def check_page_for_today(self) -> bool:
        """Check if current page has any rows with today's date"""
        rows = await self.page.query_selector_all("table.project-opt-table tbody tr")
        
        for row in rows:
            cells = await row.query_selector_all("td")
            if cells and len(cells) >= 6:
                date_text = await cells[5].inner_text()
                if date_text.strip() == self.today:
                    return True
        return False

    async def extract_table_data(self) -> List[Dict]:
        """Extract matching rows from the current page"""
        rows = await self.page.query_selector_all("table.project-opt-table tbody tr")
        table_data = []
        
        for row in rows:
            cells = await row.query_selector_all("td")
            if not cells or len(cells) < 6:
                continue

            date_text = await cells[5].inner_text()
            date_text = date_text.strip()
            
            if date_text == self.today:
                try:
                    row_data = {}
                    
                    # Extract description and its link
                    desc_cell = cells[0]
                    desc_link = await desc_cell.query_selector("a")
                    if desc_link:
                        row_data['description'] = await desc_link.inner_text()
                        row_data['description_link'] = await desc_link.get_attribute('href')
                    else:
                        row_data['description'] = await desc_cell.inner_text()

                    # Get rest of the data
                    row_data.update({
                        'country': await cells[1].inner_text(),
                        'project_title': await cells[2].inner_text(),
                        'notice_type': await cells[3].inner_text(),
                        'language': await cells[4].inner_text(),
                        'publish_date': date_text
                    })

                    # Get project link if exists
                    proj_link = await cells[2].query_selector("a")
                    if proj_link:
                        row_data['project_link'] = await proj_link.get_attribute('href')

                    table_data.append(row_data)
                    logger.info(f"Found matching row: {row_data['description'][:50]}...")
                    
                except Exception as e:
                    logger.error(f"Error processing row: {str(e)}")
                    continue
        
        return table_data

    async def check_next_page(self) -> bool:
        """Check if there's a next page and click if exists"""
        try:
            next_button = await self.page.query_selector("li:not(.disabled) a i.fa.fa-angle-right:not(.fa-angle-right + i)")
            
            if next_button:
                await next_button.evaluate("el => el.closest('a').click()")
                await self.page.wait_for_load_state("networkidle")
                
                # Check if new page has relevant dates before continuing
                if await self.check_page_for_today():
                    logger.info("Found matching dates on next page")
                    return True
                else:
                    logger.info("No matching dates found on next page")
                    return False
                
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
            
            # Wait for table to be visible
            await self.page.wait_for_selector("table.project-opt-table", state="visible")
            
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