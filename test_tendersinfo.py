# test_tendersinfo.py

import asyncio
import logging
from datetime import datetime
from playwright.async_api import async_playwright
import pandas as pd

# Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TendersInfoScraperTest:
    def __init__(self, url: str, target_date: str):
        self.url = url
        self.target_date = target_date
        self.results = []

    async def extract_tender_details(self, page, detail_url: str):
        """Extract details from a specific tender page"""
        try:
            # Create a new page for the detail view
            detail_page = await page.context.new_page()
            await detail_page.goto(detail_url)
            await detail_page.wait_for_load_state("networkidle")
            
            # Wait for the form to load
            await detail_page.wait_for_selector(".form-horizontal", state="visible")
            
            # Extract all required fields
            tender_data = {}
            
            # Extract Tender TI Ref No
            ref_no_elem = await detail_page.query_selector("label:text('Tender TI Ref No') + div p")
            if ref_no_elem:
                tender_data['ref_no'] = await ref_no_elem.inner_text()
            
            # Extract Tender Date
            date_elem = await detail_page.query_selector("label:text('Tender Date') + div p")
            if date_elem:
                tender_data['date'] = await date_elem.inner_text()
            
            # Extract Tender Description
            desc_elem = await detail_page.query_selector("label:text('Tender Description') + div p")
            if desc_elem:
                tender_data['description'] = await desc_elem.inner_text()
            
            # Extract Tender Deadline
            deadline_elem = await detail_page.query_selector("label:text('Tender Deadline') + div p")
            if deadline_elem:
                tender_data['deadline'] = await deadline_elem.inner_text()
            
            # Extract Tender Project Location
            location_elem = await detail_page.query_selector("label:text('Tender Project Location') + div p")
            if location_elem:
                tender_data['location'] = await location_elem.inner_text()
            
            # Extract Tender Sector
            sector_elem = await detail_page.query_selector("label:text('Tender Sector') + div p")
            if sector_elem:
                tender_data['sector'] = await sector_elem.inner_text()
            
            # Extract Tender CPV
            cpv_elem = await detail_page.query_selector("label:text('Tender CPV') + div p")
            if cpv_elem:
                tender_data['cpv'] = await cpv_elem.inner_text()
            
            # Extract Tender Estimated Cost
            cost_elem = await detail_page.query_selector("label:text('Tender Estimated Cost') + div p")
            if cost_elem:
                tender_data['estimated_cost'] = await cost_elem.inner_text()
            
            # Extract Tender Document Type
            doc_type_elem = await detail_page.query_selector("label:text('Tender Document Type') + div p")
            if doc_type_elem:
                tender_data['document_type'] = await doc_type_elem.inner_text()
            
            # Add original URL
            tender_data['url'] = detail_url
            
            # Close the detail page
            await detail_page.close()
            
            return tender_data
            
        except Exception as e:
            logger.error(f"Error extracting tender details from {detail_url}: {str(e)}")
            try:
                await detail_page.close()
            except:
                pass
            return None

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

                # Use simpler JS to find global tenders panel
                await page.wait_for_selector(".panel-heading:has-text('Global Tenders')")
                logger.info("Global tenders section found")
                
                # Get all tender boxes from only the global tenders section
                tender_links = await page.evaluate("""
                    () => {
                        // Find Global Tenders panel using text content
                        const headings = Array.from(document.querySelectorAll('.panel-heading'));
                        const globalHeading = headings.find(el => el.textContent.includes('Global Tenders'));
                        
                        if (!globalHeading) return [];
                        
                        const globalPanel = globalHeading.closest('.panel');
                        if (!globalPanel) return [];
                        
                        // Find all tender links within this panel
                        const links = globalPanel.querySelectorAll('a.tenderBrief');
                        
                        return Array.from(links).map(link => {
                            return {
                                title: link.textContent.trim(),
                                href: link.href
                            };
                        });
                    }
                """)
                
                logger.info(f"Found {len(tender_links)} global tender links")
                
                # Process each tender
                for i, link_data in enumerate(tender_links):
                    try:
                        title = link_data['title']
                        href = link_data['href']
                        
                        if not href:
                            continue
                        
                        logger.info(f"Processing global tender {i+1}: {title[:50]}...")
                        
                        # Get the detail page
                        detail_data = await self.extract_tender_details(page, href)
                        
                        if detail_data and 'date' in detail_data:
                            # Check if the tender date matches target date
                            tender_date = detail_data['date'].strip()
                            logger.info(f"Tender date: {tender_date}, Target: {self.target_date}")
                            
                            if tender_date == self.target_date:
                                logger.info(f"Found matching tender: {title}")
                                self.results.append(detail_data)
                    
                    except Exception as e:
                        logger.error(f"Error processing tender {i+1}: {str(e)}")
                        continue
                
                # Convert results to DataFrame
                df = pd.DataFrame(self.results)
                return df

            except Exception as e:
                logger.error(f"Error during scraping: {str(e)}")
                raise
            finally:
                await browser.close()

async def test_scraper():
    url = "https://www.tendersinfo.com/"
    
    # Your target date (format: "28 Feb 2025") - TendersInfo format
    target_date = "28 Feb 2024"
    
    try:
        scraper = TendersInfoScraperTest(url, target_date)
        df = await scraper.scrape_data()
        
        if not df.empty:
            output_file = f"tendersinfo_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(output_file, index=False)
            logger.info(f"Data saved to {output_file}")
            logger.info(f"Total tenders found: {len(df)}")
        else:
            logger.info(f"No data found for date: {target_date}")
            
    except Exception as e:
        logger.error(f"Error during scraping: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_scraper())