# main.py

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from src.scrapers.world_bank_scraper import WorldBankScraper
from src.scrapers.ebrd_scraper import EBRDScraper
from src.scrapers.tenders_info_scraper import TendersInfoScraper
from src.scrapers.isdb_scraper import ISDBScraper
from src.scrapers.afdb_scraper import AfDBScraper
from src.scrapers.aiib_scraper import AIIBScraper
from src.scrapers.afd_scraper import AFDScraper
from src.utils.logging_utils import setup_logging
from src.config.settings import WORLD_BANK_URL, EBRD_URL, TENDERS_INFO_URL, ISDB_URL, AFDB_URL, AIIB_URL, AFD_URL, OUTPUT_DIR

logger = logging.getLogger(__name__)

async def run_scraper(scraper_class, url, site_name):
    """Run a specific scraper"""
    try:
        # Initialize and run scraper
        scraper = scraper_class(url)
        df = await scraper.scrape_data()
        
        if not df.empty:
            # Save results to CSV
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = OUTPUT_DIR / f"{site_name}_data_{timestamp}.csv"
            df.to_csv(output_path, index=False)
            logger.info(f"{site_name} data saved to {output_path}")
            return len(df)
        else:
            logger.info(f"No data to save for {site_name}")
            return 0
            
    except Exception as e:
        logger.error(f"Error running {site_name} scraper: {str(e)}")
        return 0

async def main():
    """Main function to run all scrapers"""
    # Set up logging
    setup_logging()
    
    # Create output directory if it doesn't exist
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    # Run scrapers sequentially instead of concurrently to avoid browser issues
    total_rows = 0
    
    # Run World Bank scraper
    rows = await run_scraper(WorldBankScraper, WORLD_BANK_URL, "WorldBank")
    total_rows += rows
    
    # Run EBRD scraper
    rows = await run_scraper(EBRDScraper, EBRD_URL, "EBRD")
    total_rows += rows
    
    # Run TendersInfo scraper
    rows = await run_scraper(TendersInfoScraper, TENDERS_INFO_URL, "TendersInfo")
    total_rows += rows
    
    # Run ISDB scraper
    rows = await run_scraper(ISDBScraper, ISDB_URL, "ISDB")
    total_rows += rows
    
    # Run AfDB scraper
    rows = await run_scraper(AfDBScraper, AFDB_URL, "AfDB")
    total_rows += rows
    
    # Run AIIB scraper
    rows = await run_scraper(AIIBScraper, AIIB_URL, "AIIB")
    total_rows += rows
    
    # Run AFD scraper
    rows = await run_scraper(AFDScraper, AFD_URL, "AFD")
    total_rows += rows
    
    # Log summary of results
    logger.info(f"Scraping completed. Total rows extracted: {total_rows}")

if __name__ == "__main__":
    asyncio.run(main())