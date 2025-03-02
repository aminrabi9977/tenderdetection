# src/config/settings.py

from pathlib import Path

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Data directories
DATA_DIR = BASE_DIR / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'

# Logging
LOG_DIR = BASE_DIR / 'logs'
LOG_FILE = LOG_DIR / 'scraper.log'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Output directory for scraped data
OUTPUT_DIR = PROCESSED_DATA_DIR

# Browser settings
HEADLESS = False  # Set to True for production
TIMEOUT = 60000  # milliseconds

# Site URLs
WORLD_BANK_URL = "https://projects.worldbank.org/en/projects-operations/procurement?srce=both"
EBRD_URL = "https://www.ebrd.com/work-with-us/procurement/notices.html"
TENDERS_INFO_URL = "https://www.tendersinfo.com/"
ISDB_URL = "https://www.isdb.org/project-procurement/tenders"