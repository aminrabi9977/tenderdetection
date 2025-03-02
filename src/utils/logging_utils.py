import logging
from pathlib import Path
from src.config.settings import LOG_DIR, LOG_FILE, LOG_FORMAT

def setup_logging():
    """Set up logging configuration"""
    # Create log directory if it doesn't exist
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()
        ]
    )