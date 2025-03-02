# src/utils/date_utils.py

from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def normalize_date(date_str: str) -> str:
    """Normalize date string to match required format (February 20, 2025)"""
    try:
        # Handle various input date formats
        # First try the expected format
        try:
            date_obj = datetime.strptime(date_str.strip(), "%B %d, %Y")
        except ValueError:
            # Try other possible formats
            try:
                date_obj = datetime.strptime(date_str.strip(), "%m/%d/%Y")
            except ValueError:
                date_obj = datetime.strptime(date_str.strip(), "%Y-%m-%d")
        
        # Convert to required format
        return date_obj.strftime("%B %d, %Y")
    except Exception as e:
        logger.error(f"Error parsing date {date_str}: {str(e)}")
        return date_str