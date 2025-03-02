# src/utils/date_utils.py

from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def normalize_date(date_str: str) -> str:
    """Normalize date string to match required format"""
    try:
        # Handle various input date formats
        date_str = date_str.strip()
        
        # First try the "February 24, 2025" format (World Bank)
        try:
            date_obj = datetime.strptime(date_str, "%B %d, %Y")
            return date_obj.strftime("%B %d, %Y")
        except ValueError:
            pass
            
        # Try "DD MMM YYYY" format (EBRD format: 20 Feb 2025)
        try:
            date_obj = datetime.strptime(date_str, "%d %b %Y")
            return date_obj.strftime("%d %b %Y")
        except ValueError:
            pass
            
        # Try "DD Month YYYY" format (ISDB format: 28 December 2022)
        try:
            date_obj = datetime.strptime(date_str, "%d %B %Y")
            return date_obj.strftime("%d %B %Y")
        except ValueError:
            pass
            
        # Try mm/dd/yyyy format
        try:
            date_obj = datetime.strptime(date_str, "%m/%d/%Y")
            return date_obj.strftime("%B %d, %Y")  # Return in World Bank format by default
        except ValueError:
            pass
            
        # Try yyyy-mm-dd format
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            return date_obj.strftime("%B %d, %Y")  # Return in World Bank format by default
        except ValueError:
            # Add more formats if needed
            raise ValueError(f"Unsupported date format: {date_str}")
    
    except Exception as e:
        logger.error(f"Error parsing date {date_str}: {str(e)}")
        return date_str

def format_date_for_site(date_obj, site_type):
    """Format a date object for the specific site format required"""
    if site_type.lower() == "world_bank":
        return date_obj.strftime("%B %d, %Y")  # February 24, 2025
    elif site_type.lower() == "ebrd":
        return date_obj.strftime("%d %b %Y")   # 24 Feb 2025
    elif site_type.lower() == "tenders_info":
        return date_obj.strftime("%d %b %Y")   # 28 Feb 2025 (same format as EBRD)
    elif site_type.lower() == "isdb":
        return date_obj.strftime("%d %B %Y")   # 28 December 2022
    else:
        # Default format
        return date_obj.strftime("%Y-%m-%d")   # 2025-02-24