import requests
import pandas as pd
import logging
from datetime import datetime
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def download_building_permits(output_file='kitchener_permits.csv'):
    """
    Download building permit data from City of Kitchener's Open Data Portal
    and save it as a CSV file.
    
    Args:
        output_file (str): Name of the output CSV file
    
    Returns:
        bool: True if successful, False otherwise
    """
    # API endpoint URL
    url = "https://open-kitchenergis.opendata.arcgis.com/datasets/building-permits/explore"
    api_url = "https://opendata.arcgis.com/datasets/7d567a3bf2a24f39b96cd8c9f811a8ae_0.csv"
    
    try:
        # Log start of download
        logger.info(f"Starting download from {api_url}")
        
        # Make the request with a timeout
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        # Create a backup of existing file if it exists
        if os.path.exists(output_file):
            backup_file = f"{output_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            os.rename(output_file, backup_file)
            logger.info(f"Created backup of existing file: {backup_file}")
        
        # Save the content directly to a file
        with open(output_file, 'wb') as f:
            f.write(response.content)
        
        # Verify the downloaded file
        df = pd.read_csv(output_file)
        num_records = len(df)
        logger.info(f"Successfully downloaded {num_records} building permit records")
        logger.info(f"Data saved to {output_file}")
        
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading data: {str(e)}")
        return False
    except pd.errors.EmptyDataError:
        logger.error("Downloaded file is empty")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return False

if __name__ == "__main__":
    try:
        success = download_building_permits()
        if success:
            logger.info("Script completed successfully")
        else:
            logger.error("Script failed to complete successfully")
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.error(f"Script failed with error: {str(e)}") 