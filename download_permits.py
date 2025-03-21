import requests
import logging
from datetime import datetime
import os
import csv
import io

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
    try:
        # Log start of download
        logger.info("Starting download of building permit data")
        
        # Make the request with a timeout
        response = requests.get(
            "https://open-kitchenergis.opendata.arcgis.com/datasets/KitchenerGIS::building-permits.csv",
            timeout=30
        )
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
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)  # Read headers
                row_count = sum(1 for row in reader)  # Count rows
                
            if row_count == 0:
                logger.error("Downloaded file is empty")
                return False
                
            logger.info(f"Successfully downloaded {row_count} building permit records")
            logger.info(f"Data saved to {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error reading CSV file: {str(e)}")
            return False
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading data: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return False

if __name__ == "__main__":
    try:
        success = download_building_permits()
        if success:
            logger.info("Script completed successfully")
            # Now run the cleaning script
            import clean_permits
            clean_success = clean_permits.clean_building_permits()
            if clean_success:
                logger.info("Data cleaning completed successfully")
            else:
                logger.error("Data cleaning failed")
        else:
            logger.error("Script failed to complete successfully")
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.error(f"Script failed with error: {str(e)}") 