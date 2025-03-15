import pandas as pd
import logging
from datetime import datetime
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clean_building_permits(input_file='kitchener_permits.csv', 
                         output_file='cleaned_kitchener_permits.csv',
                         min_value=10000):
    """
    Clean and process building permit data from CSV file.
    
    Args:
        input_file (str): Path to input CSV file
        output_file (str): Path to output CSV file
        min_value (float): Minimum construction value threshold
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        logger.info(f"Reading data from {input_file}")
        
        # Read the CSV file
        df = pd.read_csv(input_file)
        initial_rows = len(df)
        logger.info(f"Initially loaded {initial_rows} records")

        # Create a copy of original column names for logging changes
        original_columns = df.columns.tolist()
        
        # Dictionary for column name mapping
        column_mapping = {
            'PERMIT_NO': 'permit_number',
            'PERMIT_TYPE': 'project_type',
            'STATUS': 'permit_status',
            'DESCRIPTION': 'permit_description',
            'ADDRESS': 'location',
            'WORK_TYPE': 'work_type',
            'SUB_WORK_TYPE': 'sub_work_type',
            'CONSTRUCTION_VALUE': 'construction_value',
            'PERMIT_DATE': 'submission_date',
            'ISSUE_DATE': 'issue_date',
            'FINAL_DATE': 'completion_date',
            'TOTAL_UNITS': 'total_units',
            'UNITS_CREATED': 'units_created',
            'LATITUDE': 'latitude',
            'LONGITUDE': 'longitude'
        }
        
        # Rename columns (case-insensitive mapping)
        df.columns = df.columns.str.upper()  # Convert all columns to uppercase for consistent mapping
        df = df.rename(columns=column_mapping)
        
        # Log column changes
        logger.info("Renamed columns for consistency")
        
        # Clean date columns
        date_columns = ['submission_date', 'issue_date', 'completion_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                logger.info(f"Converted {col} to datetime format")
        
        # Clean construction value (remove currency symbols and convert to float)
        if 'construction_value' in df.columns:
            df['construction_value'] = df['construction_value'].replace('[\$,]', '', regex=True).astype(float)
            logger.info("Cleaned construction value column")
        
        # Drop rows with missing values
        rows_before = len(df)
        df = df.dropna()
        rows_dropped = rows_before - len(df)
        logger.info(f"Dropped {rows_dropped} rows with missing values")
        
        # Filter by construction value
        rows_before = len(df)
        df = df[df['construction_value'] >= min_value]
        rows_filtered = rows_before - len(df)
        logger.info(f"Filtered out {rows_filtered} rows with construction value below ${min_value:,.2f}")
        
        # Sort by submission date (most recent first)
        df = df.sort_values('submission_date', ascending=False)
        
        # Calculate and log some basic statistics
        logger.info(f"Total permits after cleaning: {len(df)}")
        logger.info(f"Average construction value: ${df['construction_value'].mean():,.2f}")
        logger.info(f"Date range: {df['submission_date'].min():%Y-%m-%d} to {df['submission_date'].max():%Y-%m-%d}")
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        logger.info(f"Cleaned data saved to {output_file}")
        
        # Return success
        return True
        
    except FileNotFoundError:
        logger.error(f"Input file {input_file} not found")
        return False
    except pd.errors.EmptyDataError:
        logger.error("Input file is empty")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during data cleaning: {str(e)}")
        return False

if __name__ == "__main__":
    try:
        success = clean_building_permits()
        if success:
            logger.info("Data cleaning completed successfully")
        else:
            logger.error("Data cleaning failed")
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.error(f"Script failed with error: {str(e)}") 