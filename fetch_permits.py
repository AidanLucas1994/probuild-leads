import requests
import json
from datetime import datetime
import pandas as pd
import logging
import os
import sys

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Configure logging to write to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/permit_data.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

pd.set_option('display.max_columns', None)

def fetch_permit_data():
    """
    Fetch building permit data from the ArcGIS REST API endpoint.
    Returns a pandas DataFrame if successful, None otherwise.
    """
    # API endpoint URL
    url = ("https://services1.arcgis.com/qAo1OsXi67t7XgmS/arcgis/rest/services/"
           "Building_Permits/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json")
    
    try:
        # Send GET request
        logger.info("Fetching data from API...")
        response = requests.get(url)
        
        # Check if request was successful
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        logger.info(f"API Response received. Status code: {response.status_code}")
        
        # Extract features from the response
        features = data.get('features', [])
        logger.info(f"Found {len(features)} features in the response")
        
        if not features:
            logger.warning("No permit data found in the response.")
            return None
        
        # Convert features to a list of dictionaries
        permits = []
        for feature in features:
            permit = feature['attributes']
            # Convert timestamp fields to datetime
            date_fields = ['APPLICATION_DATE', 'ISSUE_DATE', 'FINAL_DATE', 'EXPIRY_DATE', 'EXTRACTION_DATE']
            for field in date_fields:
                if permit.get(field):
                    # Convert milliseconds to seconds and create datetime object
                    permit[field] = datetime.fromtimestamp(permit[field] / 1000)
            permits.append(permit)
        
        logger.info(f"Successfully processed {len(permits)} permits")
        
        # Create DataFrame
        df = pd.DataFrame(permits)
        logger.info(f"Created DataFrame with shape: {df.shape}")
        
        # Log column names for debugging
        logger.info(f"DataFrame columns: {df.columns.tolist()}")
        
        # Reorder columns for better readability
        important_cols = [
            'PERMITNO', 'PERMIT_TYPE', 'PERMIT_STATUS', 'APPLICATION_DATE',
            'ISSUE_DATE', 'CONSTRUCTION_VALUE', 'WORK_TYPE', 'SUB_WORK_TYPE',
            'PERMIT_DESCRIPTION', 'TOTAL_UNITS', 'UNITS_CREATED'
        ]
        # Move important columns to front, keep the rest in original order
        remaining_cols = [col for col in df.columns if col not in important_cols]
        df = df[important_cols + remaining_cols]
        
        # Log sample of the data
        logger.info("Sample of the first permit:")
        logger.info(df.iloc[0].to_dict() if not df.empty else "DataFrame is empty")
        
        return df
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON response: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return None

def transform_permit_data(df):
    """
    Transform raw permit data by filtering recent permits and adding lead priority.
    
    Args:
        df (pandas.DataFrame): Raw permit data DataFrame
    
    Returns:
        dict: JSON object containing transformed permit data and metadata or
        dict: Error response with details about missing required fields
    """
    try:
        if df is None or df.empty:
            logger.warning("Input DataFrame is None or empty")
            return {
                'status': 'error',
                'error_type': 'validation_error',
                'message': 'No permit data provided',
                'details': 'Input DataFrame is None or empty'
            }
            
        # Step 1: Required Fields Validation
        logger.info("=== Step 1: Required Fields Validation ===")
        required_fields = {
            'FOLDERNAME': 'Property Address',
            'PERMIT_TYPE': 'Permit Type',
            'APPLICATION_DATE': 'Application Date'
        }
        
        # Check for missing required fields
        missing_fields = [field for field in required_fields.keys() if field not in df.columns]
        if missing_fields:
            error_msg = f"Missing required fields: {', '.join([required_fields[f] for f in missing_fields])}"
            logger.error(error_msg)
            return {
                'status': 'error',
                'error_type': 'validation_error',
                'message': 'Missing required fields in permit data',
                'details': error_msg
            }
            
        # Check for null values in required fields
        null_counts = {field: df[field].isnull().sum() for field in required_fields.keys()}
        fields_with_nulls = {field: count for field, count in null_counts.items() if count > 0}
        
        if fields_with_nulls:
            error_details = {required_fields[field]: count for field, count in fields_with_nulls.items()}
            logger.error(f"Required fields contain null values: {error_details}")
            return {
                'status': 'error',
                'error_type': 'validation_error',
                'message': 'Required fields contain null values',
                'details': error_details
            }
            
        logger.info("All required fields present and contain values")
        logger.info(f"Field counts: {', '.join([f'{required_fields[f]}: {len(df[f].dropna())}' for f in required_fields])}")
        
        # Step 2: Initial Data Analysis
        logger.info("\n=== Step 2: Initial Data Analysis ===")
        logger.info(f"Initial DataFrame shape: {df.shape}")
        logger.info(f"Columns present: {df.columns.tolist()}")
        logger.info(f"Memory usage: {df.memory_usage().sum() / 1024 / 1024:.2f} MB")
        
        # Log data types of each column
        logger.info("\nColumn Data Types:")
        for col, dtype in df.dtypes.items():
            logger.info(f"{col}: {dtype}")
        
        # Step 3: Date Field Analysis
        logger.info("\n=== Step 3: Date Field Analysis ===")
        logger.info("Converting APPLICATION_DATE to datetime...")
        # Log initial date statistics
        logger.info("Before conversion:")
        logger.info(f"APPLICATION_DATE unique values (first 5): {df['APPLICATION_DATE'].unique()[:5]}")
        
        # Convert to datetime
        df['APPLICATION_DATE'] = pd.to_datetime(df['APPLICATION_DATE'], errors='coerce')
        
        # Check if any dates failed to convert
        invalid_dates = df['APPLICATION_DATE'].isnull().sum()
        if invalid_dates > 0:
            logger.error(f"Found {invalid_dates} invalid dates in APPLICATION_DATE")
            return {
                'status': 'error',
                'error_type': 'validation_error',
                'message': 'Invalid date formats found in Application Date field',
                'details': f'{invalid_dates} records contain invalid date formats'
            }
        
        # Log post-conversion statistics
        logger.info("\nAfter conversion:")
        logger.info(f"Date range: {df['APPLICATION_DATE'].min()} to {df['APPLICATION_DATE'].max()}")
        logger.info(f"Number of unique dates: {df['APPLICATION_DATE'].nunique()}")
        date_counts = df['APPLICATION_DATE'].dt.year.value_counts().sort_index()
        logger.info(f"Distribution by year: {date_counts.to_dict()}")
            
        # Step 4: Date Filtering
        logger.info("\n=== Step 4: Date Filtering ===")
        one_year_ago = pd.Timestamp.now() - pd.DateOffset(years=1)
        logger.info(f"Filtering permits after: {one_year_ago}")
        
        # Count permits before filtering
        total_before = len(df)
        logger.info(f"Total permits before filtering: {total_before}")
        
        # Log distribution of dates relative to cutoff
        future_dates = sum(df['APPLICATION_DATE'] > pd.Timestamp.now())
        past_year = sum((df['APPLICATION_DATE'] >= one_year_ago) & (df['APPLICATION_DATE'] <= pd.Timestamp.now()))
        older_than_year = sum(df['APPLICATION_DATE'] < one_year_ago)
        null_dates = df['APPLICATION_DATE'].isnull().sum()
        
        logger.info(f"Date distribution:")
        logger.info(f"- Future dates: {future_dates}")
        logger.info(f"- Within last year: {past_year}")
        logger.info(f"- Older than one year: {older_than_year}")
        logger.info(f"- Null dates: {null_dates}")
        
        # Apply date filter
        df = df[df['APPLICATION_DATE'] >= one_year_ago]
        
        # Count permits after filtering
        total_after = len(df)
        logger.info(f"Total permits after filtering: {total_after}")
        logger.info(f"Filtered out {total_before - total_after} permits")
        
        if df.empty:
            logger.warning("No permits found within the last 12 months")
            return None
        
        # Step 5: Column Selection and Renaming
        logger.info("\n=== Step 5: Column Selection and Renaming ===")
        key_fields = {
            'PERMITNO': 'Permit Number',
            'PERMIT_TYPE': 'Permit Type',
            'APPLICATION_DATE': 'Application Date',
            'PERMIT_STATUS': 'Status',
            'CONSTRUCTION_VALUE': 'Construction Value',
            'WORK_TYPE': 'Work Type',
            'SUB_WORK_TYPE': 'Sub Work Type',
            'PERMIT_DESCRIPTION': 'Description'
        }
        
        # Verify all required columns exist
        missing_columns = [col for col in key_fields.keys() if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return None
        
        # Log value counts for key categorical fields
        for field in ['PERMIT_TYPE', 'PERMIT_STATUS', 'WORK_TYPE', 'SUB_WORK_TYPE']:
            if field in df.columns:
                logger.info(f"\n{field} value counts:")
                logger.info(df[field].value_counts().head().to_dict())
        
        logger.info("\nSelecting and renaming columns...")
        df = df[list(key_fields.keys())].copy()
        df = df.rename(columns=key_fields)
        
        # Step 6: Lead Priority Assignment
        logger.info("\n=== Step 6: Lead Priority Assignment ===")
        logger.info("Adding lead priorities...")
        def determine_priority(row):
            work_type = str(row['Work Type']).lower()
            permit_type = str(row['Permit Type']).lower()
            
            # High priority cases
            if any(term in work_type for term in ['renovation', 'alteration', 'addition']):
                return 'High'
            elif 'new construction' in work_type and 'residential' in permit_type:
                return 'High'
            # Medium priority cases
            elif 'residential' in permit_type:
                return 'Medium'
            elif 'commercial' in permit_type and 'new construction' in work_type:
                return 'Medium'
            # Low priority cases
            else:
                return 'Low'
        
        df['Lead Priority'] = df.apply(determine_priority, axis=1)
        priority_counts = df['Lead Priority'].value_counts()
        logger.info("\nLead Priority Distribution:")
        logger.info(priority_counts.to_dict())
        
        # Step 7: Data Formatting
        logger.info("\n=== Step 7: Data Formatting ===")
        logger.info("Formatting dates and values...")
        
        # Log construction value statistics before formatting
        logger.info("\nConstruction Value Statistics (before formatting):")
        logger.info(f"Min: {df['Construction Value'].min()}")
        logger.info(f"Max: {df['Construction Value'].max()}")
        logger.info(f"Mean: {df['Construction Value'].mean()}")
        logger.info(f"Null values: {df['Construction Value'].isnull().sum()}")
        
        df['Application Date'] = df['Application Date'].dt.strftime('%Y-%m-%d')
        df['Construction Value'] = df['Construction Value'].fillna(0.0)
        df['Construction Value'] = df['Construction Value'].apply(lambda x: f"${x:,.2f}")
        
        # Step 8: Summary Statistics
        logger.info("\n=== Step 8: Summary Statistics ===")
        logger.info("Calculating summary statistics...")
        summary = {
            'total_permits': len(df),
            'priority_distribution': df['Lead Priority'].value_counts().to_dict(),
            'permit_type_distribution': df['Permit Type'].value_counts().to_dict(),
            'date_range': {
                'start': df['Application Date'].min(),
                'end': df['Application Date'].max()
            }
        }
        logger.info(f"Summary statistics: {summary}")
        
        # Step 9: Final JSON Conversion
        logger.info("\n=== Step 9: Final JSON Conversion ===")
        logger.info("Converting DataFrame to JSON structure...")
        permits = df.to_dict(orient='records')
        logger.info(f"Number of permits in final output: {len(permits)}")
        
        # Return JSON structure
        result = {
            'status': 'success',
            'summary': summary,
            'permits': permits,
            'metadata': {
                'timestamp': datetime.now().isoformat(),
                'filters_applied': {
                    'date_range': f"Last 12 months (from {one_year_ago.strftime('%Y-%m-%d')})",
                    'fields_selected': list(key_fields.values())
                }
            }
        }
        
        logger.info("\nSuccessfully transformed permit data")
        return result
        
    except Exception as e:
        logger.error(f"Error transforming permit data: {str(e)}", exc_info=True)
        logger.error(f"Error occurred at line {sys.exc_info()[2].tb_lineno}")
        return None

def main():
    # Fetch the data
    df = fetch_permit_data()
    
    if df is not None:
        # Print basic information about the dataset
        print("\nDataset Information:")
        print(f"Total number of permits: {len(df)}")
        print(f"Time range: {df['APPLICATION_DATE'].min()} to {df['APPLICATION_DATE'].max()}")
        
        # Print value statistics
        total_value = df['CONSTRUCTION_VALUE'].sum()
        avg_value = df['CONSTRUCTION_VALUE'].mean()
        print(f"\nTotal construction value: ${total_value:,.2f}")
        print(f"Average construction value: ${avg_value:,.2f}")
        
        # Print permit type distribution
        print("\nPermit Type Distribution:")
        print(df['PERMIT_TYPE'].value_counts().head())
        
        # Display first 5 rows with selected columns
        print("\nFirst 5 Building Permits:")
        display_cols = [
            'PERMITNO', 'PERMIT_TYPE', 'PERMIT_STATUS', 'APPLICATION_DATE',
            'CONSTRUCTION_VALUE', 'WORK_TYPE', 'PERMIT_DESCRIPTION'
        ]
        print(df[display_cols].head().to_string())

if __name__ == "__main__":
    main() 