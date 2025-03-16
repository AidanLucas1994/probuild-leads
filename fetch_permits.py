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
    Handles missing and invalid data with default values.
    
    Args:
        df (pandas.DataFrame): Raw permit data DataFrame
    
    Returns:
        dict: JSON object containing transformed permit data and metadata
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
            
        # Step 1: Initial Data Analysis and Default Values Setup
        logger.info("=== Step 1: Initial Data Analysis and Default Values ===")
        
        # Define default values for required fields
        default_values = {
            'PERMITNO': 'UNKNOWN',
            'PERMIT_TYPE': 'Unknown',
            'PERMIT_STATUS': 'Unknown',
            'CONSTRUCTION_VALUE': 0.0,
            'WORK_TYPE': 'Unknown',
            'SUB_WORK_TYPE': 'Unknown',
            'PERMIT_DESCRIPTION': 'No description provided',
            'FOLDERNAME': 'Address not provided'
        }
        
        # Log initial state
        logger.info(f"Initial DataFrame shape: {df.shape}")
        logger.info("Missing values before filling defaults:")
        for col in default_values.keys():
            if col in df.columns:
                missing = df[col].isnull().sum()
                logger.info(f"{col}: {missing} missing values")
        
        # Fill missing values with defaults
        for col, default_val in default_values.items():
            if col in df.columns:
                df[col] = df[col].fillna(default_val)
                
        # Step 2: Date Handling and Validation
        logger.info("\n=== Step 2: Date Handling and Validation ===")
        
        # Convert APPLICATION_DATE to datetime with error handling
        logger.info("Converting APPLICATION_DATE to datetime...")
        if 'APPLICATION_DATE' not in df.columns:
            df['APPLICATION_DATE'] = pd.NaT
            logger.warning("APPLICATION_DATE column not found, created with NaT values")
        
        # Store original length for logging
        original_length = len(df)
        
        # Convert dates and handle invalid formats
        df['APPLICATION_DATE'] = pd.to_datetime(df['APPLICATION_DATE'], errors='coerce')
        
        # Log date conversion results
        invalid_dates = df['APPLICATION_DATE'].isnull().sum()
        logger.info(f"Found {invalid_dates} invalid or missing dates")
        
        # Remove records with invalid dates
        df = df.dropna(subset=['APPLICATION_DATE'])
        records_removed = original_length - len(df)
        logger.info(f"Removed {records_removed} records with invalid dates")
        
        if df.empty:
            logger.warning("All records were invalid after date validation")
            return {
                'status': 'error',
                'error_type': 'validation_error',
                'message': 'No valid records after date validation',
                'details': f'All {original_length} records had invalid dates'
            }
            
        # Step 3: Date Filtering
        logger.info("\n=== Step 3: Date Filtering ===")
        one_year_ago = pd.Timestamp.now() - pd.DateOffset(years=1)
        logger.info(f"Filtering permits after: {one_year_ago}")
        
        # Count permits before filtering
        total_before = len(df)
        
        # Apply date filter
        df = df[df['APPLICATION_DATE'] >= one_year_ago]
        
        # Count permits after filtering
        total_after = len(df)
        logger.info(f"Filtered out {total_before - total_after} permits older than one year")
        
        if df.empty:
            logger.warning("No permits found within the last 12 months")
            return {
                'status': 'warning',
                'message': 'No recent permits found',
                'details': 'No permits found within the last 12 months',
                'metadata': {
                    'total_records_processed': original_length,
                    'invalid_dates_removed': records_removed,
                    'old_permits_filtered': total_before - total_after
                }
            }
        
        # Step 4: Data Cleaning and Standardization
        logger.info("\n=== Step 4: Data Cleaning and Standardization ===")
        
        # Standardize text fields
        text_columns = ['PERMIT_TYPE', 'PERMIT_STATUS', 'WORK_TYPE', 'SUB_WORK_TYPE']
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].str.strip().str.title()
        
        # Clean and standardize construction values
        df['CONSTRUCTION_VALUE'] = pd.to_numeric(df['CONSTRUCTION_VALUE'], errors='coerce')
        df['CONSTRUCTION_VALUE'] = df['CONSTRUCTION_VALUE'].fillna(0.0)
        
        # Step 5: Lead Priority Assignment
        logger.info("\n=== Step 5: Lead Priority Assignment ===")
        def determine_priority(row):
            try:
                work_type = str(row.get('WORK_TYPE', '')).lower()
                permit_type = str(row.get('PERMIT_TYPE', '')).lower()
                value = float(row.get('CONSTRUCTION_VALUE', 0))
                
                # High priority cases
                if value >= 1000000:  # High value projects
                    return 'High'
                elif any(term in work_type for term in ['renovation', 'alteration', 'addition']):
                    return 'High'
                elif 'new construction' in work_type and 'residential' in permit_type:
                    return 'High'
                # Medium priority cases
                elif value >= 500000:  # Medium value projects
                    return 'Medium'
                elif 'residential' in permit_type:
                    return 'Medium'
                # Low priority cases
                else:
                    return 'Low'
            except Exception as e:
                logger.warning(f"Error determining priority: {e}")
                return 'Low'  # Default to low priority if there's an error
                
        df['Lead Priority'] = df.apply(determine_priority, axis=1)
        
        # Step 6: Prepare Final Output
        logger.info("\n=== Step 6: Preparing Final Output ===")
        
        # Select and rename columns
        output_columns = {
            'PERMITNO': 'Permit Number',
            'PERMIT_TYPE': 'Permit Type',
            'APPLICATION_DATE': 'Application Date',
            'PERMIT_STATUS': 'Status',
            'CONSTRUCTION_VALUE': 'Construction Value',
            'WORK_TYPE': 'Work Type',
            'SUB_WORK_TYPE': 'Sub Work Type',
            'PERMIT_DESCRIPTION': 'Description',
            'FOLDERNAME': 'Property Address',
            'Lead Priority': 'Lead Priority'
        }
        
        # Select available columns and rename
        available_columns = [col for col in output_columns.keys() if col in df.columns]
        df_final = df[available_columns].copy()
        df_final = df_final.rename(columns={col: output_columns[col] for col in available_columns})
        
        # Format dates and values
        df_final['Application Date'] = df_final['Application Date'].dt.strftime('%Y-%m-%d')
        df_final['Construction Value'] = df_final['Construction Value'].apply(lambda x: f"${x:,.2f}")
        
        # Prepare summary statistics
        summary = {
            'total_permits': len(df_final),
            'priority_distribution': df_final['Lead Priority'].value_counts().to_dict(),
            'permit_type_distribution': df_final['Permit Type'].value_counts().to_dict(),
            'date_range': {
                'start': df_final['Application Date'].min(),
                'end': df_final['Application Date'].max()
            },
            'data_quality': {
                'original_records': original_length,
                'invalid_dates_removed': records_removed,
                'records_within_time_range': len(df_final),
                'fields_with_defaults': {
                    field: sum(df_final[output_columns[field]] == default_values[field])
                    for field in default_values.keys()
                    if field in df.columns and output_columns[field] in df_final.columns
                }
            }
        }
        
        # Return final result
        result = {
            'status': 'success',
            'summary': summary,
            'permits': df_final.to_dict(orient='records'),
            'metadata': {
                'timestamp': datetime.now().isoformat(),
                'filters_applied': {
                    'date_range': f"Last 12 months (from {one_year_ago.strftime('%Y-%m-%d')})",
                    'fields_selected': list(df_final.columns)
                }
            }
        }
        
        logger.info("Successfully transformed permit data")
        logger.info(f"Final record count: {len(df_final)}")
        return result
        
    except Exception as e:
        logger.error(f"Error transforming permit data: {str(e)}", exc_info=True)
        return {
            'status': 'error',
            'error_type': 'processing_error',
            'message': 'Error processing permit data',
            'details': str(e)
        }

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