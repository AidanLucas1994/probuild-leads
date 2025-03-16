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
        logger.info("API Response received. Status code: {}".format(response.status_code))
        
        # Extract features from the response
        features = data.get('features', [])
        logger.info("Found {} features in the response".format(len(features)))
        
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
        
        logger.info("Successfully processed {} permits".format(len(permits)))
        
        # Create DataFrame
        df = pd.DataFrame(permits)
        logger.info("Created DataFrame with shape: {}".format(df.shape))
        
        # Log column names for debugging
        logger.info("DataFrame columns: {}".format(df.columns.tolist()))
        
        return df
        
    except Exception as e:
        logger.error("Error: {}".format(str(e)))
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
            
        # Initial logging of date range
        logger.info("\n=== Initial Date Analysis ===")
        if 'APPLICATION_DATE' in df.columns:
            min_date = df['APPLICATION_DATE'].min()
            max_date = df['APPLICATION_DATE'].max()
            logger.info("Date range in raw data:")
            logger.info("Earliest date: {}".format(min_date))
            logger.info("Latest date: {}".format(max_date))
            logger.info("Total records: {}".format(len(df)))
            
            # Add distribution of dates by year and month
            year_counts = df['APPLICATION_DATE'].dt.year.value_counts().sort_index()
            logger.info("\nDistribution by year:")
            for year, count in year_counts.items():
                logger.info("{}: {} permits".format(year, count))
            
            # Get current month's permits
            current_month = pd.Timestamp.now().replace(day=1)
            current_month_permits = df[df['APPLICATION_DATE'] >= current_month]
            logger.info("\nPermits in current month: {}".format(len(current_month_permits)))

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
        logger.info("Initial DataFrame shape: {}".format(df.shape))
        logger.info("Missing values before filling defaults:")
        for col in default_values.keys():
            if col in df.columns:
                missing = df[col].isnull().sum()
                logger.info("{}: {} missing values".format(col, missing))
        
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
        
        # Log parsed dates for each record
        logger.info("\nParsed Application Dates:")
        for idx, row in df.iterrows():
            permit_no = row.get('PERMITNO', 'Unknown Permit')
            app_date = row['APPLICATION_DATE']
            if pd.isna(app_date):
                logger.info("Permit {}: Invalid or missing date".format(permit_no))
            else:
                logger.info("Permit {}: {}".format(permit_no, app_date.strftime('%Y-%m-%d')))
        
        # Log date conversion results
        invalid_dates = df['APPLICATION_DATE'].isnull().sum()
        logger.info("\nFound {} invalid or missing dates".format(invalid_dates))
        
        # Remove records with invalid dates
        df = df.dropna(subset=['APPLICATION_DATE'])
        records_removed = original_length - len(df)
        logger.info("Removed {} records with invalid dates".format(records_removed))
        
        if df.empty:
            logger.warning("All records were invalid after date validation")
            return {
                'status': 'error',
                'error_type': 'validation_error',
                'message': 'No valid records after date validation',
                'details': 'All {} records had invalid dates'.format(original_length)
            }
            
        # Enhanced date filtering section
        logger.info("\n=== Step 3: Date Filtering ===")
        
        # Instead of using absolute dates, let's use relative dates from the data
        latest_date = df['APPLICATION_DATE'].max()
        relative_cutoff = latest_date - pd.DateOffset(years=1)
        
        logger.info("\nDate Filtering Details:")
        logger.info("Latest permit date: {}".format(latest_date.strftime('%Y-%m-%d')))
        logger.info("Using relative cutoff date: {}".format(relative_cutoff.strftime('%Y-%m-%d')))
        logger.info("Will include permits from {} to {}".format(
            relative_cutoff.strftime('%Y-%m-%d'),
            latest_date.strftime('%Y-%m-%d')
        ))

        # Count permits before filtering
        total_before = len(df)
        
        # Apply date filter with detailed logging
        recent_permits = df[df['APPLICATION_DATE'] >= relative_cutoff]
        total_after = len(recent_permits)
        
        logger.info("\nFiltering Results:")
        logger.info("Total permits before filtering: {}".format(total_before))
        logger.info("Permits within last 12 months of data: {}".format(total_after))
        logger.info("Permits excluded: {}".format(total_before - total_after))
        
        if total_after == 0:
            # If no permits are within range, log the most recent ones
            logger.info("\nMost recent permits (all excluded):")
            most_recent = df.nlargest(5, 'APPLICATION_DATE')
            for _, permit in most_recent.iterrows():
                logger.info("Permit {}: {} - {}".format(
                    permit.get('PERMITNO', 'Unknown'),
                    permit['APPLICATION_DATE'].strftime('%Y-%m-%d'),
                    permit.get('PERMIT_TYPE', 'Unknown Type')
                ))

        # Update the df with filtered data
        df = recent_permits

        if df.empty:
            logger.warning("No permits found within the relative date range")
            return {
                'status': 'warning',
                'message': 'No permits found in relative date range',
                'details': 'No permits found within 12 months of the latest permit ({})'.format(
                    latest_date.strftime('%Y-%m-%d')),
                'metadata': {
                    'total_records_processed': original_length,
                    'invalid_dates_removed': records_removed,
                    'old_permits_filtered': total_before - total_after,
                    'date_range': {
                        'earliest': min_date.strftime('%Y-%m-%d'),
                        'latest': max_date.strftime('%Y-%m-%d'),
                        'relative_cutoff': relative_cutoff.strftime('%Y-%m-%d')
                    }
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
                    'date_range': f"Last 12 months (from {relative_cutoff.strftime('%Y-%m-%d')})",
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

def test_date_filtering():
    """
    Test the date filtering logic with a sample dataset.
    Includes permits from both within and outside the 12-month window.
    """
    logger.info("=== Running Date Filtering Test ===")
    
    # Create sample data
    current_date = pd.Timestamp.now()
    sample_data = {
        'PERMITNO': ['PERMIT-{:03d}'.format(i) for i in range(1, 11)],
        'APPLICATION_DATE': [
            # Within last 12 months
            current_date - pd.DateOffset(days=30),    # 1 month ago
            current_date - pd.DateOffset(months=3),   # 3 months ago
            current_date - pd.DateOffset(months=6),   # 6 months ago
            current_date - pd.DateOffset(months=9),   # 9 months ago
            current_date - pd.DateOffset(months=11),  # 11 months ago
            # Outside last 12 months
            current_date - pd.DateOffset(months=13),  # 13 months ago
            current_date - pd.DateOffset(months=15),  # 15 months ago
            current_date - pd.DateOffset(months=18),  # 18 months ago
            current_date - pd.DateOffset(months=24),  # 2 years ago
            # Invalid date
            None
        ],
        'PERMIT_TYPE': [
            'Residential', 'Commercial', 'Industrial',
            'Residential', 'Commercial', 'Industrial',
            'Residential', 'Commercial', 'Industrial',
            'Residential'
        ],
        'FOLDERNAME': [
            '123 Main St', '456 Oak Ave', '789 Pine Rd',
            '321 Elm St', '654 Maple Dr', '987 Cedar Ln',
            '147 Birch Rd', '258 Spruce Ave', '369 Willow St',
            '741 Ash Ln'
        ],
        'CONSTRUCTION_VALUE': [
            1500000,  # High value
            750000,   # Medium value
            250000,   # Low value
            1200000,  # High value
            600000,   # Medium value
            300000,   # Low value
            800000,   # Medium value
            400000,   # Low value
            900000,   # Medium value
            None      # Missing value
        ],
        'WORK_TYPE': [
            'New Construction', 'Renovation', 'Addition',
            'Interior Alteration', 'Exterior Alteration', 'Repair',
            'New Construction', 'Renovation', 'Addition',
            'Repair'
        ],
        'PERMIT_STATUS': [
            'Active', 'Pending', 'Approved',
            'Active', 'Pending', 'Approved',
            'Active', 'Pending', 'Approved',
            'Pending'
        ],
        'SUB_WORK_TYPE': [
            'Single Family', 'Office', 'Warehouse',
            'Multi-Family', 'Retail', 'Manufacturing',
            'Townhouse', 'Restaurant', 'Storage',
            'Duplex'
        ],
        'PERMIT_DESCRIPTION': [
            'New single family home construction',
            'Office building renovation',
            'Warehouse addition',
            'Apartment building renovation',
            'Retail store modification',
            'Factory repair work',
            'New townhouse development',
            'Restaurant renovation',
            'Storage facility expansion',
            'Duplex conversion'
        ]
    }
    
    # Create DataFrame
    df = pd.DataFrame(sample_data)
    
    logger.info("\nSample Dataset Created:")
    logger.info("Total records: {}".format(len(df)))
    logger.info("\nOriginal Data:")
    for idx, row in df.iterrows():
        date_str = row['APPLICATION_DATE'].strftime('%Y-%m-%d') if pd.notna(row['APPLICATION_DATE']) else 'INVALID'
        logger.info("Permit {}: {} - {} - {} - ${}".format(
            row['PERMITNO'],
            date_str,
            row['PERMIT_TYPE'],
            row['FOLDERNAME'],
            row['CONSTRUCTION_VALUE'] if pd.notna(row['CONSTRUCTION_VALUE']) else 'N/A'
        ))
    
    # Process the sample data
    logger.info("\nProcessing sample data through transform_permit_data function...")
    result = transform_permit_data(df)
    
    # Display results
    if result['status'] == 'success':
        logger.info("\nTransformation Results:")
        logger.info("Total permits processed: {}".format(result['summary']['total_permits']))
        logger.info("Original records: {}".format(result['summary']['data_quality']['original_records']))
        logger.info("Invalid dates removed: {}".format(result['summary']['data_quality']['invalid_dates_removed']))
        logger.info("Records within time range: {}".format(result['summary']['data_quality']['records_within_time_range']))
        
        logger.info("\nIncluded Permits:")
        for permit in result['permits']:
            logger.info("Permit {}: {} - {} - {} - {}".format(
                permit['Permit Number'],
                permit['Application Date'],
                permit['Permit Type'],
                permit['Property Address'],
                permit['Construction Value']
            ))
            
        logger.info("\nPriority Distribution:")
        for priority, count in result['summary']['priority_distribution'].items():
            logger.info("{}: {}".format(priority, count))
    else:
        logger.error("Transformation failed: {}".format(result['message']))
        if 'details' in result:
            logger.error("Details: {}".format(result['details']))

if __name__ == "__main__":
    # Run the test
    test_date_filtering() 