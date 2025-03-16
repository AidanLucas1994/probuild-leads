import requests
import json
from datetime import datetime
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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
        dict: JSON object containing transformed permit data and metadata
    """
    try:
        if df is None or df.empty:
            logger.warning("Input DataFrame is None or empty")
            return None
            
        # Log initial data state
        logger.info(f"Initial DataFrame shape: {df.shape}")
        logger.info(f"Columns present: {df.columns.tolist()}")
        
        # Convert APPLICATION_DATE to datetime if it's not already
        if 'APPLICATION_DATE' in df.columns:
            logger.info("Converting APPLICATION_DATE to datetime...")
            df['APPLICATION_DATE'] = pd.to_datetime(df['APPLICATION_DATE'], errors='coerce')
            logger.info(f"Date range in data: {df['APPLICATION_DATE'].min()} to {df['APPLICATION_DATE'].max()}")
            logger.info(f"Number of null dates: {df['APPLICATION_DATE'].isnull().sum()}")
        else:
            logger.error("APPLICATION_DATE column not found in DataFrame")
            return None
            
        # Filter for permits within the last year
        one_year_ago = pd.Timestamp.now() - pd.DateOffset(years=1)
        logger.info(f"Filtering permits after: {one_year_ago}")
        
        # Count permits before filtering
        total_before = len(df)
        logger.info(f"Total permits before filtering: {total_before}")
        
        # Apply date filter
        df = df[df['APPLICATION_DATE'] >= one_year_ago]
        
        # Count permits after filtering
        total_after = len(df)
        logger.info(f"Total permits after filtering: {total_after}")
        logger.info(f"Filtered out {total_before - total_after} permits")
        
        if df.empty:
            logger.warning("No permits found within the last 12 months")
            return None
        
        # Extract and rename key fields
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
        
        logger.info("Selecting and renaming columns...")
        df = df[list(key_fields.keys())].copy()
        df = df.rename(columns=key_fields)
        
        # Add Lead Priority based on permit types and work types
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
        
        # Format dates and values
        logger.info("Formatting dates and values...")
        df['Application Date'] = df['Application Date'].dt.strftime('%Y-%m-%d')
        df['Construction Value'] = df['Construction Value'].fillna(0.0)
        df['Construction Value'] = df['Construction Value'].apply(lambda x: f"${x:,.2f}")
        
        # Calculate summary statistics
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
        
        # Convert DataFrame to list of dictionaries
        logger.info("Converting DataFrame to JSON structure...")
        permits = df.to_dict(orient='records')
        
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
        
        logger.info("Successfully transformed permit data")
        return result
        
    except Exception as e:
        logger.error(f"Error transforming permit data: {str(e)}", exc_info=True)
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