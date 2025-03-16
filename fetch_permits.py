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
    logger.info("Starting permit data transformation")
    logger.info(f"Input DataFrame shape: {df.shape if df is not None else 'None'}")
    
    if df is None or df.empty:
        logger.warning("Input DataFrame is None or empty")
        return {
            'status': 'success',
            'summary': {
                'total_permits': 0,
                'priority_distribution': {},
                'permit_type_distribution': {},
                'date_range': {
                    'start': None,
                    'end': None
                }
            },
            'permits': [],
            'metadata': {
                'timestamp': datetime.now().isoformat(),
                'filters_applied': {
                    'date_range': f"Last 12 months (from {datetime.now().strftime('%Y-%m-%d')})",
                    'fields_selected': []
                },
                'message': 'No permit data available from the API'
            }
        }
        
    try:
        # Filter for permits within the last year
        one_year_ago = pd.Timestamp.now() - pd.DateOffset(years=1)
        logger.info(f"Filtering permits after: {one_year_ago}")
        
        # Log the range of dates in the DataFrame
        logger.info(f"Date range in DataFrame: {df['APPLICATION_DATE'].min()} to {df['APPLICATION_DATE'].max()}")
        
        df = df[df['APPLICATION_DATE'] >= one_year_ago]
        logger.info(f"After date filtering, DataFrame shape: {df.shape}")
        
        # If no permits after filtering, return empty response
        if df.empty:
            logger.warning("No permits found within the last 12 months")
            return {
                'status': 'success',
                'summary': {
                    'total_permits': 0,
                    'priority_distribution': {},
                    'permit_type_distribution': {},
                    'date_range': {
                        'start': None,
                        'end': None
                    }
                },
                'permits': [],
                'metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'filters_applied': {
                        'date_range': f"Last 12 months (from {one_year_ago.strftime('%Y-%m-%d')})",
                        'fields_selected': [],
                        'message': 'No permits found within the last 12 months'
                    }
                }
            }
        
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
        
        logger.info("Selecting and renaming columns")
        df = df[list(key_fields.keys())].copy()
        df = df.rename(columns=key_fields)
        
        # Add Lead Priority based on permit types and work types
        logger.info("Adding Lead Priority")
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
        logger.info("Formatting dates and values")
        df['Application Date'] = df['Application Date'].dt.strftime('%Y-%m-%d')
        df['Construction Value'] = df['Construction Value'].apply(lambda x: f"${x:,.2f}")
        
        # Calculate summary statistics
        logger.info("Calculating summary statistics")
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
        
        # Convert DataFrame to list of dictionaries
        permits = df.to_dict(orient='records')
        logger.info(f"Converted {len(permits)} permits to dictionary format")
        
        # Return JSON structure
        return {
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
        
    except Exception as e:
        logger.error(f"Error transforming permit data: {e}", exc_info=True)
        return {
            'status': 'error',
            'message': f'Error transforming permit data: {str(e)}',
            'summary': {
                'total_permits': 0,
                'priority_distribution': {},
                'permit_type_distribution': {},
                'date_range': {
                    'start': None,
                    'end': None
                }
            },
            'permits': [],
            'metadata': {
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
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