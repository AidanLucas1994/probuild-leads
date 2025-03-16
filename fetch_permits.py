import requests
import json
from datetime import datetime
import pandas as pd
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
        print("Fetching data from API...")
        response = requests.get(url)
        
        # Check if request was successful
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        
        # Extract features from the response
        features = data.get('features', [])
        
        if not features:
            print("No permit data found in the response.")
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
        
        # Create DataFrame
        df = pd.DataFrame(permits)
        
        # Reorder columns for better readability
        important_cols = [
            'PERMITNO', 'PERMIT_TYPE', 'PERMIT_STATUS', 'APPLICATION_DATE',
            'ISSUE_DATE', 'CONSTRUCTION_VALUE', 'WORK_TYPE', 'SUB_WORK_TYPE',
            'PERMIT_DESCRIPTION', 'TOTAL_UNITS', 'UNITS_CREATED'
        ]
        # Move important columns to front, keep the rest in original order
        remaining_cols = [col for col in df.columns if col not in important_cols]
        df = df[important_cols + remaining_cols]
        
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
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