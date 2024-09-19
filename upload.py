import os
import pandas as pd
import requests
from urllib.parse import urljoin

def clean_and_map_data(df, filename):
    # Remove leading zeros from patient_id and convert to int if the column exists
    if 'patient_id' in df.columns:
        df['patient_id'] = df['patient_id'].astype(str).str.lstrip('0').astype(int)

    # Map column names to REDCap field names
    form_name = filename.replace('.csv', '').replace('_', ' ').capitalize()
    mapped_columns = {}
    for column in df.columns:
        if column == 'reclutado_id':
            mapped_columns[column] = 'reclutado_id'
        elif column == 'patient_id':
            mapped_columns[column] = 'patient_id'
        elif filename == 'dbo_05_Massas.csv' and column == 'mTB':
            mapped_columns[column] = 'mtb'
        else:
            mapped_columns[column] = f"{form_name.lower().replace(' ', '_')}_{column.lower()}".replace('.', '_')

    df = df.rename(columns=mapped_columns)
    
    # Handle potential duplicates by grouping by the record ID
    df = df.groupby('reclutado_id').first().reset_index()
    
    return df

def upload_data_to_redcap(api_url, api_key, data):
    fields = {
        'token': api_key,
        'content': 'record',
        'format': 'csv',
        'type': 'flat',
        'data': data,
        'overwriteBehavior': 'overwrite',
        'returnContent': 'count',
        'returnFormat': 'json'
    }
    try:
        response = requests.post(api_url, data=fields)
        response.raise_for_status()
        
        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.text[:200]}...")
        
        if response.text.strip():
            return response.json()
        else:
            print("Warning: Empty response from REDCap API")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error making request to REDCap API: {e}")
        print(f"Response content: {response.text}")
        return None

def process_csv_files(directory, api_url, api_key):
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            try:
                df = pd.read_csv(file_path)
                df = clean_and_map_data(df, filename)
                print(f"Successfully read {filename}. Shape: {df.shape}")
                print(f"Columns after mapping: {df.columns.tolist()}")
                
                if 'reclutado_id' not in df.columns:
                    print(f"Error: 'reclutado_id' column missing in {filename}")
                    continue
                
                csv_data = df.to_csv(index=False)
                result = upload_data_to_redcap(api_url, api_key, csv_data)
                
                if result is None:
                    print(f"Failed to upload {filename}")
                elif isinstance(result, dict) and 'error' in result:
                    print(f"Error uploading {filename}: {result['error']}")
                else:
                    print(f"Successfully uploaded {filename}: {result} records processed")
                
            except Exception as e:
                print(f"Failed to process {filename}: {str(e)}")
                import traceback
                print(traceback.format_exc())

# Parameters
BASE_URL = 'http://localhost/redcap_v14.6.8/'
API_ENDPOINT = 'API/'
API_URL = urljoin(BASE_URL, API_ENDPOINT)
API_KEY = '6CB10A9DC74EC213EB0AF032337BE4DE'  # Your REDCap API key
CSV_DIRECTORY = 'csv_output/'

print(f"Using API URL: {API_URL}")
process_csv_files(CSV_DIRECTORY, API_URL, API_KEY)