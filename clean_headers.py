import os
import pandas as pd
import numpy as np

def is_header_like(row, headers):
    # Check if most of the row matches the headers
    return sum(1 for a, b in zip(row, headers) if str(a) == str(b)) > len(headers) * 0.8

def clean_csv_files(directory):
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            try:
                # Read the CSV file into a DataFrame
                df = pd.read_csv(file_path, header=0)
                
                # Get the headers
                headers = df.columns.tolist()
                
                # Convert DataFrame to list of lists
                data = df.values.tolist()
                
                # Filter out rows that are header-like
                cleaned_data = [row for row in data if not is_header_like(row, headers)]
                
                # Create a new DataFrame with cleaned data
                df_cleaned = pd.DataFrame(cleaned_data, columns=headers)
                
                # Convert numeric columns back to numbers
                for col in df_cleaned.columns:
                    df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='ignore')
                
                # Save the cleaned DataFrame back to CSV, overwriting the original file
                df_cleaned.to_csv(file_path, index=False)
                print(f"Cleaned {filename}")
            except Exception as e:
                print(f"Failed to clean {filename}: {e}")

# Specify the directory containing the CSV files
directory_path = 'csv_output/'
clean_csv_files(directory_path)