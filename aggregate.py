import os
import pandas as pd

def process_text_file(file_path, patient_id, reclutado_id):
    try:
        data = []
        max_cols = 0
        with open(file_path, 'r', encoding='latin-1') as file:
            for line in file:
                line = line.strip()
                if line:
                    data_row = line.split()
                    data.append(data_row)
                    if len(data_row) > max_cols:
                        max_cols = len(data_row)
        
        if not data:
            print(f"No data found in {file_path}")
            return None

        column_names = [f'Col_{i+1}' for i in range(max_cols)]
        padded_data = [row + [''] * (max_cols - len(row)) for row in data]
        
        df = pd.DataFrame(padded_data, columns=column_names)
        df['patient_id'] = patient_id
        df['reclutado_id'] = reclutado_id  # Consistent naming
        print(f"Created DataFrame from {file_path} with shape {df.shape}")
        return df
    except Exception as e:
        print(f"Error reading text file {file_path}: {e}")
        raise

def process_emt_file(file_path, patient_id, reclutado_id):
    try:
        with open(file_path, 'r', encoding='latin-1') as file:
            lines = file.readlines()

        header_index = None
        data_start_index = None
        for i, line in enumerate(lines):
            stripped_line = line.strip()
            if stripped_line.startswith("Values:") or stripped_line.startswith("Cycles:"):
                for j in range(i + 1, len(lines)):
                    if lines[j].strip():
                        header_index = j
                        data_start_index = j + 1
                        break
                if header_index is not None:
                    break
        
        if header_index is None or data_start_index is None:
            raise ValueError(f"No valid header or data found in {file_path}")

        headers = lines[header_index].strip().split()
        data = []
        for line in lines[data_start_index:]:
            line = line.strip()
            if not line:
                continue
            if any(char.isdigit() or char in ['-', '.'] for char in line) or 'NaN' in line:
                data_row = line.split()
                data.append(data_row)
            else:
                break

        if not data:
            print(f"No data found in {file_path}")
            return None

        if all(len(headers) == len(data_row) for data_row in data):
            df = pd.DataFrame(data, columns=headers)
            df['patient_id'] = patient_id
            df['reclutado_id'] = reclutado_id  # Consistent naming
            print(f"Created DataFrame from {file_path} with shape {df.shape}")
            return df
        else:
            print(f"Column and data length mismatch in {file_path}")
            raise ValueError(f"Column and data length mismatch in {file_path}")
    except Exception as e:
        print(f"Error reading .emt file {file_path}: {e}")
        raise

def save_to_csv(df, table_name, output_dir):
    output_path = os.path.join(output_dir, f"dbo_{table_name}.csv")
    if not os.path.exists(output_path):
        df.to_csv(output_path, index=False)
        print(f"Created new CSV file: {output_path}")
    else:
        existing_df = pd.read_csv(output_path)
        combined_df = pd.concat([existing_df, df], ignore_index=True)
        combined_df.to_csv(output_path, index=False)
        print(f"Appended data to CSV file: {output_path}")

def main():
    base_dir = "/home/blvksh33p/Documents/redcap/data"
    output_dir = os.path.join(base_dir, "csv_output")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")
    else:
        print(f"Using existing output directory: {output_dir}")
    
    for folder in os.listdir(base_dir):
        folder_path = os.path.join(base_dir, folder)
        if os.path.isdir(folder_path) and folder not in ['env', 'csv_output', '__pycache__']:
            print(f"Processing folder: {folder}")
            try:
                patient_info = folder.split("~")
                if len(patient_info) < 2:
                    print(f"Skipping folder due to unexpected name format: {folder}")
                    continue
                patient_id = patient_info[0]
                reclutado_info = patient_info[1].split(" ")
                if len(reclutado_info) < 1:
                    print(f"Skipping folder due to unexpected reclutado format: {folder}")
                    continue
                reclutado_id = reclutado_info[0]  # Consistent naming
                
                inner_dir_name = f"{patient_id}~{reclutado_id} ID RECLUTADO"
                inner_dir = os.path.join(folder_path, inner_dir_name)
                if not os.path.exists(inner_dir):
                    print(f"Directory not found: {inner_dir}")
                    continue
                else:
                    print(f"Processing inner directory: {inner_dir}")

                for file_name in os.listdir(inner_dir):
                    file_path = os.path.join(inner_dir, file_name)
                    if os.path.isfile(file_path):
                        print(f"Found file: {file_path}")
                        table_name = file_name.split('.')[0]
                        if file_name.endswith(".txt"):
                            df = process_text_file(file_path, patient_id, reclutado_id)
                            if df is not None:
                                save_to_csv(df, table_name, output_dir)
                        elif file_name.endswith(".emt"):
                            try:
                                df = process_emt_file(file_path, patient_id, reclutado_id)
                                if df is not None:
                                    save_to_csv(df, table_name, output_dir)
                            except ValueError as e:
                                print(e)
                                continue
                        else:
                            print(f"Skipping file with unsupported extension: {file_name}")
                    else:
                        print(f"Skipping directory inside inner directory: {file_name}")
            except Exception as e:
                print(f"Error processing folder {folder}: {e}")

if __name__ == "__main__":
    main()