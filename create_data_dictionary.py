import pandas as pd
import os

def infer_redcap_field_type(series):
    """Infer REDCap field type from a pandas series."""
    if pd.api.types.is_integer_dtype(series):
        return 'text', 'integer', str(series.min()), str(series.max()) if not series.empty else ''
    elif pd.api.types.is_float_dtype(series):
        return 'text', 'number', '', ''
    elif pd.api.types.is_datetime64_any_dtype(series):
        return 'text', 'datetime_seconds', '', ''
    elif pd.api.types.is_string_dtype(series):
        return 'text', '', '', ''
    return 'text', '', '', ''

def generate_unique_field_name(form_name, column_name, seen_fields):
    """Generate a unique field name by combining form name and column name."""
    base_name = f"{form_name.lower().replace(' ', '_')}_{column_name.lower()}".replace('.', '_')
    unique_name = base_name
    counter = 1
    while unique_name in seen_fields:
        unique_name = f"{base_name}_{counter}"
        counter += 1
    return unique_name

def generate_data_dictionary(csv_directory):
    """Generate a REDCap data dictionary from all CSV files in a directory."""
    data_dictionary = []
    seen_fields = set()
    reclutado_id_added = False
    patient_id_added = False
    mtb_added = False

    # Add reclutado_id field first
    data_dictionary.append({
        'Variable / Field Name': 'reclutado_id',
        'Form Name': 'Main',
        'Section Header': '',
        'Field Type': 'text',
        'Field Label': 'Reclutado ID',
        'Choices, Calculations, OR Slider Labels': '',
        'Field Note': '',
        'Text Validation Type OR Show Slider Number': '',
        'Text Validation Min': '',
        'Text Validation Max': '',
        'Identifier?': 'y',
        'Branching Logic (Show field only if)': '',
        'Required Field?': 'y',
        'Custom Alignment': '',
        'Question Number (surveys only)': '',
        'Matrix Group Name': '',
        'Matrix Ranking?': '',
        'Field Annotation': ''
    })
    seen_fields.add('reclutado_id')

    # Add patient_id field
    data_dictionary.append({
        'Variable / Field Name': 'patient_id',
        'Form Name': 'Main',
        'Section Header': '',
        'Field Type': 'text',
        'Field Label': 'Patient ID',
        'Choices, Calculations, OR Slider Labels': '',
        'Field Note': '',
        'Text Validation Type OR Show Slider Number': 'integer',
        'Text Validation Min': '',
        'Text Validation Max': '',
        'Identifier?': '',
        'Branching Logic (Show field only if)': '',
        'Required Field?': '',
        'Custom Alignment': '',
        'Question Number (surveys only)': '',
        'Matrix Group Name': '',
        'Matrix Ranking?': '',
        'Field Annotation': ''
    })
    seen_fields.add('patient_id')

    for filename in os.listdir(csv_directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(csv_directory, filename)
            if not os.access(file_path, os.R_OK):
                print(f"Warning: '{filename}' cannot be accessed. Skipping.")
                continue

            try:
                df = pd.read_csv(file_path)
                if df.empty:
                    print(f"Warning: '{filename}' is empty. Skipping.")
                    continue

                form_name = filename.replace('.csv', '').replace('_', ' ').capitalize()

                for column in df.columns:
                    if column in ['reclutado_id', 'patient_id']:
                        continue  # Skip these as they're already added
                    elif filename == 'dbo_05_Massas.csv' and column == 'mTB' and not mtb_added:
                        unique_field_name = 'mtb'
                        mtb_added = True
                    else:
                        unique_field_name = generate_unique_field_name(form_name, column, seen_fields)
                    
                    seen_fields.add(unique_field_name)

                    field_type, validation_type, val_min, val_max = infer_redcap_field_type(df[column])

                    data_dictionary.append({
                        'Variable / Field Name': unique_field_name,
                        'Form Name': form_name,
                        'Section Header': '',
                        'Field Type': field_type,
                        'Field Label': column,
                        'Choices, Calculations, OR Slider Labels': '',
                        'Field Note': '',
                        'Text Validation Type OR Show Slider Number': validation_type,
                        'Text Validation Min': val_min,
                        'Text Validation Max': val_max,
                        'Identifier?': '',
                        'Branching Logic (Show field only if)': '',
                        'Required Field?': '',
                        'Custom Alignment': '',
                        'Question Number (surveys only)': '',
                        'Matrix Group Name': '',
                        'Matrix Ranking?': '',
                        'Field Annotation': ''
                    })

            except pd.errors.EmptyDataError:
                print(f"Warning: No data to parse in '{filename}' - file may be empty or improperly formatted.")
            except Exception as e:
                print(f"Error reading '{filename}': {e}")

    return pd.DataFrame(data_dictionary)

# Example usage:
csv_directory = 'csv_output/'
data_dict_df = generate_data_dictionary(csv_directory)
data_dict_df.to_csv('redcap_data_dictionary.csv', index=False)
print("Data dictionary saved to redcap_data_dictionary.csv")