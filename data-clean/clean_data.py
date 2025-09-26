import pandas as pd
import json


def load_pfas_data(filepath):
    """Loads the main dataset containing PFAS measurements."""
    try:
        df = pd.read_csv(filepath, low_memory=False, encoding='latin1')
        print(f"✅ Successfully loaded main data from '{filepath}' ({len(df)} rows).")
        return df
    except FileNotFoundError:
        print(f"❌ Error: The file '{filepath}' was not found.")
        return None


def load_pfas_info(filepath):
    """Loads the supplementary PFAS information dataset."""
    try:
        df = pd.read_csv(filepath)
        print(f"✅ Successfully loaded PFAS info from '{filepath}' ({len(df)} rows).")
        if 'cas' in df.columns:
            df.rename(columns={'cas': 'cas_id'}, inplace=True)
        return df
    except FileNotFoundError:
        print(f"❌ Error: The file '{filepath}' was not found.")
        return None


def clean_and_unpack_pfas(df):
    """
    Transforms the main DataFrame by unpacking the 'pfas_values' JSON column.
    """
    print("\nProcessing and unpacking JSON data...")

    # Step 1: Parse the JSON string safely
    def parse_json(x):
        if pd.isna(x): return []
        try:
            return json.loads(x)
        except (json.JSONDecodeError, TypeError):
            return []

    df['pfas_values'] = df['pfas_values'].apply(parse_json)

    # Step 2: Explode the DataFrame
    df_exploded = df.explode('pfas_values')
    df_exploded.dropna(subset=['pfas_values'], inplace=True)

    # Step 3: Normalize the JSON
    pfas_normalized = pd.json_normalize(df_exploded['pfas_values'])

    # Step 4: Combine the data
    df_exploded.reset_index(drop=True, inplace=True)
    pfas_normalized.reset_index(drop=True, inplace=True)
    if 'unit' in df_exploded.columns and 'unit' in pfas_normalized.columns:
        df_final = df_exploded.drop(columns=['pfas_values']).join(pfas_normalized, rsuffix='_pfas')
    else:
        df_final = df_exploded.drop(columns=['pfas_values']).join(pfas_normalized)

    # Step 5: Clean up data types
    for col in ['value', 'less_than']:
        if col in df_final.columns:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce')

    # --- ADDED SECTION: Remove rows where 'value' is null ---
    # This filters the dataset to keep only the rows with actual measurements.
    original_rows = len(df_final)
    df_final.dropna(subset=['value'], inplace=True)
    print(f"✅ Filtered out rows with null values. Kept {len(df_final)} of {original_rows} rows.")
    # -----------------------------------------------------------

    print(f"✅ Unpacking complete. Dataset now has {len(df_final)} single-observation rows.")
    return df_final


# --- Main script execution ---
if __name__ == "__main__":
    main_data_file = 'data/pdh_export.csv'
    pfas_info_file = 'data/pfas_info.csv'
    output_file = 'cleaned_pfas_data_tidy.csv'

    main_df = load_pfas_data(main_data_file)
    info_df = load_pfas_info(pfas_info_file)

    if main_df is not None:
        unpacked_df = clean_and_unpack_pfas(main_df)
        final_df = unpacked_df
        if info_df is not None:
            print("\nMerging with PFAS information...")
            final_df = pd.merge(unpacked_df, info_df, on='cas_id', how='left')
            print("✅ Merge complete.")

        final_columns_to_keep = [
            'lat', 'lon', 'city', 'country', 'matrix', 'year', 'cas_id',
            'unit_pfas', 'Use categories', 'sub-use', 'applications', 'value',
        ]

        if 'city' in final_df.columns:
            print(f"\nOriginal row count before filtering: {len(final_df)}")
            # Use the robust filtering method
            condition = final_df['city'].str.strip().str.lower() == 'amsterdam'
            final_df = final_df[condition]
            print(f"✅ Filtered for Amsterdam. {len(final_df)} rows remain.")
        else:
            print("\n⚠️ Warning: 'city' column not found. Skipping city filter.")

        # Check which columns actually exist before trying to filter
        existing_columns = [col for col in final_columns_to_keep if col in final_df.columns]
        final_df = final_df[existing_columns]
        print(f"\n✅ Final DataFrame filtered to keep only {len(existing_columns)} specified columns.")

        final_df.to_csv(output_file, index=False)

        print(f"\n🎉 Success! Cleaned and merged data saved to '{output_file}'.")
        print("\nHere's a preview of your new tidy data:")
        print(final_df.head())