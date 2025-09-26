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

    df = df.reset_index()

    def parse_json(x):
        if pd.isna(x): return []
        try:
            return json.loads(x)
        except (json.JSONDecodeError, TypeError):
            return []

    df['pfas_values'] = df['pfas_values'].apply(parse_json)
    df_exploded = df.explode('pfas_values')
    df_exploded.dropna(subset=['pfas_values'], inplace=True)
    pfas_normalized = pd.json_normalize(df_exploded['pfas_values'])
    df_exploded.reset_index(drop=True, inplace=True)
    pfas_normalized.reset_index(drop=True, inplace=True)
    df_final = df_exploded.drop(columns=['pfas_values']).join(pfas_normalized, rsuffix='_pfas')

    for col in ['value', 'less_than']:
        if col in df_final.columns:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce')

    df_final.dropna(subset=['value'], inplace=True)
    print(f"✅ Filtered out rows with null values, leaving {len(df_final)} rows.")

    # --- KEY CHANGE: Filter out unrealistically high values ---
    rows_before_outlier_filter = len(df_final)
    df_final = df_final[df_final['value'] <= 100000].copy()
    rows_removed = rows_before_outlier_filter - len(df_final)
    if rows_removed > 0:
        print(f"✅ Filtered out {rows_removed} outlier row(s) with values > 100,000.")
    # --- END OF CHANGE ---

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
        unpacked_df = clean_and_unpack_pfas(main_df.copy())

        # --- DEBUGGING STEP (NOW SHOWS HIGHEST *VALID* VALUE) ---
        print("\n🕵️  Debugging: Locating highest valid value and its original CSV source...")
        if not unpacked_df.empty:
            highest_value_row = unpacked_df.loc[unpacked_df['value'].idxmax()]
            original_index = int(highest_value_row['index'])
            csv_row_number = original_index + 2
            original_raw_json = main_df.loc[original_index, 'pfas_values']

            print("\n--- Top Unpacked Measurement (after filtering) ---")
            print(highest_value_row[['value', 'unit_pfas', 'cas_id', 'city', 'country']])

            print(f"\n--- Original Source (Row {csv_row_number} in '{main_data_file}') ---")
            print(original_raw_json)
            print("-" * 40)
        else:
            print("No data with values <= 100,000 found.")
        # --- END OF DEBUGGING STEP ---

        final_df = unpacked_df
        if info_df is not None:
            print("\nMerging with PFAS information...")
            if 'index' in final_df.columns:
                final_df = final_df.drop(columns=['index'])
            final_df = pd.merge(final_df, info_df, on='cas_id', how='left')
            print("✅ Merge complete.")

        final_columns_to_keep = [
            'lat', 'lon', 'city', 'country', 'matrix', 'year', 'cas_id',
            'unit_pfas', 'Use categories', 'sub-use', 'applications', 'value',
        ]

        if 'city' in final_df.columns:
            print(f"\nOriginal row count before filtering: {len(final_df)}")
            condition = final_df['city'].str.strip().str.lower() == 'amsterdam'
            final_df = final_df[condition]
            print(f"✅ Filtered for Amsterdam. {len(final_df)} rows remain.")
        else:
            print("\n⚠️ Warning: 'city' column not found. Skipping city filter.")

        existing_columns = [col for col in final_columns_to_keep if col in final_df.columns]
        if 'index' in final_df.columns:
            final_df = final_df.drop(columns=['index'])
        final_df = final_df[existing_columns]

        print(f"\n✅ Final DataFrame filtered to keep only {len(existing_columns)} specified columns.")

        final_df.to_csv(output_file, index=False)

        print(f"\n🎉 Success! Cleaned and merged data saved to '{output_file}'.")
        print("\nHere's a preview of your new tidy data:")
        print(final_df.head())