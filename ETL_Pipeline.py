import pandas as pd
import json
import os

def run_etl_pipeline(input_csv_path='sample.csv', output_json_path='sales_clean.json'):
    """
    Runs a mini ETL pipeline:
    1. EXTRACT: Reads data from a CSV file.
    2. TRANSFORM: Cleans and transforms the data.
    3. LOAD: Saves the transformed data to a JSON file.
    """
    
    print(f"Starting ETL pipeline...")

    # --- 1. EXTRACT ---
    # Check if the input file exists
    if not os.path.exists(input_csv_path):
        print(f"Error: Input file not found at '{input_csv_path}'")
        return

    print(f"EXTRACT: Reading data from '{input_csv_path}'")
    try:
        # Read the CSV data into a pandas DataFrame
        sales_df = pd.read_csv(input_csv_path)
        print("Data extracted successfully.")
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    # Display info before transformation
    print("\n--- Data BEFORE Transform ---")
    print(sales_df.info())
    print("\nMissing values BEFORE transform:")
    print(sales_df.isnull().sum())

    # --- 2. TRANSFORM ---
    print("\nTRANSFORM: Cleaning and transforming data...")
    
    # Create a copy to avoid SettingWithCopyWarning
    transformed_df = sales_df.copy()

    # Fill missing 'customer_id' with the mean
    if 'customer_id' in transformed_df.columns and transformed_df['customer_id'].isnull().any():
        mean_customer_id = transformed_df['customer_id'].mean()
        transformed_df['customer_id'].fillna(mean_customer_id, inplace=True)
        print(f"- Filled missing 'customer_id' with mean ({mean_customer_id:.2f})")

    # Fill missing 'promo_code' with 'Unknown'
    if 'promo_code' in transformed_df.columns and transformed_df['promo_code'].isnull().any():
        transformed_df['promo_code'].fillna('Unknown', inplace=True)
        print("- Filled missing 'promo_code' with 'Unknown'")

    # Convert data types (as in cells A87CxWvSv-Xs and 46e2f61a)
    # Convert 'customer_id' to integer
    if 'customer_id' in transformed_df.columns:
        transformed_df['customer_id'] = transformed_df['customer_id'].round().astype('Int64')
        print("- Converted 'customer_id' to Int64")

    # Convert 'date' column to datetime objects
    if 'date' in transformed_df.columns:
        transformed_df['date'] = pd.to_datetime(transformed_df['date'], errors='coerce')
        print("- Converted 'date' column to datetime objects")

    print("Transformation complete.")

    # Display info after transformation
    print("\n--- Data AFTER Transform ---")
    print(transformed_df.info())
    print("\nMissing values AFTER transform:")
    print(transformed_df.isnull().sum())

    # --- 3. LOAD ---
    print(f"\nLOAD: Saving transformed data to '{output_json_path}'")
    try:
        # Save the cleaned DataFrame to a JSON file (as in cell odWMdmoz9kR5)
        transformed_df.to_json(output_json_path, orient="records", indent=4, date_format='iso')
        print(f"Successfully saved clean data to '{output_json_path}'")
    except Exception as e:
        print(f"Error saving JSON: {e}")

    print("\nETL pipeline finished.")

# --- Main execution ---
if __name__ == "__main__":
    # Define the file paths
    csv_file = 'sample.csv'
    json_file = 'sales_clean.json'
    
    # Run the pipeline
    run_etl_pipeline(input_csv_path=csv_file, output_json_path=json_file)