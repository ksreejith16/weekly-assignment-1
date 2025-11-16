import pandas as pd

def main():
    # --- 1. Load Sales Data ---
    csv_path = 'sample.csv' 
    
    try:
        sales_df = pd.read_csv(csv_path)
        print(f"Successfully loaded '{csv_path}'")
    except FileNotFoundError:
        print(f"Error: '{csv_path}' not found.")
        return
        sales_df = pd.DataFrame(dummy_data)

    print("\n--- Original Sales DataFrame (sales_df) Head ---")
    print(sales_df.head())
    print("\n" + "="*50 + "\n")


    # --- 2. Create DataFrames from Lists and Dictionaries ---
    
    # Create from list of lists
    data_list = [
        ['Alice', 25, 'New York'],
        ['Bob', 30, 'Los Angeles'],
        ['Charlie', 22, 'Chicago']
    ]
    columns_list = ['Name', 'Age', 'City']
    df_from_list = pd.DataFrame(data_list, columns=columns_list)
    
    print("--- DataFrame from List (df_from_list) Head ---")
    print(df_from_list.head())

    # Create from dictionary
    data_dict = {
        'Name': ['David', 'Eve', 'Frank'],
        'Age': [35, 28, 40],
        'City': ['Houston', 'Miami', 'Seattle']
    }
    df_from_dict = pd.DataFrame(data_dict)
    
    print("\n--- DataFrame from Dictionary (df_from_dict) Head ---")
    print(df_from_dict.head())
    print("\n" + "="*50 + "\n")


    # --- 3. Filtering and Column Selection ---
    
    # Filter sales_df for transactions from store_id = 1
    store_1_sales = sales_df[sales_df['store_id'] == 1]

    # Filter sales_df for 'high-value' transactions where 'total' amount is greater than 4
    high_value_sales = sales_df[sales_df['total'] > 4]

    # Create a new DataFrame with only 'product_name' and 'quantity' columns
    product_quantity_df = sales_df[['product_name', 'quantity']]

    # Display the first few rows of the new DataFrames
    print("--- First 5 rows of sales from Store ID 1 ---")
    print(store_1_sales.head())

    print("\n--- First 5 rows of high-value sales (total > 4) ---")
    print(high_value_sales.head())

    print("\n--- First 5 rows of product name and quantity ---")
    print(product_quantity_df.head())
    print("\n" + "="*50 + "\n")


    # --- 4. Descriptive Statistics and Grouping ---
    
    # Calculate and display descriptive statistics
    print("--- Descriptive Statistics for sales_df ---")
    print(sales_df.describe())

    # Group by store_id and calculate the sum of total sales
    total_sales_by_store = sales_df.groupby('store_id')['total'].sum()

    # Group by store_id and calculate the average total amount
    average_basket_size_by_store = sales_df.groupby('store_id')['total'].mean()

    # Display the results
    print("\n--- Total Sales by Store ID ---")
    print(total_sales_by_store)

    print("\n--- Average Basket Size by Store ID ---")
    print(average_basket_size_by_store)
    print("\n" + "="*50 + "\n")


    # --- 5. Handling Missing Values ---
    
    # Check for missing values before handling
    print("--- Missing values before handling ---")
    print(sales_df.isnull().sum())

    # Handle missing values
    for column in sales_df.columns:
        if sales_df[column].isnull().any():
            if pd.api.types.is_numeric_dtype(sales_df[column]):
                # Fill numerical missing values with the mean
                mean_val = sales_df[column].mean()
                sales_df[column] = sales_df[column].fillna(mean_val)
                print(f"Filled missing numerical values in '{column}' with its mean ({mean_val}).")
            else:
                # Fill categorical missing values with 'Unknown'
                sales_df[column] = sales_df[column].fillna('Unknown')
                print(f"Filled missing categorical values in '{column}' with 'Unknown'.")

    # Verify that missing values have been handled
    print("\n--- Missing values after handling ---")
    print(sales_df.isnull().sum())
    print("\n" + "="*50 + "\n")
    
    
    # --- 6. Convert customer_id to Integer Type ---
    
    print("--- Converting customer_id to Int64 ---")
    # Use Int64 (capital I) to support <NA> values if any remained, though we filled them
    sales_df['customer_id'] = sales_df['customer_id'].round().astype('Int64')
    print(sales_df)
    print("\n" + "="*50 + "\n")


    # --- 7. Convert Date Column to Datetime ---
    
    # Display the current data types
    print("--- Data types before date conversion ---")
    sales_df.info()

    # Convert the 'date' column to datetime objects
    sales_df['date'] = pd.to_datetime(sales_df['date'], errors='coerce')
    print("\nConverted 'date' column to datetime.")

    # Display the data types again to confirm
    print("\n--- Data types after date conversion ---")
    sales_df.info()
    print("\n" + "="*50 + "\n")

    
    # --- 8. Save Cleaned Data to JSON ---
    
    output_filename = "sales_clean.json"
    sales_df.to_json(output_filename, orient="records", indent=4, date_format='iso')
    print(f"Successfully saved cleaned data to {output_filename}")


if __name__ == "__main__":
    main()