import pandas as pd
import numpy as np # Import numpy to use the ceiling function for rounding up

# Define the file names
food_file = 'total_pfas_per_food.csv'
soil_file = 'cleaned_pfas_data_tidy.csv'  # Assuming this is the name of your second file

try:
    # Load the datasets, using the first row as the header
    food_df = pd.read_csv(food_file)
    soil_df = pd.read_csv(soil_file)

    # Convert the food data into a dictionary for easy lookup
    # e.g., {'Apple': 1.2376, ...}
    food_pfas_dict = food_df.set_index('Food category')['total_pfas_per_food'].to_dict()

    # Get the PFAS concentration from the 'value' column in the soil data
    soil_concentration = soil_df['value']

    # Calculate and add a new column for each food item
    for food_name, total_pfas in food_pfas_dict.items():
        # Create a clean column name (e.g., 'apple_per_kg_soil')
        # Takes the first word of the food name and makes it lowercase
        clean_col_name = f"pfa equivalents in {food_name.split(' ')[0].lower()}s"

        # Calculate how many food units are equivalent to 1kg of soil
        calculation_result = (soil_concentration / total_pfas) / 100
        soil_df[clean_col_name] = np.ceil(calculation_result)

    # Display the first few rows of the final DataFrame to check the results
    print("Preview of the updated data with food unit equivalents:")
    print(soil_df.head())

    # Save the updated data to a new CSV file
    output_filename = 'soil_data_with_food_equivalents.csv'
    soil_df.to_csv(output_filename, index=False)

    print(f"\nSuccessfully saved the results to '{output_filename}'")

except FileNotFoundError as e:
    print(f"Error: {e}. Please ensure both '{food_file}' and '{soil_file}' are in the correct directory.")
except KeyError as e:
    print(f"Error: A column name was not found: {e}. Please check your CSV files for the correct headers.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")