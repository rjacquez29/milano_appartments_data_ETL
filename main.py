import transform_data
import immobiliare_scraping
from pathlib import Path
import pandas as pd
from load_bigquery import load_to_bigquery

new_listings = immobiliare_scraping.main()

if len(new_listings) != 0:
    # Load the existing data from the Excel file
    file_path = Path(".").resolve() / "Data" / "milano_annunci.xlsx"
    all_clean_data = pd.read_excel(file_path)

    # Clean the new listings data
    clean_df = transform_data.clean_data(new_listings)
    transform_data.geocode_address(clean_df)

    # Drop rows with NaN values in 'Price' and 'Floor_area' columns
    clean_df.dropna(subset=["Price", "Floor_area"], inplace=True)

    # Reset the index for both DataFrames
    clean_df.reset_index(drop=True, inplace=True)
    all_clean_data.reset_index(drop=True, inplace=True)

    # Concatenate the DataFrames vertically
    combined_df = pd.concat([all_clean_data, clean_df], axis=0)

    # Reset the index for the combined DataFrame
    combined_df.reset_index(drop=True, inplace=True)

    # Save the combined DataFrame to the Excel file
    combined_df.to_excel(file_path, index=False)


load_to_bigquery()
