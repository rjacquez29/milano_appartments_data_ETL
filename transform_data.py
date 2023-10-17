# Import necessary libraries
import pandas as pd
from pathlib import Path
from termcolor import colored
import re

import translations
import immobiliare_scraping
import geocoder


def split_features(text):
    try:
        # Check if text is not NaN and is a string or can be converted to a string
        if pd.notna(text):
            text = str(text)
            # Split on lowercase to uppercase transitions and words starting with uppercase letters
            cleaned_text = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
            cleaned_text = re.sub(r"(\b[A-Z][a-z]+\b)", r", \1", cleaned_text)
            return cleaned_text
        else:
            return ""
    except Exception as e:
        print(f"Error in split_features: {str(e)}")
        return ""


def clean_data(df: pd.DataFrame):
    # Create an empty dataframe for cleaned data
    clean_data = pd.DataFrame()

    ############# Date

    # Extract the date from the 'Riferimento e Data annuncio' column
    clean_data["Date"] = df["Riferimento e Data annuncio"].str.extract(
        r"(\d{2}/\d{2}/\d{4})"
    )

    # Convert the 'Date' column to datetime type
    clean_data["Date"] = pd.to_datetime(
        clean_data["Date"], format="%d/%m/%Y", errors="coerce"
    )

    ############# Price

    # Clean and convert the 'prezzo' column to numeric format
    clean_data["Price"] = pd.to_numeric(
        df.prezzo.str.replace(".", "").str.strip("€ "), errors="coerce"
    )
    clean_data["Building_expenses"] = pd.to_numeric(
        df["spese condominio"].str.extract(r"(\d+)")[0], errors="coerce"
    )

    ############# Rooms and building info

    # Split the 'locali' column and extract room, bathroom, kitchen, floor area, etc.
    rooms = df.locali.str.split(", ", expand=True)
    clean_data["Rooms"] = rooms[0].str.extract(r"(\d+)")
    clean_data["Bathrooms"] = rooms[2].str.extract(r"(\d+)")
    clean_data["kitchen_type"] = (
        rooms[3].str.strip().replace(translations.kitchen_types)
    )
    clean_data["Floor_area"] = df.superficie.str.extract(r"(\d+)")
    clean_data["Floor_level"] = df["piano"].str.split(",", expand=True)[0]
    clean_data["Elevator"] = df["piano"].str.contains("con ascensore", case=False)
    clean_data["Wheelchair_accessible"] = df["piano"].str.contains(
        "con accesso disabili", case=False
    )
    clean_data["Total_building_floor"] = df["totale piani edificio"].str.extract(
        r"(\d+)"
    )
    clean_data["Year_of_construction"] = df["anno di costruzione"].astype("Int64")
    clean_data["Condition"] = df["stato"].str.strip().replace(translations.condition)

    ############### Heating and Energy efficiency

    df.riscaldamento.str.split(",", expand=True)[2].value_counts().index
    clean_data["Heating"] = (
        df.riscaldamento.str.split(",", expand=True)[0]
        .str.strip()
        .replace(translations.heating)
    )
    clean_data["Heating_source"] = (
        df.riscaldamento.str.split(",", expand=True)[2]
        .str.strip()
        .replace(translations.heating_source)
    )
    clean_data["Energy_class"] = df["Efficienza energetica"].str.extract(r"([A-Z≥])")
    clean_data["kw_per_m2"] = df["Efficienza energetica"].str.extract(r"([0-9,.]+)")
    clean_data["kw_per_m2"] = pd.to_numeric(clean_data["kw_per_m2"], errors="coerce")

    ############### Property typology

    # Split the 'tipologia' column and extract property type and class
    property_type = df.tipologia.str.split("|", expand=True)
    clean_data["Property_type"] = property_type[0].str.strip()
    clean_data["Property_type"] = (
        clean_data["Property_type"].str.strip().replace(translations.property_types)
    )
    clean_data["Property_class"] = property_type[2].str.strip()
    clean_data["Property_class"] = (
        clean_data["Property_class"].str.strip().replace(translations.property_class)
    )

    ################ Features

    # Apply the clean_column function to the column values
    clean_data["features"] = df["altre caratteristiche"].apply(split_features)

    clean_data["features"] = clean_data["features"].str.lstrip(", ")
    clean_data["features"] = clean_data["features"].apply(
        lambda text: re.sub(r"(\bPVC)([A-Za-z]+)", r"\1 \2", text)
        if isinstance(text, str)
        else text
    )
    clean_data["features"] = clean_data["features"].str.replace(" , ", ", ")

    ################# Address

    clean_data["Neighborhood"] = df.neighborhood.str.title()
    clean_data["Address"] = df.address

    # Drop rows with NaN values in 'Price' and 'Floor_area' columns
    clean_data.dropna(subset=["Price", "Floor_area"], inplace=True)
    clean_data.drop_duplicates(inplace=True)

    # Print a success message
    print(colored(f"Data cleaning completed successfully!", "green", attrs=["bold"]))
    return clean_data


def geocode_address(address: pd.DataFrame):
    # Merge dataframe with existing address coordinates
    address_coordinates = pd.read_excel(
        Path(".").resolve() / "Data" / "milano_coordinates_updated.xlsx"
    )

    # Look for addresses that are not present in address_coordinates
    unique_addresses = pd.DataFrame(address["Address"].unique(), columns=["Address"])

    # Find addresses that do not exist in address_coordinates
    non_existing_addresses = unique_addresses[
        ~unique_addresses["Address"].isin(address_coordinates["Address"])
    ]

    # Call geocoding function to get new coordinates for unique addresses
    new_coordinates = geocoder.geocode(non_existing_addresses["Address"])

    # Merge the new coordinates with the input DataFrame (address)
    address = address.merge(new_coordinates, on="Address", how="left")

    # Apply the modified function to the 'Complete_Address' column
    address[["Zipcode", "Municipio"]] = address["Complete_Address"].apply(
        lambda x: pd.Series(geocoder.extract_zipcode_and_municipio(x))
    )

    # Reset index and drop old index for non_existing_addresses
    non_existing_addresses.reset_index(drop=True, inplace=True)

    # Update the address coordinates
    combined_coordinate = pd.concat([address_coordinates, new_coordinates], axis=0)

    # Reset index and drop old index for combined_coordinate
    combined_coordinate.reset_index(drop=True, inplace=True)

    combined_coordinate[["Zipcode", "Municipio"]] = address["Complete_Address"].apply(
        lambda x: pd.Series(geocoder.extract_zipcode_and_municipio(x))
    )

    print("milano_coordinates_updated.xlsx created with new coordinates\n")

    return address
