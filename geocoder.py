import time
from geopy.geocoders import Nominatim
import geopy.geocoders
import certifi
import ssl
import pandas as pd
from termcolor import colored
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError
import re

ctx = ssl.create_default_context(cafile=certifi.where())
geopy.geocoders.options.default_ssl_context = ctx


def geocode(address_list: pd.DataFrame):
    """
    Geocode a list of addresses and store the latitude and longitude information in a DataFrame.

    Parameters:
        address_list (DataFrame): A DataFrame containing a column named 'Address' with the addresses to geocode.

    Returns:
        DataFrame: A DataFrame with 'Address', 'Latitude', and 'Longitude' columns containing geocoded information.
    """
    total_lenght = len(address_list)

    print(
        colored(
            f"Fetching coordinates for {total_lenght} addresses.",
            "blue",
            attrs=["bold"],
        )
    )

    # Record the start time
    start_time = time.time()

    # Initialize the geolocator
    geolocator = Nominatim(user_agent="my_geocoder")

    # Initialize empty lists for latitude and longitude
    latitudes = []
    longitudes = []
    complete_address = []

    unsuccessful_count, counter, attempt = 0, 0, 0

    location = None

    # Iterate over each address
    for n in address_list:
        print(f"  {counter + 1} of {total_lenght}", end="\r")

        try:
            # Perform geocoding for the address
            location = geolocator.geocode(f"{n}, Municipio , Milano, Lombardia, Italia")
        except (GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable):
            unsuccessful_count += 1
            attempt += 1
            if attempt == 5:
                time.sleep(120)
                attempt = 0
            pass

        # Extract the latitude and longitude if the location is found
        if location is not None:
            latitudes.append(location.latitude)
            longitudes.append(location.longitude)
            complete_address.append(location.address)
        else:
            latitudes.append(None)
            longitudes.append(None)
            complete_address.append(None)

        counter += 1

    # Create a DataFrame to store the results
    coordinates = pd.DataFrame(
        {
            "Address": address_list,
            "Latitude": latitudes,
            "Longitude": longitudes,
            "Complete_Address": complete_address,
        }
    )

    # Record the end time
    end_time = time.time()

    # Calculate the elapsed time in minutes
    elapsed_time_minutes = (end_time - start_time) / 60

    # Print the elapsed time in minutes
    print(f"\nElapsed time: {elapsed_time_minutes:.2f} minutes")
    print(f"Unable to geocode {unsuccessful_count} of {len(address_list)}.\n")

    return coordinates


def extract_zipcode_and_municipio(address):
    try:
        # Convert the input to a string to ensure it's a valid string or bytes-like object
        address_str = str(address)

        # Use regular expressions to find the zipcode and municipio
        zipcode_match = re.search(r"\b\d{5}\b", address_str)
        municipio_match = re.search(r"Municipio (\d+)", address_str)

        # Initialize variables for zipcode and municipio
        zipcode = None
        municipio = None

        # Check if matches were found
        if zipcode_match:
            zipcode = zipcode_match.group(0)
        if municipio_match:
            municipio = municipio_match.group(1)

        return zipcode, municipio
    except Exception as e:
        # Handle the exception, e.g., by returning a default value or None
        print(f"Error extracting zipcode and municipio: {str(e)}")
        return (None, None)
