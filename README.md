# Milano Real Estate Data Scraping and Analysis

## Overview

This repository contains Python scripts for scraping real estate data in Milano, Italy, from immobiliare.it, transforming the data, and loading it into Google BigQuery for analysis. The project is designed to gather real estate listings, clean and preprocess the data, and store it for further analysis.

## Project Structure

The repository is organized into the following main components:

1. **imobiliare_scraping.py**: This script is responsible for scraping real estate listings from immobiliare.it. It retrieves information such as price, location, property type, and more.

2. **transform_data.py**: This script handles data preprocessing and transformation. It cleans the scraped data, extracts relevant features, and prepares it for further analysis.

3. **load_bigquery.py**: This script loads the cleaned data into Google BigQuery for storage and analysis. It utilizes the Google Cloud BigQuery service to store the data.

4. **translations.py**: This module contains translation dictionaries for translating property types, property classes, kitchen types, heating types, and heating sources.

5. **main.py**: The main script that orchestrates the entire data pipeline. It scrapes new listings, combines them with existing data, cleans and preprocesses the data, and loads it into BigQuery.

## Getting Started

To use this project, follow these steps:

1. **Clone the Repository**:
   

`git clone https://github.com/rjacquez29/milano_appartments_data_ETL.git`\
`cd milano_appartments_data_ETL`



2. **Install Dependencies**:

Make sure you have Python 3.x installed. Install the required Python packages using pip:


`pip install -r requirements.txt`


3. **Set Up Google Cloud Credentials**:

Before using `load_bigquery.py`, make sure to set up your Google Cloud credentials and provide the necessary environment variables. Refer to the Google Cloud documentation for instructions on how to do this.

4. **Run the Main Script**:

Execute the main script to start the data scraping and processing pipeline:


`python main.py`


This script will scrape new listings, clean and preprocess the data, and load it into Google BigQuery.

## Configuration

You can configure various parameters, such as the number of pages to scrape, in the `main.py` script. Additionally, you can customize translations and data cleaning rules in the `translations.py` and `transform_data.py` files.


## Acknowledgments

Special thanks to [geopy](https://geopy.readthedocs.io/en/stable/), [Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/), [Tommasso Ramella](https://github.com/tommella90/milano-housing-price/) and other open-source libraries used in this project.

## Contact

If you have any questions or suggestions, please feel free to contact me:

Raymund Jacquez
Email: r.jacquez@outlook.com
LinkedIn: https://www.linkedin.com/in/raymund-jacquez-ab3023189/

Happy coding!
