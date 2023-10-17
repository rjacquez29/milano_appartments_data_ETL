import os
import time
from datetime import datetime
from dotenv import find_dotenv, load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict
import pandas as pd
from pathlib import Path


def load_to_bigquery():
    load_dotenv(find_dotenv())

    PROJECT_ID = os.environ["PROJECT_ID"]
    DATASET_NAME = os.environ["DATASET_NAME"]

    credentials = service_account.Credentials.from_service_account_file(
        os.environ["SA_ACCOUNT_PATH"],
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    client = bigquery.Client(project=f"{PROJECT_ID}", credentials=credentials)
    dataset_id = f"{PROJECT_ID}.{DATASET_NAME}"

    schemas = dict(
        milano_coordinates_updated=[
            bigquery.SchemaField("Address", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Latitude", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("Longitude", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("Complete_Address", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Zipcode", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("Municipio", "FLOAT64", mode="NULLABLE"),
        ],
        milano_annunci=[
            bigquery.SchemaField("Date", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("Price", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("Building_expenses", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("Rooms", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("Bathrooms", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("kitchen_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Floor_area", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("Floor_level", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Year_of_construction", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("Elevator", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("Wheelchair_accessible", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("Total_building_floor", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("Heating", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Heating_source", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Energy_class", "STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                "kw_per_m2", "FLOAT64", mode="NULLABLE"
            ),  # Renamed from "kw/m2" to "kw_per_m2" to make it a valid column name
            bigquery.SchemaField("Property_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Property_class", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("features", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Neighborhood", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Address", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Condition", "STRING", mode="NULLABLE"),
        ],
    )

    for table_name, schema in schemas.items():
        table = bigquery.Table(
            f"{PROJECT_ID}.{DATASET_NAME}.{table_name:s}", schema=schema
        )
        try:
            table = client.create_table(table)  # Make an API request.
            print(
                "{} | SETUP | Created table {}.{}.{}".format(
                    datetime.now().isoformat(),
                    table.project,
                    table.dataset_id,
                    table.table_id,
                )
            )
        except Conflict as e:
            table = client.get_table(
                f"{PROJECT_ID}.{DATASET_NAME}.{table_name:s}"
            )  # Make an API request.
            table.schema = schema
            client.update_table(table, fields=["schema"])  # Make an API request.
            print(
                "{} | SETUP | Updated schema for {}.{}.{}".format(
                    datetime.now().isoformat(),
                    table.project,
                    table.dataset_id,
                    table.table_id,
                )
            )

    for table_name, schema in schemas.items():
        source_df = pd.read_excel(Path(".").resolve() / "Data" / f"{table_name}.xlsx")
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE",
        )

        job = client.load_table_from_dataframe(
            source_df,
            f"{PROJECT_ID}.{DATASET_NAME}.{table_name:s}",
            job_config=job_config,
        )
        job.result()

        table = client.get_table(f"{PROJECT_ID}.{DATASET_NAME}.{table_name:s}")
        print(f"{datetime.now().isoformat()} | SYNC  | Table {table_name:s} ok")

    print(f"{datetime.now().isoformat()} | SYNC  | Tables updated")

    return None
