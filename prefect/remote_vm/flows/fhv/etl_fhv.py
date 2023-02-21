import pandas as pd
from sqlalchemy import create_engine
import os
from time import time
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task()
def read_file(url) -> pd.DataFrame:
    """Fetch File"""
    df = pd.read_csv(url)
    return df

@task()
def process_data(df) -> pd.DataFrame:
    """Process data"""
    col_name_pickup = df.filter(like='pick',axis=1).columns[0]
    col_name_dropoff = df.filter(like='drop',axis=1).columns[0]
    df[col_name_pickup] = pd.to_datetime(df[col_name_pickup])
    df[col_name_dropoff] = pd.to_datetime(df[col_name_dropoff])
    df['PUlocationID'] = df['PUlocationID'].astype(float)
    df['DOlocationID'] = df['DOlocationID'].astype(float)
    return df

@task()
def save_locally(df, year, month) -> Path:
    "Save file locally"
    dataset_file = f"fhv_tripdata_{year}_{month}.parquet"
    local_save_path = Path(f"data/fhv/{dataset_file}")
    path = Path(local_save_path)
    df.to_parquet(path)
    return path

@task()
def upload_gcs(path) -> None:
    "Upload file to GCS"
    gcs_block = GcsBucket.load("gcp-bucket")
    gcs_block.upload_from_path(from_path = path, to_path = path)
    return None


@flow()
def fhv_gcs(year, month) -> None:
    """The main ETL function"""
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02}.csv.gz"

    df = read_file(url)
    df_processed = process_data(df)
    path = save_locally(df_processed, year, month)
    upload_gcs(path)

@flow()
def etl_fhv(year, months) -> None:

    for month in months:
        fhv_gcs(year, month)

if __name__ == "__main__":
    etl_fhv()