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
    col_name_pickup = df.filter(like='pickup_datetime',axis=1).columns[0]
    col_name_dropoff = df.filter(like='dropoff_datetime',axis=1).columns[0]
    df[col_name_pickup] = pd.to_datetime(df[col_name_pickup])
    df[col_name_dropoff] = pd.to_datetime(df[col_name_dropoff])
    return df

@task()
def save_locally(df, year, month, color) -> Path:
    "Save file locally"
    dataset_file = f"{color}_tripdata_{year}_{month}.parquet"
    local_save_path = Path(f"data/{color}/{dataset_file}")
    path = Path(local_save_path)
    df.to_parquet(path)
    return path

@task()
def upload_gcs(path) -> None:
    "Upload file to GCS"
    gcs_block = GcsBucket.load("zoomcamp-gcs-bucket")
    gcs_block.upload_from_path(from_path = path, to_path = path)
    return None


@flow()
def etl_web_to_gcs(year, month, color) -> None:
    """The main ETL function"""
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-0{month}.csv.gz"

    df = read_file(url)
    df_processed = process_data(df)
    path = save_locally(df_processed, year, month, color)
    upload_gcs(path)

@flow()
def etl_parent_flow(year= 2019, 
                    months=[2,3], 
                    color= "yellow") -> None:

    for month in months:
        etl_web_to_gcs(year, month, color)

if __name__ == "__main__":
    etl_parent_flow()