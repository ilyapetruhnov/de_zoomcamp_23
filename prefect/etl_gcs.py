import pandas as pd
import argparse
from sqlalchemy import create_engine
import os
from time import time
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task()
def download_file(url, file_name) -> pd.DataFrame:
    """File download"""

    os.system(f"wget {url} -O {file_name}")
    os.system(f"gzip -d {file_name}")

    df = pd.read_csv(file_name)
    return df

@task()
def process_data(df) -> pd.DataFrame:
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    return df

@task()
def save_locally(df, local_save_path) -> Path:
    "Save file locally"
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
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    file_name = "output"
    dataset_name = f"{file_name}.csv.gz"
    pq_file_name = f"{file_name}.parquet"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{dataset_name}"

    df = download_file(url, dataset_name)
    df_processed = process_data(df)
    path = save_locally(df_processed, pq_file_name)
    upload_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()
 


