import pandas as pd
from sqlalchemy import create_engine
import os
from time import time
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download data from GCS"""

    gcs_path = f"{color}/{color}_tripdata_{year}_{month}.parquet"
    gcs_block = GcsBucket.load("zoomcamp-gcs-bucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")

@task()
def read_file(path) -> pd.DataFrame:
    """Fetch File"""
    df = pd.read_parquet(path)
    return df

@task()
def write_bq(df: pd.DataFrame) -> int:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoomcamp-gcp-creds")

    df.to_gbq(
        destination_table="datazoomcamp-377017.ny_taxi_trips",
        project_id="datazoomcamp-377017",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )
    return len(df)


@flow(log_prints=True)
def etl_gcs_to_bq(year, month, color) -> None:
    """The main ETL function"""

    gcs_path = extract_from_gcs(year, month, color)
    df = read_file(gcs_path)
    write_bq(df)
    return True

@flow()
def etl_parent_flow(year= 2020, 
                    months=[1], 
                    color= "green") -> None:

    for month in months:
        etl_gcs_to_bq(year, month, color)

if __name__ == "__main__":
    etl_parent_flow()