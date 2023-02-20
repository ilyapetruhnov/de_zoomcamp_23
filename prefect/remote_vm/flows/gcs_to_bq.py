import pandas as pd
from sqlalchemy import create_engine
import os
from time import time
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials





@task(retries=3)
def extract_from_gcs(color: str,  month: int, year: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}_{month}.parquet"
    gcs_block = GcsBucket.load("gcp-bucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./")
    return Path(f"./{gcs_path}")

@task()
def read_file(path) -> pd.DataFrame:
    """Fetch File"""
    df = pd.read_parquet(path)
    return df

@task()
def write_bq(df: pd.DataFrame) -> int:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="datazoomcamp-377017.ny_taxi_trips.rides",
        project_id="datazoomcamp-377017",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )
    return len(df)


@flow(log_prints=True)
def etl_gcs_to_bq(color, month, year) -> None:
    """The main ETL function"""

    gcs_path = extract_from_gcs(color, month, year)
    df = read_file(gcs_path)
    write_bq(df)
    return True

@flow()
def etl_bq(color= "yellow", months=[2,3],  year= 2019) -> None:

    for month in months:
        etl_gcs_to_bq(color, month, year)

if __name__ == "__main__":
    etl_parent_flow()