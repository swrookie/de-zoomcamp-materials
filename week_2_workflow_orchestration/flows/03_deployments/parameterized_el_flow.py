from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

import os


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load(os.environ["GCS_BUCKET"])
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df["passenger_count"].fillna(0, inplace=True)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load(os.environ["PREFECT_GCP_CREDS"])

    df.to_gbq(
        destination_table=os.environ["DESTINATION_TABLE"],
        project_id=os.environ["PROJECT_ID"],
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def el_gcs_to_bq(year: int, month: int, color: str):
    """Main ETL flow to load data into Big Query"""

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


@flow()
def el_parent_flow(months: list[int] = [1, 2], year: int = 2021, colors: list = ["green", "yellow"]):
    for month in months:
        for color in colors:
            el_gcs_to_bq(year, month, color)


if __name__ == "__main__":
    colors = ["green", "yellow"]
    months = [1, 2, 3]
    year = [2019, 2020, 2021, 2022]
    el_parent_flow(months, year, colors)
