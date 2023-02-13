import os

import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def fetch(url: str) -> pd.DataFrame:
    """
    Read csv.gz taxi data from Github into pandas DataFrame
    """
    df = pd.read_csv(url)

    return df


@task(log_prints=True)
def clean(df: pd.DataFrame, taxi_type: str) -> pd.DataFrame:
    """
    convert dtype
    """
    if taxi_type == "yellow":
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    elif taxi_type == "green":
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    elif taxi_type == "fhv":
        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
        df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])
        # 2019-06 cast error
        df["PUlocationID"] = df["DOlocationID"].astype("float64")
        df["DOlocationID"] = df["DOlocationID"].astype("float64")

    print(df.head(10))
    print(f"columns: {df.dtypes}")
    print(f"n_rows: {len(df)}")

    return df


@task()
def write_local(df: pd.DataFrame, taxi_type: str, dataset_file: str) -> None:
    """
    Write DataFrame as parquet into local path
    """
    path = Path(f"data/{taxi_type}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")

    # return path


@task()
def write_gcs(path: Path) -> None:
    """
    Upload local parquet file to GCS
    """
    gcp_cloud_storage_bucket_block = GcsBucket.load(os.environ["GCS_BUCKET"])
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)

    return


# @task(retries=3)
# def extract_from_gcs(taxi_type: str, year: int, month: int = None) -> Path:
#     """Download taxi data from GCS"""
#     gcs_path = f"data/{taxi_type}/{taxi_type}_tripdata_{year}-*.parquet"
#     gcs_block = GcsBucket.load(os.environ["GCS_BUCKET"])
#     temp = gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
#     print(temp)
#     return Path(f"../data/{gcs_path}")


def transform():
    pass


@task()
def write_bq(gcs_path) -> None:
    """
    Run DDL Statement in BigQuery
    """
    gcp_credentials_block = GcpCredentials.load(os.environ["PREFECT_GCP_CREDS"])

    fhv_ext_query = f"""
    CREATE OR REPLACE EXTERNAL TABLE nytaxi.external_fhv_parquet_tripdata
    OPTIONS (
        format = "parquet",
        uris = ['{gcs_path}']
    )
    """

    fhv_bq_query = f"""
    CREATE OR REPLACE TABLE nytaxi.fhv_parquet_tripdata_partitioned_clustered 
    PARTITION BY 
        date(pickup_datetime)
    CLUSTER BY  
        Affiliated_base_number
    AS
    SELECT 
        * 
    FROM 
        nytaxi.external_fhv_parquet_tripdata
    """

    # Set max_results to 0 since it is running DDL statement
    pd.read_gbq(
        query=fhv_ext_query,
        project_id=os.environ["PROJECT_ID"],
        dialect="standard",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        max_results=0,
    )

    pd.read_gbq(
        query=fhv_bq_query,
        project_id=os.environ["PROJECT_ID"],
        dialect="standard",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        max_results=0,
    )


@flow()
def el_fhv_to_bq() -> None:
    """
    Main EL Function
    """
    taxi_type = "fhv"
    year = 2019
    months = [i + 1 for i in range(12)]

    for month in months:
        dataset_file = f"{taxi_type}_tripdata_{year}-{month:02}"
        dataset_url = (
            f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{dataset_file}.csv.gz"
        )
        df = fetch(dataset_url)
        df_clean = clean(df, taxi_type)
        write_local(df_clean, taxi_type, dataset_file)
        chunk_path = Path(f"data/{taxi_type}/{dataset_file}.parquet")
        write_gcs(chunk_path)

    gcs_path = f"gs://{os.environ['GCS_BUCKET_NAME']}/data/fhv/fhv_tripdata_2019-*.parquet"
    write_bq(gcs_path)


if __name__ == "__main__":
    el_fhv_to_bq()
