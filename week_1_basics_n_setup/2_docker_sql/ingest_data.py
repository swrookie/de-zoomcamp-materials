#!/usr/bin/env python
# coding: utf-8

import argparse
import os
from time import time

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    if url.endswith(".csv.gz"):
        file_name = "output.csv.gz"
    elif url.endswith(".csv"):
        file_name = "output.csv"
    else:
        file_name = "output.parquet"

    os.system(f"wget {url} -O {file_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    if file_name.endswith(".csv.gz") or file_name.endswith(".csv"):
        df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000, low_memory=False)

        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists="replace")

        try:
            while True:
                t_start = time()

                df = next(df_iter)

                df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

                df.to_sql(name=table_name, con=engine, if_exists="append")

                t_end = time()

                print("inserted another chunk..., took %.3f second" % (t_end - t_start))
        except StopIteration:
            print("Ending Iteration")
        except Exception as e:
            print(f"Something happened...: {str(e)}")
    else:
        pq_file = pq.ParquetFile(file_name)

        for idx, batch in enumerate(pq_file.iter_batches()):
            df = batch.to_pandas()

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            try:
                t_start = time()

                df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

                if idx == 0:
                    df.to_sql(name=table_name, con=engine, if_exists="replace")
                else:
                    df.to_sql(name=table_name, con=engine, if_exists="append")

                t_end = time()

                print("inserted another chunk..., took %.3f second" % (t_end - t_start))
            except Exception as e:
                print(f"Something happened...: {str(e)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    parser.add_argument("--user", required=True, help="user name for postgres")
    parser.add_argument("--password", required=True, help="password for postgres")
    parser.add_argument("--host", required=True, help="host for postgres")
    parser.add_argument("--port", required=True, help="port for postgres")
    parser.add_argument("--db", required=True, help="database name for postgres")
    parser.add_argument("--table_name", required=True, help="name of the table where we will write the results to")
    parser.add_argument("--url", required=True, help="url of the csv file")

    args = parser.parse_args()
    main(args)
