import pandas as pd
import sys
import argparse
from sqlalchemy import create_engine
import os
import time

def main(params):

    user = params.user
    password = params.password
    db = params.db
    host = params.host
    port = params.port
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    table_name = params.table_name
    file_name = 'output.csv.gz'

    os.system(f"wget {url} -O {file_name}")
    os.system(f"gzip -d {file_name}")

    csv_name = "output.csv"

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)


    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name = table_name, con = engine, if_exists='replace')

    df.to_sql(name = table_name, con = engine, if_exists='append')


    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='ingest CSV to Postgres')
    parser.add_argument('--user', required = True, help='username for postgres')
    parser.add_argument('--password', required = True, help='password for postgres')
    parser.add_argument('--host', required = True,help='host for postgres')
    parser.add_argument('--port', required = True,help='port for postgres')
    parser.add_argument('--db', required = True,help='db for postgres')
    parser.add_argument('--table_name', required = True,help='table_name for postgres')

    args = parser.parse_args()
    main(args)