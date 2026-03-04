#!/usr/bin/env python
import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

@click.command()
@click.option("--pg_user", default="root", help="PostgreSQL user")
@click.option("--pg_pass", default="root", help="PostgreSQL password")
@click.option("--pg_host", default="pgdatabase", help="PostgreSQL host")
@click.option("--pg_port", default=5432, type=int, help="PostgreSQL port")
@click.option("--pg_db", default="ny_taxi", help="PostgreSQL database name")
@click.option("--target_table", default="green_trip_data", help="Target table name")
@click.option("--chunksize", default=100000, type=int, help="Chunk size for reading CSV")
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, chunksize):
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"
    engine = create_engine(f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

    print("Ingesting taxi zone lookup data...")
    zones_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
    df_zones = pd.read_csv(zones_url)
    df_zones.to_sql("taxi_zones", con=engine, if_exists="replace", index=False)
    print(f"Successfully ingested {len(df_zones)} taxi zones")

    print(f"Ingesting green taxi trip data from {url}...")
    df = pd.read_parquet(url)
    total_rows = len(df)
    num_chunks = (total_rows // chunksize) + 1
    first = True

    for i in tqdm(range(num_chunks)):
        start_idx = i * chunksize
        end_idx = min((i + 1) * chunksize, total_rows)
        df_chunk = df.iloc[start_idx:end_idx]

        if len(df_chunk) == 0:
            continue

        if first:
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists="replace")
            first = False

        df_chunk.to_sql(name=target_table, con=engine, if_exists="append")

if __name__ == "__main__":
    run()