"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
  - name: pickup_location_id
    type: integer
    description: "TLC taxi zone where trip started"
  - name: dropoff_location_id
    type: integer
    description: "TLC taxi zone where trip ended"
  - name: fare_amount
    type: float
    description: "Fare amount in USD"
  - name: taxi_type
    type: string
    description: "Type of taxi (e.g. yellow, green)"
  - name: payment_type
    type: integer
    description: "Payment type ID (joined with payment_lookup)"
@bruin"""

import io
import os
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# NYC TLC parquet column names by taxi type (pickup/dropoff datetime prefix differs)
DATETIME_COLS = {
    "yellow": ("tpep_pickup_datetime", "tpep_dropoff_datetime"),
    "green": ("lpep_pickup_datetime", "lpep_dropoff_datetime"),
}


def materialize():
    start_date_str = os.environ["BRUIN_START_DATE"]
    end_date_str = os.environ["BRUIN_END_DATE"]
    vars_str = os.environ.get("BRUIN_VARS", "{}")
    taxi_types = json.loads(vars_str).get("taxi_types", ["yellow"])

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    frames = []
    current = start_date
    while current <= end_date:
        year, month = current.year, current.month
        for taxi_type in taxi_types:
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            url = f"{BASE_URL}/{filename}"
            try:
                resp = requests.get(url, timeout=60)
                resp.raise_for_status()
            except requests.RequestException:
                continue
            df = pd.read_parquet(io.BytesIO(resp.content))
            if df.empty:
                continue
            pickup_col, dropoff_col = DATETIME_COLS.get(
                taxi_type, ("tpep_pickup_datetime", "tpep_dropoff_datetime")
            )
            # Normalize to schema expected by staging
            rename = {
                pickup_col: "pickup_datetime",
                dropoff_col: "dropoff_datetime",
                "PULocationID": "pickup_location_id",
                "DOLocationID": "dropoff_location_id",
            }
            df = df.rename(columns=rename)
            df["taxi_type"] = taxi_type
            needed = [
                "pickup_datetime",
                "dropoff_datetime",
                "pickup_location_id",
                "dropoff_location_id",
                "fare_amount",
                "taxi_type",
                "payment_type",
            ]
            # Keep only columns that exist
            existing = [c for c in needed if c in df.columns]
            frames.append(df[existing])
        current += relativedelta(months=1)

    if not frames:
        return pd.DataFrame(
            columns=[
                "pickup_datetime",
                "dropoff_datetime",
                "pickup_location_id",
                "dropoff_location_id",
                "fare_amount",
                "taxi_type",
                "payment_type",
            ]
        )
    result = pd.concat(frames, ignore_index=True)
    return result[["pickup_datetime", "dropoff_datetime", "pickup_location_id", "dropoff_location_id", "fare_amount", "taxi_type", "payment_type"]]
