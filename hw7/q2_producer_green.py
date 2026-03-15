import pandas as pd
import json
from kafka import KafkaProducer
from time import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns_of_interest = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount'
]

logger.info(f"Loading data from {url}...")
df = pd.read_parquet(url, columns=columns_of_interest)
logger.info(f"Loaded {len(df)} records.")

def row_to_dict(row):
    return {
        'lpep_pickup_datetime': row['lpep_pickup_datetime'].strftime('%Y-%m-%d %H:%M:%S'),
        'lpep_dropoff_datetime': row['lpep_dropoff_datetime'].strftime('%Y-%m-%d %H:%M:%S'),
        'PULocationID': int(row['PULocationID']),
        'DOLocationID': int(row['DOLocationID']),
        'passenger_count': int(row['passenger_count']) if pd.notna(row['passenger_count']) else 0,
        'trip_distance': float(row['trip_distance']),
        'tip_amount': float(row['tip_amount']),
        'total_amount': float(row['total_amount'])
    }

bootstrap_server = 'localhost:9092'
topic_name = 'green-trips'

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=[bootstrap_server],
    value_serializer=json_serializer,
)

logger.info(f"Connecting to {bootstrap_server}...")

logger.info(f"Starting to send {len(df)} messages to topic '{topic_name}'...")
t0 = time()

for index, row in df.iterrows():
    try:
        message_dict = row_to_dict(row)
        future = producer.send(topic_name, value=message_dict)
        if index % 1000 == 0 and index > 0:
            logger.info(f"Sent {index} records...")
    except Exception as e:
        logger.error(f"Error sending record {index}: {e}")

producer.flush()
t1 = time()

logger.info("Sending completed.")
print(f'\nData sending time: {(t1 - t0):.2f} seconds')

producer.close()