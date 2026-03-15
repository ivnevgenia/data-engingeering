import sys
from pathlib import Path
import json

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer

server = 'localhost:9092'
topic_name = 'green-trips'

def json_deserializer(data):
    return json.loads(data.decode('utf-8'))

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='q3-consumer',
    value_deserializer=json_deserializer,
    consumer_timeout_ms=10000
)

print(f"Counting trips with trip_distance > 5.0 in topic {topic_name}...")

count_gt5 = 0
total = 0

for message in consumer:
    data = message.value
    total += 1
    trip_distance = data.get('trip_distance')
    if trip_distance is not None and trip_distance > 5.0:
        count_gt5 += 1
    if total % 5000 == 0:
        print(f"Processed {total} records...")

consumer.close()
print(f"\nTotal processed: {total}")
print(f"Number of trips with trip_distance > 5.0: {count_gt5}")