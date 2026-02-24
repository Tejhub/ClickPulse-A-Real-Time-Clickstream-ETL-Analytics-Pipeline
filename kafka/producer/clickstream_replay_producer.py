import time
import json
import pandas as pd
from kafka import KafkaProducer

KAFKA_BROKERS = ["localhost:9092", "localhost:9093", "localhost:9094"]
TOPIC = "clickpulse_realtime_event"
FILE_PATH = "../../data/raw/ecommerce_events.csv"
CHUNK_SIZE = 5000
STREAM_DELAY = 0.001

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5,
    linger_ms=10,
    batch_size=16384,
    enable_idempotence=True
)

print("🚀 Producer Started...")

for chunk in pd.read_csv(FILE_PATH, chunksize=CHUNK_SIZE):

    id_cols = ["order_id", "product_id", "category_id", "user_id"]
    for col in id_cols:
        chunk[col] = chunk[col].astype(str)

    chunk["price"] = chunk["price"].astype(float)
    records = chunk.to_dict(orient="records")

    for event in records:
        producer.send(TOPIC, key=event["user_id"].encode("utf-8"), value=event)
        time.sleep(STREAM_DELAY)

    print(f"Sent {len(records)} records...")

producer.flush()
producer.close()
print("✅ Streaming Completed.")
