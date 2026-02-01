
import time, json
import pandas as pd
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

df = pd.read_csv("../../data/raw/ecommerce_events.csv")

# Treat large IDs as strings
id_cols = ["order_id", "product_id", "category_id", "user_id"]
for col in id_cols:
    df[col] = df[col].astype(str)

for _, row in df.iterrows():
    event = {
        "event_time": row["event_time"],
        "order_id": row["order_id"],
        "product_id": row["product_id"],
        "category_id": row["category_id"],
        "category_code": row["category_code"],
        "brand": row["brand"],
        "price": float(row["price"]),
        "user_id": row["user_id"]
    }
    producer.send("clickpulse_events", event)
    time.sleep(0.002)   # adjust speed here

producer.flush()
