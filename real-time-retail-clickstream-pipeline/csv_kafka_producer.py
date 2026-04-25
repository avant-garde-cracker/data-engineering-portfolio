from kafka import KafkaProducer
import pandas as pd
import json
import time
from datetime import datetime


# CONFIGS
TOPIC = "retail-events"
BATCH_SIZE = 100         # number of records per batch
SLEEP_BETWEEN_BATCHES = 10  # seconds
LOOP_FOREVER = False

df = pd.read_csv("data/online_retail_II.csv", encoding="ISO-8859-1")
df.columns = [col.strip() for col in df.columns]

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def transform_row(row):
    return {
        "invoice_no": str(row["Invoice"]),
        "product_id": str(row["StockCode"]),
        "product_name": str(row["Description"]),
        "quantity": int(row["Quantity"]) if not pd.isna(row["Quantity"]) else 0,
        "invoice_date": str(row["InvoiceDate"]),
        "unit_price": float(row["Price"]) if not pd.isna(row["Price"]) else 0.0,
        "customer_id": str(row["Customer ID"]) if not pd.isna(row["Customer ID"]) else None,
        "country": str(row["Country"]),
        "event_time": datetime.utcnow().isoformat()
    }

def send_batch(batch_df):
    for _, row in batch_df.iterrows():
        event = transform_row(row)
        producer.send(TOPIC, value=event)

    producer.flush()
    print(f"Sent batch of {len(batch_df)} records")

def run_stream():
    while True:
        for i in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[i:i+BATCH_SIZE]
            send_batch(batch)

            print(f"Sleeping {SLEEP_BETWEEN_BATCHES} sec...\n")
            time.sleep(SLEEP_BETWEEN_BATCHES)

        if not LOOP_FOREVER:
            break

if __name__ == "__main__":
    run_stream()

## To send all rows at once
# for _, row in df.iterrows():
#     event = transform_row(row)
#
#     producer.send("retail-events", value=event)
#     print("Sent:", event)
#
#     time.sleep(0.05)
#
# producer.flush()