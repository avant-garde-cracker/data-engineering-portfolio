from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum, desc
from pyspark.sql.types import *
from pyspark.sql.functions import to_timestamp
import requests
import json
import os

def create_druid_spec(file_path):
    return {
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": "clickstream_batch",
                "timestampSpec": {
                    "column": "start",
                    "format": "iso"
                },
                "dimensionsSpec": {
                    "dimensions": [
                        "product_id",
                        "product_name"
                    ]
                },
                "metricsSpec": [
                    {
                        "type": "doubleSum",
                        "name": "total_revenue",
                        "fieldName": "total_revenue"
                    }
                ]
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {
                    "type": "local",
                    "baseDir": "/tmp",
                    "filter": os.path.basename(file_path)
                },
                "inputFormat": {
                    "type": "json"
                }
            },
            "tuningConfig": {
                "type": "index_parallel"
            }
        }
    }

DRUID_URL = "http://localhost:8888/druid/indexer/v1/task"

def process_batch(batch_df, batch_id):
    print(f"\n===== Batch: {batch_id} =====\n")
    # batch_df.orderBy(desc("total_revenue")) #\
    #     .show(10, truncate=False)

    # batch_df.write \
    #     .mode("append") \
    #     .json("druid_input/")
    if batch_df.count() == 0:
        return
    batch_df = batch_df.withColumn("event_time", col("window.start"))


    # Create unique file per batch
    file_path = f"/tmp/clickstream_data/druid_batch_{batch_id}.json"

    # Write batch to JSON
    batch_df.select(
        "product_id",
        "product_name",
        "total_revenue",
        "window.start"
    ).toPandas().to_json(file_path, orient="records", lines=True)

    print(f"Written batch to {file_path}")

    # Create ingestion spec
    ingestion_spec = create_druid_spec(file_path)

    # Submit ingestion job
    response = requests.post(DRUID_URL, json=ingestion_spec)

    print("Druid Response:", response.status_code, response.text)

# Define schema (matches your Kafka JSON)
schema = StructType([
    StructField("invoice_no", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("quantity", IntegerType()),
    StructField("invoice_date", StringType()),
    StructField("unit_price", DoubleType()),
    StructField("customer_id", StringType()),
    StructField("country", StringType()),
    StructField("event_time", TimestampType())
])

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaRetailStream") \
    .master("local[*]") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
"""
- This creates a streaming DataFrame
- It does NOT contain the actual parsed data yet. The Schema looks like the below:- 
key: binary (null)
value: binary (b'{"invoice_no":"123","product_id":"P1",...}')
topic: string (retail-events)
partition: int (0)
offset: long (101)
timestamp: timestamp
timestampType: int (2026-04-24 21:00:00)
"""
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "retail-events") \
    .option("startingOffsets", "latest") \
    .load()

# Kafka value is binary → convert to string
df_string = df_raw.selectExpr("CAST(value AS STRING)")

# Parse JSON
df_parsed = df_string.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

df_time = df_parsed.withColumn(
    "event_time",
    to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss")
).drop("invoice_date")

"""
Watermark defines how long Spark should wait for late data before closing a window and cleaning up state.
Event event_time arrives_at	  Result
A	    10:00	    10:01	  ✅ counted
B	    10:02	    10:03	  ✅ counted
C	    10:01	    10:15	  ❌ DROPPED
"""
df_watermarked = df_time.withWatermark(
    "event_time",
    "10 minutes"
)

# Top 10 products by revenue in last 5 minutes
df_revenue = df_watermarked.withColumn("revenue",col("quantity") * col("unit_price"))

df_agg = df_revenue.groupBy(
    window(col("event_time"), "5 minutes", "1 minute"),  # window + slide
    col("product_id"),
    col("product_name")
).agg(sum("revenue").alias("total_revenue"))

# df_top10 = df_agg.orderBy(desc("total_revenue"))

# Show streaming output
# query = df_agg.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .option("truncate", False) \
#     .start()

# TO show records using foreach one by one
# query = df_agg.writeStream \
#     .outputMode("append") \
#     .foreachBatch(process_batch) \
#     .start()

# writing output to json files
# query = df_agg.writeStream \
#     .format("json") \
#     .option("path", "druid_input/") \
#     .option("checkpointLocation", "checkpoints/retail_stream") \
#     .outputMode("append") \
#     .start()

query = df_agg.writeStream \
    .outputMode("update") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()