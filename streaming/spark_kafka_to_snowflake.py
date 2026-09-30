import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    from_unixtime,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
)


load_dotenv()


spark = (
    SparkSession.builder
    .appName("StocksKafkaToSnowflake")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Kafka message schema
schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("change", DoubleType()),
    StructField("change_percent", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("open", DoubleType()),
    StructField("previous_close", DoubleType()),
    StructField("timestamp", LongType()),
])


# Read streaming data from Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stock-prices")
    .option("startingOffsets", "latest")
    .load()
)


# Convert Kafka value from binary to JSON string
json_df = kafka_df.select(
    from_json(
        col("value").cast("string"),
        schema
    ).alias("data")
)


# Extract JSON fields
stocks_df = json_df.select("data.*")


# Convert Unix timestamp to Spark timestamp
stocks_df = stocks_df.withColumn(
    "EVENT_TIME",
    from_unixtime(col("timestamp")).cast("timestamp")
)


# Add ingestion timestamp
stocks_df = stocks_df.withColumn(
    "INGESTED_AT",
    current_timestamp()
)


# Keep the exact Snowflake table column order
stocks_df = stocks_df.select(
    "symbol",
    "price",
    "change",
    "change_percent",
    "high",
    "low",
    "open",
    "previous_close",
    "timestamp",
    "EVENT_TIME",
    "INGESTED_AT",
)


# Snowflake connection settings
snowflake_options = {
    "sfURL": f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
    "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
    "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "dbtable": "STOCK_PRICES",
}


# Write each Spark micro-batch to Snowflake
def write_to_snowflake(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    (
        batch_df
        .write
        .format("snowflake")
        .options(**snowflake_options)
        .mode("append")
        .save()
    )

    print(f"Batch {batch_id} written to Snowflake.")


# Start streaming query
query = (
    stocks_df.writeStream
    .foreachBatch(write_to_snowflake)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "checkpoints/stock_prices"
    )
    .start()
)


query.awaitTermination()