import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


load_dotenv()


spark = (
    SparkSession.builder
    .appName("TestSparkSnowflake")
    .getOrCreate()
)


data = [
    (
        "TEST",
        100.0,
        1.5,
        1.5,
        105.0,
        95.0,
        98.0,
        98.5,
        1758776400,
    ),
]


columns = [
    "SYMBOL",
    "PRICE",
    "CHANGE",
    "CHANGE_PERCENT",
    "HIGH",
    "LOW",
    "OPEN",
    "PREVIOUS_CLOSE",
    "TIMESTAMP",
]


df = spark.createDataFrame(data, columns)


df = df.withColumn(
    "EVENT_TIME",
    current_timestamp()
)


df = df.withColumn(
    "INGESTED_AT",
    current_timestamp()
)


snowflake_options = {
    "sfURL": f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
    "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
    "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "dbtable": "STOCK_PRICES",
}


df.write \
    .format("snowflake") \
    .options(**snowflake_options) \
    .mode("append") \
    .save()


print("Data written to Snowflake successfully!")


spark.stop()