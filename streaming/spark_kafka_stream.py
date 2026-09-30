from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
)


spark = (
    SparkSession.builder
    .appName("StocksKafkaStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


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


df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stock-prices")
    .option("startingOffsets", "latest")
    .load()
)


json_df = df.select(
    from_json(
        col("value").cast("string"),
        schema
    ).alias("data")
)


stocks = json_df.select("data.*")

stocks = stocks.withColumn(
    "event_time",
    from_unixtime(col("timestamp")).cast("timestamp")
)


query = (
    stocks.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)


query.awaitTermination()