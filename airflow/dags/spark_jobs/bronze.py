from datetime import date

from pyspark.sql.connect.session import SparkSession
from pyspark.sql.functions import col, lit


def bronze(
        spark: SparkSession,
        clickstream_events_path: str,
        bronze_table_path: str,
        processing_date: date,
):
    spark.sql("CREATE DATABASE IF NOT EXISTS vod_bronze")
    spark.sql("CREATE DATABASE IF NOT EXISTS vod_silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS vod_gold")

    print(f"Reading from base source: {clickstream_events_path}events_{processing_date:%Y-%m-%d}.jsonl.gz")
    raw_events_df = spark.read.json(f"{clickstream_events_path}events_{processing_date:%Y-%m-%d}.jsonl.gz")

    bronze_df = raw_events_df.withColumn(
        "event_date",
        (col("timestamp") / 1000).cast("timestamp").cast("date")
    ).withColumn("processing_date", lit(processing_date).cast("date"))

    if spark.catalog.tableExists(bronze_table_path):
        print(f"Table {bronze_table_path} exists. Overwriting partition.")
        bronze_df.writeTo(bronze_table_path).overwritePartitions()
    else:
        print(f"Table {bronze_table_path} does not exist. Creating it now.")
        (
            bronze_df.writeTo(bronze_table_path)
            .partitionedBy("processing_date", "event_date")
            .create()
        )
