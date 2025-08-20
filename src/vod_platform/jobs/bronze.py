import argparse
from datetime import date

from pyspark.sql.functions import col, to_date

from vod_platform.utils.spark_session import get_spark_session


def run_job(process_dt: date):
    spark = get_spark_session(app_name=f"VOD_Events_Ingestion_{process_dt:%Y-%m-%d}")
    base_source_path = "s3a://vod/events/"
    bronze_table = "rest_catalog.vod_bronze.events"

    spark.sql("CREATE DATABASE IF NOT EXISTS vod_bronze")
    spark.sql("CREATE DATABASE IF NOT EXISTS vod_silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS vod_gold")

    file_path = f"{base_source_path}events_{process_dt:%Y-%m-%d}.jsonl.gz"
    print(f"Reading from base source: {file_path}")
    raw_events_df = spark.read.json(file_path)

    bronze_df = raw_events_df.withColumn(
        "event_date",
        (col("timestamp") / 1000).cast("timestamp").cast("date")
    )
    if bronze_df.rdd.isEmpty():
        print("No data found for the specified date. Exiting cleanly.")
        return

    # (CRUCIAL DIAGNOSTIC STEP) Verify the partition column before writing
    # print("Verifying partition key values and checking for nulls:")
    # bronze_df.groupBy("event_date").count().orderBy("count", ascending=False).show()

    if spark.catalog.tableExists(bronze_table):
        print(f"Table {bronze_table} exists. Overwriting partition.")
        (
            bronze_df.writeTo(bronze_table)
            .overwritePartitions()
        )
    else:
        print(f"Table {bronze_table} does not exist. Creating it now.")
        (
            bronze_df.writeTo(bronze_table)
            .partitionedBy("event_date")
            .create()
        )

if __name__ == "__main__":
        parser = argparse.ArgumentParser(description="Run the VOD events ingestion job.")
        parser.add_argument(
            "--process-datetime", type=str, required=True,
            help="The UTC datetime for the job run in 'YYYY-MM-DD' format.",
        )
        args = parser.parse_args()
        job_datetime = date.fromisoformat(args.process_datetime)
        run_job(process_dt=job_datetime)