# src/vod_platform/jobs/manual_events_ingestion.py

import argparse
from datetime import datetime
import pyspark.sql.functions as F
from vod_platform.utils.spark_session import get_spark_session


def run_job(process_dt: datetime):
    spark = get_spark_session(app_name=f"VOD_Manual_Events_Ingestion_{process_dt:%Y-%m-%d-%H}")

    base_source_path = "s3a://source-bucket/video_views"
    bronze_path = "s3a://vod-bronze-datalake/events/"

    print(f"Reading from base source: {base_source_path}")

    raw_df_all = spark.read.parquet(base_source_path)

    print("Full table schema discovered by Spark:")
    raw_df_all.printSchema()

    # --- Now, filter down to the specific hour for this job run ---
    date_str = process_dt.strftime('%Y-%m-%d')
    hour_int = process_dt.hour

    print(f"Filtering for date = '{date_str}' and hour = {hour_int}")

    raw_df_for_hour = raw_df_all.where(
        (F.col("date") == date_str) &
        (F.col("hour") == hour_int)
    )

    # Check if the filter resulted in any data
    if raw_df_for_hour.rdd.isEmpty():
        print("No data found for the specified date and hour. Exiting cleanly.")
        return

    # Add metadata and derive new partition columns
    bronze_df = raw_df_for_hour.withColumn("year", F.year(F.col("date"))) \
        .withColumn("month", F.month(F.col("date"))) \
        .withColumn("day", F.dayofmonth(F.col("date"))) \
        .withColumn("ingestion_timestamp", F.current_timestamp()) \
        .withColumn("source_file_name", F.input_file_name()) \
        .drop("date")

    print("The schema of the bronze dataframe is: ")
    bronze_df.printSchema()
    print(f"Writing {bronze_df.count()} records to Bronze layer...")
    (
        bronze_df.write
        .mode("overwrite")
        .partitionBy("region", "year", "month", "day", "hour")
        .parquet(bronze_path)
    )
    print("Manual events ingestion for the hour complete.")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the manual VOD events ingestion job.")
    parser.add_argument(
        "--process-datetime", type=str, required=True,
        help="The UTC datetime for the job run in 'YYYY-MM-DDTHH:MM:SS' format.",
    )
    args = parser.parse_args()
    job_datetime = datetime.fromisoformat(args.process_datetime)
    run_job(process_dt=job_datetime)