# src/vod_platform/jobs/manual_events_ingestion.py

import argparse
from datetime import datetime
from py4j.protocol import Py4JJavaError

import pyspark.sql.functions as F
from vod_platform.utils.spark_session import get_spark_session


def run_job(process_dt: datetime):
    spark = get_spark_session(app_name=f"VOD_Manual_Events_Ingestion_{process_dt:%Y-%m-%d-%H}")

    # conf = spark.sparkContext.getConf()
    # print("spark.jars.packages:", conf.get("spark.jars.packages", "<none>"))
    # print("spark.jars:", conf.get("spark.jars", "<none>"))
    #
    # cp = spark._jvm.java.lang.System.getProperty("java.class.path")
    # print(cp)
    # # optionally search for iceberg jars:
    # print([p for p in cp.split(":") if "iceberg" in p.lower()])
    #
    # jlocation = spark._jvm.org.apache.iceberg.spark.SparkCatalog.getClass().getProtectionDomain().getCodeSource().getLocation()
    # print("SparkCatalog loaded from:", jlocation)

    # hconf = spark.sparkContext._jsc.hadoopConfiguration()
    # it = hconf.iterator()
    # while it.hasNext():
    #     entry = it.next()
    #     k = entry.getKey()
    #     v = entry.getValue()
    #     if k.startswith("fs.s3a."):
    #         print(k, "=", v)

    base_source_path = "s3a://raw/video_views"
    bronze_table = "rest_catalog.bronze.raw_transactions"

    print(f"Reading from base source: {base_source_path}")

    # --- ADD THESE LINES TO PAUSE THE SCRIPT ---
    # print("=" * 50)
    # print("Spark UI is now available at http://localhost:4040")
    # print("The script is paused. Press Enter in this terminal to continue...")
    # print("=" * 50)
    # input()  # This will halt the program until you press Enter
    # -------------------------------------------

    try:
        # the code that fails, e.g. reading or writing
        raw_df_all = spark.read.parquet(base_source_path).repartition(100)
    except Py4JJavaError as e:
        print("=== Py4JJavaError: Python message ===")
        print(e)  # short message
        # full Java exception object (stringified)
        try:
            print("=== Java exception class ===")
            print(type(e.java_exception), e.java_exception)
        except Exception:
            pass
        print("=== Java stacktrace (string) ===")
        print(e.java_traceback)  # most useful — paste this into your next message
        raise

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
        bronze_df
        .writeTo(bronze_table)
        # .partitionBy("region", "year", "month", "day", "hour")
        .createOrReplace()
    )
    print("Manual events ingestion for the hour complete.")

    bronze_table_df = spark.table(bronze_table)
    print(f"Verification: Reading from {bronze_table}. Count: {bronze_table_df.count()}")
    bronze_table_df.show(5)

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