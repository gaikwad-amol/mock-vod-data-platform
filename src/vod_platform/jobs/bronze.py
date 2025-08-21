import argparse
import logging
import sys
from datetime import date

from pyspark.sql.functions import col

from vod_platform.utils.spark_session import get_spark_session
from bronze_data_quality_checks import run_data_quality_checks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def run_job(process_dt: date):
    spark = None
    try:
        spark = get_spark_session(app_name=f"VOD_Events_Ingestion_{process_dt:%Y-%m-%d}")
        base_source_path = "s3a://vod/events/"
        bronze_table = "rest_catalog.vod_bronze.events"

        spark.sql("CREATE DATABASE IF NOT EXISTS vod_bronze")
        spark.sql("CREATE DATABASE IF NOT EXISTS vod_silver")
        spark.sql("CREATE DATABASE IF NOT EXISTS vod_gold")

        source_file = f"{base_source_path}events_{process_dt:%Y-%m-%d}.jsonl.gz"
        logger.info(f"Reading from source: {source_file}")
        raw_events_df = spark.read.json(source_file)

        if raw_events_df.rdd.isEmpty():
            logger.info("No data found for the specified date. Exiting cleanly.")
            return

        if not run_data_quality_checks(spark, raw_events_df, process_dt):
            logger.error("Data quality checks failed. Aborting job.")
            sys.exit(1)

        logger.info("Data quality checks passed. Proceeding to write to bronze table.")
        bronze_df = raw_events_df.withColumn(
            "event_date",
            (col("timestamp") / 1000).cast("timestamp").cast("date")
        )

        if spark.catalog.tableExists(bronze_table):
            logger.info(f"Table {bronze_table} exists. Overwriting partition.")
            (
                bronze_df.writeTo(bronze_table)
                .overwritePartitions()
            )
        else:
            logger.info(f"Table {bronze_table} does not exist. Creating it now.")
            (
                bronze_df.writeTo(bronze_table)
                .partitionedBy("event_date")
                .create()
            )
        logger.info("Bronze layer ingestion complete.")

    except Exception as e:
        logger.error(f"An unexpected error occurred during the job run: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            logger.info("Stopping Spark session.")
            spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the VOD events ingestion job.")
    parser.add_argument(
        "--process-datetime", type=str, required=True,
        help="The UTC datetime for the job run in 'YYYY-MM-DD' format.",
    )
    args = parser.parse_args()
    job_datetime = date.fromisoformat(args.process_datetime)
    run_job(process_dt=job_datetime)
