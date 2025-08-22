import argparse
from datetime import date

from pyspark.sql.functions import col, to_date
from pyspark.sql import functions as F, SparkSession, DataFrame

from vod_platform.utils.spark_session import get_spark_session


def apply_transformations_and_validations(spark: SparkSession, bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Applies schema transformations, validates the data, and sessionizes events into user actions.
    Returns a tuple of two DataFrames: (silver_df, quarantine_df)
    """
    # --- STAGE 1: Basic Cleaning and Validation ---

    # A. Initial Transformation
    base_events_df = (
        bronze_df
        .select(
            "event_id", "user_id", "content_id", "event_type", "event_date",
            "device_id", "timestamp", F.col("geo_location.country").alias("country")
        )
        .withColumn("event_timestamp", (F.col("timestamp") / 1000).cast("timestamp"))
    )

    # B. Row-level Validation Rules
    validation_rules = [
        F.when(F.col("event_id").isNull(), "event_id_is_null"),
        F.when(F.col("user_id").isNull(), "user_id_is_null"),
        F.when(F.col("event_timestamp").isNull(), "event_timestamp_is_null"),
        F.when((F.col("country").isNull()) | (F.length(F.col("country")) != 2), "invalid_country_code")
    ]

    validated_df = base_events_df.withColumn(
        "validation_errors", F.array_compact(F.array(*validation_rules))
    )

    # C. Split into Valid Events and Quarantined Events
    valid_events_df = validated_df.filter(F.size(F.col("validation_errors")) == 0).drop("validation_errors")
    quarantine_df = validated_df.filter(F.size(F.col("validation_errors")) > 0)

    # --- STAGE 2: Sessionization using Spark SQL ---

    # Register the clean, valid events table as a temporary view to run SQL on it
    valid_events_df.createOrReplaceTempView("valid_events_view")

    sessionization_sql = """
        WITH event_action AS (
            SELECT 
                user_id,
                content_id,
                device_id,
                country,
                CASE 
                    WHEN event_type IN ('video_start', 'video_progress', 'video_end') THEN 'WATCHED'
                    WHEN event_type IN ('video_pause', 'video_resume') THEN 'PAUSED'
                    ELSE 'IGNORE' 
                END AS action,
                event_timestamp,
                event_date
            FROM valid_events_view
            WHERE content_id IS NOT NULL
        ),
        numbered AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY user_id, content_id, device_id ORDER BY event_timestamp) AS rn_all,
                ROW_NUMBER() OVER (PARTITION BY user_id, content_id, device_id, action ORDER BY event_timestamp) AS rn_action
            FROM event_action
            WHERE action <> 'IGNORE'
        ), 
        runs AS (
            SELECT
                user_id,
                content_id,
                device_id,
                country,
                action,
                event_date,
                MIN(event_timestamp) AS start_time,
                MAX(event_timestamp) AS last_event_time,
                (rn_all - rn_action) AS grp -- This identifies the island
            FROM numbered
            GROUP BY user_id, content_id, device_id, country, action, event_date, grp
        ),
        runs_with_next AS (
            SELECT
                r.*,
                LEAD(r.start_time) OVER (PARTITION BY r.user_id, r.content_id, r.device_id ORDER BY r.start_time) AS next_start_time
            FROM runs r
            LEFT JOIN valid_events_view v
                ON r.user_id = v.user_id 
                AND r.content_id = v.content_id 
                AND r.device_id = v.device_id
                AND r.last_event_time = v.event_timestamp
        ),        
        final_sessions AS (
            SELECT
                *,
                COALESCE(next_start_time, last_event_time) AS end_time
            FROM runs_with_next
        )
        SELECT
            md5(concat(user_id, content_id, device_id, action, cast(start_time as string))) as action_id,
            user_id,
            content_id,
            device_id,
            country,
            action,
            start_time,
            end_time,
            CAST(end_time AS LONG) - CAST(start_time AS LONG) AS duration_seconds,
            event_date
        FROM final_sessions
    """

    # Execute the SQL to get the final silver DataFrame
    silver_df = spark.sql(sessionization_sql)

    return silver_df, quarantine_df.drop("timestamp")


def write_to_silver(spark: SparkSession, silver_table_name: str, clean_df: DataFrame):
    """
    Writes the clean DataFrame to the Silver Iceberg table using MERGE INTO
    for idempotent upserts.
    """
    """
       Writes the clean DataFrame to the Silver Iceberg table using MERGE INTO
       for idempotent upserts based on the generated action_id.
       """
    if clean_df.rdd.isEmpty():
        print("No new clean records to write to the Silver table.")
        return

    clean_df.createOrReplaceTempView("silver_updates")

    # Use the generated unique 'action_id' for a robust merge condition.
    merge_sql = f"""
           MERGE INTO {silver_table_name} t
           USING silver_updates s
           ON t.user_id = s.user_id and 
            t.content_id = s.content_id and 
            t.device_id = s.device_id and 
            t.action = s.action and 
            t.start_time = s.start_time 
           WHEN MATCHED THEN
               UPDATE SET
                   t.end_time = s.end_time,
                   t.duration_seconds = s.duration_seconds
           WHEN NOT MATCHED THEN
               INSERT *
       """
    print(f"Executing MERGE INTO on {silver_table_name}...")
    spark.sql(merge_sql)
    print("Successfully merged data into the Silver table.")

def run_job(process_dt: date):
    """
        Main ETL job function to process data from Bronze to Silver.
        """
    spark = get_spark_session(app_name=f"VOD_Bronze_to_Silver_{process_dt:%Y-%m-%d}")

    # Define table names
    bronze_table = "rest_catalog.vod_bronze.events"
    silver_table = "rest_catalog.vod_silver.user_actions"
    # quarantine_table = "rest_catalog.vod_quarantine.events_failed"

    print(f"Starting job for processing date: {process_dt}")

    # --- 1. EXTRACT ---
    # Read incremental data from the Bronze table for the given processing date.
    # This assumes your Bronze table is partitioned by a date column, e.g., 'ingestion_date'.
    # If not, you might need to derive it from the timestamp.
    try:
        bronze_df = (
            spark.table(bronze_table)
            .filter(F.to_date(F.col("event_date")) == process_dt)
        )
        print(f"Successfully read {bronze_df.count()} records from {bronze_table} for {process_dt}.")
    except Exception as e:
        print(f"Error reading from Bronze table {bronze_table}. Aborting job. Error: {e}")
        return

    # --- 2. TRANSFORM & VALIDATE ---
    clean_df, quarantine_df = apply_transformations_and_validations(spark, bronze_df)

    # Cache the dataframes as they are used in multiple actions (write and count)
    clean_df.cache()
    quarantine_df.cache()

    # --- 3. LOAD ---
    write_to_silver(spark, silver_table, clean_df)
    # write_to_quarantine(quarantine_table, quarantine_df)

    # Unpersist cached data
    clean_df.unpersist()
    quarantine_df.unpersist()

    print(f"Job finished for processing date: {process_dt}")
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