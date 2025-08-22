import logging
import time

from pyspark.sql import DataFrame, SparkSession
from datetime import date
from pydeequ.checks import Check, CheckLevel, ConstrainableDataTypes
from pydeequ.verification import VerificationSuite, VerificationResult
from pydeequ.analyzers import (AnalysisRunner, AnalyzerContext,
                               Completeness, Mean, Size, Uniqueness, Distinctness, Entropy)

logger = logging.getLogger(__name__)


def run_data_quality_checks(spark: SparkSession, df: DataFrame, process_dt: date) -> bool:
    # --- 1. Data Profiling with Analyzers ---
    logger.info("Starting data profiling with Analyzers...")
    analysis_runner = AnalysisRunner(spark).onData(df)

    # Add the analyzers you want to run
    analysis_runner = (analysis_runner
                       .addAnalyzer(Size())
                       .addAnalyzer(Completeness("user_id"))
                       .addAnalyzer(Uniqueness(["user_id"]))
                       .addAnalyzer(Completeness("content_id"))
                       .addAnalyzer(Distinctness("content_id"))
                       .addAnalyzer(Completeness("event_type"))
                       .addAnalyzer(Distinctness("event_type"))
                       .addAnalyzer(Entropy("event_type"))
                       .addAnalyzer(Completeness("timestamp"))
                       .addAnalyzer(Mean("timestamp"))
                       )

    # Run the analysis and get the results as a DataFrame
    analysis_result = analysis_runner.run()
    result_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysis_result)

    logger.info("Data Profiling Results:")
    result_df.show(truncate=False)

    logger.info("Starting data quality validation for bronze...")

    # --- Timestamp Checks (Bronze Layer) ---
    # Get the current epoch milliseconds for future-date checks.
    # Add a small buffer (e.g., 5 minutes) to account for minor clock skew.
    current_epoch_ms = int(time.time() * 1000)
    future_check_buffer_ms = 5 * 60 * 1000  # Jan 1, 2020 in epoch milliseconds.
    start_of_2020_ms = 1577836800000

    check = Check(spark, CheckLevel.Error, f"Raw VOD Events Quality - {process_dt:%Y-%m-%d}")
    check_suite = (
        check
        # General Not Null checks for essential fields
        .isComplete("timestamp", hint="Timestamp must not be null")
        .isComplete("event_type", hint="Event Type must not be null")
        .isComplete("user_id", hint="User ID must not be null")
        .hasCompleteness("user_id", lambda x: x > 0.95, "At least 95% of records must have a user_id")

        # Technical checks for the timestamp field
        .isNonNegative("timestamp", hint="Timestamp must be a non-negative value")
        .hasDataType("timestamp", ConstrainableDataTypes.Integral, hint="Timestamp must be an Integral integer type")

        # A simple range check: greater than 2020, less than today + buffer.
        .satisfies(f"timestamp >= {start_of_2020_ms} AND timestamp <= {current_epoch_ms + future_check_buffer_ms}",
                   "Timestamp is within a valid range",
                   lambda x: x == 1.0,  # This asserts that 100% of rows must satisfy the condition
                   "Timestamp should be after 2020 and not in the future")
        # Add your custom conditional check here
        .satisfies("event_type IN ('login', 'logout') OR content_id IS NOT NULL",
                   "content_id must be present for non-login/logout events",
                   lambda x: x == 1.0,  # Asserts that 100% of rows must pass this rule
                   "Custom check for conditional content_id completeness")
    )

    # Run the checks
    verification_result = VerificationSuite(spark).onData(df).addCheck(check_suite).run()
    result_df = VerificationResult.checkResultsAsDataFrame(spark, verification_result)

    logger.info("Data Quality Check Results:")
    result_df.show(truncate=False)

    if verification_result.status != "Success":
        logger.error("Data quality checks failed. Halting the ingestion job. ❌")
        return False
    else:
        logger.info("Data quality checks passed! Proceeding with ingestion. ✅")
        return True
