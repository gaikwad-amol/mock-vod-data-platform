import os

from pyspark.sql import SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    """Creates and returns a SparkSession object with comprehensive S3/MinIO config."""

    builder = (SparkSession.builder
               .master("spark://spark-master:7077")
               .appName(app_name)
               )

    return builder.getOrCreate()
