import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    """Creates and returns a SparkSession object with comprehensive S3/MinIO config."""

    aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    minio_endpoint = os.environ.get("MINIO_ENDPOINT")

    builder = SparkSession.builder.appName(app_name)

    if not aws_access_key or not aws_secret_key:
        raise ValueError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set")

    # Required JAR packages for S3 connectivity
    builder.config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367"
    )

    # Core Hadoop-S3A configurations
    builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    builder.config("spark.hadoop.fs.s3a.access.key", aws_access_key)
    builder.config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)

    if minio_endpoint:
        print("----- RUNNING IN LOCAL MINIO MODE -----")
        # --- Comprehensive MinIO Settings ---
        builder.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
        builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        builder.config("spark.hadoop.fs.s3a.aws.credentials.provider",
                       "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    return builder.getOrCreate()
