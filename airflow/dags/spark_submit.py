import pyspark
from airflow.decorators import dag, task
from datetime import datetime

from airflow.sdk import get_current_context
from pyspark import SparkContext
from pyspark.sql.connect.session import SparkSession
from spark_jobs.bronze import bronze


spark_conf = {
    "spark.jars.packages": (
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
        "org.apache.iceberg:iceberg-aws-bundle:1.5.2"
    ),

    # Hadoop S3A Client Configuration
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",

    # AWS Region
    "spark.hadoop.aws.region": "ap-south-1",
    "spark.hadoop.fs.s3a.aws.region": "ap-south-1",

    # Iceberg REST Catalog
    "spark.sql.catalog.rest_catalog": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.rest_catalog.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
    "spark.sql.catalog.rest_catalog.uri": "http://rest-catalog:8181",
    "spark.sql.catalog.rest_catalog.warehouse": "s3a://vod/",

    # Iceberg S3FileIO Client Configuration
    "spark.sql.catalog.rest_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.rest_catalog.s3.endpoint": "http://minio:9000",
    "spark.sql.catalog.rest_catalog.s3.path-style-access": "true",
    "spark.sql.catalog.rest_catalog.s3.access-key-id": "minioadmin",
    "spark.sql.catalog.rest_catalog.s3.secret-access-key": "minioadmin",

    # Custom Ivy Cache
    "spark.jars.ivy": "/tmp/.ivy2",
}



@dag(
    start_date=datetime(2023, 1, 2),
    schedule=None,
    catchup=False,
    params={
        "clickstream_events_path": "s3a://vod/events/",
        "bronze_table_path": "rest_catalog.vod_bronze.events",
    }
)
def spark_dag():
    @task.pyspark(
        conn_id="spark_local",
    )
    def read_data(spark: SparkSession, sc: SparkContext) -> None:
        ctx = get_current_context()

        logical_date = ctx.get("logical_date").date()

        bronze(
            spark,
            ctx["params"]["clickstream_events_path"],
            ctx["params"]["bronze_table_path"],
            logical_date,
        )

    read_data()

spark_dag()