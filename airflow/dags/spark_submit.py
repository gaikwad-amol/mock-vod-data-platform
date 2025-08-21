# my_dag.py
from airflow.decorators import dag, task
from datetime import datetime

from pyspark import SparkContext
from pyspark.sql import SparkSession
import pandas as pd

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def spark_dag():

    @task.pyspark(conn_id="spark_local")
    def read_data(spark: SparkSession, sc: SparkContext) -> pd.DataFrame:
        df = spark.read.json("s3a://raw/content/movies.jsonl.gz")
        df.show()

        return df.toPandas()

    read_data()

spark_dag()