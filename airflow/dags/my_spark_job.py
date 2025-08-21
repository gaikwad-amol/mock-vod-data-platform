from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SimpleSparkJob").getOrCreate()

    # Create a simple DataFrame
    data = [("A", 1), ("B", 2), ("C", 3)]
    df = spark.createDataFrame(data, ["col1", "col2"])

    # Perform a simple action
    print(f"Number of rows: {df.count()}")

    spark.stop()