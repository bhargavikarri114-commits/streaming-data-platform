from pyspark.sql import SparkSession  # type: ignore

spark = SparkSession.builder \
    .appName("KafkaStreamingProject") \
    .getOrCreate()

print("Spark Session created successfully.")

spark.stop()