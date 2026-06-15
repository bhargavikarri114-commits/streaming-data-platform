from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import col # type: ignore

spark = SparkSession.builder \
    .appName("DecodeKafka") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

decoded_df = df.select(
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("value").cast("string").alias("order_json")
)

decoded_df.show(truncate=False)

spark.stop()

