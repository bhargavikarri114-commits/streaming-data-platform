from pyspark.sql import SparkSession # type: ignore

spark = SparkSession.builder \
    .appName("KafkaBatchRead") \
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

df.show(truncate=False)

spark.stop()