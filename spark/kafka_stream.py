from pyspark.sql import SparkSession  # type: ignore

spark = SparkSession.builder \
    .appName("KafkaStreamingProject") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
    ) \
    .getOrCreate()

spark.conf.set(
    "spark.sql.streaming.forceDeleteTempCheckpointLocation",
    "true"
)

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

df.printSchema()

query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "checkpoint/orders") \
    .start()

query.awaitTermination()