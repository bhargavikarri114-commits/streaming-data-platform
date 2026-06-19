import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\HP\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\HP\AppData\Local\Programs\Python\Python311\python.exe"

from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.types import * # type: ignore
from pyspark.sql.functions import * # type: ignore
from pyspark.sql.functions import col, from_json, sum, count, round, to_date, to_timestamp # type: ignore

spark = SparkSession.builder \
    .appName("LoadToPostgres") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
    ) \
    .config(
        "spark.jars",
        r"C:\Users\HP\Data_Engineering_projects\streaming-data-platform\jars\postgresql-42.7.3.jar"
    ) \
    .config(
        "spark.driver.extraJavaOptions",
        "-Duser.timezone=UTC"
    ) \
    .config(
        "spark.executor.extraJavaOptions",
        "-Duser.timezone=UTC"
    ) \
    .getOrCreate()

spark.conf.set("spark.sql.session.timeZone", "UTC")

spark.sparkContext.setLogLevel("WARN")

schema = StructType([ # type: ignore
    StructField("order_id", IntegerType(), True), # type: ignore
    StructField("customer_id", IntegerType(), True), # type: ignore
    StructField("product", StringType(), True), # type: ignore
    StructField("category", StringType(), True), # type: ignore
    StructField("quantity", IntegerType(), True), # type: ignore
    StructField("price", IntegerType(), True), # type: ignore
    StructField("timestamp", StringType(), True), # type: ignore
])

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

parsed_df = df.select(
    from_json(
        col("value").cast("string"), 
            schema
        ).alias("data")
    ).select("data.*")

sales_df = parsed_df.withColumn(
    "total_amount",
    col("quantity") * col("price")
)
    
postgres_df = sales_df.withColumnRenamed(
    "timestamp",
    "order_timestamp"
)

postgres_df = postgres_df.withColumn(
    "order_timestamp",
    to_timestamp(col("order_timestamp"))
)

postgres_df.write \
    .format("jdbc") \
    .option(
        "url",
        "jdbc:postgresql://localhost:5432/streaming_db?options=-c%20TimeZone=UTC"
    ) \
    .option("dbtable", "sales_orders") \
    .option("user", "postgres") \
    .option("password", "Pmnbvcxz@1") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()