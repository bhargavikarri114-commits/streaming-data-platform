from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.types import * # type: ignore
from pyspark.sql.functions import col, from_json, sum, count, round, to_date # type: ignore

spark = SparkSession.builder \
        .appName("ParseOrders") \
        .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
        .getOrCreate()

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


parsed_df.show(truncate=False)

sales_df = parsed_df.withColumn(
    "total_amount",
    col("quantity") * col("price")
)

sales_df.show(truncate=False)

category_sales_df = sales_df.groupBy("category") \
    .agg(
        sum("total_amount").alias("total_revenue")
    )

category_sales_df.show(truncate=False)

product_sales_df = sales_df.groupBy("product") \
    .agg(
        sum("total_amount").alias("total_revenue")
    ).orderBy(col("total_revenue").desc())

product_sales_df.limit(5).show(truncate=False)

avg_order_value_df = sales_df.agg(
    sum("total_amount").alias("total_revenue"),
    count("order_id").alias("total_orders")
).withColumn(
    "avg_order_value",
    round(
    col("total_revenue") / col("total_orders"), 2)
)

avg_order_value_df.show(truncate=False)

top_customers_df = sales_df.groupBy("customer_id") \
    .agg(
        sum("total_amount").alias("total_spent")
    ).orderBy(col("total_spent").desc())

top_customers_df.limit(5).show(truncate=False)

daily_sales_df = sales_df.withColumn(
    "order_date",to_date(col("timestamp"))
)

daily_revenue_df = daily_sales_df.groupBy("order_date") \
    .agg( 
        sum("total_amount").alias("daily_revenue")
    ).orderBy(col("order_date").asc())

daily_revenue_df.show(truncate=False)

spark.stop()