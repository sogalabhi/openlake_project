import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, 
    col, 
    to_timestamp, 
    sum as spark_sum, 
    approx_count_distinct
) 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("LiveOrderStreaming") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD")) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

schema = StructType([
    StructField("Invoice", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Customer ID", StringType(), True),
    StructField("Country", StringType(), True)
])

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "live_orders") \
    .option("startingOffsets", "earliest") \
    .load()

parsed = raw_stream \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

typed = parsed \
    .withColumn("invoice_date", to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("revenue", col("Quantity") * col("Price")) \
    .withWatermark("invoice_date", "10 minutes")

aggregated = typed.groupBy() \
    .agg(
        approx_count_distinct("Invoice", rsd=0.05).alias("total_orders"),
        spark_sum("revenue").alias("total_revenue"),
        spark_sum("Quantity").alias("total_items_sold")
    )

query = aggregated.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/live_order_metrics") \
    .trigger(processingTime="10 seconds") \
    .start("s3a://lakehouse/gold/live_order_metrics")

query.awaitTermination()