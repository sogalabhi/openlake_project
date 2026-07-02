import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def main(execution_date):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("BronzeToSilver-Deduplication") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER", "admin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD", "password")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    print(f"Starting processing for execution date: {execution_date}")

    raw_schema = StructType([
        StructField("Invoice", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", StringType(), True),
        StructField("Price", DoubleType(), True),
        StructField("Customer ID", StringType(), True),
        StructField("Country", StringType(), True)
    ])

    bronze_path = f"s3a://lakehouse/bronze/{execution_date}/online_retail_II.csv"
    
    df_raw = spark.read \
        .option("header", "true") \
        .schema(raw_schema) \
        .csv(bronze_path)

    df_cleaned = df_raw \
        .filter(col("Invoice").isNotNull()) \
        .withColumn("invoice_date", to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumnRenamed("Customer ID", "customer_id") \
        .withColumnRenamed("StockCode", "product_id") \
        .withColumn("quantity", col("Quantity").cast(IntegerType())) \
        .withColumn("price", col("Price").cast(DoubleType())) \
        .withColumn("ingested_at", current_timestamp()) \
        .select(
            col("Invoice").alias("invoice_id"),
            col("product_id"),
            col("Description").alias("description"),
            col("quantity"),
            col("invoice_date"),
            col("price"),
            col("customer_id"),
            col("Country").alias("country"),
            col("ingested_at")
        )

    silver_path = "s3a://lakehouse/silver/retail_transactions"
    
    print(f"Writing conformed data to Silver Delta Table at: {silver_path}")

    df_cleaned.write \
        .format("delta") \
        .mode("append") \
        .save(silver_path)

    print("Silver processing layer complete!")
    spark.stop()

if __name__ == "__main__":
    # Allow execution date to be passed via command line argument (from Airflow)
    if len(sys.argv) > 1:
        exec_date = sys.argv[1]
    else:
        from datetime import datetime
        exec_date = datetime.today().strftime('%Y-%m-%d')
        
    main(exec_date)