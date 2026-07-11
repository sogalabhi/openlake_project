import os
import pickle
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, countDistinct, sum, datediff, lit, when
from datetime import datetime, timedelta

OBSERVATION_WINDOW_DAYS = 180
CHURN_WINDOW_DAYS = 90


def main():
    
    spark = SparkSession.builder \
        .appName("PushChurnScores") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER", "admin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD", "password")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    
    silver_path = "s3a://lakehouse/silver/retail_transactions"
    df = spark.read.format("delta").load(silver_path)
    df_filtered = df.filter(col("customer_id").isNotNull())

    max_date = df_filtered.agg(max("invoice_date")).collect()[0][0]
    cutoff_date = max_date - timedelta(days=OBSERVATION_WINDOW_DAYS)

    print(f"Dataset max date : {max_date}")
    print(f"Cutoff date      : {cutoff_date}  (features computed up to here)")
    print(f"Feature window   : {OBSERVATION_WINDOW_DAYS} days before cutoff used for RFM")
    print(f"Label window     : {CHURN_WINDOW_DAYS} days after cutoff used to assign churn label")

    df_before = df_filtered.filter(col("invoice_date") < lit(cutoff_date))

    rfm = df_before.groupBy("customer_id").agg(
        datediff(lit(cutoff_date), max("invoice_date")).alias("recency_days"),
        countDistinct("invoice_id").alias("frequency"),
        sum("revenue").alias("monetary")
    )

    df_after = df_filtered.filter(
        (col("invoice_date") > lit(cutoff_date)) &
        (col("invoice_date") <= lit(cutoff_date + timedelta(days=CHURN_WINDOW_DAYS)))
    )

    returned_customers = df_after.select("customer_id").distinct() \
        .withColumn("returned", lit(1))

    rfm = rfm.join(returned_customers, on="customer_id", how="left")
    rfm = rfm.withColumn(
        "churned",
        when(col("returned").isNull(), 1).otherwise(0)
    ).drop("returned")

    df_pandas = rfm.toPandas()

    spark.stop()

    model_path = "/opt/airflow/scripts/model.pkl"
    with open(model_path, "rb") as f:
        model = pickle.load(f)

    X = df_pandas[["recency_days", "frequency", "monetary"]]
    df_pandas["churn_probability"] = model.predict_proba(X)[:, 1]
    df_pandas["churn_label"] = model.predict(X)
    df_pandas["scored_at"] = datetime.now()
    print(f"Scored {len(df_pandas)} customers")
    print(f"Predicted churners: {df_pandas['churn_label'].sum()}")

    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        user="airflow",
        password="airflow",
        dbname="crm"
    )

    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_churn_scores (
            customer_id VARCHAR(255) PRIMARY KEY,
            churn_probability DOUBLE PRECISION,
            churn_label INTEGER,
            recency_days INTEGER,
            frequency INTEGER,
            monetary DOUBLE PRECISION,
            scored_at TIMESTAMP
        );
    """)
    conn.commit()

    # Build the UPSERT SQL statement
    sql = """
    INSERT INTO customer_churn_scores 
        (customer_id, churn_probability, churn_label, recency_days, frequency, monetary, scored_at)
    VALUES %s
    ON CONFLICT (customer_id) 
    DO UPDATE SET
        churn_probability = EXCLUDED.churn_probability,
        churn_label = EXCLUDED.churn_label,
        recency_days = EXCLUDED.recency_days,
        frequency = EXCLUDED.frequency,
        monetary = EXCLUDED.monetary,
        scored_at = EXCLUDED.scored_at;
    """

    records = [
        (row.customer_id, float(row.churn_probability), int(row.churn_label), 
         int(row.recency_days), int(row.frequency), float(row.monetary), row.scored_at)
        for row in df_pandas.itertuples()
    ]

    execute_values(cursor, sql, records)
    conn.commit()
    print(f"Upserted {len(records)} records to customer_churn_scores")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()

