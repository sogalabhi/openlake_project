import os
import pickle
from datetime import timedelta
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, countDistinct, sum, datediff, lit, when


OBSERVATION_WINDOW_DAYS = 180  
CHURN_WINDOW_DAYS = 90 

def main():
    
    spark = SparkSession.builder \
        .appName("ChurnModelTraining") \
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

    #   timeline:  |--- feature window ---|-- churn window --|
    #              dataset_start        cutoff_date       max_date
    #
    #   cutoff_date = max(invoice_date) - OBSERVATION_WINDOW_DAYS
    #   Customers with NO purchase after cutoff_date are labelled churned.

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

    print(f"\nDataset size : {len(df_pandas)} customers")
    print(f"Churn rate   : {df_pandas['churned'].mean():.2%}\n")

    X = df_pandas[["recency_days", "frequency", "monetary"]]
    y = df_pandas["churned"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42,
        stratify=y      
    )
    
    model = RandomForestClassifier(
        n_estimators=100,
        random_state=42,
        class_weight="balanced"
    )
    model.fit(X_train, y_train)
    
    predictions = model.predict(X_test)
    print(classification_report(y_test, predictions))

    print("Feature importances:")
    for feature, importance in zip(["recency_days", "frequency", "monetary"], model.feature_importances_):
        print(f"  {feature}: {importance:.3f}")

    model_path = os.path.join(os.path.dirname(__file__), "model.pkl")
    with open(model_path, "wb") as f:
        pickle.dump(model, f)
    print(f"\nModel saved to {model_path}")

    spark.stop()

if __name__ == "__main__":
    main()
