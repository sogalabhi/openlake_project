import os
import pickle
import pandas as pd
import pymssql
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, countDistinct, sum, datediff, lit, when
from datetime import datetime, timedelta

OBSERVATION_WINDOW_DAYS = 180
CHURN_WINDOW_DAYS = 90


def main():

    spark = (
        SparkSession.builder.appName("PushChurnScores")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "fs.azure.account.key.stopenlakeabhijith.dfs.core.windows.net",
            os.environ.get("AZURE_STORAGE_KEY"),
        )
        .getOrCreate()
    )

    silver_path = "abfss://lakehouse@stopenlakeabhijith.dfs.core.windows.net/silver/retail_transactions"
    df = spark.read.format("delta").load(silver_path)
    df_filtered = df.filter(col("customer_id").isNotNull())

    max_date = df_filtered.agg(max("invoice_date")).collect()[0][0]
    cutoff_date = max_date - timedelta(days=OBSERVATION_WINDOW_DAYS)

    print(f"Dataset max date : {max_date}")
    print(f"Cutoff date      : {cutoff_date}  (features computed up to here)")
    print(
        f"Feature window   : {OBSERVATION_WINDOW_DAYS} days before cutoff used for RFM"
    )
    print(
        f"Label window     : {CHURN_WINDOW_DAYS} days after cutoff used to assign churn label"
    )

    df_before = df_filtered.filter(col("invoice_date") < lit(cutoff_date))

    rfm = df_before.groupBy("customer_id").agg(
        datediff(lit(cutoff_date), max("invoice_date")).alias("recency_days"),
        countDistinct("invoice_id").alias("frequency"),
        sum("revenue").alias("monetary"),
    )

    df_after = df_filtered.filter(
        (col("invoice_date") > lit(cutoff_date))
        & (col("invoice_date") <= lit(cutoff_date + timedelta(days=CHURN_WINDOW_DAYS)))
    )

    returned_customers = (
        df_after.select("customer_id").distinct().withColumn("returned", lit(1))
    )

    rfm = rfm.join(returned_customers, on="customer_id", how="left")
    rfm = rfm.withColumn(
        "churned", when(col("returned").isNull(), 1).otherwise(0)
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

    conn = pymssql.connect(
        server=os.environ.get("AZURE_SQL_SERVER"),
        user=os.environ.get("AZURE_SQL_USER"),
        password=os.environ.get("AZURE_SQL_PASSWORD"),
        database="crm",
    )

    cursor = conn.cursor()
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[customer_churn_scores]') AND type in (N'U'))
        CREATE TABLE customer_churn_scores (
            customer_id VARCHAR(255) PRIMARY KEY,
            churn_probability FLOAT,
            churn_label INT,
            recency_days INT,
            frequency INT,
            monetary FLOAT,
            scored_at DATETIME
        );
    """)
    conn.commit()

    # Build and execute the MERGE statement in batches of 100
    batch_size = 100
    records = [
        (
            row.customer_id,
            float(row.churn_probability),
            int(row.churn_label),
            int(row.recency_days),
            int(row.frequency),
            float(row.monetary),
            row.scored_at,
        )
        for row in df_pandas.itertuples()
    ]

    print(f"Starting batched upsert of {len(records)} records...")

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]

        # Create placeholders: (%s, %s, %s, %s, %s, %s, %s), (%s, %s, %s, %s, %s, %s, %s), ...
        placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s)"] * len(batch))

        sql = f"""
        MERGE customer_churn_scores AS target
        USING (VALUES {placeholders}) AS source (customer_id, churn_probability, churn_label, recency_days, frequency, monetary, scored_at)
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN
            UPDATE SET 
                churn_probability = source.churn_probability,
                churn_label = source.churn_label,
                recency_days = source.recency_days,
                frequency = source.frequency,
                monetary = source.monetary,
                scored_at = source.scored_at
        WHEN NOT MATCHED THEN
            INSERT (customer_id, churn_probability, churn_label, recency_days, frequency, monetary, scored_at)
            VALUES (source.customer_id, source.churn_probability, source.churn_label, source.recency_days, source.frequency, source.monetary, source.scored_at);
        """

        # Flatten parameters for the execute call
        params = []
        for r in batch:
            params.extend(r)

        cursor.execute(sql, params)
        conn.commit()
        print(f"Upserted records {i} to {i + len(batch)}")

    print(f"Upsert complete! Total {len(records)} records processed.")
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
