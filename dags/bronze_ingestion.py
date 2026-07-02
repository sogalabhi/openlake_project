import datetime

import boto3
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

from botocore.exceptions import ClientError

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

def validate_source_file():
    csv_path = "/opt/airflow/data/online_retail_II.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError("Source CSV file not found.")
    return csv_path

def upload_to_bronze(**context):
    s3_client = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id=os.environ.get("MINIO_ROOT_USER"),
        aws_secret_access_key=os.environ.get("MINIO_ROOT_PASSWORD"),
        region_name="us-east-1"
    )
    ti = context["ti"]
    execution_date = context["ds"]
    csv_path = ti.xcom_pull(task_ids="validate_source_file")
    bronze_key = f"bronze/{execution_date}/online_retail_II.csv"
    try:
        s3_client.create_bucket(Bucket="lakehouse")
    except ClientError as e:
        error_code = None
        if hasattr(e, "response"):
            error_code = e.response.get("Error", {}).get("Code")
        if error_code != "BucketAlreadyOwnedByYou":
            raise

    s3_client.upload_file(Filename = csv_path, Bucket="lakehouse", Key=bronze_key)
with DAG(
    dag_id="bronze_ingestion",
    start_date=datetime.datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task_1 = PythonOperator(
        task_id="validate_source_file",
        python_callable=validate_source_file,
    )

    task_2 = PythonOperator(
        task_id="upload_to_bronze",
        python_callable=upload_to_bronze,
    )

    task_3 = SparkSubmitOperator(
        task_id="transform_bronze_to_silver",
        conn_id="spark_default",
        application="/opt/airflow/scripts/transform_bronze_to_silver.py",
        application_args=["{{ ds }}"], 
        packages="io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4",
        name="airflow-bronze-to-silver",
        verbose=True
    )

    task_1 >> task_2 >> task_3