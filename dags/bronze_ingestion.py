import datetime

from azure.storage.blob import BlobServiceClient, BlobClient
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


def validate_source_file():
    csv_path = "/opt/airflow/data/online_retail_II.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError("Source CSV file not found.")
    return csv_path


class ProgressFile(object):
    def __init__(self, filename, callback):
        self._f = open(filename, "rb")
        self._callback = callback
        self._total = os.path.getsize(filename)
        self._read_so_far = 0

    def read(self, size=-1):
        data = self._f.read(size)
        self._read_so_far += len(data)
        self._callback(self._read_so_far, self._total)
        return data

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._f.close()


def progress_callback(current, total):
    percent = (current / total) * 100
    print(f"Upload progress: {current}/{total} bytes ({percent:.2f}%)")


def upload_to_bronze(**context):
    execution_date = context["ds"]
    bronze_key = f"bronze/{execution_date}/online_retail_II.csv"

    account_url = "https://stopenlakeabhijith.blob.core.windows.net"

    blob_client = BlobClient(
        account_url=account_url,
        container_name="lakehouse",
        blob_name=bronze_key,
        credential=os.environ.get("AZURE_STORAGE_KEY"),
    )

    source_url = "https://stopenlakeabhijith.blob.core.windows.net/lakehouse/landing/online_retail_II.csv"

    print(f"Initiating server-side copy from {source_url} to {bronze_key}")
    blob_client.start_copy_from_url(source_url)

    properties = blob_client.get_blob_properties()
    print(f"Server-side copy status: {properties.copy.status}")


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
        packages="io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-azure:3.3.4",
        name="airflow-bronze-to-silver",
        conf={"spark.master": "spark://spark-master:7077"},
        verbose=True,
    )

    task_4 = SparkSubmitOperator(
        task_id="train_churn_model",
        conn_id="spark_default",
        application="/opt/airflow/scripts/train_churn_model.py",
        packages="io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-azure:3.3.4",
        name="airflow-train-churn-model",
        conf={"spark.master": "spark://spark-master:7077"},
        verbose=True,
    )

    task_5 = SparkSubmitOperator(
        task_id="push_churn_scores",
        conn_id="spark_default",
        application="/opt/airflow/reverse_etl/push_churn_scores.py",
        packages="io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-azure:3.3.4",
        name="airflow-push-churn-scores",
        conf={"spark.master": "spark://spark-master:7077"},
        verbose=True,
    )

    task_1 >> task_2 >> task_3 >> task_4 >> task_5
