import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="streaming_pipeline",
    start_date=datetime.datetime(2026, 1, 1),
    schedule_interval="@once",
    catchup=False,
) as dag:

    task_1 = SparkSubmitOperator(
        task_id="stream_live_orders",
        conn_id="spark_default",
        application="/opt/airflow/scripts/stream_live_orders.py",
        packages="io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-azure:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.0",
        name="live-order-streaming",
        verbose=True,
        execution_timeout=None,
    )