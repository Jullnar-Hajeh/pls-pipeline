from datetime import datetime, timezone
from pathlib import Path
import os

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id="pls_smoke_test",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    doc_md="Verify that the local Airflow environment can write and read Delta with Spark.",
) as dag:
    SparkSubmitOperator(
        task_id="spark_delta_round_trip",
        conn_id="pls_spark",
        application=str(Path(os.environ["PLS_JOBS_DIR"]) / "delta_smoke_test.py"),
        conf={"spark.ui.enabled": "false", "spark.sql.shuffle.partitions": "2"},
    )
