from datetime import datetime, timezone
import json
import os
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


SOURCE_CONFIG = Path(os.environ["AIRFLOW_HOME"]) / "config" / "ae_sources.json"
JOBS_DIR = Path(os.environ["PLS_JOBS_DIR"])
FISCAL_YEAR = "{{ params.fiscal_year if params.fiscal_year is not none else 'all' }}"
JOB_ARGS = ["--config", str(SOURCE_CONFIG), "--fiscal-year", FISCAL_YEAR]
SPARK_CONF = {
    "spark.ui.enabled": "false",
    "spark.sql.shuffle.partitions": "2",
    "spark.databricks.delta.snapshotPartitions": "2",
}


def check_source_files(fiscal_year):
    sources = json.loads(SOURCE_CONFIG.read_text(encoding="utf-8"))
    data_dir = Path(os.environ["PLS_DATA_DIR"])
    years = sorted(sources) if fiscal_year == "all" else [fiscal_year]
    for year in years:
        path = data_dir / sources[year]["file"]
        if not path.is_file() or path.stat().st_size == 0:
            raise ValueError(f"FY{year} source file is missing or empty: {path}")
        print(f"FY{year} source file is ready: {path.name}")


with DAG(
    dag_id="pls_pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2},
    params={
        "fiscal_year": Param(
            None,
            type=["null", "integer"],
            enum=[None, 2020, 2021, 2022],
            description="Select one fiscal year, or leave empty to process all three years.",
        ),
    },
    doc_md=(
        "Load and validate the selected PLS fiscal years, then rebuild Gold state-year KPIs "
        "from all available Silver years so year-over-year comparisons stay consistent."
    ),
) as dag:
    check_sources = PythonOperator(
        task_id="check_source_files",
        python_callable=check_source_files,
        op_kwargs={"fiscal_year": FISCAL_YEAR},
    )

    load_bronze = SparkSubmitOperator(
        task_id="bronze_ingest",
        conn_id="pls_spark",
        application=str(JOBS_DIR / "bronze_ingest.py"),
        application_args=JOB_ARGS.copy(),
        conf=SPARK_CONF.copy(),
    )

    clean_silver = SparkSubmitOperator(
        task_id="silver_clean",
        conn_id="pls_spark",
        application=str(JOBS_DIR / "silver_clean.py"),
        application_args=JOB_ARGS.copy(),
        conf=SPARK_CONF.copy(),
    )

    validate_data = SparkSubmitOperator(
        task_id="quality_check",
        conn_id="pls_spark",
        application=str(JOBS_DIR / "quality_check.py"),
        application_args=JOB_ARGS.copy(),
        conf=SPARK_CONF.copy(),
    )

    build_gold = SparkSubmitOperator(
        task_id="gold_state_kpis",
        conn_id="pls_spark",
        application=str(JOBS_DIR / "gold_state_kpis.py"),
        conf=SPARK_CONF.copy(),
    )

    check_sources >> load_bronze >> clean_silver >> validate_data >> build_gold
