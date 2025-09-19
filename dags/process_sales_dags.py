# dags/industrial_process_dag.py
"""
Airflow DAG: Fault-tolerant ingestion pipeline for files in MinIO.  
- Wait for files in incoming/
  - List files
  - For each file:
      - Validate format/schema
      - If invalid -> quarantine
      - If valid -> process -> load to Postgres -> finalize
  - Log all events to file_ingestion_log
  - Move files to processed/ or failed/<reason>/
  - Send email notifications on DAG success/failure
"""

import io
import logging
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.sensors.python import PythonSensor
from airflow.utils.email import send_email

# Ensure src package is available
sys.path.append("/opt/airflow/src")

from src.minio_client import list_objects, download_to_bytes, move_object, ensure_bucket
from src.validator import detect_format, validate_csv, validate_json, validate_parquet
from src.processor import read_bytes_to_df, clean_df
from src.db import upsert_sales, log_file_status
from src.config import MINIO_BUCKET


# -------------------------------
# Email notification callbacks
# -------------------------------
def notify_success(context):
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    subject = f"✅ DAG {dag_id} succeeded!"
    html_content = f"""
    <h3>DAG Run Succeeded</h3>
    <p>DAG: {dag_id}</p>
    <p>Run ID: {run_id}</p>
    <p>Execution Time: {context['execution_date']}</p>
    <p><a href="{context['task_instance'].log_url}">View Logs</a></p>
    """
    send_email(to=["kwame.boateng@amalitechtraining.org"], subject=subject, html_content=html_content)


def notify_failure(context):
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    subject = f"❌ DAG {dag_id} failed!"
    html_content = f"""
    <h3>DAG Run Failed</h3>
    <p>DAG: {dag_id}</p>
    <p>Run ID: {run_id}</p>
    <p>Execution Time: {context['execution_date']}</p>
    <p><a href="{context['task_instance'].log_url}">View Logs</a></p>
    """
    send_email(to=["kwame.boateng@amalitechtraining.org"], subject=subject, html_content=html_content)


# -------------------------------
# Default Args
# -------------------------------
DEFAULT_ARGS = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "email_on_failure": False,   # we handle via callback
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}


# -------------------------------
# DAG Definition
# -------------------------------
@dag(
    dag_id="industrial_sales_ingest",
    default_args=DEFAULT_ARGS,
    schedule_interval="* * * * *",#enable trigger via sensor on MINIO
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingest", "minio", "sales"],
    on_success_callback=notify_success,
    on_failure_callback=notify_failure,
)
def industrial_ingest_dag():

    # ---- Sensor: Wait for new files ----
    def _files_exist():
        ensure_bucket(MINIO_BUCKET)
        objs = list_objects(MINIO_BUCKET, prefix="incoming/")
        return len(objs) > 0

    wait_for_files = PythonSensor(
        task_id="wait_for_incoming_files",
        python_callable=_files_exist,
        poke_interval=30,
        timeout=60 * 15,  # 15 min
        mode="reschedule",
    )
