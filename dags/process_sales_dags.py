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

    # ---- List files ----
    @task()
    def list_files():
        ensure_bucket(MINIO_BUCKET)
        objects = list_objects(MINIO_BUCKET, prefix="incoming/")
        keys = [obj["Key"] for obj in objects]
        logging.info("Found %d files: %s", len(keys), keys)
        return keys

    # ---- Validate file ----
    @task()
    def validate_file(key: str):
        try:
            data = download_to_bytes(MINIO_BUCKET, key)
            fmt = detect_format(key, data)

            if fmt == "csv":
                valid, msg = validate_csv(data)
            elif fmt == "json":
                valid, msg = validate_json(data)
            elif fmt == "parquet":
                valid, msg = validate_parquet(data)
            else:
                valid, msg = False, f"Unknown format: {fmt}"

            log_file_status(key, MINIO_BUCKET,
                            "validated" if valid else "validation_failed",
                            rows=None, error=None if valid else msg)

            return {"key": key, "valid": valid, "error": msg}

        except Exception as e:
            logging.exception("Validation error for %s: %s", key, e)
            log_file_status(key, MINIO_BUCKET, "validation_failed", rows=None, error=str(e))
            return {"key": key, "valid": False, "error": str(e)}
        

        # ---- Quarantine invalid ----
    @task()
    def quarantine_file(result: dict):
        key = result["key"]
        dest = key.replace("incoming/", "failed/validation_failed/")
        try:
            move_object(MINIO_BUCKET, key, dest)
            log_file_status(key, MINIO_BUCKET, "validation_failed", rows=0, error=result.get("error"))
        except Exception as e:
            logging.exception("Failed to quarantine %s: %s", key, e)

     # ---- Process valid ----
    @task(retries=2, retry_delay=timedelta(seconds=30))
    def process_file(result: dict):
        key = result["key"]
        try:
            data = download_to_bytes(MINIO_BUCKET, key)
            df = read_bytes_to_df(data, key)
            df_clean = clean_df(df)

            rows = len(df_clean)
            csv_bytes = df_clean.to_csv(index=False).encode("utf-8")

            log_file_status(key, MINIO_BUCKET, "processed", rows=rows, error=None)
            return {"key": key, "rows": rows, "payload": csv_bytes.decode("utf-8")}

        except Exception as e:
            logging.exception("Processing failed for %s: %s", key, e)
            log_file_status(key, MINIO_BUCKET, "processing_failed", rows=0, error=str(e))
            move_object(MINIO_BUCKET, key, key.replace("incoming/", "failed/processing_failed/"))
            return {"key": key, "rows": 0, "payload": None, "error": str(e)}
        
    # ---- Load to Postgres ----
    @task(retries=2, retry_delay=timedelta(seconds=30))
    def load_to_postgres(process_result: dict):
        import pandas as pd
        key = process_result["key"]
        try:
            payload = process_result.get("payload")
            if not payload:
                raise ValueError("No payload to load")

            df = pd.read_csv(io.StringIO(payload))
            rows_loaded = upsert_sales(df)

            move_object(MINIO_BUCKET, key, key.replace("incoming/", "processed/"))
            log_file_status(key, MINIO_BUCKET, "loaded", rows=rows_loaded, error=None)

            return {"key": key, "rows_loaded": rows_loaded}

        except Exception as e:
            logging.exception("Load failed for %s: %s", key, e)
            log_file_status(key, MINIO_BUCKET, "load_failed", rows=0, error=str(e))
            move_object(MINIO_BUCKET, key, key.replace("incoming/", "failed/loading_failed/"))
            raise




