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
