
# src/config.py
"""
Configuration for MinIO and Postgres connections.
Reads settings from environment and exposes convenient variables.
"""
import os

# MinIO (S3-compatible) settings
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "YOUR_MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "YOUR_MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "YOUR_MINIO_BUCKET")

# Postgres settings
PG_USER = os.getenv("POSTGRES_USER", "YOUR_POSTGRES_USER")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "YOUR_POSTGRES_PASSWORD")
PG_HOST = os.getenv("POSTGRES_HOST", "YOUR_POSTGRES_HOST_NAME")
PG_PORT = int(os.getenv("POSTGRES_PORT", 5432))
PG_DB = os.getenv("POSTGRES_DB", "YOUR_POSTGRES_DB")
PG_CONN_DICT = {
    "host": PG_HOST,
    "port": PG_PORT,
    "dbname": PG_DB,
    "user": PG_USER,
    "password": PG_PASS,
}
PG_CONN_STRING = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# Processing defaults
DEFAULT_CHUNK_SIZE = int(os.getenv("DEFAULT_CHUNK_SIZE", 10000))
