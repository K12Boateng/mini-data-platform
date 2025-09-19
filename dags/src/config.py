
# src/config.py
"""
Configuration for MinIO and Postgres connections.
Reads settings from environment and exposes convenient variables.
"""
import os

# MinIO (S3-compatible) settings
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "sales-incoming")

# Postgres settings
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "postgrespass007")
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", 5432))
PG_DB = os.getenv("POSTGRES_DB", "sales_db")
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