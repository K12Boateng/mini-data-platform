
# src/db.py
"""
Postgres database helpers: upsert sales in batch and log file ingestion status.
"""
import logging
from psycopg2 import connect, sql
from psycopg2.extras import execute_values
from typing import Optional
from .config import PG_CONN_DICT

logger = logging.getLogger(__name__)


def get_conn():
    """
    Get a psycopg2 connection using config values.
    """
    return connect(**PG_CONN_DICT)


def upsert_sales(df, batch_size=1000) -> int:
    """
    Upsert DataFrame rows into sales table. Return number of rows upserted.
    """
    if df is None or df.empty:
        logger.info("Empty DataFrame passed to upsert_sales.")
        return 0

    tuples = [
        (
            str(row.sale_id),
            row.sale_date.to_pydatetime() if hasattr(row.sale_date, "to_pydatetime") else row.sale_date,
            row.customer_id,
            row.product_id,
            int(row.quantity),
            float(row.amount)
        )
        for row in df.itertuples(index=False)
    ]

    insert_sql = """
    INSERT INTO sales (sale_id, sale_date, customer_id, product_id, quantity, amount)
    VALUES %s
    ON CONFLICT (sale_id) DO UPDATE
      SET sale_date = EXCLUDED.sale_date,
          customer_id = EXCLUDED.customer_id,
          product_id = EXCLUDED.product_id,
          quantity = EXCLUDED.quantity,
          amount = EXCLUDED.amount;
    """
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, tuples, page_size=batch_size)
        conn.commit()
        logger.info("Upserted %d rows", len(tuples))
        return len(tuples)
    except Exception as e:
        if conn:
            conn.rollback()
        logger.exception("Error during upsert: %s", e)
        raise
    finally:
        if conn:
            conn.close()


def log_file_status(file_key: str, bucket: str, status: str, rows: Optional[int] = None, error: Optional[str] = None) -> None:
    """
    Insert or update file_ingestion_log entry for auditability.
    """
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO file_ingestion_log (file_key, bucket, status, rows_processed, error, updated_at)
                VALUES (%s, %s, %s, %s, %s, now())
                ON CONFLICT (file_key) DO UPDATE
                  SET status = EXCLUDED.status,
                      rows_processed = EXCLUDED.rows_processed,
                      error = EXCLUDED.error,
                      updated_at = now();
                """,
                (file_key, bucket, status, rows, error)
            )
        conn.commit()
        logger.info("Logged file status: %s -> %s", file_key, status)
    except Exception as e:
        if conn:
            conn.rollback()
        logger.exception("Failed to log file status: %s", e)
        # Do not raise to avoid failing pipeline due to audit logging
    finally:
        if conn:
            conn.close()
