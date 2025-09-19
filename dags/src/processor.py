# src/processor.py
"""
Processing functions that convert raw bytes to cleaned pandas DataFrame.
Handles CSV, JSON, and Parquet inputs. Implements null handling and deduplication.
"""
from typing import Tuple
import logging
import pandas as pd
import io
from .validator import detect_format

logger = logging.getLogger(__name__)


def read_bytes_to_df(data: bytes, key: str) -> pd.DataFrame:
    """
    Load bytes into a DataFrame based on detected format.
    Raises on read error.
    """
    fmt = detect_format(key, data)
    logger.debug("Detected format %s for key %s", fmt, key)
    if fmt == "csv":
        df = pd.read_csv(io.BytesIO(data), dtype=str)
    elif fmt == "json":
        text = data.decode("utf-8")
        # handle ndjson or array
        lines = [l for l in text.splitlines() if l.strip()]
        if len(lines) > 0 and lines[0].strip().startswith("["):
            df = pd.read_json(io.StringIO(text), dtype=str)
        else:
            # ndjson
            records = [pd.read_json(io.StringIO(l), typ='series') for l in lines]
            df = pd.DataFrame(records)
    elif fmt == "parquet":
        df = pd.read_parquet(io.BytesIO(data))
    else:
        raise ValueError(f"Unsupported format: {fmt}")

    # normalize column names
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DataFrame:
    - Ensure required columns present
    - Trim strings
    - Coerce types with safe defaults
    - Drop rows missing sale_id
    - Deduplicate by sale_id keeping latest sale_date
    """
    required = ["sale_id", "sale_date", "customer_id", "product_id", "quantity", "amount"]
    for col in required:
        if col not in df.columns:
            df[col] = None

    # Trim whitespace for object columns
    obj_cols = df.select_dtypes(include=["object"]).columns
    for c in obj_cols:
        df[c] = df[c].astype(str).str.strip().replace({"nan": None})

    # convert types
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(1).astype(int)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)

    # drop missing sale_id
    before = len(df)
    df = df.dropna(subset=["sale_id"])
    logger.info("Dropped %d rows missing sale_id", before - len(df))

    # deduplicate
    if "sale_date" in df.columns:
        df = df.sort_values("sale_date").drop_duplicates(subset=["sale_id"], keep="last")
    else:
        df = df.drop_duplicates(subset=["sale_id"], keep="last")

    # reorder
    df = df[required]
    return df
