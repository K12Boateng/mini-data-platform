# src/validator.py
"""
Validation utilities. Supports CSV, JSON, and Parquet input validation.
Returns a boolean valid flag and an optional error message.
"""
from typing import Tuple
import logging
import pandas as pd
import json
import io

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"sale_id", "sale_date", "customer_id", "product_id", "quantity", "amount"}


def detect_format(key: str, data: bytes) -> str:
    """
    Detect file format from key extension or content heuristics.
    Supported: csv, json, parquet
    """
    key_lower = key.lower()
    if key_lower.endswith(".csv"):
        return "csv"
    if key_lower.endswith(".json") or key_lower.endswith(".ndjson"):
        return "json"
    if key_lower.endswith(".parquet"):
        return "parquet"
    # fallback: try to infer from content
    head = data[:4]
    if head.startswith(b'PK') or head[:4] == b'PAR1':
        return "parquet"
    # default to csv
    return "csv"


def validate_csv(data: bytes) -> Tuple[bool, str]:
    """
    Validate CSV content for required columns and reasonable types.
    """
    try:
        df = pd.read_csv(io.BytesIO(data), nrows=1000)  # sample for validation
    except Exception as e:
        logger.exception("CSV parse error: %s", e)
        return False, f"CSV parse error: {e}"

    cols = set([c.strip().lower() for c in df.columns])
    missing = REQUIRED_COLUMNS - cols
    if missing:
        msg = f"Missing required columns: {sorted(list(missing))}"
        logger.warning(msg)
        return False, msg

    # basic type checks (sample)
    try:
        pd.to_datetime(df["sale_date"].iloc[:50], errors="raise")
    except Exception as e:
        logger.warning("sale_date parse issue: %s", e)
        return False, f"sale_date parse issue: {e}"

    return True, ""


def validate_json(data: bytes) -> Tuple[bool, str]:
    """
    Validate JSON (ndjson or array) for required fields in a sample of records.
    """
    try:
        text = data.decode("utf-8")
        # try ndjson first
        lines = [l for l in text.splitlines() if l.strip()]
        sample = lines[:100]
        for line in sample:
            rec = json.loads(line)
            if not REQUIRED_COLUMNS.issubset(set([k.lower() for k in rec.keys()])):
                missing = REQUIRED_COLUMNS - set([k.lower() for k in rec.keys()])
                return False, f"Missing columns: {missing}"
    except Exception as e:
        logger.exception("JSON parse error: %s", e)
        return False, f"JSON parse error: {e}"

    return True, ""


def validate_parquet(data: bytes) -> Tuple[bool, str]:
    """
    Validate parquet by reading columns.
    """
    try:
        df = pd.read_parquet(io.BytesIO(data))
    except Exception as e:
        logger.exception("Parquet parse error: %s", e)
        return False, f"Parquet parse error: {e}"

    cols = set([c.strip().lower() for c in df.columns])
    missing = REQUIRED_COLUMNS - cols
    if missing:
        return False, f"Missing required columns: {sorted(list(missing))}"
    return True, ""
