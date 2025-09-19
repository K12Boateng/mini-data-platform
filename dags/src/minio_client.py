
# src/minio_client.py
"""
S3-compatible wrapper using boto3 configured for MinIO.
Provides list/download/upload/move helpers with logging and retries.
"""
from typing import List, Dict
import logging
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError
from io import BytesIO
from .config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

logger = logging.getLogger(__name__)


def _s3_client():
    """
    Return a boto3 S3 client configured to use the MinIO endpoint.
    """
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(bucket_name: str) -> None:
    """
    Ensure the bucket exists; create if missing.
    """
    s3 = _s3_client()
    try:
        s3.head_bucket(Bucket=bucket_name)
        logger.info("Bucket exists: %s", bucket_name)
    except ClientError:
        logger.info("Creating bucket: %s", bucket_name)
        s3.create_bucket(Bucket=bucket_name)


def list_objects(bucket_name: str, prefix: str = "") -> List[Dict]:
    """
    List objects under prefix and return list of dicts with Key, Size, LastModified.
    """
    s3 = _s3_client()
    try:
        paginator = s3.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        items = []
        for page in page_iterator:
            contents = page.get("Contents") or []
            for obj in contents:
                items.append(obj)
        logger.debug("Listed %d objects in %s/%s", len(items), bucket_name, prefix)
        return items
    except EndpointConnectionError as e:
        logger.exception("Endpoint connection error when listing objects: %s", e)
        raise
    except ClientError as e:
        logger.exception("Failed to list objects: %s", e)
        raise


def download_to_bytes(bucket_name: str, key: str) -> bytes:
    """
    Download an object and return its bytes.
    """
    s3 = _s3_client()
    try:
        resp = s3.get_object(Bucket=bucket_name, Key=key)
        data = resp["Body"].read()
        logger.info("Downloaded %s/%s (%d bytes)", bucket_name, key, len(data))
        return data
    except ClientError as e:
        logger.exception("Failed to download %s/%s: %s", bucket_name, key, e)
        raise


def upload_file_bytes(bucket_name: str, key: str, data: bytes) -> None:
    """
    Upload bytes to a key in the bucket.
    """
    s3 = _s3_client()
    try:
        s3.put_object(Bucket=bucket_name, Key=key, Body=data)
        logger.info("Uploaded %s/%s (%d bytes)", bucket_name, key, len(data))
    except ClientError as e:
        logger.exception("Failed to upload to %s/%s: %s", bucket_name, key, e)
        raise


def move_object(bucket_name: str, src_key: str, dest_key: str) -> None:
    """
    Move object in bucket by copy + delete.
    """
    s3 = _s3_client()
    try:
        copy_source = {"Bucket": bucket_name, "Key": src_key}
        s3.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=dest_key)
        s3.delete_object(Bucket=bucket_name, Key=src_key)
        logger.info("Moved %s -> %s in bucket %s", src_key, dest_key, bucket_name)
    except ClientError as e:
        logger.exception("Failed to move object: %s", e)
        raise