"""Serialise to gzip CSV and write to S3 (or local disk for dry runs).

Object keys are unique per run (run_id in the file name) because Snowpipe de-duplicates
by file name for 14 days: re-using a name would silently skip the load. Idempotency is
therefore handled downstream by MERGE on business keys, not by overwriting files.
"""
from __future__ import annotations

import gzip
import hashlib
import io
import json
import logging
from datetime import date
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)


def build_key(prefix: str, source: str, run_date: date, run_id: str) -> str:
    return f"{prefix}/{source}/dt={run_date.isoformat()}/{source}_{run_id}.csv.gz"


def to_csv_gz_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as gz:  # mtime=0 -> reproducible bytes
        gz.write(df.to_csv(index=False).encode("utf-8"))
    return buffer.getvalue()


def sha256_bytes(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def upload_s3(bucket: str, key: str, body: bytes, region: str, content_type: str = "application/gzip") -> None:
    import boto3  # imported lazily so unit tests and dry runs need no AWS SDK

    client = boto3.client("s3", region_name=region)
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
        ServerSideEncryption="AES256",
    )
    log.info("uploaded", extra={"ctx": {"bucket": bucket, "key": key, "bytes": len(body)}})


def write_local(out_dir: str, key: str, body: bytes) -> Path:
    path = Path(out_dir) / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    log.info("wrote local file", extra={"ctx": {"path": str(path), "bytes": len(body)}})
    return path


def manifest_key(run_date: date, run_id: str) -> str:
    return f"manifests/dt={run_date.isoformat()}/{run_id}.json"


def manifest_bytes(manifest: dict) -> bytes:
    return json.dumps(manifest, indent=2, default=str).encode("utf-8")
