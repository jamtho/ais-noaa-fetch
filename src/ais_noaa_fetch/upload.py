# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 James Thompson

"""Upload Parquet files to S3-compatible storage."""

from __future__ import annotations

import os
from pathlib import Path

import boto3
from dotenv import load_dotenv
from tqdm import tqdm


def _get_client() -> boto3.client:
    """Create an S3 client from environment variables."""
    load_dotenv()
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    if not endpoint_url:
        raise RuntimeError("S3_ENDPOINT_URL not set in .env")
    return boto3.client("s3", endpoint_url=endpoint_url)


def _get_bucket() -> str:
    """Return the configured S3 bucket name."""
    load_dotenv()
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise RuntimeError("S3_BUCKET not set in .env")
    return bucket


def ensure_bucket(client: boto3.client, bucket: str) -> None:
    """Create the bucket if it doesn't already exist."""
    try:
        client.head_bucket(Bucket=bucket)
    except client.exceptions.ClientError:
        client.create_bucket(Bucket=bucket)


def _remote_size(client: boto3.client, bucket: str, key: str) -> int | None:
    """Return the size of an object in S3, or None if it doesn't exist."""
    try:
        resp = client.head_object(Bucket=bucket, Key=key)
        return resp["ContentLength"]
    except client.exceptions.ClientError:
        return None


def upload_parquet(
    data_dir: Path,
    skip_existing: bool = True,
) -> list[str]:
    """Upload all Parquet files from data_dir/parquet/ to S3.

    When *skip_existing* is True (the default), files whose S3 size
    matches the local size are skipped.

    Returns a list of S3 keys that were uploaded.
    """
    client = _get_client()
    bucket = _get_bucket()
    ensure_bucket(client, bucket)

    parquet_dir = data_dir / "parquet"
    if not parquet_dir.exists():
        raise FileNotFoundError(f"No parquet directory at {parquet_dir}")

    files = sorted(parquet_dir.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No .parquet files found in {parquet_dir}")

    uploaded: list[str] = []
    skipped = 0

    for path in tqdm(files, desc="Uploading", unit="file"):
        key = str(path.relative_to(parquet_dir))
        local_size = path.stat().st_size

        if skip_existing:
            remote = _remote_size(client, bucket, key)
            if remote is not None and remote == local_size:
                skipped += 1
                continue

        size_mb = local_size / (1024 * 1024)
        tqdm.write(f"  {key} ({size_mb:.1f} MB)")
        client.upload_file(str(path), bucket, key)
        uploaded.append(key)

    if skipped:
        print(f"Skipped {skipped} files (already in S3).")

    return uploaded
