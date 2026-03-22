"""
S3 Data Lake Manager — handles upload/download of raw articles
and warehouse artefacts to/from Amazon S3.
"""

import os
import json
import boto3
from botocore.exceptions import ClientError

from src.utils.logger import get_logger

log = get_logger("storage.s3")


class S3DataLakeManager:
    """Manages the S3-based data lake for the pipeline."""

    def __init__(self, config_path="config/aws_config.yaml"):
        import yaml
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

        self.region = cfg["aws"]["region"]
        self.bucket_name = cfg["s3"]["bucket_name"]
        self.raw_prefix = cfg["s3"]["raw_prefix"]
        self.raw_key = cfg["s3"]["raw_key"]
        self.warehouse_prefix = cfg["s3"]["warehouse_prefix"]

        self.s3 = boto3.client("s3", region_name=self.region)
        self._ensure_bucket()

    # ── Bucket management ─────────────────────────────────────

    def _ensure_bucket(self):
        """Create the S3 bucket if it doesn't exist."""
        try:
            self.s3.head_bucket(Bucket=self.bucket_name)
            log.info(f"S3 bucket '{self.bucket_name}' exists")
        except ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                log.info(f"Creating S3 bucket '{self.bucket_name}'...")
                if self.region == "us-east-1":
                    self.s3.create_bucket(Bucket=self.bucket_name)
                else:
                    self.s3.create_bucket(
                        Bucket=self.bucket_name,
                        CreateBucketConfiguration={
                            "LocationConstraint": self.region
                        },
                    )
                log.info(f"S3 bucket '{self.bucket_name}' created")
            else:
                log.error(f"S3 bucket check failed: {e}")
                raise

    # ── Upload / Download raw articles ────────────────────────

    def upload_raw_articles(self, local_path: str):
        """Upload the local raw JSON file to S3."""
        if not os.path.exists(local_path):
            log.warning(f"Local file not found: {local_path}")
            return False

        try:
            self.s3.upload_file(local_path, self.bucket_name, self.raw_key)
            log.info(
                f"Uploaded {local_path} → s3://{self.bucket_name}/{self.raw_key}"
            )
            return True
        except ClientError as e:
            log.error(f"S3 upload failed: {e}")
            return False

    def download_raw_articles(self, local_path: str):
        """Download the raw JSON file from S3 to local disk."""
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        try:
            self.s3.download_file(self.bucket_name, self.raw_key, local_path)
            log.info(
                f"Downloaded s3://{self.bucket_name}/{self.raw_key} → {local_path}"
            )
            return True
        except ClientError as e:
            log.warning(f"S3 download failed (may not exist yet): {e}")
            return False

    def upload_warehouse(self, local_db_path: str):
        """Upload the SQLite warehouse DB to S3 for backup."""
        if not os.path.exists(local_db_path):
            log.warning(f"Warehouse DB not found: {local_db_path}")
            return False

        s3_key = self.warehouse_prefix + os.path.basename(local_db_path)
        try:
            self.s3.upload_file(local_db_path, self.bucket_name, s3_key)
            log.info(
                f"Uploaded warehouse → s3://{self.bucket_name}/{s3_key}"
            )
            return True
        except ClientError as e:
            log.error(f"Warehouse upload failed: {e}")
            return False

    # ── Utility ───────────────────────────────────────────────

    def list_raw_objects(self):
        """List all objects under the raw prefix."""
        try:
            response = self.s3.list_objects_v2(
                Bucket=self.bucket_name, Prefix=self.raw_prefix
            )
            objects = response.get("Contents", [])
            for obj in objects:
                log.info(f"  s3://{self.bucket_name}/{obj['Key']}  "
                         f"({obj['Size']} bytes)")
            return objects
        except ClientError as e:
            log.error(f"S3 list failed: {e}")
            return []

    def get_s3_uri(self, key: str) -> str:
        """Return the s3:// URI for a given key."""
        return f"s3://{self.bucket_name}/{key}"
