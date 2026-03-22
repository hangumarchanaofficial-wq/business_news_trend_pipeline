"""
Stage 2a: Load the raw JSON data lake into a PySpark DataFrame.
Tries to download the latest version from S3 first.
"""

import json
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from src.utils.logger import get_logger

log = get_logger("staging.parse")

RAW_SCHEMA = StructType([
    StructField("article_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("section", StringType(), True),
    StructField("title", StringType(), True),
    StructField("url", StringType(), True),
    StructField("body", StringType(), True),
    StructField("scraped_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])


def load_raw_to_spark(spark: SparkSession, raw_json_path: str,
                      s3_manager=None):
    """Read the raw JSON array file into a Spark DataFrame with enforced schema.
    If s3_manager is provided, downloads the latest file from S3 first."""

    # Pull latest from S3 before reading
    if s3_manager:
        log.info("Downloading latest raw data from S3...")
        s3_manager.download_raw_articles(raw_json_path)

    log.info(f"Loading raw JSON from {raw_json_path}")

    with open(raw_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        if isinstance(data, dict) and "articles" in data:
            articles = data["articles"]
        elif isinstance(data, list):
            articles = data
        else:
            articles = []

    for a in articles:
        if "full_text" in a and "body" not in a:
            a["body"] = a.pop("full_text")
        if "article_id" not in a:
            a["article_id"] = str(uuid.uuid4())

    log.info(f"Loaded {len(articles)} raw articles")

    if not articles:
        return spark.createDataFrame([], RAW_SCHEMA)

    df = spark.createDataFrame(articles, schema=RAW_SCHEMA)
    log.info(f"Spark DataFrame created: {df.count()} rows, "
             f"{len(df.columns)} columns")
    return df
