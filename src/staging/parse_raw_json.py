"""
Stage 2a: Load the raw JSON data lake into a PySpark DataFrame.
"""

import json
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

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


def load_raw_to_spark(spark: SparkSession, raw_json_path: str):
    """Read the raw JSON array file into a Spark DataFrame with enforced schema."""

    log.info(f"Loading raw JSON from {raw_json_path}")

    with open(raw_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        if isinstance(data, dict) and "articles" in data:
            articles = data["articles"]
        elif isinstance(data, list):
            articles = data
        else:
            articles = []
            
    import uuid
    for a in articles:
        if "full_text" in a and "body" not in a:
            a["body"] = a.pop("full_text")
        if "article_id" not in a:
            a["article_id"] = str(uuid.uuid4())

    log.info(f"Loaded {len(articles)} raw articles")

    if not articles:
        return spark.createDataFrame([], RAW_SCHEMA)

    df = spark.createDataFrame(articles, schema=RAW_SCHEMA)
    log.info(f"Spark DataFrame created: {df.count()} rows, {len(df.columns)} columns")
    return df
