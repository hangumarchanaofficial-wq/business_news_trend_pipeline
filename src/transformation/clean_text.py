"""
Stage 3b: Clean and normalise article body text.
"""

import re
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.utils.logger import get_logger

log = get_logger("transform.clean")


def _clean(text: str) -> str:
    """Pure-Python text cleaner used as a Spark UDF."""
    if not text:
        return ""

    # Remove URLs
    text = re.sub(r"https?://\S+", "", text)
    # Remove email addresses
    text = re.sub(r"\S+@\S+\.\S+", "", text)
    # Remove HTML entities that slipped through
    text = re.sub(r"&[a-z]+;", " ", text)
    # Remove non-ASCII (Sinhala / Tamil fragments sometimes appear)
    text = re.sub(r"[^\x00-\x7F]+", " ", text)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()

    return text


clean_udf = F.udf(_clean, StringType())


def clean_text(df: DataFrame) -> DataFrame:
    """Apply text cleaning to body and title columns."""

    df = df.withColumn("cleaned_body", clean_udf(F.col("body")))
    df = df.withColumn("title", F.trim(F.col("title")))

    # Word count on cleaned body
    df = df.withColumn(
        "word_count",
        F.size(F.split(F.col("cleaned_body"), "\\s+"))
    )

    log.info("Text cleaning complete")
    return df
