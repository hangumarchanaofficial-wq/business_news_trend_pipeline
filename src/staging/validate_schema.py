"""
Stage 2b: Validate schema — drop rows with null required fields, flag corrupt data.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import get_logger

log = get_logger("staging.validate")


def validate_schema(df: DataFrame) -> DataFrame:
    """
    Validate required fields and flag corrupt rows.

    Required fields: article_id, source, url
    Corrupt data check: body that is just whitespace or under 20 chars
    """
    before_count = df.count()

    # ── Drop rows missing required fields ─────────────────────
    df = df.dropna(subset=["article_id", "source", "url"])

    # ── Flag corrupt / too-short body ─────────────────────────
    df = df.withColumn(
        "is_valid_body",
        F.when(
            (F.col("body").isNotNull()) &
            (F.length(F.trim(F.col("body"))) > 20),
            True
        ).otherwise(False)
    )

    # ── Keep only valid-body rows ─────────────────────────────
    valid_df = df.filter(F.col("is_valid_body") == True).drop("is_valid_body")
    after_count = valid_df.count()

    removed = before_count - after_count
    log.info(
        f"Schema validation: {before_count} → {after_count} "
        f"({removed} invalid rows removed)"
    )
    return valid_df
