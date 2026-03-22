"""
Stage 3a: Handle missing values in the scraped data.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import get_logger

log = get_logger("transform.missing")


def handle_missing(df: DataFrame) -> DataFrame:
    """
    Fill or handle missing values:
      - title: replace nulls with 'Untitled'
      - section: replace nulls with 'general'
      - body: replace nulls with empty string (these should have been
              filtered in validation, but belt-and-braces)
      - scraped_at: fill with current timestamp
    """

    fills = {
        "title": "Untitled",
        "section": "general",
        "body": "",
    }
    df = df.fillna(fills)

    df = df.withColumn(
        "scraped_at",
        F.when(
            F.col("scraped_at").isNull(),
            F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss")
        ).otherwise(F.col("scraped_at"))
    )

    log.info("Missing value handling complete")
    return df
