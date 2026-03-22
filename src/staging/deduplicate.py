"""
Stage 2c: Remove duplicate articles — uses URL as the deduplication key.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.logger import get_logger

log = get_logger("staging.dedup")


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Deduplicate on URL, keeping the most recently scraped version.
    """
    before_count = df.count()

    window = Window.partitionBy("url").orderBy(F.col("scraped_at").desc())
    df = df.withColumn("_row_num", F.row_number().over(window))
    df = df.filter(F.col("_row_num") == 1).drop("_row_num")

    after_count = df.count()
    removed = before_count - after_count
    log.info(
        f"Deduplication: {before_count} → {after_count} "
        f"({removed} duplicates removed)"
    )
    return df
