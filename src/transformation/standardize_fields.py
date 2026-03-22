"""
Stage 3c: Standardise source names, sections, and date formats.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import get_logger

log = get_logger("transform.standardize")

SOURCE_MAP = {
    "DailyMirror": "Daily Mirror",
    "TheMorning": "The Morning",
    "FT.lk": "FT.lk",
    "EconomyNext": "EconomyNext",
    "EconomicTimes.lk": "EconomyNext",
    "SundayTimes": "Sunday Times",
    "LMD": "LMD",
}


def standardize_fields(df: DataFrame) -> DataFrame:
    """
    - Map source names to a canonical form
    - Lowercase section names
    - Parse scraped_at into a proper timestamp and extract date parts
    """

    # ── Standardise source name ───────────────────────────────
    mapping_expr = F.create_map(
        [F.lit(x) for pair in SOURCE_MAP.items() for x in pair]
    )
    df = df.withColumn(
        "source_standard",
        F.coalesce(mapping_expr[F.col("source")], F.col("source"))
    )

    # ── Lowercase section ─────────────────────────────────────
    df = df.withColumn("section", F.lower(F.trim(F.col("section"))))

    # ── Parse datetime ────────────────────────────────────────
    df = df.withColumn(
        "scraped_ts",
        F.to_timestamp(F.col("scraped_at"), "yyyy-MM-dd HH:mm:ss")
    )
    df = df.withColumn("scraped_date", F.to_date(F.col("scraped_ts")))
    df = df.withColumn("day_of_week", F.dayofweek(F.col("scraped_date")))
    df = df.withColumn("week_number", F.weekofyear(F.col("scraped_date")))
    df = df.withColumn("month", F.month(F.col("scraped_date")))
    df = df.withColumn("year", F.year(F.col("scraped_date")))

    log.info("Field standardisation complete")
    return df
