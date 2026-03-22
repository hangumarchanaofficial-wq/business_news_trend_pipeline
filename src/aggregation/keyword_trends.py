"""
Stage 4b: Explode keywords and compute daily mention counts / sentiment.
→ produces fact_keyword_daily
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import get_logger

log = get_logger("aggregation.keywords")


def aggregate_keyword_daily(df: DataFrame) -> DataFrame:
    """
    Explode the comma-separated keywords column, then aggregate
    per (scraped_date, keyword).
    """

    # Explode comma-separated keywords into individual rows
    exploded = df.withColumn(
        "keyword",
        F.explode(F.split(F.col("keywords"), ",\\s*"))
    ).filter(
        (F.col("keyword").isNotNull()) &
        (F.length(F.trim(F.col("keyword"))) > 2)
    ).withColumn("keyword", F.lower(F.trim(F.col("keyword"))))

    agg_df = exploded.groupBy("scraped_date", "keyword").agg(
        F.count("article_id").alias("mention_count"),
        F.round(F.avg("sentiment_score"), 4).alias("avg_sentiment"),
        F.countDistinct("source_standard").alias("sources_count"),
    )

    log.info(f"Keyword daily aggregation: {agg_df.count()} rows")
    return agg_df