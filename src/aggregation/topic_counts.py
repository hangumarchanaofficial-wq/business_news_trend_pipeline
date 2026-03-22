"""
Stage 4a: Aggregate article counts and average sentiment by topic per day.
→ produces fact_topic_daily
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import get_logger

log = get_logger("aggregation.topics")


def aggregate_topic_daily(df: DataFrame) -> DataFrame:
    """
    Group by (scraped_date, topic) and compute:
      - article_count
      - avg_sentiment
      - top_keywords (concatenated)
    """
    agg_df = df.groupBy("scraped_date", "topic").agg(
        F.count("article_id").alias("article_count"),
        F.round(F.avg("sentiment_score"), 4).alias("avg_sentiment"),
        F.concat_ws(
            " | ",
            F.collect_set(F.col("keywords"))
        ).alias("top_keywords"),
    )

    log.info(f"Topic daily aggregation: {agg_df.count()} rows")
    return agg_df