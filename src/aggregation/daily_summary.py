"""
Stage 4c: Overall daily summary across all sources.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import get_logger

log = get_logger("aggregation.daily")


def aggregate_daily_summary(df: DataFrame) -> DataFrame:
    """
    Per-day summary:
      - total articles
      - avg sentiment
      - articles per source
      - dominant topic
    """
    agg_df = df.groupBy("scraped_date").agg(
        F.count("article_id").alias("total_articles"),
        F.round(F.avg("sentiment_score"), 4).alias("avg_sentiment"),
        F.countDistinct("source_standard").alias("active_sources"),
    )

    # Find dominant topic per day via a sub-aggregation
    topic_per_day = df.groupBy("scraped_date", "topic").agg(
        F.count("article_id").alias("cnt")
    )
    from pyspark.sql.window import Window
    w = Window.partitionBy("scraped_date").orderBy(F.col("cnt").desc())
    dominant = topic_per_day.withColumn("rn", F.row_number().over(w)) \
        .filter(F.col("rn") == 1) \
        .select("scraped_date", F.col("topic").alias("dominant_topic"))

    result = agg_df.join(dominant, on="scraped_date", how="left")

    log.info(f"Daily summary aggregation: {result.count()} rows")
    return result