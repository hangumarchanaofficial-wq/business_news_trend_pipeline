"""
Stage 3d: Sentiment analysis using VADER.
"""

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType

from src.utils.logger import get_logger

log = get_logger("transform.sentiment")

analyzer = SentimentIntensityAnalyzer()


def _sentiment_score(text: str) -> float:
    if not text:
        return 0.0
    scores = analyzer.polarity_scores(text)
    return float(scores["compound"])


def _sentiment_label(score: float) -> str:
    if score is None:
        return "neutral"
    if score >= 0.05:
        return "positive"
    elif score <= -0.05:
        return "negative"
    return "neutral"


score_udf = F.udf(_sentiment_score, FloatType())
label_udf = F.udf(_sentiment_label, StringType())


def add_sentiment(df: DataFrame) -> DataFrame:
    """Add sentiment_score (-1 to 1) and sentiment_label columns."""

    df = df.withColumn("sentiment_score", score_udf(F.col("cleaned_body")))
    df = df.withColumn("sentiment_label", label_udf(F.col("sentiment_score")))

    # ── Log distribution ──────────────────────────────────────
    log.info("Sentiment analysis complete")
    return df
