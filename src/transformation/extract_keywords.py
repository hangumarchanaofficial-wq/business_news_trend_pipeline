"""
Stage 3e: Extract top keywords using RAKE (Rapid Automatic Keyword Extraction).
"""

from rake_nltk import Rake

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.utils.logger import get_logger

log = get_logger("transform.keywords")


def _extract_top_keywords(text: str, top_n: int = 10) -> str:
    """Return comma-separated top-N keywords from the text."""
    if not text or len(text.strip()) < 30:
        return ""
    try:
        rake = Rake(max_length=3)
        rake.extract_keywords_from_text(text)
        phrases = rake.get_ranked_phrases()[:top_n]
        return ", ".join(phrases)
    except Exception:
        return ""


keywords_udf = F.udf(_extract_top_keywords, StringType())


def extract_keywords(df: DataFrame) -> DataFrame:
    """Add a 'keywords' column with top-10 RAKE keywords."""

    df = df.withColumn("keywords", keywords_udf(F.col("cleaned_body")))
    log.info("Keyword extraction complete")
    return df
