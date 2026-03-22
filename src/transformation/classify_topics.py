"""
Stage 3f: Classify each article into a business topic using keyword rules.
"""

import yaml

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.broadcast import Broadcast

from src.utils.logger import get_logger

log = get_logger("transform.topics")


def _load_topic_rules(config_path="config/pipeline_config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg.get("topics", {})


def classify_topics(df: DataFrame, spark=None) -> DataFrame:
    """
    Add 'topic' column based on keyword rules.
    Matches against cleaned_body (lowercased).  If no topic matches,
    assigns 'General'.
    """
    topic_rules = _load_topic_rules()

    def _classify(text: str) -> str:
        if not text:
            return "General"
        lower = text.lower()
        scores = {}
        for topic, kws in topic_rules.items():
            count = sum(1 for kw in kws if kw in lower)
            if count > 0:
                scores[topic] = count
        if not scores:
            return "General"
        return max(scores, key=scores.get)

    classify_udf = F.udf(_classify, StringType())
    df = df.withColumn("topic", classify_udf(F.col("cleaned_body")))

    log.info("Topic classification complete")
    return df