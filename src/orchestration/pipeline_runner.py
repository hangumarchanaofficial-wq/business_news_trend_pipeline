"""
Pipeline orchestrator — runs the entire ETL pipeline on EC2 with S3 data lake.
"""

import time
import os
import sys
import yaml
from datetime import datetime

from pyspark.sql import SparkSession

from src.utils.logger import get_logger

# ── Stage imports ─────────────────────────────────────────────
from src.ingestion.raw_storage import RawStorage
from src.ingestion.news_scraper import NewsScraper
from src.storage.s3_manager import S3DataLakeManager
from src.staging.parse_raw_json import load_raw_to_spark
from src.staging.validate_schema import validate_schema
from src.staging.deduplicate import deduplicate
from src.transformation.handle_missing import handle_missing
from src.transformation.clean_text import clean_text
from src.transformation.standardize_fields import standardize_fields
from src.transformation.sentiment_analysis import add_sentiment
from src.transformation.extract_keywords import extract_keywords
from src.transformation.classify_topics import classify_topics
from src.aggregation.topic_counts import aggregate_topic_daily
from src.aggregation.keyword_trends import aggregate_keyword_daily
from src.aggregation.daily_summary import aggregate_daily_summary
from src.loading.load_warehouse import WarehouseLoader

log = get_logger("orchestration")


def _load_config(path="config/pipeline_config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _load_source_types(path="config/scraper_config.yaml") -> dict:
    """Build a {source_standard_name: source_type} mapping."""
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    mapping = {}
    name_map = {
        "daily_mirror": "Daily Mirror",
        "the_morning": "The Morning",
        "ft_lk": "FT.lk",
        "economic_times": "EconomyNext",
        "sunday_times": "Sunday Times",
        "lmd": "LMD",
    }
    for key, details in cfg.get("sources", {}).items():
        standard = name_map.get(key, key)
        mapping[standard] = details.get("source_type", "")
    return mapping


def run_pipeline(skip_scraping: bool = False):
    """
    Execute the full pipeline on EC2:
        1. Ingest  (web scrape → local JSON → S3 data lake)
        2. Stage   (S3 → local → Spark DF, validate, deduplicate)
        3. Transform (clean, sentiment, keywords, topics)
        4. Aggregate (topic daily, keyword daily, daily summary)
        5. Load    (write to SQLite star-schema warehouse → backup to S3)
    """

    cfg = _load_config()
    start = time.time()
    log.info("=" * 60)
    log.info(f"PIPELINE START — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"Environment: EC2 + S3 Data Lake")
    log.info("=" * 60)

    # ── Initialize S3 Data Lake Manager ───────────────────────
    s3_mgr = S3DataLakeManager()
    log.info(f"S3 bucket: {s3_mgr.bucket_name}")

    # ── 1. INGESTION ──────────────────────────────────────────
    raw = RawStorage(cfg["paths"]["raw_articles_file"], s3_manager=s3_mgr)

    if not skip_scraping:
        log.info("STAGE 1: Ingestion (web scraping)")
        removed = raw.cleanup_old_articles(cfg["ingestion"]["retention_days"])
        log.info(f"Cleaned up {removed} expired articles")

        stats_before = raw.get_stats()
        log.info(f"Articles before scrape: {stats_before['total_articles']}")

        scraper = NewsScraper(raw)
        scraper.run_all()

        stats_after = raw.get_stats()
        new_count = stats_after["total_articles"] - stats_before["total_articles"]
        log.info(f"New articles scraped: {new_count}")
        log.info(f"Total in data lake: {stats_after['total_articles']}")
    else:
        log.info("STAGE 1: Ingestion SKIPPED (--skip-scraping)")

    raw.close()  # saves locally + syncs to S3

    # ── 2. STAGING (Spark) ────────────────────────────────────
    log.info("STAGE 2: Staging")

    spark = SparkSession.builder \
        .appName(cfg["spark"]["app_name"]) \
        .master(cfg["spark"]["master"]) \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.pyspark.python", sys.executable) \
        .config("spark.pyspark.driver.python", sys.executable) \
        .getOrCreate()
    spark.sparkContext.setLogLevel(cfg["spark"]["log_level"])

    df = load_raw_to_spark(
        spark, cfg["paths"]["raw_articles_file"], s3_manager=s3_mgr
    )

    if df.count() == 0:
        log.warning("No articles to process — pipeline complete (empty)")
        spark.stop()
        return

    df = validate_schema(df)
    df = deduplicate(df)

    # ── 3. TRANSFORMATION ─────────────────────────────────────
    log.info("STAGE 3: Transformation")

    df = handle_missing(df)
    df = clean_text(df)
    df = standardize_fields(df)
    df = add_sentiment(df)
    df = extract_keywords(df)
    df = classify_topics(df, spark)

    # Cache the enriched DataFrame — used by all aggregations
    df.cache()
    log.info(f"Enriched DataFrame: {df.count()} articles ready")

    # ── 4. AGGREGATION ────────────────────────────────────────
    log.info("STAGE 4: Aggregation")

    topic_daily_df = aggregate_topic_daily(df)
    keyword_daily_df = aggregate_keyword_daily(df)
    daily_summary_df = aggregate_daily_summary(df)

    # ── 5. LOADING ────────────────────────────────────────────
    log.info("STAGE 5: Loading to warehouse")

    source_types = _load_source_types()
    warehouse = WarehouseLoader(cfg["paths"]["warehouse_db"])

    warehouse.load_fact_articles(df, source_types)
    warehouse.load_fact_topic_daily(topic_daily_df)
    warehouse.load_fact_keyword_daily(keyword_daily_df)
    warehouse.load_fact_daily_summary(daily_summary_df)

    warehouse.close()

    # ── Backup warehouse to S3 ────────────────────────────────
    log.info("Backing up warehouse to S3...")
    s3_mgr.upload_warehouse(cfg["paths"]["warehouse_db"])

    # ── Cleanup ───────────────────────────────────────────────
    df.unpersist()
    spark.stop()

    elapsed = round(time.time() - start, 1)
    log.info("=" * 60)
    log.info(f"PIPELINE COMPLETE — {elapsed}s elapsed")
    log.info(f"Raw data: s3://{s3_mgr.bucket_name}/{s3_mgr.raw_key}")
    log.info(f"Warehouse: {cfg['paths']['warehouse_db']} (backed up to S3)")
    log.info("=" * 60)
