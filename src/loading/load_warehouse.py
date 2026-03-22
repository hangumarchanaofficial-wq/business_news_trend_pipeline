"""
Stage 5: Load transformed and aggregated DataFrames into a SQLite warehouse
using the star schema defined in sql/create_tables.sql.
"""

import os
import sqlite3

from pyspark.sql import DataFrame

from src.utils.logger import get_logger

log = get_logger("loading.warehouse")


class WarehouseLoader:
    """Write Spark DataFrames into SQLite warehouse tables."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self._create_tables()

    # ── Schema creation ───────────────────────────────────────

    def _create_tables(self):
        sql_path = "sql/create_tables.sql"
        if os.path.exists(sql_path):
            with open(sql_path, "r") as f:
                self.conn.executescript(f.read())
            log.info("Warehouse tables created / verified")
        else:
            log.warning(f"{sql_path} not found — creating tables inline")
            self._create_tables_inline()

    def _create_tables_inline(self):
        cur = self.conn.cursor()

        cur.execute("""
                    CREATE TABLE IF NOT EXISTS dim_source (
                                                              source_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                                                              source_name TEXT UNIQUE NOT NULL,
                                                              source_type TEXT
                    )
                    """)

        cur.execute("""
                    CREATE TABLE IF NOT EXISTS dim_topic (
                                                             topic_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                                                             topic_name TEXT UNIQUE NOT NULL
                    )
                    """)

        cur.execute("""
                    CREATE TABLE IF NOT EXISTS dim_date (
                                                            date_id     INTEGER PRIMARY KEY AUTOINCREMENT,
                                                            full_date   TEXT UNIQUE NOT NULL,
                                                            day_of_week INTEGER,
                                                            week_number INTEGER,
                                                            month       INTEGER,
                                                            year        INTEGER
                    )
                    """)

        cur.execute("""
                    CREATE TABLE IF NOT EXISTS fact_articles (
                                                                 article_id      TEXT PRIMARY KEY,
                                                                 source_id       INTEGER REFERENCES dim_source(source_id),
                        date_id         INTEGER REFERENCES dim_date(date_id),
                        topic_id        INTEGER REFERENCES dim_topic(topic_id),
                        title           TEXT,
                        url             TEXT,
                        cleaned_body    TEXT,
                        word_count      INTEGER,
                        sentiment_score REAL,
                        sentiment_label TEXT,
                        keywords        TEXT
                        )
                    """)

        cur.execute("""
                    CREATE TABLE IF NOT EXISTS fact_topic_daily (
                                                                    date_id       INTEGER REFERENCES dim_date(date_id),
                        topic_id      INTEGER REFERENCES dim_topic(topic_id),
                        article_count INTEGER,
                        avg_sentiment REAL,
                        top_keywords  TEXT,
                        PRIMARY KEY (date_id, topic_id)
                        )
                    """)

        cur.execute("""
                    CREATE TABLE IF NOT EXISTS fact_keyword_daily (
                                                                      date_id       INTEGER REFERENCES dim_date(date_id),
                        keyword       TEXT,
                        mention_count INTEGER,
                        avg_sentiment REAL,
                        sources_count INTEGER,
                        PRIMARY KEY (date_id, keyword)
                        )
                    """)

        cur.execute("""
                    CREATE TABLE IF NOT EXISTS fact_daily_summary (
                                                                      date_id        INTEGER PRIMARY KEY REFERENCES dim_date(date_id),
                        total_articles INTEGER,
                        avg_sentiment  REAL,
                        active_sources INTEGER,
                        dominant_topic TEXT
                        )
                    """)

        self.conn.commit()

    # ── Dimension loaders ─────────────────────────────────────

    def _upsert_dim_source(self, source_name, source_type=""):
        cur = self.conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO dim_source (source_name, source_type) VALUES (?, ?)",
            (source_name, source_type),
        )
        self.conn.commit()
        cur.execute(
            "SELECT source_id FROM dim_source WHERE source_name = ?",
            (source_name,),
        )
        return cur.fetchone()[0]

    def _upsert_dim_topic(self, topic_name):
        cur = self.conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO dim_topic (topic_name) VALUES (?)",
            (topic_name,),
        )
        self.conn.commit()
        cur.execute(
            "SELECT topic_id FROM dim_topic WHERE topic_name = ?",
            (topic_name,),
        )
        return cur.fetchone()[0]

    def _upsert_dim_date(self, full_date, dow, week, month, year):
        cur = self.conn.cursor()
        cur.execute(
            """INSERT OR IGNORE INTO dim_date
               (full_date, day_of_week, week_number, month, year)
               VALUES (?, ?, ?, ?, ?)""",
            (str(full_date), dow, week, month, year),
        )
        self.conn.commit()
        cur.execute(
            "SELECT date_id FROM dim_date WHERE full_date = ?",
            (str(full_date),),
        )
        return cur.fetchone()[0]

    # ── Fact loaders ──────────────────────────────────────────

    def load_fact_articles(self, df: DataFrame, source_types: dict):
        """Write the enriched article DataFrame into fact_articles + dimensions."""
        rows = df.collect()
        log.info(f"Loading {len(rows)} articles into warehouse...")

        for row in rows:
            source_type = source_types.get(row["source_standard"], "")
            source_id = self._upsert_dim_source(row["source_standard"], source_type)
            topic_id = self._upsert_dim_topic(row["topic"])
            date_id = self._upsert_dim_date(
                row["scraped_date"], row["day_of_week"],
                row["week_number"], row["month"], row["year"],
            )

            self.conn.execute(
                """INSERT OR REPLACE INTO fact_articles
                   (article_id, source_id, date_id, topic_id, title, url,
                    cleaned_body, word_count, sentiment_score,
                    sentiment_label, keywords)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    row["article_id"], source_id, date_id, topic_id,
                    row["title"], row["url"], row["cleaned_body"],
                    row["word_count"], row["sentiment_score"],
                    row["sentiment_label"], row["keywords"],
                ),
            )

        self.conn.commit()
        log.info("fact_articles loaded")

    def load_fact_topic_daily(self, df: DataFrame):
        rows = df.collect()
        log.info(f"Loading {len(rows)} topic-daily rows...")

        for row in rows:
            date_id = self._get_date_id(str(row["scraped_date"]))
            topic_id = self._upsert_dim_topic(row["topic"])
            if date_id is None:
                continue

            self.conn.execute(
                """INSERT OR REPLACE INTO fact_topic_daily
                   (date_id, topic_id, article_count, avg_sentiment, top_keywords)
                   VALUES (?, ?, ?, ?, ?)""",
                (date_id, topic_id, row["article_count"],
                 row["avg_sentiment"], row["top_keywords"]),
            )
        self.conn.commit()
        log.info("fact_topic_daily loaded")

    def load_fact_keyword_daily(self, df: DataFrame):
        rows = df.collect()
        log.info(f"Loading {len(rows)} keyword-daily rows...")

        for row in rows:
            date_id = self._get_date_id(str(row["scraped_date"]))
            if date_id is None:
                continue

            self.conn.execute(
                """INSERT OR REPLACE INTO fact_keyword_daily
                   (date_id, keyword, mention_count, avg_sentiment, sources_count)
                   VALUES (?, ?, ?, ?, ?)""",
                (date_id, row["keyword"], row["mention_count"],
                 row["avg_sentiment"], row["sources_count"]),
            )
        self.conn.commit()
        log.info("fact_keyword_daily loaded")

    def load_fact_daily_summary(self, df: DataFrame):
        rows = df.collect()
        log.info(f"Loading {len(rows)} daily summary rows...")

        for row in rows:
            date_id = self._get_date_id(str(row["scraped_date"]))
            if date_id is None:
                continue

            self.conn.execute(
                """INSERT OR REPLACE INTO fact_daily_summary
                   (date_id, total_articles, avg_sentiment,
                    active_sources, dominant_topic)
                   VALUES (?, ?, ?, ?, ?)""",
                (date_id, row["total_articles"], row["avg_sentiment"],
                 row["active_sources"], row.get("dominant_topic", "")),
            )
        self.conn.commit()
        log.info("fact_daily_summary loaded")

    # ── Helpers ───────────────────────────────────────────────

    def _get_date_id(self, full_date: str):
        cur = self.conn.cursor()
        cur.execute(
            "SELECT date_id FROM dim_date WHERE full_date = ?", (full_date,)
        )
        result = cur.fetchone()
        return result[0] if result else None

    def close(self):
        self.conn.close()
        log.info("Warehouse connection closed")