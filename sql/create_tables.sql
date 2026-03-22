-- ══════════════════════════════════════════════════════════════
--  Sri Lankan Business News Trend Pipeline — Star Schema DDL
-- ══════════════════════════════════════════════════════════════

-- ── Dimension tables ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_source (
                                          source_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                                          source_name TEXT    UNIQUE NOT NULL,
                                          source_type TEXT
);

CREATE TABLE IF NOT EXISTS dim_topic (
                                         topic_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                                         topic_name TEXT    UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
                                        date_id     INTEGER PRIMARY KEY AUTOINCREMENT,
                                        full_date   TEXT    UNIQUE NOT NULL,
                                        day_of_week INTEGER,
                                        week_number INTEGER,
                                        month       INTEGER,
                                        year        INTEGER
);

-- ── Fact tables ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_articles (
                                             article_id      TEXT    PRIMARY KEY,
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
    );

CREATE TABLE IF NOT EXISTS fact_topic_daily (
                                                date_id       INTEGER REFERENCES dim_date(date_id),
    topic_id      INTEGER REFERENCES dim_topic(topic_id),
    article_count INTEGER,
    avg_sentiment REAL,
    top_keywords  TEXT,
    PRIMARY KEY (date_id, topic_id)
    );

CREATE TABLE IF NOT EXISTS fact_keyword_daily (
                                                  date_id       INTEGER REFERENCES dim_date(date_id),
    keyword       TEXT,
    mention_count INTEGER,
    avg_sentiment REAL,
    sources_count INTEGER,
    PRIMARY KEY (date_id, keyword)
    );

CREATE TABLE IF NOT EXISTS fact_daily_summary (
                                                  date_id        INTEGER PRIMARY KEY REFERENCES dim_date(date_id),
    total_articles INTEGER,
    avg_sentiment  REAL,
    active_sources INTEGER,
    dominant_topic TEXT
    );