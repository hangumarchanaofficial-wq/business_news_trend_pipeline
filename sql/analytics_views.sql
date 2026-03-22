-- ══════════════════════════════════════════════════════════════
--  Analytical views for BI dashboards
-- ══════════════════════════════════════════════════════════════

-- View 1: Sector Sentiment Tracker
CREATE VIEW IF NOT EXISTS vw_sector_sentiment AS
SELECT
    d.full_date,
    t.topic_name,
    ftd.article_count,
    ftd.avg_sentiment
FROM fact_topic_daily ftd
         JOIN dim_date  d ON d.date_id  = ftd.date_id
         JOIN dim_topic t ON t.topic_id = ftd.topic_id
ORDER BY d.full_date, t.topic_name;


-- View 2: Trending Keywords
CREATE VIEW IF NOT EXISTS vw_trending_keywords AS
SELECT
    d.full_date,
    fkd.keyword,
    fkd.mention_count,
    fkd.avg_sentiment,
    fkd.sources_count
FROM fact_keyword_daily fkd
         JOIN dim_date d ON d.date_id = fkd.date_id
ORDER BY d.full_date DESC, fkd.mention_count DESC;


-- View 3: Source Coverage Comparison
CREATE VIEW IF NOT EXISTS vw_source_coverage AS
SELECT
    d.full_date,
    s.source_name,
    t.topic_name,
    COUNT(fa.article_id) AS article_count
FROM fact_articles fa
         JOIN dim_date   d ON d.date_id   = fa.date_id
         JOIN dim_source s ON s.source_id = fa.source_id
         JOIN dim_topic  t ON t.topic_id  = fa.topic_id
GROUP BY d.full_date, s.source_name, t.topic_name
ORDER BY d.full_date, s.source_name;


-- View 4: Sentiment Divergence (same topic, different sources, same day)
CREATE VIEW IF NOT EXISTS vw_sentiment_divergence AS
SELECT
    d.full_date,
    t.topic_name,
    s.source_name,
    fa.sentiment_score,
    fa.title
FROM fact_articles fa
         JOIN dim_date   d ON d.date_id   = fa.date_id
         JOIN dim_source s ON s.source_id = fa.source_id
         JOIN dim_topic  t ON t.topic_id  = fa.topic_id
WHERE t.topic_name IN (
    SELECT t2.topic_name
    FROM fact_articles fa2
             JOIN dim_date   d2 ON d2.date_id   = fa2.date_id
             JOIN dim_topic  t2 ON t2.topic_id  = fa2.topic_id
    WHERE d2.full_date = d.full_date
    GROUP BY d2.full_date, t2.topic_name
    HAVING MAX(fa2.sentiment_score) - MIN(fa2.sentiment_score) > 0.5
)
ORDER BY d.full_date, t.topic_name, fa.sentiment_score DESC;