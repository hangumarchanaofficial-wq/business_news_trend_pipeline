import sqlite3

conn = sqlite3.connect("data/warehouse/news_warehouse.db")
cur = conn.cursor()

print("=== TABLES ===")
tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
for t in tables:
    count = cur.execute(f"SELECT COUNT(*) FROM {t[0]}").fetchone()[0]
    print(f"  {t[0]}: {count} rows")

print("\n=== SOURCES ===")
for r in cur.execute("SELECT * FROM dim_source").fetchall():
    print(f"  {r}")

print("\n=== TOPICS ===")
for r in cur.execute("SELECT * FROM dim_topic").fetchall():
    print(f"  {r}")

print("\n=== DAILY SUMMARY ===")
for r in cur.execute("SELECT * FROM fact_daily_summary").fetchall():
    print(f"  {r}")

print("\n=== SAMPLE ARTICLES (top 5) ===")
for r in cur.execute("""
                     SELECT fa.title, ds.source_name, dt.topic_name, fa.sentiment_label, fa.sentiment_score
                     FROM fact_articles fa
                              JOIN dim_source ds ON ds.source_id = fa.source_id
                              JOIN dim_topic dt ON dt.topic_id = fa.topic_id
                         LIMIT 5
                     """).fetchall():
    print(f"  [{r[1]}] [{r[2]}] [{r[3]} ({r[4]:.2f})] {r[0][:70]}...")

conn.close()
