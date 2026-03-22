"""
Raw data-lake storage — persists scraped articles as JSON on local disk,
then syncs to S3 for durable cloud storage.
"""

import json
import os
import uuid
from datetime import datetime, timedelta

from src.utils.logger import get_logger

log = get_logger("ingestion.raw_storage")


class RawStorage:
    """Manages the JSON-based raw data lake at *filepath* with optional S3 sync."""

    def __init__(self, filepath: str, s3_manager=None):
        self.filepath = filepath
        self.s3_manager = s3_manager
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        # If S3 manager provided, try downloading latest from S3 first
        if self.s3_manager:
            self.s3_manager.download_raw_articles(self.filepath)

        self.articles = self._load()

    # ── Persistence ───────────────────────────────────────────

    def _load(self) -> list:
        if os.path.exists(self.filepath):
            with open(self.filepath, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                    if isinstance(data, dict) and "articles" in data:
                        return data["articles"]
                    elif isinstance(data, list):
                        return data
                    return []
                except json.JSONDecodeError:
                    log.warning("Corrupt JSON file — starting fresh")
                    return []
        return []

    def _save(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(self.articles, f, indent=2, ensure_ascii=False)

    def _sync_to_s3(self):
        """Upload local JSON to S3 after save."""
        if self.s3_manager:
            self.s3_manager.upload_raw_articles(self.filepath)

    # ── Write operations ──────────────────────────────────────

    def save_article(self, source: str, section: str, title: str,
                     url: str, body: str) -> bool:
        """
        Upsert an article.  Returns True if new / updated, False if unchanged.
        Deduplication key is the URL.
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for existing in self.articles:
            if existing.get("url") == url:
                if existing.get("body") != body:
                    existing["body"] = body
                    existing["updated_at"] = now
                    self._save()
                    return True
                return False

        article = {
            "article_id": str(uuid.uuid4()),
            "source": source,
            "section": section,
            "title": title,
            "url": url,
            "body": body,
            "scraped_at": now,
            "updated_at": now,
        }
        self.articles.append(article)
        self._save()
        return True

    # ── Maintenance ───────────────────────────────────────────

    def cleanup_old_articles(self, retention_days: int = 3) -> int:
        cutoff = datetime.now() - timedelta(days=retention_days)
        before = len(self.articles)

        valid_articles = []
        for a in self.articles:
            dt_str = a.get("scraped_at", "")
            try:
                dt = datetime.fromisoformat(dt_str)
            except ValueError:
                try:
                    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    dt = datetime.now()
            if dt >= cutoff:
                valid_articles.append(a)

        self.articles = valid_articles
        removed = before - len(self.articles)
        if removed:
            self._save()
            log.info(f"Cleaned up {removed} articles older than {retention_days} days")
        return removed

    # ── Read helpers ──────────────────────────────────────────

    def get_all(self) -> list:
        return self.articles

    def get_stats(self) -> dict:
        total = len(self.articles)
        sources = {}
        for a in self.articles:
            sources[a["source"]] = sources.get(a["source"], 0) + 1
        return {"total_articles": total, "by_source": sources}

    def close(self):
        self._save()
        self._sync_to_s3()
        log.info("Raw storage closed and synced to S3")
