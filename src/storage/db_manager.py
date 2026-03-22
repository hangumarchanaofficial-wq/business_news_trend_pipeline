import json
import os
from datetime import datetime, timedelta


class DatabaseManager:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = {
            "articles": [],
            "updated_articles": 0
        }

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        if os.path.exists(file_path):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    self.data = json.load(f)
            except Exception:
                self.data = {"articles": [], "updated_articles": 0}

    def save_article(self, source, section, title, url, full_text):
        for article in self.data["articles"]:
            if article["url"] == url:
                if article["full_text"] != full_text:
                    article["full_text"] = full_text
                    article["updated_at"] = datetime.now().isoformat()
                    self.data["updated_articles"] += 1
                    self._save()
                    return True
                return False

        self.data["articles"].append({
            "source": source,
            "section": section,
            "title": title,
            "url": url,
            "full_text": full_text,
            "scraped_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        })
        self._save()
        return True

    def cleanup_old_articles(self, retention_days=3):
        cutoff = datetime.now() - timedelta(days=retention_days)
        before = len(self.data["articles"])

        filtered = []
        for article in self.data["articles"]:
            scraped_at = article.get("scraped_at")
            try:
                dt = datetime.fromisoformat(scraped_at)
                if dt >= cutoff:
                    filtered.append(article)
            except Exception:
                filtered.append(article)

        self.data["articles"] = filtered
        self._save()
        return before - len(filtered)

    def get_stats(self):
        return {
            "total_articles": len(self.data["articles"]),
            "updated_articles": self.data.get("updated_articles", 0)
        }

    def close(self):
        self._save()

    def _save(self):
        with open(self.file_path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)