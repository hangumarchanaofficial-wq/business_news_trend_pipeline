"""
Base scraper with reusable helpers for all news sources.
"""

from urllib.parse import urljoin
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup

from src.utils.logger import get_logger

log = get_logger("ingestion.base")


class BaseScraper:
    def __init__(self, raw_storage, config):
        self.storage = raw_storage
        self.config = config

    # ── Navigation ────────────────────────────────────────────

    def safe_goto(self, page, url, wait_until="domcontentloaded", timeout=60000):
        """Navigate to *url*; tolerate partial loads on timeout."""
        try:
            page.goto(url, wait_until=wait_until, timeout=timeout)
        except PlaywrightTimeoutError:
            log.warning(f"Timeout loading {url} — using partial page")

    # ── Text helpers ──────────────────────────────────────────

    @staticmethod
    def limit_words(text: str, max_words: int = 1000) -> str:
        words = text.split()
        if len(words) <= max_words:
            return text
        return " ".join(words[:max_words]) + "..."

    # ── Link extraction ───────────────────────────────────────

    @staticmethod
    def extract_links(soup, selectors, base_url):
        """Return deduplicated absolute URLs matched by one or more CSS selectors."""
        if isinstance(selectors, str):
            selectors = [selectors]

        links = []
        for selector in selectors:
            for a_tag in soup.select(selector):
                href = a_tag.get("href")
                if not href:
                    continue
                full = urljoin(base_url, href).split("#")[0]
                links.append(full)

        return list(dict.fromkeys(links))

    # ── Article content extraction ────────────────────────────

    @staticmethod
    def extract_article_content(soup: BeautifulSoup):
        """Pull title and concatenated paragraph text from an article page."""
        title_tag = (
                soup.find("h1", class_="entry_title")
                or soup.find("h1", class_="entry-title")
                or soup.find("h1")
                or soup.find("h3")
                or soup.find("h2", class_="wp-block-heading")
        )
        title = title_tag.get_text(strip=True) if title_tag else ""

        paras = soup.select("div.entry-content p")
        if not paras:
            paras = soup.find_all("p")

        full_text = " ".join(
            p.get_text(" ", strip=True)
            for p in paras
            if p.get_text(" ", strip=True)
        )
        return title, full_text
