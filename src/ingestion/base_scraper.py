from urllib.parse import urljoin
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup


class BaseScraper:
    def __init__(self, db_manager, config):
        self.db = db_manager
        self.config = config

    def safe_goto(self, page, url, wait_until="domcontentloaded", timeout=60000):
        """Safely navigate to a page and tolerate partial loads."""
        try:
            page.goto(url, wait_until=wait_until, timeout=timeout)
        except PlaywrightTimeoutError:
            print(f"[WARN] Timeout while loading {url}. Using partially loaded page.")

    def limit_words(self, text, max_words=1000):
        """Trim article body to a manageable length."""
        words = text.split()
        if len(words) <= max_words:
            return text
        return " ".join(words[:max_words]) + "..."

    def extract_links(self, soup, selectors, base_url):
        """Extract unique absolute links from one or more CSS selectors."""
        links = []

        if isinstance(selectors, str):
            selectors = [selectors]

        for selector in selectors:
            for a in soup.select(selector):
                href = a.get("href")
                if not href:
                    continue
                full = urljoin(base_url, href).split("#")[0]
                links.append(full)

        return list(dict.fromkeys(links))

    def extract_article_content(self, soup: BeautifulSoup):
        """Extract a title and concatenated paragraph body."""
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
            p.get_text(" ", strip=True) for p in paras if p.get_text(" ", strip=True)
        )

        return title, full_text