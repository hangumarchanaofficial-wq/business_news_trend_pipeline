"""
Concrete scraper for six Sri Lankan business news outlets.
"""

import time
import yaml
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from src.ingestion.base_scraper import BaseScraper
from src.utils.logger import get_logger

log = get_logger("ingestion.scraper")


class NewsScraper(BaseScraper):
    def __init__(self, raw_storage,
                 config_path="config/scraper_config.yaml"):
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        super().__init__(raw_storage, config)

    # ── Individual source scrapers ────────────────────────────

    def scrape_daily_mirror_business(self, browser):
        """Daily Mirror — business section."""
        page = browser.new_page()
        url = self.config["sources"]["daily_mirror"]["business_url"]
        log.info(f"Loading: {url}")
        self.safe_goto(page, url, timeout=self.config["scraping"]["timeout"])
        time.sleep(self.config["scraping"]["wait_time"])

        soup = BeautifulSoup(page.content(), "html.parser")
        page.close()

        selectors = self.config["sources"]["daily_mirror"]["selectors"]["business"]
        links = []
        for sel in selectors:
            for a in soup.select(sel):
                href = a.get("href")
                if not href:
                    continue
                full = urljoin(url, href).split("#")[0]
                if full.rstrip("/") != url.rstrip("/"):
                    links.append(full)

        links = list(dict.fromkeys(links))
        log.info(f"Found {len(links)} article links")
        for link in links:
            self._scrape_article(browser, link, "DailyMirror", "business")

    def scrape_the_morning(self, browser):
        """The Morning — general news."""
        page = browser.new_page()
        url = self.config["sources"]["the_morning"]["news_url"]
        self.safe_goto(page, url, timeout=self.config["scraping"]["timeout"])
        time.sleep(self.config["scraping"]["wait_time"])

        soup = BeautifulSoup(page.content(), "html.parser")
        page.close()

        selector = self.config["sources"]["the_morning"]["selectors"]["articles"]
        links = self.extract_links(soup, selector, url)
        for link in links:
            self._scrape_article(browser, link, "TheMorning", "news")

    def scrape_ft_lk(self, browser):
        """FT.lk — Financial Times Sri Lanka."""
        page = browser.new_page()
        url = self.config["sources"]["ft_lk"]["base_url"] + "/"
        self.safe_goto(page, url, timeout=self.config["scraping"]["timeout"])
        time.sleep(self.config["scraping"]["wait_time"])

        soup = BeautifulSoup(page.content(), "html.parser")
        page.close()

        selectors = self.config["sources"]["ft_lk"]["selectors"]["articles"]
        links = self.extract_links(soup, selectors, url)

        business_list = self.config["sources"]["ft_lk"]["business_list"]
        filtered = []
        for link in links:
            if link.rstrip("/") == business_list.rstrip("/"):
                continue
            if "/business/" in link and "34-" not in link:
                continue
            if "/front-page/" in link and "44-" not in link:
                continue
            filtered.append(link)

        for link in filtered:
            self._scrape_article(browser, link, "FT.lk", "business/front-page")

    def scrape_economic_times(self, browser):
        """EconomyNext."""
        urls = [
            self.config["sources"]["economic_times"]["base_url"],
            self.config["sources"]["economic_times"]["economy_url"],
        ]
        for url in urls:
            page = browser.new_page()
            self.safe_goto(page, url, timeout=self.config["scraping"]["timeout"])
            time.sleep(self.config["scraping"]["wait_time"])

            soup = BeautifulSoup(page.content(), "html.parser")
            page.close()

            selector = self.config["sources"]["economic_times"]["selectors"]["articles"]
            links = self.extract_links(soup, selector, url)
            for link in links:
                self._scrape_article(browser, link, "EconomyNext", "economy")

    def scrape_sunday_times(self, browser):
        """Sunday Times — Business Times section."""
        page = browser.new_page()
        url = self.config["sources"]["sunday_times"]["business_url"]
        self.safe_goto(page, url, timeout=self.config["scraping"]["timeout"])
        time.sleep(self.config["scraping"]["wait_time"])

        soup = BeautifulSoup(page.content(), "html.parser")
        page.close()

        selector = self.config["sources"]["sunday_times"]["selectors"]["articles"]
        links = self.extract_links(soup, selector, url)
        for link in links:
            self._scrape_article(browser, link, "SundayTimes", "business-times")

    def scrape_lmd(self, browser):
        """Lanka Monthly Digest."""
        page = browser.new_page()
        url = self.config["sources"]["lmd"]["base_url"]
        self.safe_goto(page, url, timeout=self.config["scraping"]["timeout"])
        time.sleep(self.config["scraping"]["wait_time"])

        soup = BeautifulSoup(page.content(), "html.parser")
        page.close()

        selector = self.config["sources"]["lmd"]["selectors"]["articles"]
        links = self.extract_links(soup, selector, url)
        for link in links:
            self._scrape_article(browser, link, "LMD", "home")

    # ── Single article scraper ────────────────────────────────

    def _scrape_article(self, browser, url, source, section):
        """Scrape one article page and persist to raw storage."""
        try:
            page = browser.new_page()
            self.safe_goto(page, url, timeout=60000)
            soup = BeautifulSoup(page.content(), "html.parser")
            page.close()

            title, full_text = self.extract_article_content(soup)
            full_text = self.limit_words(
                full_text, self.config["scraping"]["max_words"]
            )

            if not title or not full_text:
                log.debug(f"[{source}] Skipped (empty): {url}")
                return

            saved = self.storage.save_article(source, section, title, url, full_text)
            if saved:
                log.info(f"[{source}] {title[:60]}...")
            else:
                log.debug(f"[{source}] {title[:60]}... (unchanged)")

        except Exception as e:
            log.error(f"[{source}] Failed scraping {url}: {e}")

    # ── Orchestrated entry point ──────────────────────────────

    def run_all(self):
        """Launch browser and run every source scraper."""
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=self.config["scraping"]["headless"]
            )
            try:
                log.info("Starting scraping process...")

                sources = [
                    ("Daily Mirror", self.scrape_daily_mirror_business),
                    ("Sunday Times", self.scrape_sunday_times),
                    ("The Morning", self.scrape_the_morning),
                    ("FT.lk", self.scrape_ft_lk),
                    ("EconomyNext", self.scrape_economic_times),
                    ("LMD", self.scrape_lmd),
                ]

                for source_name, scraper_func in sources:
                    try:
                        log.info(f"{'=' * 50}")
                        log.info(f"Scraping {source_name}...")
                        scraper_func(browser)
                        log.info(f"{source_name} completed")
                    except Exception as e:
                        log.error(f"{source_name} failed: {e}")
                        continue

                log.info("Scraping completed!")

            except Exception as e:
                log.critical(f"Critical error during scraping: {e}")
            finally:
                try:
                    if browser.is_connected():
                        browser.close()
                except Exception:
                    pass
