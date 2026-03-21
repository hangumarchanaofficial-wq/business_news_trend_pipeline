import time
import yaml
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from .base_scraper import BaseScraper


class NewsScraper(BaseScraper):
    def __init__(self, db_manager, config_path="config/scraper_config.yaml"):
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        super().__init__(db_manager, config)

    def scrape_daily_mirror_business(self, browser):
        """Scrape Daily Mirror business section."""
        page = browser.new_page()
        url = self.config["sources"]["daily_mirror"]["business_url"]

        print(f"Loading: {url}")
        self.safe_goto(page, url, timeout=self.config["scraping"]["timeout"])
        time.sleep(60)

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
        print(f"Found {len(links)} article links")

        for link in links:
            self._scrape_article(browser, link, "DailyMirror", "business")

    def scrape_the_morning(self, browser):
        """Scrape The Morning news."""
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
        """Scrape FT.lk."""
        page = browser.new_page()
        url = self.config["sources"]["ft_lk"]["base_url"] + "/"

        self.safe_goto(page, url, timeout=self.config["scraping"]["timeout"])
        time.sleep(self.config["scraping"]["wait_time"])

        soup = BeautifulSoup(page.content(), "html.parser")
        page.close()

        selectors = self.config["sources"]["ft_lk"]["selectors"]["articles"]
        links = self.extract_links(soup, selectors, url)

        business_list = self.config["sources"]["ft_lk"]["business_list"]
        filtered_links = []

        for link in links:
            if link.rstrip("/") == business_list.rstrip("/"):
                continue
            if "/business/" in link and "34-" not in link:
                continue
            if "/front-page/" in link and "44-" not in link:
                continue
            filtered_links.append(link)

        for link in filtered_links:
            self._scrape_article(browser, link, "FT.lk", "business/front-page")

    def scrape_economic_times(self, browser):
        """Scrape Economic Times."""
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
                self._scrape_article(browser, link, "EconomicTimes.lk", "economy")

    def scrape_sunday_times(self, browser):
        """Scrape Sunday Times Business."""
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
        """Scrape LMD."""
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

    def _scrape_article(self, browser, url, source, section):
        """Scrape a single article page."""
        page = browser.new_page()
        self.safe_goto(page, url, timeout=60000)

        soup = BeautifulSoup(page.content(), "html.parser")
        page.close()

        title, full_text = self.extract_article_content(soup)
        full_text = self.limit_words(
            full_text,
            self.config["scraping"]["max_words"],
        )

        status = self.db.save_article(source, section, title, url, full_text)

        if status:
            print(f"[{source}] {title[:50]}...")
        else:
            print(f"[{source}] {title[:50]}... (unchanged)")

    def run_all(self):
        """Run all configured scrapers."""
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=self.config["scraping"]["headless"]
            )

            try:
                print("Starting scraping process...\n")

                sources = [
                    ("Daily Mirror", self.scrape_daily_mirror_business),
                    ("Sunday Times", self.scrape_sunday_times),
                    ("The Morning", self.scrape_the_morning),
                    ("FT.lk", self.scrape_ft_lk),
                    ("Economic Times", self.scrape_economic_times),
                    ("LMD", self.scrape_lmd),
                ]

                for source_name, scraper_func in sources:
                    try:
                        print("=" * 60)
                        print(f"Scraping {source_name}...")
                        print("=" * 60)
                        scraper_func(browser)
                        print(f"{source_name} completed\n")
                    except Exception as e:
                        print(f"{source_name} failed: {e}\n")
                        continue

                print("Scraping completed!")

            except Exception as e:
                print(f"Critical error during scraping: {e}")

            finally:
                try:
                    if browser.is_connected():
                        browser.close()
                except Exception:
                    pass