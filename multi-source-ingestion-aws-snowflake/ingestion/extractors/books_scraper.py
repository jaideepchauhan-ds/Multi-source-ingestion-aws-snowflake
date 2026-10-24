"""Source 3 - web scraping: books.toscrape.com (a sandbox site built for scraping practice).

Good-citizen scraping: honours robots.txt, sends an identifying User-Agent, rate-limits,
retries with backoff, and fails closed if robots.txt cannot be evaluated.
Swap the base URL / selectors for any other site, after checking its terms of use.
"""
from __future__ import annotations

import logging
import time
from datetime import date
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser

import pandas as pd
import requests
from bs4 import BeautifulSoup

from ingestion.config import Config

log = logging.getLogger(__name__)

RAW_COLUMNS = ["product_url", "title", "price", "star_rating", "availability"]


def parse_books_page(html: str, page_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict] = []
    for card in soup.select("article.product_pod"):
        link = card.select_one("h3 a")
        price = card.select_one("p.price_color")
        rating = card.select_one("p.star-rating")
        stock = card.select_one("p.availability")
        if link is None or price is None:
            continue
        rating_classes = rating.get("class", []) if rating is not None else []
        rows.append(
            {
                "product_url": urljoin(page_url, link["href"]),
                "title": link.get("title") or link.get_text(strip=True),
                "price": price.get_text(strip=True),
                "star_rating": next((c for c in rating_classes if c != "star-rating"), None),
                "availability": stock.get_text(strip=True) if stock is not None else None,
            }
        )
    return rows


def _load_robots(base_url: str) -> RobotFileParser:
    parser = RobotFileParser()
    parser.set_url(f"{base_url}/robots.txt")
    try:
        parser.read()  # a 404 is treated as "allow all"; network errors raise
    except OSError as exc:
        raise RuntimeError(f"cannot evaluate robots.txt for {base_url}: {exc}") from exc
    return parser


def extract(cfg: Config, run_date: date, session: requests.Session) -> pd.DataFrame:
    robots = _load_robots(cfg.books_base_url)
    rows: list[dict] = []

    for page in range(1, cfg.max_book_pages + 1):
        url = f"{cfg.books_base_url}/catalogue/page-{page}.html"
        if not robots.can_fetch(cfg.user_agent, url):
            log.warning("blocked by robots.txt, stopping", extra={"ctx": {"url": url}})
            break
        resp = session.get(url, timeout=cfg.request_timeout)
        if resp.status_code == 404:
            break  # ran past the last page
        resp.raise_for_status()
        resp.encoding = "utf-8"
        page_rows = parse_books_page(resp.text, url)
        log.info("scraped page", extra={"ctx": {"page": page, "rows": len(page_rows)}})
        rows.extend(page_rows)
        time.sleep(cfg.scrape_delay_seconds)

    return pd.DataFrame(rows, columns=RAW_COLUMNS)
