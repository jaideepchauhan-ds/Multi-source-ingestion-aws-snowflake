"""Source 2 - flat-file download: AMFI (India) mutual fund NAV, semicolon-delimited text.

The file mixes data rows with section lines (scheme category, AMC name), so the parser
is a small state machine that carries the latest category / AMC onto each data row.
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd
import requests

from ingestion.config import Config

log = logging.getLogger(__name__)

RAW_COLUMNS = [
    "scheme_code",
    "isin_payout_growth",
    "isin_reinvest",
    "scheme_name",
    "nav",
    "nav_date",
    "amc_name",
    "scheme_category",
]


def parse_nav_text(text: str) -> pd.DataFrame:
    rows: list[dict] = []
    amc_name: str | None = None
    category: str | None = None

    for line in text.splitlines():
        line = line.strip()
        if not line or line.lower().startswith("scheme code"):
            continue
        if ";" in line:
            parts = [p.strip() for p in line.split(";")]
            if len(parts) < 6 or not parts[0].isdigit():
                continue
            rows.append(
                {
                    "scheme_code": parts[0],
                    "isin_payout_growth": parts[1],
                    "isin_reinvest": parts[2],
                    "scheme_name": parts[3],
                    "nav": parts[4],
                    "nav_date": parts[5],
                    "amc_name": amc_name,
                    "scheme_category": category,
                }
            )
        elif "(" in line and "scheme" in line.lower():
            category = line
        else:
            amc_name = line
    return pd.DataFrame(rows, columns=RAW_COLUMNS)


def extract(cfg: Config, run_date: date, session: requests.Session) -> pd.DataFrame:
    log.info("downloading nav file", extra={"ctx": {"url": cfg.amfi_nav_url}})
    resp = session.get(cfg.amfi_nav_url, timeout=cfg.request_timeout)
    resp.raise_for_status()
    resp.encoding = resp.encoding or "utf-8"
    return parse_nav_text(resp.text)
