"""Source 1 - REST API: daily foreign-exchange reference rates (Frankfurter, ECB data).

Handles both response shapes the API can return:
  * time series  -> {"rates": {"2025-01-02": {"EUR": 0.96}, ...}}
  * single day   -> {"date": "2025-01-02", "rates": {"EUR": 0.96}}
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

import pandas as pd
import requests

from ingestion.config import Config

log = logging.getLogger(__name__)

RAW_COLUMNS = ["rate_date", "base_currency", "quote_currency", "rate"]


def parse_fx_payload(payload: dict) -> pd.DataFrame:
    base = payload["base"]
    rates = payload.get("rates") or {}
    rows: list[dict] = []

    first = next(iter(rates.values()), None)
    if isinstance(first, dict):  # time series
        for rate_date, day in rates.items():
            for quote, rate in day.items():
                rows.append(
                    {"rate_date": rate_date, "base_currency": base, "quote_currency": quote, "rate": rate}
                )
    else:  # single day
        for quote, rate in rates.items():
            rows.append(
                {"rate_date": payload["date"], "base_currency": base, "quote_currency": quote, "rate": rate}
            )
    return pd.DataFrame(rows, columns=RAW_COLUMNS)


def extract(cfg: Config, run_date: date, session: requests.Session) -> pd.DataFrame:
    start = run_date - timedelta(days=cfg.fx_lookback_days)
    url = f"{cfg.fx_api_base}/{start.isoformat()}..{run_date.isoformat()}"
    params = {"base": cfg.fx_base_currency, "symbols": ",".join(cfg.fx_symbols)}
    log.info("fetching fx rates", extra={"ctx": {"url": url, "params": params}})
    resp = session.get(url, params=params, timeout=cfg.request_timeout)
    resp.raise_for_status()
    return parse_fx_payload(resp.json())
