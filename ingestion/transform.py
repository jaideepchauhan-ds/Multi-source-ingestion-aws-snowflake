"""Source-specific cleaning plus the audit columns every raw record carries.

Output column order is a CONTRACT with the Snowpipe COPY statements in sql/04_pipes.sql
(positional $1..$n). Change one, change the other.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from ingestion.utils import sha256_hex

AUDIT_COLUMNS = ["source_system", "run_id", "ingested_at", "row_hash"]

FX_COLUMNS = ["rate_date", "base_currency", "quote_currency", "rate"]
NAV_COLUMNS = [
    "scheme_code", "scheme_name", "amc_name", "scheme_category",
    "isin_payout_growth", "isin_reinvest", "nav_date", "nav",
]
BOOKS_COLUMNS = ["product_url", "title", "price_gbp", "star_rating", "in_stock"]

STAR_MAP = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}


@dataclass(frozen=True)
class RunContext:
    run_id: str
    run_date: date
    ingested_at: str  # "YYYY-MM-DD HH:MM:SS" UTC, parseable by Snowflake TIMESTAMP_NTZ


def _clean(value) -> str:
    return "" if pd.isna(value) else str(value)


def _finalize(df: pd.DataFrame, source: str, business_cols: list[str], ctx: RunContext) -> pd.DataFrame:
    """Add audit columns + a content hash used by Snowflake MERGE to skip unchanged rows."""
    out = df.copy()
    out["source_system"] = source
    out["run_id"] = ctx.run_id
    out["ingested_at"] = ctx.ingested_at
    out["row_hash"] = [
        sha256_hex("|".join(_clean(v) for v in row))
        for row in out[business_cols].itertuples(index=False, name=None)
    ]
    return out[business_cols + AUDIT_COLUMNS].reset_index(drop=True)


def transform_fx(raw: pd.DataFrame, ctx: RunContext) -> pd.DataFrame:
    df = raw.copy()
    df["rate_date"] = pd.to_datetime(df["rate_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    for col in ("base_currency", "quote_currency"):
        df[col] = df[col].astype("string").str.strip().str.upper()
    df["rate"] = pd.to_numeric(df["rate"], errors="coerce")
    keys = ["rate_date", "base_currency", "quote_currency"]
    df = df.dropna(subset=keys + ["rate"]).drop_duplicates(subset=keys, keep="last")
    return _finalize(df[FX_COLUMNS], "fx_rates", FX_COLUMNS, ctx)


def transform_nav(raw: pd.DataFrame, ctx: RunContext) -> pd.DataFrame:
    df = raw.copy()
    df["scheme_code"] = df["scheme_code"].astype("string").str.strip()
    for col in ("scheme_name", "amc_name", "scheme_category", "isin_payout_growth", "isin_reinvest"):
        df[col] = df[col].astype("string").str.strip().replace({"": pd.NA, "-": pd.NA})
    df["nav"] = pd.to_numeric(df["nav"], errors="coerce")  # "N.A." -> NaN
    df["nav_date"] = pd.to_datetime(df["nav_date"], format="%d-%b-%Y", errors="coerce").dt.strftime("%Y-%m-%d")
    keys = ["scheme_code", "nav_date"]
    df = df.dropna(subset=keys + ["nav"]).drop_duplicates(subset=keys, keep="last")
    return _finalize(df[NAV_COLUMNS], "mf_nav", NAV_COLUMNS, ctx)


def transform_books(raw: pd.DataFrame, ctx: RunContext) -> pd.DataFrame:
    df = raw.copy()
    df["title"] = df["title"].astype("string").str.strip()
    cleaned_price = df["price"].astype("string").str.replace(r"[^0-9.]", "", regex=True)
    df["price_gbp"] = pd.to_numeric(cleaned_price, errors="coerce")
    df["star_rating"] = df["star_rating"].map(STAR_MAP)
    df["in_stock"] = df["availability"].astype("string").str.contains("in stock", case=False, na=False)
    df = df.dropna(subset=["product_url", "title", "price_gbp"]).drop_duplicates(
        subset=["product_url"], keep="last"
    )
    return _finalize(df[BOOKS_COLUMNS], "books", BOOKS_COLUMNS, ctx)
