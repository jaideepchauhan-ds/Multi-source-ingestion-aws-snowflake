"""Source registry and the per-source extract -> transform -> validate -> load flow."""
from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

import pandas as pd
import requests

from ingestion import loader
from ingestion.config import Config
from ingestion.extractors import amfi_nav, books_scraper, fx_api
from ingestion.quality import Rules, freshness, validate
from ingestion.transform import (
    AUDIT_COLUMNS,
    BOOKS_COLUMNS,
    FX_COLUMNS,
    NAV_COLUMNS,
    RunContext,
    transform_books,
    transform_fx,
    transform_nav,
)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SourceSpec:
    name: str  # also the S3 folder and Snowflake raw-table stem
    extract: Callable[[Config, date, requests.Session], pd.DataFrame]
    transform: Callable[[pd.DataFrame, RunContext], pd.DataFrame]
    rules: Rules


SOURCES: dict[str, SourceSpec] = {
    "fx_rates": SourceSpec(
        name="fx_rates",
        extract=fx_api.extract,
        transform=transform_fx,
        rules=Rules(
            columns=FX_COLUMNS + AUDIT_COLUMNS,
            key_cols=["rate_date", "base_currency", "quote_currency"],
            not_null=["rate_date", "base_currency", "quote_currency", "rate"],
            row_checks={"rate_must_be_positive": lambda d: ~(d["rate"] > 0)},
            dataset_checks=[freshness("rate_date", max_age_days=7)],
        ),
    ),
    "mf_nav": SourceSpec(
        name="mf_nav",
        extract=amfi_nav.extract,
        transform=transform_nav,
        rules=Rules(
            columns=NAV_COLUMNS + AUDIT_COLUMNS,
            key_cols=["scheme_code", "nav_date"],
            not_null=["scheme_code", "scheme_name", "nav_date", "nav"],
            min_rows=1000,  # the full file has thousands of schemes; far fewer means a broken download
            row_checks={"nav_must_be_positive": lambda d: ~(d["nav"] > 0)},
            dataset_checks=[freshness("nav_date", max_age_days=15)],
        ),
    ),
    "books": SourceSpec(
        name="books",
        extract=books_scraper.extract,
        transform=transform_books,
        rules=Rules(
            columns=BOOKS_COLUMNS + AUDIT_COLUMNS,
            key_cols=["product_url"],
            not_null=["product_url", "title", "price_gbp", "star_rating"],
            min_rows=10,  # zero rows usually means the HTML layout changed (selector drift)
            row_checks={
                "price_must_be_non_negative": lambda d: ~(d["price_gbp"] >= 0),
                "rating_must_be_1_to_5": lambda d: ~d["star_rating"].between(1, 5),
            },
        ),
    ),
}


@dataclass
class SourceResult:
    source: str
    status: str = "pending"  # loaded | dq_failed | failed
    rows: int = 0
    s3_key: str | None = None
    sha256: str | None = None
    failures: list[str] | None = None
    error: str | None = None
    seconds: float = 0.0


def run_source(
    spec: SourceSpec,
    cfg: Config,
    ctx: RunContext,
    session: requests.Session,
    *,
    dry_run: bool,
    out_dir: str,
) -> SourceResult:
    """One source never blocks another: every failure is captured in the result."""
    started = time.monotonic()
    result = SourceResult(source=spec.name)
    try:
        raw = spec.extract(cfg, ctx.run_date, session)
        log.info("extracted", extra={"ctx": {"source": spec.name, "raw_rows": len(raw)}})

        df = spec.transform(raw, ctx)
        log.info(
            "transformed",
            extra={"ctx": {"source": spec.name, "rows": len(df), "dropped": len(raw) - len(df)}},
        )

        failures = validate(df, spec.rules, ctx.run_date)
        if failures:
            result.status, result.failures = "dq_failed", failures
            log.error("data quality failed", extra={"ctx": {"source": spec.name, "failures": failures}})
            return result

        body = loader.to_csv_gz_bytes(df)
        key = loader.build_key(cfg.s3_prefix, spec.name, ctx.run_date, ctx.run_id)
        if dry_run:
            loader.write_local(out_dir, key, body)
        else:
            loader.upload_s3(cfg.s3_bucket, key, body, cfg.aws_region)

        result.status, result.rows, result.s3_key = "loaded", len(df), key
        result.sha256 = loader.sha256_bytes(body)
    except Exception as exc:  # noqa: BLE001 - isolate failures per source, report them
        result.status, result.error = "failed", f"{type(exc).__name__}: {exc}"
        log.exception("source failed", extra={"ctx": {"source": spec.name}})
    finally:
        result.seconds = round(time.monotonic() - started, 2)
    return result
