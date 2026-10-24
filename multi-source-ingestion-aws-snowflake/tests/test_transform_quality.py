from datetime import date

import pandas as pd

from ingestion.extractors.amfi_nav import parse_nav_text
from ingestion.extractors.books_scraper import parse_books_page
from ingestion.pipeline import SOURCES
from ingestion.quality import validate
from ingestion.transform import AUDIT_COLUMNS, RunContext, transform_books, transform_fx, transform_nav
from tests.test_parsers import BOOKS_HTML, NAV_SAMPLE

CTX = RunContext(run_id="20251024T020000Z-abc123", run_date=date(2025, 10, 24), ingested_at="2025-10-24 02:00:00")


def _fx_raw(rows):
    return pd.DataFrame(rows, columns=["rate_date", "base_currency", "quote_currency", "rate"])


def test_fx_transform_cleans_dedupes_and_adds_audit():
    raw = _fx_raw(
        [
            ("2025-10-24", " usd ", "eur", "0.87"),
            ("2025-10-24", "USD", "EUR", "0.88"),  # duplicate key -> keep last
            ("2025-10-24", "USD", "INR", "bad"),   # non numeric -> dropped
        ]
    )
    out = transform_fx(raw, CTX)
    assert len(out) == 1
    assert out.iloc[0]["rate"] == 0.88
    assert list(out.columns[-4:]) == AUDIT_COLUMNS
    assert out.iloc[0]["run_id"] == CTX.run_id
    assert len(out.iloc[0]["row_hash"]) == 64


def test_row_hash_is_deterministic_and_content_sensitive():
    a = transform_fx(_fx_raw([("2025-10-24", "USD", "EUR", "0.87")]), CTX)
    b = transform_fx(_fx_raw([("2025-10-24", "USD", "EUR", "0.87")]), CTX)
    c = transform_fx(_fx_raw([("2025-10-24", "USD", "EUR", "0.99")]), CTX)
    assert a.iloc[0]["row_hash"] == b.iloc[0]["row_hash"]
    assert a.iloc[0]["row_hash"] != c.iloc[0]["row_hash"]


def test_nav_transform_drops_unavailable_nav_and_nulls_dash_isin():
    out = transform_nav(parse_nav_text(NAV_SAMPLE), CTX)
    assert len(out) == 1  # the "N.A." row is dropped
    assert out.iloc[0]["nav_date"] == "2025-10-24"
    assert out.iloc[0]["nav"] == 123.4567


def test_books_transform_maps_rating_price_and_stock():
    raw = pd.DataFrame(parse_books_page(BOOKS_HTML, "https://books.toscrape.com/catalogue/page-1.html"))
    out = transform_books(raw, CTX)
    row = out.iloc[0]
    assert row["price_gbp"] == 51.77
    assert row["star_rating"] == 3
    assert bool(row["in_stock"]) is True


def test_quality_passes_for_good_fx_data():
    out = transform_fx(_fx_raw([("2025-10-24", "USD", "EUR", "0.87"), ("2025-10-24", "USD", "INR", "88.1")]), CTX)
    assert validate(out, SOURCES["fx_rates"].rules, CTX.run_date) == []


def test_quality_flags_empty_negative_and_stale_data():
    rules = SOURCES["fx_rates"].rules
    empty = transform_fx(_fx_raw([]), CTX)
    assert any(f.startswith("row_count") for f in validate(empty, rules, CTX.run_date))

    negative = transform_fx(_fx_raw([("2025-10-24", "USD", "EUR", "-1")]), CTX)
    assert any(f.startswith("rate_must_be_positive") for f in validate(negative, rules, CTX.run_date))

    stale = transform_fx(_fx_raw([("2025-09-01", "USD", "EUR", "0.87")]), CTX)
    assert any(f.startswith("stale_data") for f in validate(stale, rules, CTX.run_date))


def test_quality_detects_schema_drift():
    df = pd.DataFrame({"unexpected": [1]})
    failures = validate(df, SOURCES["fx_rates"].rules, CTX.run_date)
    assert failures and failures[0].startswith("schema_drift")


def test_books_scrape_returning_zero_rows_fails_min_rows():
    empty = transform_books(pd.DataFrame(columns=["product_url", "title", "price", "star_rating", "availability"]), CTX)
    failures = validate(empty, SOURCES["books"].rules, CTX.run_date)
    assert any(f.startswith("row_count") for f in failures)
