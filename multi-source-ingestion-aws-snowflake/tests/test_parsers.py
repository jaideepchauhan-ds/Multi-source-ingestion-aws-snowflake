from ingestion.extractors.amfi_nav import parse_nav_text
from ingestion.extractors.books_scraper import parse_books_page
from ingestion.extractors.fx_api import parse_fx_payload

NAV_SAMPLE = """Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date

Open Ended Schemes(Debt Scheme - Banking and PSU Fund)

Aditya Birla Sun Life Mutual Fund

119551;INF209KA12Z1;INF209KA13Z9;Aditya Birla Sun Life Banking & PSU Debt Fund - DIRECT - IDCW;123.4567;24-Oct-2025
119552;-;-;Some Fund - REGULAR;N.A.;24-Oct-2025
"""

BOOKS_HTML = """
<html><body><ol>
<li><article class="product_pod">
  <p class="star-rating Three"></p>
  <h3><a href="a-light-in-the-attic_1000/index.html" title="A Light in the Attic">A Light in ...</a></h3>
  <div class="product_price"><p class="price_color">Â£51.77</p>
  <p class="instock availability"><i class="icon-ok"></i> In stock </p></div>
</article></li>
</ol></body></html>
"""


def test_parse_nav_carries_amc_and_category():
    df = parse_nav_text(NAV_SAMPLE)
    assert len(df) == 2
    first = df.iloc[0]
    assert first["scheme_code"] == "119551"
    assert first["amc_name"] == "Aditya Birla Sun Life Mutual Fund"
    assert first["scheme_category"].startswith("Open Ended Schemes")


def test_parse_books_page_extracts_fields():
    rows = parse_books_page(BOOKS_HTML, "https://books.toscrape.com/catalogue/page-1.html")
    assert len(rows) == 1
    row = rows[0]
    assert row["title"] == "A Light in the Attic"
    assert row["star_rating"] == "Three"
    assert row["product_url"].endswith("a-light-in-the-attic_1000/index.html")
    assert "51.77" in row["price"]


def test_parse_fx_time_series():
    payload = {"base": "USD", "rates": {"2025-10-23": {"EUR": 0.86, "INR": 88.1}, "2025-10-24": {"EUR": 0.87}}}
    df = parse_fx_payload(payload)
    assert len(df) == 3
    assert set(df["quote_currency"]) == {"EUR", "INR"}


def test_parse_fx_single_day():
    df = parse_fx_payload({"base": "USD", "date": "2025-10-24", "rates": {"EUR": 0.87}})
    assert df.to_dict("records") == [
        {"rate_date": "2025-10-24", "base_currency": "USD", "quote_currency": "EUR", "rate": 0.87}
    ]


def test_parse_fx_empty_rates():
    assert parse_fx_payload({"base": "USD", "date": "2025-10-24", "rates": {}}).empty
