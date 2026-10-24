"""Runtime configuration, read from environment variables (12-factor style).

No credentials live in code. AWS access uses the default boto3 credential chain
(IAM role, SSO profile or environment variables).
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    s3_bucket: str
    s3_prefix: str
    aws_region: str
    sns_topic_arn: str
    fx_api_base: str
    fx_base_currency: str
    fx_symbols: tuple[str, ...]
    fx_lookback_days: int
    amfi_nav_url: str
    books_base_url: str
    max_book_pages: int
    scrape_delay_seconds: float
    request_timeout: int
    user_agent: str

    @classmethod
    def from_env(cls) -> "Config":
        symbols = os.getenv("FX_SYMBOLS", "INR,EUR,GBP,JPY,AUD,CAD")
        return cls(
            s3_bucket=os.getenv("S3_BUCKET", ""),
            s3_prefix=os.getenv("S3_PREFIX", "raw").strip("/"),
            aws_region=os.getenv("AWS_REGION", "ap-south-1"),
            sns_topic_arn=os.getenv("SNS_TOPIC_ARN", ""),
            fx_api_base=os.getenv("FX_API_BASE", "https://api.frankfurter.dev/v1").rstrip("/"),
            fx_base_currency=os.getenv("FX_BASE_CURRENCY", "USD").upper(),
            fx_symbols=tuple(s.strip().upper() for s in symbols.split(",") if s.strip()),
            fx_lookback_days=int(os.getenv("FX_LOOKBACK_DAYS", "7")),
            amfi_nav_url=os.getenv("AMFI_NAV_URL", "https://www.amfiindia.com/spages/NAVAll.txt"),
            books_base_url=os.getenv("BOOKS_BASE_URL", "https://books.toscrape.com").rstrip("/"),
            max_book_pages=int(os.getenv("MAX_BOOK_PAGES", "5")),
            scrape_delay_seconds=float(os.getenv("SCRAPE_DELAY_SECONDS", "1.0")),
            request_timeout=int(os.getenv("REQUEST_TIMEOUT", "30")),
            user_agent=os.getenv(
                "USER_AGENT",
                "data-ingestion-portfolio/1.0 (+https://github.com/your-username)",
            ),
        )
