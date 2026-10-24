import gzip
from datetime import date

import pandas as pd

from ingestion.loader import build_key, sha256_bytes, to_csv_gz_bytes


def test_key_layout_is_partitioned_and_unique_per_run():
    key = build_key("raw", "fx_rates", date(2025, 10, 24), "RUN1")
    assert key == "raw/fx_rates/dt=2025-10-24/fx_rates_RUN1.csv.gz"


def test_csv_gz_roundtrip_and_reproducible_bytes():
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    body = to_csv_gz_bytes(df)
    assert gzip.decompress(body).decode().splitlines() == ["a,b", "1,x", "2,y"]
    assert sha256_bytes(body) == sha256_bytes(to_csv_gz_bytes(df))
