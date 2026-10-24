-- =====================================================================
-- 02_raw_tables_and_streams.sql : landing tables + change-capture streams
-- Column ORDER of the first block in each table must match the CSV written by
-- ingestion/transform.py (positional $1..$n in the Snowpipe COPY statements).
-- =====================================================================
USE ROLE INGEST_ROLE; USE WAREHOUSE INGEST_WH; USE SCHEMA INGEST_DB.RAW;

CREATE TABLE IF NOT EXISTS RAW.FX_RATES_RAW (
  rate_date       DATE,
  base_currency   VARCHAR(3),
  quote_currency  VARCHAR(3),
  rate            NUMBER(18,8),
  source_system   VARCHAR,
  run_id          VARCHAR,
  ingested_at     TIMESTAMP_NTZ,
  row_hash        VARCHAR(64),
  file_name       VARCHAR,                                   -- METADATA$FILENAME
  loaded_at       TIMESTAMP_NTZ DEFAULT SYSDATE()
);

CREATE TABLE IF NOT EXISTS RAW.MF_NAV_RAW (
  scheme_code         VARCHAR,
  scheme_name         VARCHAR,
  amc_name            VARCHAR,
  scheme_category     VARCHAR,
  isin_payout_growth  VARCHAR,
  isin_reinvest       VARCHAR,
  nav_date            DATE,
  nav                 NUMBER(18,4),
  source_system       VARCHAR,
  run_id              VARCHAR,
  ingested_at         TIMESTAMP_NTZ,
  row_hash            VARCHAR(64),
  file_name           VARCHAR,
  loaded_at           TIMESTAMP_NTZ DEFAULT SYSDATE()
);

CREATE TABLE IF NOT EXISTS RAW.BOOKS_RAW (
  product_url    VARCHAR,
  title          VARCHAR,
  price_gbp      NUMBER(10,2),
  star_rating    NUMBER(1),
  in_stock       BOOLEAN,
  source_system  VARCHAR,
  run_id         VARCHAR,
  ingested_at    TIMESTAMP_NTZ,
  row_hash       VARCHAR(64),
  file_name      VARCHAR,
  loaded_at      TIMESTAMP_NTZ DEFAULT SYSDATE()
);

-- Append-only streams: raw tables only ever receive inserts from Snowpipe.
-- IF NOT EXISTS on purpose: CREATE OR REPLACE would reset the stream offset and lose pending changes.
CREATE STREAM IF NOT EXISTS RAW.FX_RATES_RAW_STRM ON TABLE RAW.FX_RATES_RAW APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS RAW.MF_NAV_RAW_STRM   ON TABLE RAW.MF_NAV_RAW   APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS RAW.BOOKS_RAW_STRM    ON TABLE RAW.BOOKS_RAW    APPEND_ONLY = TRUE;
