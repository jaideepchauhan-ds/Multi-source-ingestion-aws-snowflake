-- =====================================================================
-- 03_curated_tables.sql : de-duplicated, query-ready tables (maintained by MERGE tasks)
-- =====================================================================
USE ROLE INGEST_ROLE; USE WAREHOUSE INGEST_WH; USE SCHEMA INGEST_DB.CURATED;

CREATE TABLE IF NOT EXISTS CURATED.FX_RATES (
  rate_date       DATE         NOT NULL,
  base_currency   VARCHAR(3)   NOT NULL,
  quote_currency  VARCHAR(3)   NOT NULL,
  rate            NUMBER(18,8) NOT NULL,
  row_hash        VARCHAR(64),
  first_loaded_at TIMESTAMP_NTZ DEFAULT SYSDATE(),
  last_updated_at TIMESTAMP_NTZ DEFAULT SYSDATE(),
  last_run_id     VARCHAR,
  CONSTRAINT pk_fx_rates PRIMARY KEY (rate_date, base_currency, quote_currency)  -- informational in Snowflake
);

CREATE TABLE IF NOT EXISTS CURATED.MF_NAV (
  scheme_code         VARCHAR      NOT NULL,
  nav_date            DATE         NOT NULL,
  scheme_name         VARCHAR,
  amc_name            VARCHAR,
  scheme_category     VARCHAR,
  isin_payout_growth  VARCHAR,
  isin_reinvest       VARCHAR,
  nav                 NUMBER(18,4) NOT NULL,
  row_hash            VARCHAR(64),
  first_loaded_at     TIMESTAMP_NTZ DEFAULT SYSDATE(),
  last_updated_at     TIMESTAMP_NTZ DEFAULT SYSDATE(),
  last_run_id         VARCHAR,
  CONSTRAINT pk_mf_nav PRIMARY KEY (scheme_code, nav_date)
);

CREATE TABLE IF NOT EXISTS CURATED.BOOKS (
  product_url     VARCHAR      NOT NULL,
  title           VARCHAR,
  price_gbp       NUMBER(10,2),
  star_rating     NUMBER(1),
  in_stock        BOOLEAN,
  row_hash        VARCHAR(64),
  first_loaded_at TIMESTAMP_NTZ DEFAULT SYSDATE(),
  last_updated_at TIMESTAMP_NTZ DEFAULT SYSDATE(),
  last_run_id     VARCHAR,
  CONSTRAINT pk_books PRIMARY KEY (product_url)
);
