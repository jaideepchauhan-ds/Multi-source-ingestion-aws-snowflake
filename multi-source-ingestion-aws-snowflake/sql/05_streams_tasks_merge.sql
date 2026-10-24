-- =====================================================================
-- 05_streams_tasks_merge.sql : stream-driven CDC into curated tables
-- Each task wakes every 5 minutes but only spends warehouse credits when its stream has data
-- (WHEN SYSTEM$STREAM_HAS_DATA is evaluated by the cloud-services layer, not the warehouse).
--
-- Why QUALIFY ROW_NUMBER inside the USING subquery: several files can carry the same business
-- key (re-runs, overlapping FX look-back window). MERGE needs one source row per target row,
-- so we keep the most recently ingested version. This makes re-runs idempotent.
-- The row_hash comparison skips writes when nothing actually changed.
-- =====================================================================
USE ROLE INGEST_ROLE; USE WAREHOUSE INGEST_WH; USE SCHEMA INGEST_DB.CURATED;

CREATE OR REPLACE TASK CURATED.T_MERGE_FX_RATES
  WAREHOUSE = INGEST_WH
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('INGEST_DB.RAW.FX_RATES_RAW_STRM')
AS
MERGE INTO INGEST_DB.CURATED.FX_RATES t
USING (
  SELECT rate_date, base_currency, quote_currency, rate, row_hash, run_id
  FROM INGEST_DB.RAW.FX_RATES_RAW_STRM
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rate_date, base_currency, quote_currency
    ORDER BY ingested_at DESC, loaded_at DESC) = 1
) s
ON  t.rate_date = s.rate_date
AND t.base_currency = s.base_currency
AND t.quote_currency = s.quote_currency
WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET
  t.rate = s.rate, t.row_hash = s.row_hash, t.last_updated_at = SYSDATE(), t.last_run_id = s.run_id
WHEN NOT MATCHED THEN INSERT
  (rate_date, base_currency, quote_currency, rate, row_hash, last_run_id)
  VALUES (s.rate_date, s.base_currency, s.quote_currency, s.rate, s.row_hash, s.run_id);

CREATE OR REPLACE TASK CURATED.T_MERGE_MF_NAV
  WAREHOUSE = INGEST_WH
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('INGEST_DB.RAW.MF_NAV_RAW_STRM')
AS
MERGE INTO INGEST_DB.CURATED.MF_NAV t
USING (
  SELECT scheme_code, nav_date, scheme_name, amc_name, scheme_category,
         isin_payout_growth, isin_reinvest, nav, row_hash, run_id
  FROM INGEST_DB.RAW.MF_NAV_RAW_STRM
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY scheme_code, nav_date
    ORDER BY ingested_at DESC, loaded_at DESC) = 1
) s
ON t.scheme_code = s.scheme_code AND t.nav_date = s.nav_date
WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET
  t.scheme_name = s.scheme_name, t.amc_name = s.amc_name, t.scheme_category = s.scheme_category,
  t.isin_payout_growth = s.isin_payout_growth, t.isin_reinvest = s.isin_reinvest,
  t.nav = s.nav, t.row_hash = s.row_hash, t.last_updated_at = SYSDATE(), t.last_run_id = s.run_id
WHEN NOT MATCHED THEN INSERT
  (scheme_code, nav_date, scheme_name, amc_name, scheme_category,
   isin_payout_growth, isin_reinvest, nav, row_hash, last_run_id)
  VALUES (s.scheme_code, s.nav_date, s.scheme_name, s.amc_name, s.scheme_category,
          s.isin_payout_growth, s.isin_reinvest, s.nav, s.row_hash, s.run_id);

CREATE OR REPLACE TASK CURATED.T_MERGE_BOOKS
  WAREHOUSE = INGEST_WH
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('INGEST_DB.RAW.BOOKS_RAW_STRM')
AS
MERGE INTO INGEST_DB.CURATED.BOOKS t
USING (
  SELECT product_url, title, price_gbp, star_rating, in_stock, row_hash, run_id
  FROM INGEST_DB.RAW.BOOKS_RAW_STRM
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_url
    ORDER BY ingested_at DESC, loaded_at DESC) = 1
) s
ON t.product_url = s.product_url
WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET
  t.title = s.title, t.price_gbp = s.price_gbp, t.star_rating = s.star_rating,
  t.in_stock = s.in_stock, t.row_hash = s.row_hash, t.last_updated_at = SYSDATE(), t.last_run_id = s.run_id
WHEN NOT MATCHED THEN INSERT
  (product_url, title, price_gbp, star_rating, in_stock, row_hash, last_run_id)
  VALUES (s.product_url, s.title, s.price_gbp, s.star_rating, s.in_stock, s.row_hash, s.run_id);

-- Tasks are created SUSPENDED. Start them:
ALTER TASK CURATED.T_MERGE_FX_RATES RESUME;
ALTER TASK CURATED.T_MERGE_MF_NAV   RESUME;
ALTER TASK CURATED.T_MERGE_BOOKS    RESUME;

-- Optional: run once immediately instead of waiting for the schedule
-- EXECUTE TASK CURATED.T_MERGE_FX_RATES;
