-- =====================================================================
-- 06_audit_and_analytics.sql : reconciliation view + consumer-facing views
-- =====================================================================
USE ROLE INGEST_ROLE; USE WAREHOUSE INGEST_WH; USE DATABASE INGEST_DB;

-- Rows landed per run. Compare with "rows" in the run manifest (s3://<BUCKET>/manifests/...)
-- to prove every extracted row reached Snowflake.
CREATE OR REPLACE VIEW AUDIT.V_LOAD_RECONCILIATION AS
SELECT 'fx_rates' AS source, run_id, COUNT(*) AS rows_landed, COUNT(DISTINCT file_name) AS files,
       MIN(loaded_at) AS first_loaded_at, MAX(loaded_at) AS last_loaded_at
FROM RAW.FX_RATES_RAW GROUP BY run_id
UNION ALL
SELECT 'mf_nav', run_id, COUNT(*), COUNT(DISTINCT file_name), MIN(loaded_at), MAX(loaded_at)
FROM RAW.MF_NAV_RAW GROUP BY run_id
UNION ALL
SELECT 'books', run_id, COUNT(*), COUNT(DISTINCT file_name), MIN(loaded_at), MAX(loaded_at)
FROM RAW.BOOKS_RAW GROUP BY run_id;

-- Day-over-day FX movement
CREATE OR REPLACE VIEW CURATED.VW_FX_DAILY_CHANGE AS
WITH ordered AS (
  SELECT rate_date, base_currency, quote_currency, rate,
         LAG(rate) OVER (PARTITION BY base_currency, quote_currency ORDER BY rate_date) AS prev_rate
  FROM CURATED.FX_RATES
)
SELECT rate_date, base_currency, quote_currency, rate, prev_rate,
       ROUND((rate / NULLIF(prev_rate, 0) - 1) * 100, 4) AS pct_change
FROM ordered;

-- Latest NAV per scheme
CREATE OR REPLACE VIEW CURATED.VW_MF_NAV_LATEST AS
SELECT *
FROM CURATED.MF_NAV
QUALIFY ROW_NUMBER() OVER (PARTITION BY scheme_code ORDER BY nav_date DESC) = 1;

-- Scheme counts by fund house and category (from the latest snapshot)
CREATE OR REPLACE VIEW CURATED.VW_MF_SCHEME_SUMMARY AS
SELECT amc_name, scheme_category, COUNT(*) AS schemes
FROM CURATED.VW_MF_NAV_LATEST
GROUP BY amc_name, scheme_category;

-- Catalogue price by rating
CREATE OR REPLACE VIEW CURATED.VW_BOOKS_BY_RATING AS
SELECT star_rating, COUNT(*) AS books, ROUND(AVG(price_gbp), 2) AS avg_price_gbp,
       SUM(IFF(in_stock, 1, 0)) AS in_stock_books
FROM CURATED.BOOKS
GROUP BY star_rating
ORDER BY star_rating;
