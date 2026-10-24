-- =====================================================================
-- 04_pipes.sql : Snowpipe auto-ingest, one pipe per source folder
-- S3 object-created events -> SQS -> Snowpipe -> COPY INTO raw table (event-driven, no polling)
-- =====================================================================
USE ROLE INGEST_ROLE; USE WAREHOUSE INGEST_WH; USE SCHEMA INGEST_DB.RAW;

CREATE PIPE IF NOT EXISTS RAW.PIPE_FX_RATES AUTO_INGEST = TRUE AS
COPY INTO RAW.FX_RATES_RAW
  (rate_date, base_currency, quote_currency, rate, source_system, run_id, ingested_at, row_hash, file_name)
FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, METADATA$FILENAME FROM @RAW.S3_RAW_STAGE/fx_rates/)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_GZ')
ON_ERROR = SKIP_FILE;

CREATE PIPE IF NOT EXISTS RAW.PIPE_MF_NAV AUTO_INGEST = TRUE AS
COPY INTO RAW.MF_NAV_RAW
  (scheme_code, scheme_name, amc_name, scheme_category, isin_payout_growth, isin_reinvest,
   nav_date, nav, source_system, run_id, ingested_at, row_hash, file_name)
FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, METADATA$FILENAME FROM @RAW.S3_RAW_STAGE/mf_nav/)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_GZ')
ON_ERROR = SKIP_FILE;

CREATE PIPE IF NOT EXISTS RAW.PIPE_BOOKS AUTO_INGEST = TRUE AS
COPY INTO RAW.BOOKS_RAW
  (product_url, title, price_gbp, star_rating, in_stock, source_system, run_id, ingested_at, row_hash, file_name)
FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, METADATA$FILENAME FROM @RAW.S3_RAW_STAGE/books/)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_GZ')
ON_ERROR = SKIP_FILE;

-- 1) Copy the notification_channel (an SQS ARN) from this output into
--    aws/s3_event_notification.json, then apply it to the bucket (see README).
SHOW PIPES IN SCHEMA RAW;

-- 2) Load files that were already sitting in S3 before the event notification existed
--    (REFRESH looks back ~7 days of staged files).
ALTER PIPE RAW.PIPE_FX_RATES REFRESH;
ALTER PIPE RAW.PIPE_MF_NAV   REFRESH;
ALTER PIPE RAW.PIPE_BOOKS    REFRESH;

-- 3) Health check: executionState should be RUNNING
SELECT SYSTEM$PIPE_STATUS('RAW.PIPE_FX_RATES');
