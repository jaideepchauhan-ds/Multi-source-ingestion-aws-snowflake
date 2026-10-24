-- =====================================================================
-- 07_monitoring.sql : operational queries to keep on hand (and mention in interviews)
-- =====================================================================
USE ROLE INGEST_ROLE; USE WAREHOUSE INGEST_WH; USE DATABASE INGEST_DB;

-- Is each pipe running, and how many files are pending?
SELECT SYSTEM$PIPE_STATUS('INGEST_DB.RAW.PIPE_FX_RATES');
SELECT SYSTEM$PIPE_STATUS('INGEST_DB.RAW.PIPE_MF_NAV');
SELECT SYSTEM$PIPE_STATUS('INGEST_DB.RAW.PIPE_BOOKS');

-- What did Snowpipe load in the last 24h, and did anything fail?
SELECT file_name, status, row_count, row_parsed, first_error_message, last_load_time
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'INGEST_DB.RAW.FX_RATES_RAW', START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())))
ORDER BY last_load_time DESC;

-- Row-level errors of files Snowpipe skipped (ON_ERROR = SKIP_FILE)
SELECT *
FROM TABLE(VALIDATE_PIPE_LOAD(
  PIPE_NAME => 'INGEST_DB.RAW.PIPE_FX_RATES', START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())));

-- Task runs and failures
SELECT name, state, scheduled_time, completed_time, error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
  SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()), RESULT_LIMIT => 50))
ORDER BY scheduled_time DESC;

-- Rows waiting in each stream (should drain to ~0 shortly after a load)
SELECT 'fx_rates' AS stream, COUNT(*) AS pending FROM RAW.FX_RATES_RAW_STRM
UNION ALL SELECT 'mf_nav', COUNT(*) FROM RAW.MF_NAV_RAW_STRM
UNION ALL SELECT 'books',  COUNT(*) FROM RAW.BOOKS_RAW_STRM;

-- Snowpipe cost (account-level view, ~hours of latency)
SELECT pipe_name, SUM(credits_used) AS credits, SUM(files_inserted) AS files, SUM(bytes_inserted) AS bytes
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY pipe_name;

-- Freshness SLA check per curated table
SELECT 'fx_rates' AS tbl, MAX(rate_date) AS latest_business_date, MAX(last_updated_at) AS last_write FROM CURATED.FX_RATES
UNION ALL SELECT 'mf_nav', MAX(nav_date), MAX(last_updated_at) FROM CURATED.MF_NAV
UNION ALL SELECT 'books',  NULL, MAX(last_updated_at) FROM CURATED.BOOKS;
