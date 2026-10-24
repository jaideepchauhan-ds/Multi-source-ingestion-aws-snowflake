-- =====================================================================
-- 99_teardown.sql : remove everything so a demo account stops spending credits
-- =====================================================================
USE ROLE ACCOUNTADMIN;
ALTER TASK IF EXISTS INGEST_DB.CURATED.T_MERGE_FX_RATES SUSPEND;
ALTER TASK IF EXISTS INGEST_DB.CURATED.T_MERGE_MF_NAV   SUSPEND;
ALTER TASK IF EXISTS INGEST_DB.CURATED.T_MERGE_BOOKS    SUSPEND;
DROP DATABASE IF EXISTS INGEST_DB;          -- drops pipes, streams, tasks, tables, stage, views
DROP WAREHOUSE IF EXISTS INGEST_WH;
DROP RESOURCE MONITOR IF EXISTS INGEST_RM;
DROP STORAGE INTEGRATION IF EXISTS S3_INT;
DROP ROLE IF EXISTS INGEST_ROLE;
