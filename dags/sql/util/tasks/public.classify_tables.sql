USE DATABASE UTIL;
USE SCHEMA PUBLIC;

CREATE OR REPLACE TASK CLASSIFY_TABLES
  SCHEDULE = 'USING CRON 0,30 * * * * America/Los_Angeles'
  WAREHOUSE = 'DA_WH_ETL'
  TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24'
AS
BEGIN
CALL UTIL.PUBLIC.CLASSIFY_TABLE_ADD_TAGS([
'ACCOUNTING',
'DBT_EDW_DEV',
'DBT_EDW_PROD',
'DBT_REPORTING_BASE_PROD',
'EDW_PROD',
'LAKE',
'LAKE_ARCHIVE',
'LAKE_CONSOLIDATED',
'LAKE_FL',
'LAKE_HISTORY',
'LAKE_JFB',
'LAKE_SXF',
'REPORTING_BASE_DEV',
'REPORTING_BASE_PROD',
'REPORTING_DEV',
'REPORTING_MEDIA_BASE_PROD',
'REPORTING_PROD'
],
False);
-- add coluns to exclusion if they follow a pattern in column name
-- filter condition is shared in update PII columns list
INSERT INTO UTIL.PUBLIC.PII_COLUMNS_EXCLUSION(table_catalog,table_schema,table_name,column_name,REASON)
select
SPLIT_PART(table_name, '.', 1),SPLIT_PART(table_name, '.', 2),SPLIT_PART(table_name, '.', 3),column_name,'Ignore column name pattern '|| current_timestamp()::string--*
from UTIL.PUBLIC.PII_COLUMNS
where 1=1
and (
    1=0
    or column_name ilike '%state%'
    or column_name ilike '%city%'
    or column_name ilike '%store%'
    or column_name ilike '%color%'
    or column_name ilike '%_ID%'
    or column_name ilike '%bundle%'
    or column_name ilike '%product%'
    or column_name ilike '%county%'
    or column_name ilike '%APOLITICAL_NAME%'
    or column_name ilike '%material%'
    or column_name ilike '%CBSA_NAME%'
    or column_name ilike '%WAREHOUSE_NAME%'
    or column_name ilike '%INCOMEPERHOUSEHOLD%'
    or column_name ilike '%gender%'
)
and (SPLIT_PART(table_name, '.', 1),SPLIT_PART(table_name, '.', 2),SPLIT_PART(table_name, '.', 3),column_name) not in (
select table_catalog,table_schema,table_name,column_name
from UTIL.PUBLIC.PII_COLUMNS_EXCLUSION
)
;

UPDATE UTIL.PUBLIC.PII_COLUMNS
SET is_masking_required = False,
    comments = 'Excluded after classification '||current_timestamp()::string
WHERE 1=1
-- and SPLIT_PART(table_name, '.', 1) = 'EDW_PROD'
and (
1=0
or column_name ilike '%state%'
or column_name ilike '%city%'
or column_name ilike '%store%'
or column_name ilike '%color%'
or column_name ilike '%_ID%'
or column_name ilike '%bundle%'
or column_name ilike '%product%'
or column_name ilike '%county%'
or column_name ilike '%APOLITICAL_NAME%'
or column_name ilike '%material%'
or column_name ilike '%CBSA_NAME%'
or column_name ilike '%WAREHOUSE_NAME%'
or column_name ilike '%INCOMEPERHOUSEHOLD%'
or column_name ilike '%gender%'
)
and is_masking_required = True
;
END;

ALTER TASK CLASSIFY_TABLES RESUME;
