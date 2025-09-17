USE DATABASE UTIL;
USE SCHEMA EDW;

CREATE OR REPLACE PROCEDURE WATERMARK_UPDATE(
    table_name VARCHAR,
    watermark_value VARCHAR
    )
RETURNS VARCHAR
LANGUAGE javascript
RETURNS NULL ON NULL INPUT
EXECUTE AS CALLER
AS

$$

var sql_text = "";

// Check if watermark_value is valid
sql_text = `
SELECT CAST(TRY_CAST(RTRIM(LTRIM('WATERMARK_VALUE')) AS TIMESTAMP_LTZ(9)) AS VARCHAR);
`;
sql_text = sql_text.replace('WATERMARK_VALUE', WATERMARK_VALUE);
sql_exec = snowflake.createStatement({sqlText: sql_text});
watermark_output = sql_exec.execute();
watermark_output.next();
watermark_dt = watermark_output.getColumnValue(1);
if (watermark_dt === null) {
    return "Watermark_value must be a timestamp or date!"
};

// Check if table_name is valid
sql_text = `
SELECT DISTINCT table_name
FROM EDW.STG.META_TABLE_DEPENDENCY_WATERMARK
WHERE LOWER(table_name) = LOWER(RTRIM(LTRIM('TABLE_NAME_VAR')));
`;
sql_text = sql_text.replace('TABLE_NAME_VAR', TABLE_NAME);
sql_exec = snowflake.createStatement({sqlText: sql_text});
table_name_output = sql_exec.execute();
if (table_name_output.next() === false) {
return "Table Name not found in EDW.STG.META_TABLE_DEPENDENCY_WATERMARK!"
};

// Grab Username of the Proc-Caller
sql_text = `
SELECT CURRENT_USER();
`;
sql_exec = snowflake.createStatement({sqlText: sql_text});
user_name_output = sql_exec.execute();
user_name_output.next();
USER_NAME = user_name_output.getColumnValue(1);

// Update Watermark Table
sql_text = `
UPDATE EDW.STG.META_TABLE_DEPENDENCY_WATERMARK
SET
high_watermark_datetime = CAST('WATERMARK_VAR' AS TIMESTAMP_LTZ(9)),
meta_update_datetime = CURRENT_TIMESTAMP()
WHERE LOWER(table_name) = LOWER(RTRIM(LTRIM('TABLE_NAME_VAR')));
`;
sql_text = sql_text.replace('WATERMARK_VAR', watermark_dt);
sql_text = sql_text.replace('TABLE_NAME_VAR', TABLE_NAME);
sql_exec = snowflake.createStatement({sqlText: sql_text});
sql_exec.execute();

// Insert into Log table
sql_text = `
INSERT INTO UTIL.EDW.META_TABLE_DEPENDENCY_WATERMARK_PROC_LOG
(table_name, watermark_value, user_name, meta_create_datetime, meta_update_datetime)
SELECT
    LOWER('TABLE_NAME_VAR') AS table_name,
    CAST('WATERMARK_VAR' AS TIMESTAMP_LTZ(9)) AS watermark_value,
    UPPER(RTRIM(LTRIM('USER_NAME_VAR'))) AS user_name,
    CURRENT_TIMESTAMP() AS meta_create_datetime,
    CURRENT_TIMESTAMP() AS meta_update_datetime;
`;
sql_text = sql_text.replace('WATERMARK_VAR', watermark_dt);
sql_text = sql_text.replace('TABLE_NAME_VAR', TABLE_NAME);
sql_text = sql_text.replace('USER_NAME_VAR', USER_NAME);
sql_exec = snowflake.createStatement({sqlText: sql_text});
sql_exec.execute();

return "Watermark table has been updated!";

$$
;
