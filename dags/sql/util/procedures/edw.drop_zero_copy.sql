/*
CALL UTIL.EDW.DROP_ZERO_COPY('_DB_TO_BE_DROPPED');

Will drop a zero copy database with a name starting wih _edw.

Examples:
CALL UTIL.EDW.DROP_ZERO_COPY('_EDW_ABC_DEV');
*/

USE ROLE SYSADMIN;
USE DATABASE UTIL;
USE SCHEMA EDW;

CREATE OR REPLACE PROCEDURE DROP_ZERO_COPY(
    database_name VARCHAR
    )
RETURNS VARCHAR
LANGUAGE javascript
RETURNS NULL ON NULL INPUT
EXECUTE AS OWNER
AS
$$

var result = "";
var sql_text = "";
var sql_exec = "";


if (DATABASE_NAME.substring(0,1) === '_') {
    sql_text = "DROP DATABASE " + DATABASE_NAME;
    sql_exec = snowflake.createStatement({sqlText: sql_text});
    sql_exec.execute();

    result = DATABASE_NAME + " is dropped."

// FAIL if naming conventions not followed
} else {
       result = "Failed: Database that you want to drop must begin with an _ (underscore)! This is to protect PROD DBs and other zero copies from being dropped.";
       return result;
}

return result;

$$
;
