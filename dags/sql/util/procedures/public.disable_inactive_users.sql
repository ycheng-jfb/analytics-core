USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE PROCEDURE UTIL.PUBLIC.DISABLE_INACTIVE_USERS()
RETURNS VARCHAR
LANGUAGE javascript
EXECUTE AS OWNER
AS
$$

// Query to identify inactive users
var SQLCOMMAND = `
    SELECT DISTINCT
        U.NAME AS USER_NAME
    FROM UTIL.PUBLIC.USERS AS U
    WHERE U.DISABLED = 'false'
        AND U.DELETED_ON IS NULL
        AND U.NAME LIKE '%@%'
        AND U.NAME NOT IN (
            SELECT DISTINCT USER_NAME
            FROM UTIL.PUBLIC.USER_CONFIG_EVENTLOG
            WHERE EVENT_TIME >= DATEADD(DAY, -3, CURRENT_DATE)
                AND EVENT_TYPE = 'USER ENABLED'
        )
        AND COALESCE(U.LAST_SUCCESS_LOGIN, U.CREATED_ON) <= DATEADD(DAY, -90, CURRENT_DATE());
`;

sql_dict = {sqlText: SQLCOMMAND};
stmt = snowflake.createStatement(sql_dict);

rs = stmt.execute();

// Build SQL Query pieces
var s = '';
var sql_query = '';
var sql_alter_insert_pre = `ALTER USER "`;
var sql_alter_insert_post = `" SET DISABLED = TRUE; `;
var sql_log_insert_pre = `
    INSERT INTO UTIL.PUBLIC.USER_CONFIG_EVENTLOG (
        USER_NAME, EVENT_TIME, EVENT_TYPE, EVENT_DESCRIPTION
    ) SELECT \'`;
var sql_log_insert_post = `\' AS USER_NAME,
    GETDATE() AS EVENT_TIME,
    \'DISABLED\' AS EVENT_TYPE,
    \'DISABLED FOR INACTIVITY\' AS EVENT_DESCRIPTION;
`;

while (rs.next())  {
    sql2_dict = {sqlText: rs.getColumnValue('USER_NAME')};
    user_name = sql2_dict.sqlText

    // Disable User
    sql_query = '';
    sql_query = sql_query.concat(sql_alter_insert_pre, user_name, sql_alter_insert_post);
    sql_stmt = {sqlText: sql_query};
    stmtEx = snowflake.createStatement(sql_stmt);
    stmtEx.execute();

    // Log User Disablement
    sql_query = '';
    sql_query = sql_query.concat(sql_log_insert_pre, user_name, sql_log_insert_post);
    sql_stmt = {sqlText: sql_query};
    stmtEx = snowflake.createStatement(sql_stmt);
    stmtEx.execute();

    s += rs.getColumnValue(1) + "\n";
    }

result = "All Users inactive for 90+ days have been disabled.";
return result;

$$
;

GRANT OWNERSHIP OF PROCEDURE UTIL.PUBLIC.DISABLE_INACTIVE_USERS()
TO ROLE AAD_PROVISIONER COPY CURRENT GRANTS;
