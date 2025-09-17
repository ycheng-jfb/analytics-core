USE ROLE AAD_PROVISIONER;

CREATE OR REPLACE PROCEDURE UTIL.PUBLIC.SET_DEFAULT_ROLES()
RETURNS VARCHAR
LANGUAGE javascript
EXECUTE AS OWNER
AS
$$

// Query to identify Users without a Default Role
var SQLCOMMAND = `
    SELECT
        CONCAT('ALTER USER "', Name, '" SET DEFAULT_ROLE = "', Role,'"; ') AS QUERY
    FROM (
        SELECT
            U.Name,
            GU.Role,
            ROW_NUMBER() OVER(PARTITION BY U.NAME ORDER BY R.Rank ASC NULLS LAST) AS Rank
        FROM UTIL.PUBLIC.USERS AS U
            JOIN UTIL.PUBLIC.GRANTS_TO_USERS AS GU
                ON U.NAME = GU.GRANTEE_NAME
            JOIN (
                SELECT 'AZURE_ENG_ANALYST' AS ROLE, 1 AS RANK
                UNION ALL
                SELECT 'AZURE_DA_ROLE_ANALYTICS' AS ROLE, 3 AS RANK
                UNION ALL
                SELECT 'DA_ROLE_ANALYTICS' AS ROLE, 4 AS RANK
                UNION ALL
                SELECT 'AZURE_TFG_ROLE_READ_ONLY' AS ROLE, 5 AS RANK
                UNION ALL
                SELECT 'TFG_MEDIA_ADMIN' AS ROLE, 7 AS RANK
                UNION ALL
                SELECT 'DATA_SCIENCE_RW' AS ROLE, 8 AS RANK
                UNION ALL
                SELECT 'TFG_ENGINEERING_ROLE' AS ROLE, 10 AS RANK
                ) AS R
            ON R.ROLE = GU.ROLE
        WHERE U.DISABLED = 'false'
            AND U.DELETED_ON IS NULL
            AND GU.DELETED_ON IS NULL
            AND GU.ROLE NOT IN (
                'ACCOUNTADMIN',
                'SECURITYADMIN',
                'SYSADMIN',
                'DA_TASKADMIN_DAGS',
                'DA_TASKADMIN_WH',
                'AZURE_ETL_SERVICE_ACCOUNT'
                )
            AND (
                U.DEFAULT_ROLE IS NULL
                OR U.DEFAULT_ROLE IN ('ACCOUNTADMIN', 'SECURITYADMIN', 'AZURE_ETL_SERVICE_ACCOUNT')
                )
            AND U.NAME LIKE '%@%'
        ) AS A
    WHERE Rank = 1
    ORDER BY NAME ASC;
`;

sql_dict = {sqlText: SQLCOMMAND};
stmt = snowflake.createStatement(sql_dict);

rs = stmt.execute();

var s = '';

while (rs.next())  {
    sql2_dict = {sqlText: rs.getColumnValue('QUERY')};
    stmtEx = snowflake.createStatement(sql2_dict);
    stmtEx.execute();
    s += rs.getColumnValue(1) + "\n";
    }

result = "All users now have a Default Role set.";
return result;

$$
;
