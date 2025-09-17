USE ROLE AAD_PROVISIONER;

CREATE OR REPLACE PROCEDURE UTIL.PUBLIC.SET_DEFAULT_WAREHOUSES()
RETURNS VARCHAR
LANGUAGE javascript
EXECUTE AS OWNER
AS
$$

// Query to identify Users without a Default Warehouse
var SQLCOMMAND = `
    SELECT DISTINCT
        CONCAT('ALTER USER "', Name, '" SET DEFAULT_WAREHOUSE = DA_WH_ANALYTICS; ') AS QUERY
    FROM (
        SELECT
            U.Name
        FROM UTIL.PUBLIC.USERS AS U
        WHERE U.DISABLED = 'false'
            AND U.DELETED_ON IS NULL
            AND U.DEFAULT_WAREHOUSE IS NULL
            AND U.NAME LIKE '%@%'
        ) AS A
    ORDER BY QUERY ASC;
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

result = "All users now have a Default Warehouse set.";
return result;

$$
;
