/*

CALL UTIL.EDW.CLONE_DB('DB_TO_BE_CLONED', '_CLONED_DB_NAME');

Will clone a database and set Dev Permissions.
2nd Param requires an Underscore to protect Prod DBs from being overwritten!

Examples:
CALL UTIL.EDW.CLONE_DB('EDW', '_EDW_ABC_DEV');
*/

/* Proc must be executed as AccountAdmin in order to Drop/Replace DBs. */
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE PROCEDURE UTIL.EDW.CLONE_DB("SOURCE_DATABASE" VARCHAR(16777216), "DESTINATION_DATABASE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS '
// DESTINATION_DATABASE must be prepended with an underscore to protect against overwriting Prod DBs
// EDW clones must have special Access Grants
// CALL UTIL.EDW.CLONE_DB(''EDW'', ''_EDW_ABC_DEV'') to generate a Clone named ''_EDW_ABC_DEV''
var result = "";
var sql_text = "";
var sql_exec = "";
var ROLE_NAME = ''__CLONE_OWNER_RWC'';

if (DESTINATION_DATABASE.charAt(0) === ''_'') {
    sql_text = "CREATE OR REPLACE DATABASE " + DESTINATION_DATABASE + " CLONE " + SOURCE_DATABASE + "; ";
    sql_exec = snowflake.createStatement({sqlText: sql_text});
    sql_exec.execute();


    // Set SYSADMIN as OWNER of the DB
    sql_text = "GRANT OWNERSHIP ON DATABASE " + DESTINATION_DATABASE + " TO ROLE SYSADMIN COPY CURRENT GRANTS; ";
    sql_exec = snowflake.createStatement({sqlText: sql_text});
    sql_exec.execute();

    // Drop Snapshot Schema from the destination and disable time travel on the destination EDW
    sql_text ="DROP SCHEMA IF EXISTS " + DESTINATION_DATABASE + ".snapshot;"
    sql_exec = snowflake.createStatement({sqlText: sql_text});
    sql_exec.execute();

    schema_list = ["stg","analytics_base","reporting","reference"];
    for (const element of schema_list) {
        sql_text = "ALTER SCHEMA IF EXISTS " + DESTINATION_DATABASE + "."+element+" SET DATA_RETENTION_TIME_IN_DAYS =0;"
        sql_exec = snowflake.createStatement({sqlText: sql_text});
        sql_exec.execute();
    }


    // Must revoke Future Grants as Future Grants can''t be overwritten
    SQLCOMMAND = `
        SELECT DISTINCT
            CASE WHEN (
                A.SCHEMA_OWNER ILIKE ''%WORK%''
                 OR A.SCHEMA_OWNER IN (''__CLONE_OWNER_RWC'')
                 )
            THEN CONCAT(''REVOKE OWNERSHIP ON FUTURE '', OBJECT, '' IN SCHEMA DESTINATION_DATABASE.'', SCHEMA_NAME, '' FROM ROLE '', SCHEMA_OWNER,''; '')
            ELSE CONCAT(''REVOKE OWNERSHIP ON FUTURE '', OBJECT, '' IN SCHEMA DESTINATION_DATABASE.'', SCHEMA_NAME, '' FROM ROLE __'', CATALOG_NAME, ''_'', SCHEMA_NAME, ''_RWC; '')
            END AS QUERY
        FROM snowflake.account_usage.schemata AS A
            FULL OUTER JOIN(
                SELECT ''TABLES'' AS OBJECT
                UNION
                SELECT ''VIEWS'' AS OBJECT
                UNION
                SELECT ''MATERIALIZED VIEWS'' AS OBJECT
                UNION
                SELECT ''FUNCTIONS'' AS OBJECT
                UNION
                SELECT ''PROCEDURES'' AS OBJECT
                )
        WHERE CATALOG_NAME = ''SOURCE_DATABASE''
            AND SCHEMA_NAME NOT IN (''INFORMATION_SCHEMA'',''SNAPSHOT'')
            AND DELETED IS NULL
        ORDER BY QUERY ASC;
    `;

    SQLCOMMAND = SQLCOMMAND.replace(''DESTINATION_DATABASE'', DESTINATION_DATABASE);
    SQLCOMMAND = SQLCOMMAND.replace(''DESTINATION_DATABASE'', DESTINATION_DATABASE);
    SQLCOMMAND_DICT = SQLCOMMAND.replace(''SOURCE_DATABASE'', SOURCE_DATABASE);

    sql_dict = {sqlText: SQLCOMMAND_DICT};
    stmt = snowflake.createStatement(sql_dict);

    rs = stmt.execute();

    var s = '''';

    while (rs.next())  {
        sql2_dict = {sqlText: rs.getColumnValue(''QUERY'')};
        stmtEx = snowflake.createStatement(sql2_dict);
        stmtEx.execute();
        s += rs.getColumnValue(1) + "\\n";
        }

    // Set privileges for the Clone
    sql_text = "GRANT USAGE, MONITOR ON DATABASE " + DESTINATION_DATABASE + " TO ROLE " + ROLE_NAME + "; ";
    sql_exec = snowflake.createStatement({sqlText: sql_text});
    sql_exec.execute();

    sql_text = "GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE " + DESTINATION_DATABASE + " TO ROLE " + ROLE_NAME + " COPY CURRENT GRANTS; ";
    sql_exec = snowflake.createStatement({sqlText: sql_text});
    sql_exec.execute();
    sql_text = "GRANT OWNERSHIP ON FUTURE SCHEMAS IN DATABASE " + DESTINATION_DATABASE + " TO ROLE " + ROLE_NAME + " COPY CURRENT GRANTS; ";
    sql_exec = snowflake.createStatement({sqlText: sql_text});
    sql_exec.execute();
    sql_text = "GRANT OWNERSHIP ON ALL TABLES IN DATABASE " + DESTINATION_DATABASE + " TO ROLE " + ROLE_NAME + " COPY CURRENT GRANTS; ";
    sql_exec = snowflake.createStatement({sqlText: sql_text});
    sql_exec.execute();
    sql_text = "GRANT OWNERSHIP ON FUTURE TABLES IN DATABASE " + DESTINATION_DATABASE + " TO ROLE " + ROLE_NAME + " COPY CURRENT GRANTS; ";
    sql_exec = snowflake.createStatement({sqlText: sql_text});
    sql_exec.execute();
    sql_text = "GRANT OWNERSHIP ON ALL VIEWS IN DATABASE " + DESTINATION_DATABASE + " TO ROLE " + ROLE_NAME + " COPY CURRENT GRANTS; ";
    sql_exec = snowflake.createStatement({sqlText: sql_text});
    sql_exec.execute();
    sql_text = "GRANT OWNERSHIP ON FUTURE VIEWS IN DATABASE " + DESTINATION_DATABASE + " TO ROLE " + ROLE_NAME + " COPY CURRENT GRANTS; ";
    sql_exec = snowflake.createStatement({sqlText: sql_text});
    sql_exec.execute();
    sql_text = "GRANT OWNERSHIP ON ALL MATERIALIZED VIEWS IN DATABASE " + DESTINATION_DATABASE + " TO ROLE " + ROLE_NAME + " COPY CURRENT GRANTS; ";
    sql_exec = snowflake.createStatement({sqlText: sql_text});
    sql_exec.execute();
    sql_text = "GRANT OWNERSHIP ON FUTURE MATERIALIZED VIEWS IN DATABASE " + DESTINATION_DATABASE + " TO ROLE " + ROLE_NAME + " COPY CURRENT GRANTS; ";
    sql_exec = snowflake.createStatement({sqlText: sql_text});
    sql_exec.execute();
    sql_text = "GRANT OWNERSHIP ON ALL FUNCTIONS IN DATABASE " + DESTINATION_DATABASE + " TO ROLE " + ROLE_NAME + " COPY CURRENT GRANTS; ";
    sql_exec = snowflake.createStatement({sqlText: sql_text});
    sql_exec.execute();
    sql_text = "GRANT OWNERSHIP ON FUTURE FUNCTIONS IN DATABASE " + DESTINATION_DATABASE + " TO ROLE " + ROLE_NAME + " COPY CURRENT GRANTS; ";
    sql_exec = snowflake.createStatement({sqlText: sql_text});
    sql_exec.execute();
    sql_text = "GRANT OWNERSHIP ON ALL PROCEDURES IN DATABASE " + DESTINATION_DATABASE + " TO ROLE " + ROLE_NAME + " COPY CURRENT GRANTS; ";
    sql_exec = snowflake.createStatement({sqlText: sql_text});
    sql_exec.execute();
    sql_text = "GRANT OWNERSHIP ON FUTURE PROCEDURES IN DATABASE " + DESTINATION_DATABASE + " TO ROLE " + ROLE_NAME + " COPY CURRENT GRANTS; ";
    sql_exec = snowflake.createStatement({sqlText: sql_text});
    sql_exec.execute();

    // Apply Future Grants on Schemas because Revoked Schema Future Grants still block Database Future Grants
    SQLCOMMAND = `
        SELECT DISTINCT
            CONCAT(''GRANT OWNERSHIP ON FUTURE '', OBJECT, '' IN SCHEMA DESTINATION_DATABASE.'', SCHEMA_NAME, '' TO ROLE ROLE_NAME; '') AS QUERY
        FROM snowflake.account_usage.schemata AS A
            FULL OUTER JOIN(
                SELECT ''TABLES'' AS OBJECT
                UNION
                SELECT ''VIEWS'' AS OBJECT
                UNION
                SELECT ''MATERIALIZED VIEWS'' AS OBJECT
                UNION
                SELECT ''FUNCTIONS'' AS OBJECT
                UNION
                SELECT ''PROCEDURES'' AS OBJECT
                )
        WHERE CATALOG_NAME = ''DESTINATION_DATABASE''
            AND SCHEMA_NAME NOT IN (''INFORMATION_SCHEMA'',''SNAPSHOT'')
            AND DELETED IS NULL
        ORDER BY QUERY ASC;
    `;
    SQLCOMMAND = SQLCOMMAND.replace(''ROLE_NAME'', ROLE_NAME);
    SQLCOMMAND = SQLCOMMAND.replace(''DESTINATION_DATABASE'', DESTINATION_DATABASE);
    SQLCOMMAND_DICT = SQLCOMMAND.replace(''DESTINATION_DATABASE'', DESTINATION_DATABASE);

    sql_dict = {sqlText: SQLCOMMAND_DICT};
    stmt = snowflake.createStatement(sql_dict);

    rs = stmt.execute();

    var s = '''';

    while (rs.next())  {
        sql2_dict = {sqlText: rs.getColumnValue(''QUERY'')};
        stmtEx = snowflake.createStatement(sql2_dict);
        stmtEx.execute();
        s += rs.getColumnValue(1) + "\\n";
        }

    if (DESTINATION_DATABASE === ''_EDW_DEV'') {
        sql_text = "ALTER DATABASE _EDW_DEV RENAME TO EDW_DEV;"
        sql_exec = snowflake.createStatement({sqlText: sql_text});
        sql_exec.execute();
    }

    // Consider New Database classified tables as per the current Database Tables classified table
    SQLCOMMAND = `
                MERGE INTO UTIL.PUBLIC.CLASSIFIED_TABLES tgt
        USING (

                WITH db_classified_tables AS (
                    SELECT * FROM UTIL.PUBLIC.CLASSIFIED_TABLES WHERE TABLE_CATALOG = ''${SOURCE_DATABASE}''
                ),
                new_db_tables AS (
                    SELECT
                        CASE
                        WHEN IS_TRANSIENT = ''YES'' THEN ''TRANSIENT TABLE''
                        ELSE ''TABLE''
                        END AS TABLE_TYPE ,
                    TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, LAST_DDL
                    FROM ${DESTINATION_DATABASE}.INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE=''BASE TABLE''
                                AND IS_TEMPORARY = ''NO''
                                AND ROW_COUNT>25
                                AND TABLE_SCHEMA != ''INFORMATION_SCHEMA''
                ),
                new_tables_to_be_classified AS (
                    SELECT
                    ndb.TABLE_TYPE, ndb.TABLE_CATALOG, ndb.TABLE_SCHEMA, ndb.TABLE_NAME, ndb.last_ddl
                    FROM db_classified_tables db JOIN new_db_tables ndb ON
                        db.TABLE_TYPE = ndb.TABLE_TYPE AND db.TABLE_SCHEMA = ndb.TABLE_SCHEMA AND db.TABLE_NAME = ndb.TABLE_NAME
                )
                SELECT * FROM new_tables_to_be_classified

        ) AS src
        ON src.TABLE_TYPE = tgt.TABLE_TYPE AND src.TABLE_CATALOG = tgt.TABLE_CATALOG AND src.TABLE_SCHEMA = tgt.TABLE_SCHEMA AND src.TABLE_NAME = tgt.TABLE_NAME
        WHEN NOT matched THEN insert (table_type, table_catalog, table_schema, table_name, last_ddl) VALUES (src.table_type, src.table_catalog, src.table_schema, src.table_name, src.last_ddl);
    `;

    sql_dict = {sqlText: SQLCOMMAND};
    stmt = snowflake.createStatement(sql_dict);

    rs = stmt.execute();

    // Apply Currently classified database''s table columns to new_database

    SQLCOMMAND = `
        MERGE INTO UTIL.PUBLIC.PII_COLUMNS tgt
        USING (
            WITH db_pii_tag_logs AS (
                    SELECT * FROM UTIL.PUBLIC.PII_COLUMNS WHERE SPLIT(TABLE_NAME, ''.'')[0] = UPPER(''${SOURCE_DATABASE}'')
            ) SELECT CONCAT(UPPER(''${DESTINATION_DATABASE}''), ''.'', ARRAY_TO_STRING(ARRAY_SLICE((SPLIT(TABLE_NAME, ''.'')), 0, -1), ''.''))
 AS TABLE_NAME, COLUMN_NAME, IS_MASKING_REQUIRED, POLICY_NAME, COMMENTS from db_pii_tag_logs
        ) AS src
        ON src.TABLE_NAME = tgt.TABLE_NAME AND src.COLUMN_NAME = tgt.COLUMN_NAME
        WHEN NOT matched THEN insert (table_name, column_name, is_masking_required, policy_name, datetime_added, comments) VALUES (src.table_name, src.column_name, src.is_masking_required, src.policy_name,CURRENT_TIMESTAMP(), src.comments);
    `;

    sql_dict = {sqlText: SQLCOMMAND};
    stmt = snowflake.createStatement(sql_dict);

    rs = stmt.execute();

    result = DESTINATION_DATABASE + " has been created."

// FAIL if naming conventions not followed
} else {
       result = "Failed: Destination Database must begin with an underscore! This is to protect Prod DBs from being written over.";
       return result;
}

return result;

';
