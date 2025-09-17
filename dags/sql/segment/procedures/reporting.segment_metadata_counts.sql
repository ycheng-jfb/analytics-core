CREATE OR REPLACE PROCEDURE REPORTING.SEGMENT_METADATA_COUNTS(schema_ VARCHAR(16777216), timestamp_ VARCHAR(16777216), extra_where VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    const schema = SCHEMA_.toUpperCase()

    const output = {}
    const sql_tables_template = `
        SELECT table_name
        FROM LAKE.INFORMATION_SCHEMA.TABLES
        WHERE table_schema = ''SCHEMA_REPLACE''
            AND NOT table_name ILIKE ''%VIEWED_CHECKOUT_ORDER_COMPLETE_PAGE''
        `
    const sql_tables_query = sql_tables_template.replace(''SCHEMA_REPLACE'', schema)
    const result_tables = snowflake.execute({sqlText: sql_tables_query})

    const sql_delete_template = "DELETE FROM SEGMENT.REPORTING.SEGMENT_API_VOLUME_STG WHERE SCHEMA = ''SCHEMA_REPLACE''"
    const sql_delete_query = sql_delete_template.replace(''SCHEMA_REPLACE'', schema)
    snowflake.execute({sqlText: sql_delete_query})

    while(result_tables.next()) {
        const sql_insert_template = `
        INSERT INTO SEGMENT.REPORTING.SEGMENT_API_VOLUME_STG
        SELECT
        convert_timezone(''UTC'',''America/Los_Angeles'', t.TIMESTAMP)::date as REPORT_DATE,
        CASE
            WHEN ''REPLACE_TABLE'' ILIKE ''JAVASCRIPT%'' THEN ''Javascript''
            WHEN ''REPLACE_TABLE'' ILIKE ''JAVA%ECOM_MOBILE_APP%'' THEN ''Java Ecom Mobile App''
            WHEN ''REPLACE_TABLE'' ILIKE ''JAVA%ECOM_APP%'' THEN ''Java Ecom App''
            WHEN ''REPLACE_TABLE'' ILIKE ''JAVA%'' THEN ''Java''
            WHEN ''REPLACE_TABLE'' ILIKE ''REACT_NATIVE%FITNESS_APP%'' THEN ''React native Fitness App''
            WHEN ''REPLACE_TABLE'' ILIKE ''React%'' THEN ''React native''
            WHEN ''REPLACE_TABLE'' ILIKE ''Node%'' THEN ''Shoedazzle''
            ELSE SPLIT(SEGMENT_SHARD, ''_'')[0]
        END AS SOURCE,
        CASE
            WHEN ''REPLACE_SCHEMA'' ILIKE ''%SEGMENT_FL%'' THEN ''Fabletics''
            WHEN ''REPLACE_SCHEMA'' ILIKE ''%SEGMENT_SXF%'' THEN ''Savage X''
            WHEN ''REPLACE_SCHEMA'' ILIKE ''%SEGMENT_GFB%'' AND ''REPLACE_TABLE'' ILIKE ''%FABKIDS%'' THEN ''Fabkids''
            WHEN ''REPLACE_SCHEMA'' ILIKE ''%SEGMENT_GFB%'' AND ''REPLACE_TABLE'' ILIKE ''%SHOEDAZZLE%'' THEN ''Shoedazzle''
            WHEN ''REPLACE_SCHEMA'' ILIKE ''%SEGMENT_GFB%'' THEN ''JustFab''
            ELSE SPLIT(SEGMENT_SHARD, ''_'')[1]
        END AS STORE_BRAND,
        NVL(NULLIF(country, ''''), ''us'') AS STORE_COUNTRY,
        ''Prod'' AS ENVIRONMENT,
        MAX(event) AS API_CALL,
        COUNT(DISTINCT t.MESSAGEID) AS VOLUME,
        ''REPLACE_SCHEMA'' AS SCHEMA,
        ''REPLACE_TABLE'' AS TABLE_NAME
        from LAKE.REPLACE_SCHEMA.REPLACE_TABLE t
        WHERE t.TIMESTAMP::DATE >= ''REPLACE_TIMESTAMP''
        REPLACE_EXTRA_WHERE
        GROUP BY REPORT_DATE, SEGMENT_SHARD, STORE_COUNTRY`

        let sql_insert_query = sql_insert_template.replaceAll(''REPLACE_SCHEMA'', schema).replaceAll(''REPLACE_TABLE'', result_tables.getColumnValue(1)).replace(''REPLACE_TIMESTAMP'', TIMESTAMP_).replace(''REPLACE_EXTRA_WHERE'', EXTRA_WHERE)
        const table_parts = result_tables.getColumnValue(1).split(''_'')
        if (table_parts[table_parts.length - 1].toUpperCase() == ''PAGE'') {
            sql_insert_query = sql_insert_query.replace(''MAX(event)'', "''page''")
        } else if (table_parts[table_parts.length - 1].toUpperCase() == ''IDENTIFY'') {
            sql_insert_query = sql_insert_query.replace(''MAX(event)'', "''identifies''")
        } else if (table_parts[table_parts.length - 1].toUpperCase() == ''SCREEN'') {
            sql_insert_query = sql_insert_query.replace(''MAX(event)'', "''screens''")
        }
        snowflake.execute({sqlText: sql_insert_query});
    }

    return ''Success''
';
