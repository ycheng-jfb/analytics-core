CREATE PROCEDURE util.public.backup_table_to_s3 (
    FULL_TABLE_STR VARCHAR
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    var full_table_str = FULL_TABLE_STR;
    var copyCommand = `call util.public.backup_table_to_s3('${full_table_str}', '');`

    try {
        var stmt = snowflake.createStatement({sqlText: copyCommand});
        var rs = stmt.execute();
        return 'Success; Sql Command: ' + copyCommand;
    } catch (err) {
        return 'Error occurred: ' + err.message;
    }
$$;

CREATE PROCEDURE util.public.backup_table_to_s3 (
    FULL_TABLE_STR VARCHAR,
    WHERE_CLAUSE VARCHAR
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    var full_table_str = FULL_TABLE_STR.toLowerCase();
    var where_clause = WHERE_CLAUSE;
    var current_date = new Date();

    // date_formatted and datetime_formatted for organizing and creating uniqueness in backups
    var date_formatted = current_date.toISOString().split('T')[0].replace(/-/g, '_');
    var datetime_formatted = current_date.toISOString().replace(/-/g, '_');

    var s3_path = `@util.public.tsos_da_int_backup/${full_table_str}/created_on_${date_formatted}/${full_table_str}_${datetime_formatted}`

    var copyCommand = `COPY INTO ${s3_path}
                       FROM (
						    SELECT
						        *
						    FROM ${full_table_str}
						    ${where_clause}
						)
                        FILE_FORMAT=(
						    TYPE = CSV,
						    FIELD_DELIMITER = '\t',
						    RECORD_DELIMITER = '\n',
						    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
						    ESCAPE_UNENCLOSED_FIELD = None,
						    NULL_IF = ('')
						)
						OVERWRITE = True,
						HEADER = True,
						MAX_FILE_SIZE = 5000000000;`;

    try {
        var stmt = snowflake.createStatement({sqlText: copyCommand});
        var rs = stmt.execute();
        return 'Success; Sql Command: ' + copyCommand;
    } catch (err) {
        return 'Error occurred: ' + err.message;
    }
$$;
