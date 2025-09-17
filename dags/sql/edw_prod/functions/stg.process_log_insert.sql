CREATE OR REPLACE PROCEDURE stg.process_log_insert (
    table_name VARCHAR,
    transform_start_time VARCHAR,
    transform_end_time VARCHAR,
    base_record_count VARCHAR,
    staging_record_count VARCHAR,
    is_full_refresh VARCHAR,
    user_name VARCHAR
    )
    RETURNS VARCHAR
    LANGUAGE JAVASCRIPT
AS
$$
    var cmd = ""
        + "INSERT INTO stg.meta_table_transform_process_log "
            + "(table_name, transform_start_time, transform_end_time, base_record_count, staging_record_count, is_full_refresh, user_name) "
        + "VALUES (?, ?, ?, ?, ?, ?, ?); ";
    try {
        snowflake.execute(
            {
            sqlText: cmd,
            binds: [TABLE_NAME, TRANSFORM_START_TIME, TRANSFORM_END_TIME, BASE_RECORD_COUNT, STAGING_RECORD_COUNT, IS_FULL_REFRESH, USER_NAME]
            }
            );
        return "CALL stg.process_log_insert - Success";
        }
    catch (err) {
        return "CALL stg.process_log_insert - Failed: " + err;
        }
$$;
