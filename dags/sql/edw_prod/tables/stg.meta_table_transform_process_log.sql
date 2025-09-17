CREATE TABLE IF NOT EXISTS stg.meta_table_transform_process_log (
	table_name VARCHAR,
	transform_start_time TIMESTAMP_LTZ(3),
	transform_end_time TIMESTAMP_LTZ(3),
	transform_elapsed_time VARCHAR AS (TO_VARCHAR(TO_TIMESTAMP(DATEDIFF('ms', transform_start_time, transform_end_time) / 1000), 'HH24:MI:SS.FF3')),
    base_record_count INT,
    staging_record_count INT,
    is_full_refresh BOOLEAN,
    user_name VARCHAR,
	meta_create_datetime TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(3),
    meta_update_datetime TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (table_name, transform_start_time)
    );

GRANT OWNERSHIP ON TABLE stg.meta_table_transform_process_log TO ROLE __DB_RW__EDW COPY CURRENT GRANTS;
