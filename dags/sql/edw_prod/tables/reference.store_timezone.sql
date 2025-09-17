CREATE TABLE reference.store_timezone (
    store_id INT NOT NULL PRIMARY KEY,
    country_abbr VARCHAR,
    time_zone VARCHAR,
    country_name VARCHAR,
    meta_row_hash INT,
    meta_create_datetime TIMESTAMP_LTZ DEFAULT current_timestamp,
    meta_update_datetime TIMESTAMP_LTZ DEFAULT current_timestamp
);
