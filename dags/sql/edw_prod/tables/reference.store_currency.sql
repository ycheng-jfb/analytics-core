CREATE TABLE reference.store_currency (
    store_id INT NOT NULL,
    country VARCHAR,
    currency_code VARCHAR,
    effective_start_date TIMESTAMP_LTZ,
    effective_end_date TIMESTAMP_LTZ,
    is_current BOOLEAN,
    meta_row_hash INT,
    meta_create_datetime TIMESTAMP_LTZ DEFAULT current_timestamp,
    meta_update_datetime TIMESTAMP_LTZ DEFAULT current_timestamp,
    PRIMARY KEY (store_id, effective_start_date)
);
