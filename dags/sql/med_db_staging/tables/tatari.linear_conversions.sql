CREATE OR REPLACE TABLE lake.tatari.linear_conversions (
    spot_id VARCHAR,
    broadcast_week date,
    spot_datetime datetime,
    creative_name VARCHAR,
    creative_code VARCHAR,
    network VARCHAR,
    rotation VARCHAR,
    spend DECIMAL(20,2),
    conversion_metric VARCHAR,
    lift DECIMAL(20,2),
    account_name VARCHAR,
    filename_date DATE,
    meta_row_hash INT,
    meta_create_datetime TIMESTAMP_LTZ(3),
    meta_update_datetime TIMESTAMP_LTZ(3),
    PRIMARY KEY (spot_id, conversion_metric, account_name)
);
