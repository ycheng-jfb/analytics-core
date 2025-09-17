CREATE OR REPLACE TABLE lake.tatari.streaming_conversions (
    campaign_id VARCHAR,
    impression_date date,
    creative_code VARCHAR,
    publisher VARCHAR,
    spend DECIMAL(20,2),
    methodology VARCHAR,
    conversion_metric VARCHAR,
    lift DECIMAL(20,2),
    creative_name VARCHAR,
    account_name VARCHAR,
    filename_date DATE,
    meta_row_hash INT,
    meta_create_datetime TIMESTAMP_LTZ(3),
    meta_update_datetime TIMESTAMP_LTZ(3),
    PRIMARY KEY (campaign_id, conversion_metric, methodology, impression_date, creative_name, creative_code, account_name)
);
