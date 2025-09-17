CREATE OR REPLACE TABLE lake.tatari.streaming_spend_and_impression (
    date DATE,
    platform VARCHAR,
    measured_spend DECIMAL(20,2),
    booked_cpm DECIMAL(20,2),
    impressions INT,
    effective_spend DECIMAL(20,2),
    effective_cpm DECIMAL(20,2),
    creative_code VARCHAR,
    campaign_id VARCHAR,
    creative_name VARCHAR,
    account_name VARCHAR,
    filename_date DATE,
    meta_row_hash INT,
    meta_create_datetime TIMESTAMP_LTZ(3),
    meta_update_datetime TIMESTAMP_LTZ(3),
    PRIMARY KEY (date, platform, creative_code, campaign_id, creative_name, account_name)
);
