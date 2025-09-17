CREATE OR REPLACE TABLE lake.tatari.streaming_spend_and_impression_stg (
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
    filename_date DATE
);
