CREATE OR REPLACE TABLE lake.tatari.streaming_conversions_stg (
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
);
