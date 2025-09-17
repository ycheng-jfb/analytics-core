CREATE OR REPLACE TABLE lake.tatari.linear_spend_and_impressions_stg (
    spot_datetime TIMESTAMP_NTZ(3),
    creative_name VARCHAR,
    creative_code VARCHAR,
    network VARCHAR,
    program VARCHAR,
    spend DECIMAL(20,2),
    impressions INT,
    lift DECIMAL(20,2),
    is_deadzoned BOOLEAN,
    spot_id VARCHAR,
    account_name VARCHAR,
    filename_date DATE
);
