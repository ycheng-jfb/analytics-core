CREATE OR REPLACE TABLE lake.tatari.linear_conversions_stg (
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
);
