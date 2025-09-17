CREATE TABLE IF NOT EXISTS stg.dim_currency (
    currency_key INT IDENTITY (1,1) NOT NULL,
    iso_currency_code VARCHAR(7) NOT NULL,
    currency_exchange_rate_type VARCHAR(25) NOT NULL,
    currency_name VARCHAR(25) NOT NULL,
    currency_symbol VARCHAR(7) NOT NULL,
    effective_start_datetime TIMESTAMP_LTZ,
    effective_end_datetime TIMESTAMP_LTZ,
    is_current BOOLEAN,
    meta_create_datetime TIMESTAMP_LTZ,
    meta_update_datetime TIMESTAMP_LTZ,
    PRIMARY KEY (currency_key)
);
