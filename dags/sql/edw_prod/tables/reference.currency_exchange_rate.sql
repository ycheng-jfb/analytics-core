CREATE TABLE IF NOT EXISTS reference.currency_exchange_rate
(
    src_currency VARCHAR NOT NULL,
    dest_currency VARCHAR NOT NULL,
    rate_datetime DATETIME NOT NULL,
    exchange_rate NUMBER(18, 6) NOT NULL,
    effective_start_datetime TIMESTAMP_LTZ,
    effective_end_datetime TIMESTAMP_LTZ,
    meta_create_datetime TIMESTAMP_LTZ,
    meta_update_datetime TIMESTAMP_LTZ,
    meta_row_hash INT,
    PRIMARY KEY (src_currency,dest_currency,rate_datetime)
);
