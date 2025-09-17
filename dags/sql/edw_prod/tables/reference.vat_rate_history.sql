CREATE TABLE reference.vat_rate_history (
    country_code VARCHAR NOT NULL,
    rate NUMBER(18, 6) NOT NULL,
    start_date DATE NOT NULL,
    expires_date DATE NOT NULL,
    meta_create_datetime TIMESTAMP_LTZ DEFAULT current_timestamp(3),
    meta_update_datetime TIMESTAMP_LTZ DEFAULT current_timestamp(3),
    PRIMARY KEY (country_code, start_date)
);
