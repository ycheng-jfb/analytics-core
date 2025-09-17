CREATE OR REPLACE TABLE reference.finance_currency_conversion
(
    local_currency       VARCHAR(10),
    target_currency      VARCHAR(10),
    budgeted_fx_rate     NUMBER(18, 6),
    effective_date_from  TIMESTAMP_NTZ(9) NOT NULL,
    effective_date_to    TIMESTAMP_NTZ(9) NOT NULL,
    meta_create_datetime TIMESTAMP_NTZ(9),
    meta_update_datetime TIMESTAMP_NTZ(9)
);
