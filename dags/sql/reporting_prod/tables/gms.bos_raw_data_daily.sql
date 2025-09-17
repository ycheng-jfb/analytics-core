CREATE TRANSIENT TABLE IF NOT EXISTS gms.bos_raw_data_daily(
    storefront VARCHAR(16777216),
    billing_month date,
    total_planned NUMBER(38,0),
    planned NUMBER(38,0),
    actuals NUMBER(38,0),
    billing_count NUMBER(38,0),
    prev_3_month_actuals NUMBER(38,0),
    prev_3_month_billing_count NUMBER(38,0),
    day VARCHAR(5),
    meta_row_hash NUMBER(38,0),
    meta_create_datetime TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    meta_update_datetime TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
);
