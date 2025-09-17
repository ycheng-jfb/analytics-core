CREATE OR REPLACE TABLE reference.credit_historical_activity
(
    store_credit_id          NUMBER(38, 0),
    administrator_id         NUMBER(38, 0),
    activity_source          VARCHAR(155),
    activity_source_reason   VARCHAR(155),
    activity_type            VARCHAR(155),
    activity_type_reason     VARCHAR(155),
    activity_datetime        TIMESTAMP_NTZ(3),
    activity_amount          NUMBER(19, 4),
    redemption_order_id      NUMBER(38, 0),
    redemption_store_id      NUMBER(38, 0),
    vat_rate_ship_to_country VARCHAR(50),
    meta_create_datetime     TIMESTAMP_LTZ(9) DEFAULT current_timestamp,
    meta_update_datetime     TIMESTAMP_LTZ(9) DEFAULT current_timestamp
);
