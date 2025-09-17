CREATE OR REPLACE TRANSIENT TABLE validation.fact_credit_event_comparison
(
    credit_id                                  NUMBER(38, 0),
    store_id                                   NUMBER(38, 0),
    credit_activity_local_datetime             TIMESTAMP_TZ(9),
    credit_activity_type                       VARCHAR,
    credit_issued_hq_datetime                  TIMESTAMP_TZ(9),
    redemption_order_id                        NUMBER(38, 0),
    administrator_id                           NUMBER(38, 0),
    original_credit_activity_type_action       VARCHAR(60),
    credit_activity_type_reason                VARCHAR(155),
    credit_activity_source                     VARCHAR(155),
    credit_activity_source_reason              VARCHAR(155),
    redemption_store_id                        NUMBER(38, 0),
    vat_rate_ship_to_country                   VARCHAR(50),
    credit_activity_vat_rate                   NUMBER(18, 6),
    credit_activity_usd_conversion_rate        NUMBER(18, 6),
    credit_activity_equivalent_count           NUMBER(38, 10),
    credit_activity_gross_vat_local_amount     NUMBER(31, 4),
    credit_activity_local_amount               NUMBER(38, 10),
    activity_amount_local_amount_issuance_date NUMBER(38, 10),
    meta_create_datetime                       TIMESTAMP_LTZ(3),
    meta_update_datetime                       TIMESTAMP_LTZ(3)
);
