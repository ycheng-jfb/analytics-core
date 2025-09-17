CREATE OR REPLACE TABLE reference.fact_chargeback_history
(
    order_id NUMBER(38,0) NOT NULL,
    meta_original_order_id NUMBER(38,0),
    customer_id NUMBER(38,0),
    store_id NUMBER(38,0),
    chargeback_reason_key NUMBER(38,0),
    chargeback_status_key NUMBER(38,0),
    chargeback_payment_key NUMBER(38,0),
    chargeback_datetime TIMESTAMP_TZ(3) NOT NULL,
    chargeback_quantity NUMBER(38,0),
    chargeback_date_eur_conversion_rate NUMBER(18,6),
    chargeback_date_usd_conversion_rate NUMBER(18,6),
    chargeback_local_amount NUMBER(19,4),
    chargeback_payment_transaction_local_amount NUMBER(19,4),
    chargeback_tax_local_amount NUMBER(19,4),
    effective_vat_rate NUMBER(18,6),
    is_deleted BOOLEAN,
    meta_row_hash NUMBER(38,0),
    meta_create_datetime TIMESTAMP_LTZ(9),
    meta_update_datetime TIMESTAMP_LTZ(9)
);
