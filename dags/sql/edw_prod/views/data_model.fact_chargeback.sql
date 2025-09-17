CREATE OR REPLACE VIEW data_model.fact_chargeback
(
    order_id,
    customer_id,
    activation_key,
    first_activation_key,
    store_id,
    chargeback_status_key,
    chargeback_payment_key,
    chargeback_reason_key,
    first_chargeback_reason_key,
    chargeback_datetime,
    chargeback_date_eur_conversion_rate,
    chargeback_date_usd_conversion_rate,
    chargeback_local_amount,
    chargeback_payment_transaction_local_amount,
    chargeback_tax_local_amount,
    chargeback_vat_local_amount,
    effective_vat_rate,
    source,
    chargeback_outcome,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    fc.order_id,
    fc.customer_id,
    fc.activation_key,
    fc.first_activation_key,
    fc.store_id,
    fc.chargeback_status_key,
    fc.chargeback_payment_key,
    fc.chargeback_reason_key,
    fc.first_chargeback_reason_key,
    fc.chargeback_datetime,
    fc.chargeback_date_eur_conversion_rate,
    fc.chargeback_date_usd_conversion_rate,
    fc.chargeback_local_amount,
    fc.chargeback_payment_transaction_local_amount,
    fc.chargeback_tax_local_amount,
    fc.chargeback_vat_local_amount,
    fc.effective_vat_rate,
    fc.source,
    IFF(fc.chargeback_payment_transaction_local_amount>0, 'lost', 'won') AS chargeback_outcome,
    fc.meta_create_datetime,
    fc.meta_update_datetime
FROM stg.fact_chargeback fc
LEFT JOIN stg.dim_store AS ds
        ON ds.store_id = fc.store_id
WHERE NOT fc.is_deleted
    AND ds.store_brand NOT IN ('Legacy')
    AND NOT NVL(fc.is_test_customer,False);
