CREATE OR REPLACE VIEW data_model.fact_refund_line
AS
SELECT
frl.refund_id,
frl.refund_line_id,
frl.order_id,
frl.order_line_id,
frl.customer_id,
frl.activation_key,
frl.first_activation_key,
frl.store_id,
frl.refund_status_key,
frl.refund_payment_method_key,
frl.refund_request_local_datetime,
frl.refund_completion_local_datetime,
frl.refund_completion_date_usd_conversion_rate,
frl.refund_completion_date_eur_conversion_rate,
frl.effective_vat_rate,
frl.product_refund_local_amount,
frl.product_cash_refund_local_amount,
frl.product_store_credit_refund_local_amount,
frl.product_cash_store_credit_refund_local_amount,
frl.product_noncash_store_credit_refund_local_amount,
frl.product_unknown_store_credit_refund_local_amount,
frl.is_chargeback,
frl.meta_create_datetime,
frl.meta_update_datetime
FROM stg.fact_refund_line AS frl
         LEFT JOIN stg.dim_store AS ds
                   ON frl.store_id = ds.store_id
WHERE NOT frl.is_deleted
  AND NOT NVL(frl.is_test_customer, FALSE)
  AND ds.store_brand NOT IN ('Legacy');
