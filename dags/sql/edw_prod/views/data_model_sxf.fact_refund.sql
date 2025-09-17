CREATE OR REPLACE VIEW data_model_sxf.fact_refund
AS
SELECT stg.udf_unconcat_brand(fr.refund_id) AS refund_id,
       fr.refund_payment_method_key,
       fr.refund_reason_key,
       fr.refund_comment_key,
       fr.refund_status_key,
       stg.udf_unconcat_brand(fr.order_id) AS order_id,
       stg.udf_unconcat_brand(fr.customer_id) AS customer_id,
       fr.activation_key,
       fr.first_activation_key,
       fr.store_id,
       stg.udf_unconcat_brand(fr.return_id) AS return_id,
       fr.refund_request_local_datetime,
       fr.refund_completion_local_datetime,
       stg.udf_unconcat_brand(fr.rma_id) AS rma_id,
       fr.refund_administrator_id,
       fr.refund_approver_id,
       stg.udf_unconcat_brand(fr.refund_payment_transaction_id) AS refund_payment_transaction_id,
       fr.refund_product_local_amount,
       fr.refund_freight_local_amount,
       fr.refund_tax_local_amount,
       fr.refund_total_local_amount,
       fr.refund_completion_date_eur_conversion_rate,
       fr.refund_completion_date_usd_conversion_rate,
       fr.effective_vat_rate,
       fr.raw_refund_status_key,
       fr.is_chargeback,
       fr.meta_create_datetime,
       fr.meta_update_datetime,
       CASE
           WHEN drpm.refund_payment_method NOT IN ('Store Credit', 'Membership Token') THEN fr.refund_total_local_amount
           ELSE 0 END AS cash_refund_local_amount,
       CASE
           WHEN drpm.refund_payment_method NOT IN ('Store Credit', 'Membership Token') THEN 1
           ELSE 0 END AS cash_refund_count,
       CASE
           WHEN drpm.refund_payment_method IN ('Store Credit', 'Membership Token') THEN fr.refund_total_local_amount
           ELSE 0 END AS store_credit_refund_local_amount,
       CASE
           WHEN drpm.refund_payment_method IN ('Store Credit', 'Membership Token') THEN 1
           ELSE 0 END AS store_credit_refund_count,
       CASE
           WHEN drpm.refund_payment_method IN ('Store Credit', 'Membership Token') AND
                drpm.refund_payment_method_type = 'Cash Credit'
               THEN fr.refund_total_local_amount
           ELSE 0 END AS cash_store_credit_refund_local_amount,
       CASE
           WHEN drpm.refund_payment_method IN ('Store Credit', 'Membership Token') AND
                drpm.refund_payment_method_type = 'Cash Credit'
               THEN 1
           ELSE 0 END AS cash_store_credit_refund_count,
       CASE
           WHEN drpm.refund_payment_method IN ('Store Credit', 'Membership Token') AND
                drpm.refund_payment_method_type = 'NonCash Credit'
               THEN fr.refund_total_local_amount
           ELSE 0 END AS noncash_store_credit_refund_local_amount,
       CASE
           WHEN drpm.refund_payment_method IN ('Store Credit', 'Membership Token') AND
                drpm.refund_payment_method_type = 'NonCash Credit'
               THEN 1
           ELSE 0 END AS noncash_store_credit_refund_count,
       CASE
           WHEN drpm.refund_payment_method IN ('Store Credit', 'Membership Token') AND
                drpm.refund_payment_method_type = 'Unknown'
               THEN fr.refund_total_local_amount
           ELSE 0 END AS unknown_store_credit_refund_local_amount,
       CASE
           WHEN drpm.refund_payment_method IN ('Store Credit', 'Membership Token') AND
                drpm.refund_payment_method_type = 'Unknown'
               THEN 1
           ELSE 0 END AS unknown_store_credit_refund_count
FROM stg.fact_refund AS fr
         LEFT JOIN stg.fact_order AS fo
                   ON fo.order_id = fr.order_id
         LEFT JOIN stg.dim_order_sales_channel AS osc
                    ON osc.order_sales_channel_key = fo.order_sales_channel_key
         LEFT JOIN stg.dim_refund_payment_method AS drpm
                   ON fr.refund_payment_method_key = drpm.refund_payment_method_key
         LEFT JOIN stg.dim_store AS ds
                   ON fr.store_id = ds.store_id
WHERE NOT fr.is_deleted
  AND NOT NVL(fr.is_test_customer, FALSE)
  AND ds.store_brand NOT IN ('Legacy')
  AND substring(fr.refund_id, -2) = '30';
