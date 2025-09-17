CREATE OR REPLACE TRANSIENT TABLE month_end.retail_sales_and_refund_detail AS
SELECT st.store_brand || ' ' || st.store_country                                                  AS business_unit,
       iff(st.store_id IS NULL, 'Unclassified',
           iff(st.store_full_name = 'JustFab Retail (Glendale)', 'Glendale', st.store_full_name)) AS retail_location,
       st.store_retail_state,
       st.store_retail_city,
       'Sales'                                                                                    AS activity_type,
       d.full_date                                                                                AS activity_date,
       d.month_date                                                                               AS activity_month,
       o.order_id,
       NULL                                                                                       AS refund_id,
       zeroifnull(ca.amount)                                                                       AS physical_cash_collected,
       zeroifnull(cc.amount)                                                                      AS creditcard_amount_collected,
       iff(cd.card_type = 'amex', zeroifnull(cc.amount), 0)                                       AS creditcard_amount_collected_amex,
       iff(cd.card_type = 'paypal', zeroifnull(cc.amount), 0)                                     AS creditcard_amount_collected_paypal,
       iff(cd.card_type NOT IN ('amex', 'paypal'), zeroifnull(cc.amount),0)                       AS creditcard_amount_collected_other,
       zeroifnull(physical_cash_collected) + zeroifnull(creditcard_amount_collected)              AS total_tenders_collected,
       1                                                                                          AS order_count,
       0                                                                                          AS refund_count,
       o.subtotal - o.discount                                                                    AS product_dollars,
       o.credit                                                                                   AS credit_used,
       o.tax                                                                                      AS tax_collected,
       o.shipping                                                                                 AS shipping_collected,
       0                                                                                          AS physical_cash_refunded,
       0                                                                                          AS physical_cash_refunded_tax,
       0                                                                                          AS creditcard_refunded,
       0                                                                                          AS creditcard_refunded_amex,
       0                                                                                          AS creditcard_refunded_paypal,
       0                                                                                          AS creditcard_refunded_other,
       0                                                                                          AS creditcard_refunded_tax,
       0                                                                                          AS credit_issued_from_refund
FROM lake_consolidated_view.ultra_merchant."ORDER" o
         JOIN edw_prod.data_model.dim_store st ON o.store_id = st.store_id
    AND st.store_type = 'Retail' AND st.store_id <> 54
         LEFT JOIN lake_consolidated_view.ultra_merchant.payment_transaction_creditcard cc
                   ON cc.payment_transaction_id = o.capture_payment_transaction_id
    AND o.payment_method = 'creditcard'
    LEFT JOIN lake_consolidated_view.ultra_merchant.creditcard cd ON cd.creditcard_id = cc.creditcard_id
    LEFT JOIN lake_consolidated_view.ultra_merchant.payment_transaction_cash ca
    ON ca.payment_transaction_id = o.capture_payment_transaction_id
    AND o.payment_method = 'cash'
    LEFT JOIN edw_prod.data_model.dim_date d ON d.full_date = COALESCE (CAST (cc.datetime_added AS DATE),
                                                                   CAST (ca.datetime_added AS DATE),
                                                                   CAST (o.datetime_added AS DATE))
WHERE o.payment_statuscode >= 2600

UNION ALL

SELECT st.store_brand || ' ' || st.store_country                                                  AS business_unit,
       iff(st.store_id IS NULL, 'Unclassified',
           iff(st.store_full_name = 'JustFab Retail (Glendale)', 'Glendale', st.store_full_name)) AS retail_location,
       st.store_retail_state,
       st.store_retail_city,
       'Refund'                                                                                   AS activity_type,
       d.full_date                                                                                AS activity_date,
       d.month_date                                                                               AS activity_month,
       o.order_id,
       r.refund_id,
       0                                                                                          AS physical_cash_collected,
       0                                                                                          AS creditcard_amount_collected,
       0                                                                                          AS creditcard_amount_collected_amex,
       0                                                                                          AS creditcard_amount_collected_paypal,
       0                                                                                          AS creditcard_amount_collected_other,
       0                                                                                          AS total_tenders_collected,

       0                                                                                          AS order_count,
       1                                                                                          AS refund_count,
       0                                                                                          AS product_dollars,
       0                                                                                          AS credit_used,
       0                                                                                          AS tax_collected,
       0                                                                                          AS shipping_collected,

       iff(r.payment_method = 'cash', r.total_refund, 0)                                          AS physical_cash_refunded,
       iff(r.payment_method = 'cash', r.tax_refund, 0)                                            AS physical_cash_refunded_tax,
       iff(r.payment_method = 'creditcard', r.total_refund, 0)                                    AS creditcard_refunded,
       iff(r.payment_method = 'creditcard' AND cd.card_type = 'amex', r.total_refund,
           0)                                                                                     AS creditcard_refunded_amex,
       iff(r.payment_method = 'creditcard' AND cd.card_type = 'paypal', r.total_refund,
           0)                                                                                     AS creditcard_refunded_paypal,
       iff(r.payment_method = 'creditcard' AND cd.card_type NOT IN ('amex', 'paypal'), r.total_refund,
           0)                                                                                     AS creditcard_refunded_other,
       iff(r.payment_method = 'creditcard', r.tax_refund, 0)                                      AS creditcard_refunded_tax,
       iff(r.payment_method = 'store_credit', zeroifnull(r.total_refund),
           0)                                                                                     AS credit_issued_from_refund
FROM lake_consolidated_view.ultra_merchant."ORDER" o
         JOIN edw_prod.data_model.dim_store st ON o.store_id = st.store_id
    AND st.store_type = 'Retail'
    AND st.store_id <> 54
         JOIN lake_consolidated_view.ultra_merchant.refund r ON r.order_id = o.order_id
    AND r.statuscode = 4570
    AND r.payment_method IN ('creditcard', 'cash', 'store_credit')
         JOIN edw_prod.data_model.dim_date d ON d.full_date = CAST(r.datetime_refunded AS DATE)
         LEFT JOIN lake_consolidated_view.ultra_merchant.payment_transaction_creditcard cc
                   ON cc.payment_transaction_id = o.capture_payment_transaction_id
    AND o.payment_method = 'creditcard'
    LEFT JOIN lake_consolidated_view.ultra_merchant.creditcard cd
ON cd.creditcard_id = cc.creditcard_id;

ALTER TABLE month_end.retail_sales_and_refund_detail SET DATA_RETENTION_TIME_IN_DAYS = 0;


INSERT INTO month_end.retail_sales_and_refund_detail_snapshot
SELECT business_unit,
       retail_location,
       store_retail_state,
       store_retail_city,
       activity_type,
       activity_date,
       activity_month,
       order_id,
       refund_id,
       physical_cash_collected,
       creditcard_amount_collected,
       creditcard_amount_collected_amex,
       creditcard_amount_collected_paypal,
       creditcard_amount_collected_other,
       total_tenders_collected,
       order_count,
       refund_count,
       product_dollars,
       credit_used,
       tax_collected,
       shipping_collected,
       physical_cash_refunded,
       physical_cash_refunded_tax,
       creditcard_refunded,
       creditcard_refunded_amex,
       creditcard_refunded_paypal,
       creditcard_refunded_other,
       creditcard_refunded_tax,
       credit_issued_from_refund,
       CURRENT_TIMESTAMP AS snapshot_timestamp
FROM month_end.retail_sales_and_refund_detail;

DELETE
FROM month_end.retail_sales_and_refund_detail_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -12, getdate());
