CREATE OR REPLACE TEMPORARY TABLE _store AS
SELECT *
FROM edw_prod.data_model.dim_store st
WHERE store_full_name NOT LIKE '%SWAG%'
  AND store_full_name NOT LIKE '%DM%'
  AND store_full_name NOT LIKE '%Sample Request%'
  AND store_full_name NOT LIKE '%Heels%'
  AND store_full_name NOT LIKE '%Wholesale%'
  AND store_full_name NOT LIKE '%Retail Replen%'
  AND store_brand NOT IN ('Not Applicable', 'Unknown');


CREATE OR REPLACE TRANSIENT TABLE month_end.chargeback_waterfall_detail AS
SELECT CASE
           WHEN st.store_id = 116 THEN st.store_name || ' ' || st.store_country
           WHEN st.store_type = 'Retail' AND st.store_brand = 'Fabletics' THEN 'Fabletics ' || st.store_country || ' Retail'
           ELSE st.store_brand || ' ' || st.store_country END             AS store,
       CASE
           WHEN oc.is_ps_order = 'TRUE' THEN 'PS Order'
           WHEN oc.order_classification_l2 IN ('Reship', 'Exchange') THEN 'Reship/Exchange'
           ELSE oc.order_classification_l2
           END                                                            AS order_classification,
       o.order_id,
       date_trunc(MONTH, iff(o.payment_transaction_local_datetime IS NULL, o.order_local_datetime::DATE,
                             o.payment_transaction_local_datetime::DATE)) AS transaction_month,
       d.month_date                                                       AS chargeback_month,
       datediff(MONTH, transaction_month, chargeback_month)               AS monthoffset,
       -1 * cb.chargeback_payment_transaction_local_amount                AS chargeback_amount_gross_vat,
       -1 * cb.chargeback_payment_transaction_local_amount /
       (1 + zeroifnull(cb.effective_vat_rate))                            AS chargeback_amount_net_vat
FROM edw_prod.data_model.fact_chargeback cb
         JOIN edw_prod.data_model.dim_date d ON d.full_date = CAST(cb.chargeback_datetime AS DATE)
         JOIN edw_prod.data_model.fact_order o ON o.order_id = cb.order_id
         JOIN edw_prod.data_model.dim_order_sales_channel oc ON o.order_sales_channel_key = oc.order_sales_channel_key
         JOIN _store st ON st.store_id = o.store_id;

ALTER TABLE month_end.chargeback_waterfall_detail SET DATA_RETENTION_TIME_IN_DAYS = 0;

--snap shot
INSERT INTO month_end.chargeback_waterfall_detail_snapshot
SELECT store,
       order_classification,
       order_id,
       transaction_month,
       chargeback_month,
       monthoffset,
       chargeback_amount_gross_vat,
       chargeback_amount_net_vat,
       CURRENT_TIMESTAMP AS snapshot_timestamp
FROM month_end.chargeback_waterfall_detail;

DELETE
FROM month_end.chargeback_waterfall_detail_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -12, getdate());
