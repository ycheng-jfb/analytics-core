SET process_to_date = DATE_TRUNC(MONTH, CURRENT_DATE);

CREATE OR REPLACE TEMPORARY TABLE _store AS
SELECT *
FROM edw_prod.data_model.dim_store st
WHERE store_full_name NOT LIKE '%SWAG%'
  AND store_full_name NOT LIKE '%DM%'
  AND store_full_name NOT LIKE '%Sample Request%'
  AND store_full_name NOT LIKE '%Heels%'
  AND store_full_name NOT LIKE '%Wholesale%'
  AND store_full_name NOT LIKE '%Retail Replen%'
  AND store_brand NOT LIKE '%Unknown%';

-- first part is regular, second part is psp collected
CREATE OR REPLACE TRANSIENT TABLE month_end.credit_billing_refund_waterfalls_order_detail AS
SELECT CASE
           WHEN st.store_id = 54 THEN 26
           WHEN st.store_type = 'Retail' THEN 100
           ELSE st.store_id
           END                                            AS store_id,
       CASE
           WHEN o.store_id = 54 THEN 'JustFab Retail'
           WHEN o.store_id = 116 THEN st.store_brand || ' ' || st.store_country --JF personal styling
           WHEN st.store_type = 'Retail' AND st.store_brand_abbr = 'FL' THEN 'Fabletics Retail'
           WHEN st.store_type = 'Retail' AND st.store_brand_abbr = 'SX' THEN 'SavageX Retail'
           ELSE st.store_brand || ' ' || st.store_country
           END                                            AS individual_bu,
       st.store_country                                      store_country_abbr,
       CAST(DATE_TRUNC(MONTH, cc.datetime_added) AS DATE) AS payment_month,
       o.order_id,
       cc.amount - o.tax                                  AS cash_collected,
       o.payment_method,
       o.capture_payment_transaction_id

FROM lake_consolidated_view.ultra_merchant.payment_transaction_creditcard cc
         JOIN lake_consolidated_view.ultra_merchant."ORDER" o ON o.order_id = cc.order_id
         JOIN _store st ON st.store_id = o.store_id
WHERE cc.transaction_type = 'PRIOR_AUTH_CAPTURE'
  AND cc.statuscode = 4001
  AND cc.datetime_added < $process_to_date
  AND EXISTS(SELECT 1
             FROM lake_consolidated_view.ultra_merchant.order_classification oc
             WHERE oc.order_id = o.order_id
               AND oc.order_type_id IN (10, 39))

UNION ALL

SELECT CASE
           WHEN st.store_id = 54 THEN 26
           WHEN st.store_type = 'Retail' THEN 100
           ELSE st.store_id
           END                                             AS store_id,
       CASE
           WHEN o.store_id = 54 THEN 'JustFab Retail'
           WHEN o.store_id = 116 THEN st.store_brand || ' ' || st.store_country
           WHEN st.store_type = 'Retail' AND st.store_brand_abbr = 'FL' THEN 'Fabletics Retail'
           WHEN st.store_type = 'Retail' AND st.store_brand_abbr = 'SX' THEN 'SavageX Retail'
           ELSE st.store_brand || ' ' || st.store_country
           END                                             AS individual_bu,
       st.store_country                                       store_country_abbr,
       CAST(DATE_TRUNC(MONTH, psp.datetime_added) AS DATE) AS payment_month,
       o.order_id,
       psp.amount / (1 + ZEROIFNULL(v.rate)) - o.tax       AS cash_collected,
       o.payment_method,
       o.capture_payment_transaction_id
FROM lake_consolidated_view.ultra_merchant.payment_transaction_psp psp
         JOIN lake_consolidated_view.ultra_merchant."ORDER" o ON o.order_id = psp.order_id
         JOIN _store st ON st.store_id = o.store_id
         LEFT JOIN edw_prod.reference.vat_rate_history v
                   ON v.country_code = REPLACE(REPLACE(st.store_country, 'UK', 'GB'), 'EU', 'NL')
                       AND psp.datetime_added BETWEEN v.start_date AND v.expires_date
WHERE psp.transaction_type = 'PRIOR_AUTH_CAPTURE'
  AND psp.statuscode IN (4001, 4040)
  AND psp.datetime_added < $process_to_date
  AND EXISTS(SELECT 1
             FROM lake_consolidated_view.ultra_merchant.order_classification oc
             WHERE oc.order_id = o.order_id
               AND oc.order_type_id IN (10, 39))

UNION
-- for butter payments
SELECT CASE
           WHEN st.store_id = 54 THEN 26
           WHEN st.store_type = 'Retail' THEN 100
           ELSE st.store_id
           END                                            AS store_id,
       CASE
           WHEN o.store_id = 54 THEN 'JustFab Retail'
           WHEN o.store_id = 116 THEN st.store_brand || ' ' || st.store_country --JF personal styling
           WHEN st.store_type = 'Retail' AND st.store_brand_abbr = 'FL' THEN 'Fabletics Retail'
           WHEN st.store_type = 'Retail' AND st.store_brand_abbr = 'SX' THEN 'SavageX Retail'
           ELSE st.store_brand || ' ' || st.store_country
           END                                            AS individual_bu,
       st.store_country                                      store_country_abbr,
       CAST(DATE_TRUNC(MONTH, cc.datetime_added) AS DATE) AS payment_month,
       o.order_id,
       cc.amount - o.tax                                  AS cash_collected,
       o.payment_method,
       o.capture_payment_transaction_id

FROM lake_consolidated_view.ultra_merchant.payment_transaction_creditcard cc
         JOIN lake_consolidated_view.ultra_merchant."ORDER" o ON o.order_id = cc.order_id
         JOIN _store st ON st.store_id = o.store_id
WHERE cc.transaction_type = 'SALE_REDIRECT'
  AND cc.statuscode = 4001
  AND o.payment_statuscode <> 2510
  AND o.processing_statuscode <> 2200
  AND cc.datetime_added::DATE >= '2024-01-01'
  AND cc.datetime_added < $process_to_date
  AND EXISTS(SELECT 1
             FROM lake_consolidated_view.ultra_merchant.order_classification oc
             WHERE oc.order_id = o.order_id
               AND oc.order_type_id IN (10, 39));

ALTER TABLE month_end.credit_billing_refund_waterfalls_order_detail
    SET DATA_RETENTION_TIME_IN_DAYS = 0;
