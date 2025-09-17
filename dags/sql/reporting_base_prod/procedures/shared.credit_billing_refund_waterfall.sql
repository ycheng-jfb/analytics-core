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
CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.credit_billing_refund_waterfalls_order_detail AS
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

ALTER TABLE reporting_base_prod.shared.credit_billing_refund_waterfalls_order_detail
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

/************************************* GET REFUNDS **********************************/
-- need to get the transaction date

CREATE OR REPLACE TEMP TABLE _refund_details AS
SELECT o.order_id,
       r.refund_id,
       o.store_id,
       o.individual_bu,
       CAST(COALESCE(r.datetime_refunded, r.date_added) AS DATE)                         AS date_refunded,
       r.total_refund,
       r.tax_refund,
       (r.total_refund / (1 + ZEROIFNULL(v.rate))) - r.tax_refund                        AS credit_billing_refund_as_cash,
       o.capture_payment_transaction_id,
       o.payment_method,
       CAST(DATE_TRUNC(MONTH, o.payment_month) AS DATE)                                  AS payment_month,
       CAST(DATE_TRUNC(MONTH, COALESCE(r.datetime_refunded, r.date_added)) AS DATE)      AS refund_month,
       DATEDIFF(MONTH, o.payment_month, COALESCE(r.datetime_refunded, r.date_added)) + 1 AS refund_month_offset
FROM reporting_base_prod.shared.credit_billing_refund_waterfalls_order_detail o
         LEFT JOIN lake_consolidated_view.ultra_merchant.refund r
                   ON o.order_id = r.order_id
         LEFT JOIN edw_prod.reference.vat_rate_history v
                   ON v.country_code = REPLACE(REPLACE(o.store_country_abbr, 'UK', 'GB'), 'EU', 'NL')
                       AND o.payment_month >= v.start_date
                       AND o.payment_month < v.expires_date -- Added this join for the VAT rates
WHERE 1 = 1
  AND (r.statuscode = 4570 OR r.statuscode IS NULL) -- refunded
  AND (r.payment_method IN ('psp', 'creditcard', 'cash', 'Check_request') OR r.payment_method IS NULL)
  AND EXISTS(SELECT 1
             FROM lake_consolidated_view.ultra_merchant.order_classification oc
             WHERE oc.order_id = o.order_id
               AND oc.order_type_id IN (10, 39));

CREATE OR REPLACE TEMP TABLE _credit_billing_refund_waterfalls_refund_detail AS
SELECT t.*,
       dd.month_date                                       AS new_refund_month,
       DATEDIFF(MONTH, t.payment_month, dd.month_date) + 1 AS new_refund_month_offset
FROM _refund_details t
         JOIN
         (SELECT DISTINCT month_date FROM edw_prod.data_model.dim_date) AS dd
         ON dd.month_date < CURRENT_DATE AND dd.month_date >= t.payment_month;

UPDATE _credit_billing_refund_waterfalls_refund_detail
SET refund_id                     = NULL,
    date_refunded                 = NULL,
    total_refund                  = 0,
    tax_refund                    = 0,
    credit_billing_refund_as_cash = 0
WHERE new_refund_month < refund_month;

DELETE
FROM _credit_billing_refund_waterfalls_refund_detail
WHERE refund_id IS NOT NULL
  AND DATEDIFF(MONTH, refund_month, new_refund_month) >= 1;

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.credit_billing_refund_waterfalls_refund_detail AS
SELECT order_id,
       refund_id,
       store_id,
       individual_bu,
       date_refunded,
       total_refund,
       tax_refund,
       credit_billing_refund_as_cash,
       capture_payment_transaction_id,
       payment_method,
       payment_month,
       new_refund_month        AS refund_month,
       new_refund_month_offset AS refund_month_offset
FROM _credit_billing_refund_waterfalls_refund_detail;

ALTER TABLE reporting_base_prod.shared.credit_billing_refund_waterfalls_refund_detail
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

DELETE
FROM reporting_base_prod.shared.credit_billing_refund_waterfalls_refund_detail
WHERE payment_month IS NULL;

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.credit_billing_refund_waterfalls_order_refund_detail AS
SELECT rd.individual_bu,
       rd.payment_month,
       refund_month,
       refund_month_offset,
       rd.date_refunded,
       COALESCE(rd.date_refunded, rd.refund_month)     AS date_refunded_not_null,
       cash_collected,
       COALESCE(SUM(credit_billing_refund_as_cash), 0) AS credit_billing_refund_as_cash
FROM reporting_base_prod.shared.credit_billing_refund_waterfalls_refund_detail rd
         LEFT JOIN (SELECT individual_bu, payment_month, SUM(cash_collected) AS cash_collected
                    FROM reporting_base_prod.shared.credit_billing_refund_waterfalls_order_detail
                    GROUP BY 1, 2) od ON od.individual_bu = rd.individual_bu AND rd.payment_month = od.payment_month
GROUP BY 1, 2, 3, 4, 5, 6, 7;

ALTER TABLE reporting_base_prod.shared.credit_billing_refund_waterfalls_order_refund_detail
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

--snap shot

/*
 CREATE TABLE credit_billing_refund_waterfalls_refund_detail_snapshot
(
    order_id                       NUMBER,
    refund_id                      NUMBER,
    store_id                       NUMBER,
    individual_bu                  VARCHAR(61),
    date_refunded                  DATE,
    total_refund                   NUMBER(19, 4),
    tax_refund                     NUMBER(19, 4),
    credit_billing_refund_as_cash  NUMBER(32, 10),
    capture_payment_transaction_id NUMBER,
    payment_method                 VARCHAR(25),
    payment_month                  DATE,
    refund_month                   DATE,
    refund_month_offset            NUMBER(10),
    snapshot_timestamp             TIMESTAMPLTZ
);
 */
INSERT INTO reporting_base_prod.shared.credit_billing_refund_waterfalls_refund_detail_snapshot
SELECT order_id,
       refund_id,
       store_id,
       individual_bu,
       date_refunded,
       total_refund,
       tax_refund,
       credit_billing_refund_as_cash,
       capture_payment_transaction_id,
       payment_method,
       payment_month,
       refund_month,
       refund_month_offset,
       CURRENT_TIMESTAMP AS snapshot_timestamp
FROM reporting_base_prod.shared.credit_billing_refund_waterfalls_refund_detail;

DELETE
FROM reporting_base_prod.shared.credit_billing_refund_waterfalls_refund_detail_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -18, getdate());
