SET last_month = DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_DATE));

CREATE OR REPLACE TRANSIENT TABLE month_end.captured_transactions AS
-- EU part of the report
SELECT 'EU'                                                            AS region,
       o.order_id,
       ds.store_name                                                   business_unit,
       ds.store_retail_location                                        AS retail_location,
       ds.store_country,
       a.country_code                                                  ship_to_country,
       iff(oc.order_id IS NOT NULL, 'Credit Billing', 'Product Order') order_type,
       'psp'                                                           transaction_type,
       ptp.gateway_name                                                gateway_name,
       ifnull(psp.type, '')                                            payment_type,
       sc.label                                                        statuscode,
       convert_timezone('America/Los_Angeles', tz.time_zone,
                        ptp.datetime_added)::date                      full_date,
       ptp.amount                                                      amount,
       ptp.amount * (v.rate / (1 + v.rate))                            vat,
       ptp.amount / (1 + zeroifnull(v.rate))                           amount_less_vat,
       o.tax                                                           tax,
       ptp.amount - o.tax                                              amount_less_tax,
       date_trunc(MONTH, convert_timezone('America/Los_Angeles', tz.time_zone, ptp.datetime_added)) ::date report_month
FROM lake_consolidated_view.ultra_merchant.payment_transaction_psp ptp
         LEFT JOIN lake_consolidated_view.ultra_merchant.psp psp ON psp.psp_id = ptp.psp_id
         JOIN lake_consolidated_view.ultra_merchant."ORDER" o ON ptp.order_id = o.order_id
         JOIN edw_prod.data_model.dim_store ds ON ds.store_id = o.store_id
    AND ds.store_region <> 'NA'
         JOIN edw_prod.reference.store_timezone tz ON tz.store_id = ds.store_id
         JOIN lake_consolidated_view.ultra_merchant.statuscode sc ON ptp.statuscode = sc.statuscode
         LEFT JOIN edw_prod.data_model.dim_address a ON o.shipping_address_id = a.address_id
         LEFT JOIN edw_prod.reference.vat_rate_history v
                   ON v.country_code = replace(replace(a.country_code, 'UK', 'GB'), 'EU', 'NL')
                       AND ptp.datetime_added BETWEEN v.start_date AND v.expires_date
         LEFT JOIN lake_consolidated_view.ultra_merchant.order_classification oc ON oc.order_id = o.order_id
    AND oc.order_type_id = 10
WHERE ptp.transaction_type = 'PRIOR_AUTH_CAPTURE'
  AND ptp.statuscode IN (4001, 4040)
  AND report_month = $last_month

UNION ALL

---- NA part of the report
SELECT ds.store_region                                                                       AS region,
       o.order_id,
       iff((oc.order_id IS NOT NULL OR o.store_id = 116), 'PS by JustFab US', ds.store_name) AS business_unit,
       ds.store_retail_location                                                              AS retail_location,
       ds.store_country,
       a.country_code                                                                           ship_to_country,
       iff(oc.order_id IS NOT NULL, 'Credit Billing', 'Product Order')                          order_type,
       'creditcard'                                                                          AS transaction_type,
       NULL                                                                                  AS gateway_name,
       LOWER(cc.card_type)                                                                   AS payment_type,
       'Success'                                                                             AS statuscode,
       c.datetime_added::date                                                                AS full_date,
       c.amount,
       0                                                                                        vat,
       c.amount                                                                                 amount_less_vat,
       o.tax,
       c.amount - o.tax                                                                         amount_less_tax,
       date_trunc(MONTH, c.datetime_added) ::date                                               report_month
FROM lake_consolidated_view.ultra_merchant.payment_transaction_creditcard c
         LEFT JOIN lake_consolidated_view.ultra_merchant.creditcard cc ON cc.creditcard_id = c.creditcard_id
         JOIN lake_consolidated_view.ultra_merchant."ORDER" o ON o.order_id = c.order_id
         LEFT JOIN edw_prod.data_model.dim_address a ON o.shipping_address_id = a.address_id
         LEFT JOIN lake_consolidated_view.ultra_merchant.order_classification oc ON oc.order_id = o.order_id
    AND oc.order_type_id = 25 -- Box Subscription
         JOIN edw_prod.data_model.dim_store ds ON ds.store_id = o.store_id
    AND ds.store_region = 'NA'
WHERE c.transaction_type IN ('PRIOR_AUTH_CAPTURE', 'SALE_REDIRECT')
  AND c.statuscode = 4001 -- Success
  AND c.datetime_added::DATE >= '2018-01-01'
  AND report_month = $last_month

UNION ALL

SELECT ds.store_region                                                                       AS region,
       o.order_id,
       iff((oc.order_id IS NOT NULL OR o.store_id = 116), 'PS by JustFab US', ds.store_name) AS business_unit,
       ds.store_retail_location                                                              AS retail_location,
       ds.store_country,
       a.country_code                                                                           ship_to_country,
       iff(oc.order_id IS NOT NULL, 'Credit Billing', 'Product Order')                          order_type,
       'cash'                                                                                AS transaction_type,
       NULL                                                                                  AS gateway_name,
       'cash'                                                                                AS payment_type,
       'Success'                                                                             AS statuscode,
       c.datetime_added::date                                                                AS full_date,
       c.amount,
       0                                                                                        vat,
       c.amount                                                                                 amount_less_vat,
       o.tax,
       c.amount - o.tax                                                                         amount_less_tax,
       date_trunc(MONTH, c.datetime_added) ::date                                               report_month
FROM lake_consolidated_view.ultra_merchant.payment_transaction_cash c
         JOIN lake_consolidated_view.ultra_merchant."ORDER" o ON o.order_id = c.order_id
         LEFT JOIN edw_prod.data_model.dim_address a ON o.shipping_address_id = a.address_id
         LEFT JOIN lake_consolidated_view.ultra_merchant.order_classification oc ON oc.order_id = o.order_id
    AND oc.order_type_id = 25
         JOIN edw_prod.data_model.dim_store ds ON ds.store_id = o.store_id
    AND ds.store_region = 'NA'
WHERE c.transaction_type = 'SALE'
  AND c.statuscode = 4001 -- Success
  AND c.datetime_added::DATE >= '2018-01-01'
  AND report_month = $last_month

UNION ALL

SELECT ds.store_region                                                                       AS region,
       o.order_id,
       iff((oc.order_id IS NOT NULL OR o.store_id = 116), 'PS by JustFab US', ds.store_name) AS business_unit,
       ds.store_retail_location                                                              AS retail_location,
       ds.store_country,
       a.country_code                                                                           ship_to_country,
       iff(oc.order_id IS NOT NULL, 'Credit Billing', 'Product Order')                          order_type,
       'paypal'                                                                              AS transaction_type,
       NULL                                                                                  AS gateway_name,
       'paypal'                                                                              AS payment_type,
       'Success'                                                                             AS statuscode,
       c.datetime_added::date                                                                AS full_date,
       c.amount,
       0                                                                                        vat,
       c.amount                                                                                 amount_less_vat,
       o.tax,
       c.amount - o.tax                                                                         amount_less_tax,
       date_trunc(MONTH, c.datetime_added) ::date                                               report_month
FROM lake_consolidated_view.ultra_merchant.payment_transaction_psp c
         JOIN lake_consolidated_view.ultra_merchant."ORDER" o ON o.order_id = c.order_id
         LEFT JOIN edw_prod.data_model.dim_address a ON o.shipping_address_id = a.address_id
         LEFT JOIN lake_consolidated_view.ultra_merchant.order_classification oc ON oc.order_id = o.order_id
    AND oc.order_type_id = 25
         JOIN edw_prod.data_model.dim_store ds ON ds.store_id = o.store_id
    AND ds.store_region = 'NA'
WHERE c.transaction_type = 'PRIOR_AUTH_CAPTURE'
  AND c.statuscode = 4001 -- Success
  AND c.datetime_added::DATE >= '2018-01-01'
  AND report_month = $last_month;

ALTER TABLE month_end.captured_transactions SET DATA_RETENTION_TIME_IN_DAYS = 0;

CREATE TRANSIENT TABLE IF NOT EXISTS month_end.captured_transactions_snapshot (
	region VARCHAR(32),
	order_id NUMBER(38,0),
	business_unit VARCHAR(50),
	retail_location VARCHAR(50),
	store_country VARCHAR(50),
	ship_to_country VARCHAR(50),
	order_type VARCHAR(14),
	transaction_type VARCHAR(10),
	gateway_name VARCHAR(25),
	payment_type VARCHAR(50),
	statuscode VARCHAR(50),
	full_date DATE,
	amount NUMBER(19,4),
	vat NUMBER(38,12),
	amount_less_vat NUMBER(31,10),
	tax NUMBER(19,4),
	amount_less_tax NUMBER(20,4),
	report_month DATE,
	snapshot_timestamp TIMESTAMP_LTZ(9)
);

INSERT INTO month_end.captured_transactions_snapshot
SELECT region,
       order_id,
       business_unit,
       retail_location,
       store_country,
       ship_to_country,
       order_type,
       transaction_type,
       gateway_name,
       payment_type,
       statuscode,
       full_date,
       amount,
       vat,
       amount_less_vat,
       tax,
       amount_less_tax,
       report_month,
       CURRENT_TIMESTAMP AS snapshot_timestamp
FROM month_end.captured_transactions;
