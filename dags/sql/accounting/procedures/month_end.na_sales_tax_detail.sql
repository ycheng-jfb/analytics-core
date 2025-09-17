CREATE OR REPLACE TEMPORARY TABLE _store AS
SELECT *
FROM edw_prod.data_model.dim_store st
WHERE store_id IN (26, 52, 55, 46, 41, 79, 121, 146, 151);

CREATE OR REPLACE TEMPORARY TABLE _token_order AS
SELECT DISTINCT store_id, mtt.object_id order_id
FROM reporting_base_prod.shared.dim_credit dc
         JOIN lake_consolidated_view.ultra_merchant.membership_token_transaction mtt
              ON dc.credit_id = mtt.membership_token_id
                  AND membership_token_transaction_type_id = 20 -- redeem
                  AND (mtt.cancelled = 0 OR mtt.cancelled IS NULL)
                  AND mtt.object = 'order'
                  AND dc.source_credit_id_type = 'Token';

CREATE OR REPLACE TEMPORARY TABLE _na_sales_tax_detail AS
-- Online Sales
SELECT UPPER(a.country_code)                                     AS country,
       'SALES'                                                   AS segment,
       st.store_brand || ' ' || st.store_country                 AS business_unit,
       st.store_type,
       CAST(NULL AS VARCHAR(100))                                AS retail_location,
       d.month_date                                              AS activity_month,
       o.store_id,
       o.order_id,
       IFF(t.order_id IS NOT NULL, 'Token', 'Cash/Store Credit') AS credit_type,
       NULL                                                      AS refund_id,
       a.state,
       zcs.county,
       COALESCE(zcs.city, a.city)                                AS city,
       NULL                                                      AS refund_type,
       doc2.order_classification_l2                                 transaction_type,
       o.subtotal - o.discount                                   AS product_dollars, -- collected sales
       CASE WHEN t.order_id IS NOT NULL THEN o.credit ELSE 0 END AS credit_used,
       o.subtotal - o.discount - o.credit                        AS cash_collected,
       o.shipping                                                AS shipping,        -- collected_shipping
       o.tax                                                     AS tax,             -- collected_tax
       (o.subtotal - o.discount) + o.tax + o.shipping            AS total
FROM lake_consolidated_view.ultra_merchant."ORDER" o
         JOIN edw_prod.data_model.fact_order fo ON o.order_id = fo.order_id
         LEFT JOIN reporting_base_prod.shared.dim_credit dc ON dc.credit_order_id = o.order_id
         JOIN edw_prod.data_model.dim_order_sales_channel doc2
              ON doc2.order_sales_channel_key = fo.order_sales_channel_key
                  AND is_border_free_order = 0
         JOIN edw_prod.data_model.dim_date d ON d.full_date = CAST(o.date_shipped AS DATE)
         JOIN edw_prod.data_model.dim_store st ON st.store_id = o.store_id
    AND st.store_type IN ('Online', 'Mobile App')
         JOIN lake_consolidated_view.ultra_merchant.address a
              ON o.shipping_address_id = a.address_id
         LEFT JOIN lake_consolidated_view.ultra_merchant.zip_city_state zcs ON SUBSTRING(a.zip, 1, 5) = zcs.zip
         LEFT JOIN _token_order t ON o.order_id = t.order_id AND o.store_id = t.store_id
WHERE o.store_id IN (SELECT store_id FROM _store)
  AND NOT EXISTS(SELECT 1
                 FROM lake_consolidated_view.ultra_merchant.box ps
                 WHERE ps.order_id = o.order_id)

-------------------------------------------------------------------
-- Online Refunds

UNION ALL

SELECT UPPER(a.country_code)                                        AS country,
       'REFUNDS'                                                    AS segment,
       st.store_brand || ' ' || st.store_country,
       st.store_type,
       CAST(NULL AS VARCHAR(100))                                   AS retail_location,
       d.month_date,
       o.store_id,
       o.order_id,
       IFF(t.order_id IS NOT NULL, 'Token', 'Cash/Store Credit')    AS credit_type,
       r.refund_id                                                  AS refund_id,
       a.state,
       zcs.county,
       COALESCE(zcs.city, a.city)                                   AS city,
       CASE
           WHEN r.payment_method = 'Check_request' THEN 'Check Request'
           WHEN r.payment_method = 'creditcard' THEN 'Credit Card'
           WHEN r.payment_method = 'moneyorder' THEN 'Money Order'
           WHEN r.payment_method = 'store_credit' THEN 'Store Credit'
           ELSE r.payment_method
           END                                                      AS refund_type,
       doc2.order_classification_l2                                    transaction_type,
       (-1) * (r.product_refund)                                    AS product_dollars, -- refunded_product_dollars
       0                                                            AS credit_used,
       0                                                            AS cash_collected,
       (-1) * (r.shipping_refund)                                   AS shipping,        -- refunded_shipping
       (-1) * r.tax_refund                                          AS tax,             -- refunded_tax
       (-1) * (r.product_refund + r.tax_refund + r.shipping_refund) AS total
FROM lake_consolidated_view.ultra_merchant."ORDER" o
         JOIN edw_prod.data_model.fact_order fo ON o.order_id = fo.order_id
         LEFT JOIN reporting_base_prod.shared.dim_credit dc ON dc.credit_order_id = o.order_id
         JOIN edw_prod.data_model.dim_order_sales_channel doc2
              ON doc2.order_sales_channel_key = fo.order_sales_channel_key
                  AND is_border_free_order = 0
         JOIN lake_consolidated_view.ultra_merchant.refund r ON o.order_id = r.order_id
         JOIN edw_prod.data_model.dim_date d ON d.full_date = CAST(r.datetime_refunded AS DATE)
         JOIN edw_prod.data_model.dim_store st ON st.store_id = o.store_id
    AND st.store_type IN ('Online', 'Mobile App')
         JOIN lake_consolidated_view.ultra_merchant.address a ON o.shipping_address_id = a.address_id
         LEFT JOIN lake_consolidated_view.ultra_merchant.zip_city_state zcs ON SUBSTRING(a.zip, 1, 5) = zcs.zip
         LEFT JOIN _token_order t ON o.order_id = t.order_id AND o.store_id = t.store_id
WHERE o.store_id IN (SELECT store_id FROM _store)
  AND o.date_shipped IS NOT NULL
  --AND r.payment_method <> 'store_credit'
  AND r.datetime_refunded IS NOT NULL
  AND NOT EXISTS(SELECT 1
                 FROM lake_consolidated_view.ultra_merchant.box ps
                 WHERE ps.order_id = o.order_id)

-----------------------------------------------------------------
-- RETAIL SALES

UNION ALL

SELECT UPPER(a.country_code)                                     AS country,
       'SALES',
       st.store_brand || ' ' || st.store_country,
       st.store_type,
       st.store_full_name,
       d.month_date,
       o.store_id,
       o.order_id,
       IFF(t.order_id IS NOT NULL, 'Token', 'Cash/Store Credit') AS credit_type,
       NULL                                                      AS refund_id,
       st.store_retail_state,
       zcs.county,
       st.store_retail_city,
       NULL,
       doc2.order_classification_l2                                 transaction_type,
       o.subtotal - o.discount, -- collected sales
       CASE WHEN t.order_id IS NOT NULL THEN o.credit ELSE 0 END AS credit_used,
       o.subtotal - o.discount - o.credit                        AS cash_collected,
       o.shipping,              -- collected_shipping
       o.tax,                   -- collected_tax
       (o.subtotal - o.discount) + o.tax + o.shipping
FROM lake_consolidated_view.ultra_merchant."ORDER" o
         JOIN edw_prod.data_model.fact_order fo ON o.order_id = fo.order_id
         LEFT JOIN reporting_base_prod.shared.dim_credit dc ON dc.credit_order_id = o.order_id
         JOIN edw_prod.data_model.dim_order_sales_channel doc2
              ON doc2.order_sales_channel_key = fo.order_sales_channel_key
                  AND is_border_free_order = 0
         JOIN edw_prod.data_model.dim_date d ON d.full_date = CAST(o.date_placed AS DATE)
         JOIN edw_prod.data_model.dim_store st ON st.store_id = o.store_id
    AND st.store_type = 'Retail'
         JOIN lake_consolidated_view.ultra_merchant.address a ON o.shipping_address_id = a.address_id
         LEFT JOIN lake_consolidated_view.ultra_merchant.zip_city_state zcs ON SUBSTRING(a.zip, 1, 5) = zcs.zip
         LEFT JOIN _token_order t ON LEFT(o.order_id, LEN(o.order_id) - 2) = t.order_id AND o.store_id = t.store_id
WHERE o.payment_statuscode >= 2600 -- make sure it's successful

-----------------------------------------------------------------
-- RETAIL REFUNDS

UNION ALL

SELECT UPPER(a.country_code)                                     AS country,
       'REFUNDS',
       st.store_brand || ' ' || st.store_country,
       st.store_type,
       st.store_full_name,
       d.month_date,
       o.store_id,
       o.order_id,
       IFF(t.order_id IS NOT NULL, 'Token', 'Cash/Store Credit') AS credit_type,
       r.refund_id                                               AS refund_id,
       st.store_retail_state,
       zcs.county,
       st.store_retail_city,
       CASE
           WHEN r.payment_method = 'Check_request' THEN 'Check Request'
           WHEN r.payment_method = 'creditcard' THEN 'Credit Card'
           WHEN r.payment_method = 'moneyorder' THEN 'Money Order'
           WHEN r.payment_method = 'store_credit' THEN 'Store Credit'
           ELSE r.payment_method
           END,
       doc2.order_classification_l2                                 transaction_type,
       (-1) * r.product_refund,  -- refunded_product_dollars
       0                                                         AS credit_used,
       0                                                         AS cash_collected,
       (-1) * r.shipping_refund, -- refunded_shipping
       (-1) * r.tax_refund,      -- refunded_tax
       (-1) * r.product_refund + r.tax_refund + r.shipping_refund
FROM lake_consolidated_view.ultra_merchant."ORDER" o
         JOIN edw_prod.data_model.fact_order fo ON o.order_id = fo.order_id
         LEFT JOIN reporting_base_prod.shared.dim_credit dc ON dc.credit_order_id = fo.order_id
         JOIN lake_consolidated_view.ultra_merchant.refund r ON o.order_id = r.order_id
         JOIN edw_prod.data_model.dim_date d ON d.full_date = CAST(r.datetime_refunded AS DATE)
         JOIN edw_prod.data_model.dim_store st ON st.store_id = o.store_id
    AND st.store_type = 'Retail'
         JOIN edw_prod.data_model.dim_order_sales_channel doc2
              ON doc2.order_sales_channel_key = fo.order_sales_channel_key
                  AND is_border_free_order = 0
         JOIN lake_consolidated_view.ultra_merchant.address a ON o.shipping_address_id = a.address_id
         LEFT JOIN lake_consolidated_view.ultra_merchant.zip_city_state zcs ON SUBSTRING(a.zip, 1, 5) = zcs.zip
         LEFT JOIN _token_order t ON o.order_id = t.order_id AND o.store_id = t.store_id
WHERE r.datetime_refunded IS NOT NULL;
--AND r.payment_method <> 'store_credit'

CREATE OR REPLACE TRANSIENT TABLE month_end.na_sales_tax_detail AS
SELECT month_end.udf_correct_state_country(state, country, 'country') AS country,
       segment,
       business_unit,
       store_type,
       retail_location,
       activity_month,
       store_id,
       order_id,
       credit_type,
       refund_id,
       month_end.udf_correct_state_country(state, country, 'state')   AS state,
       county,
       city,
       refund_type,
       transaction_type,
       product_dollars,
       credit_used,
       cash_collected,
       shipping,
       tax,
       total
FROM _na_sales_tax_detail;

ALTER TABLE month_end.na_sales_tax_detail
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

UPDATE month_end.na_sales_tax_detail
SET state  = 'IN',
    county = 'Marion County',
    city   = 'Indianapolis'
WHERE store_id = 117;

UPDATE month_end.na_sales_tax_detail
SET state  = 'KY',
    county = 'Fayette County',
    city   = 'Lexington'
WHERE store_id = 119;

UPDATE month_end.na_sales_tax_detail
SET state  = 'TX',
    county = 'Collin County',
    city   = 'Plano'
WHERE store_id = 118;

UPDATE month_end.na_sales_tax_detail
SET state  = 'IL',
    county = 'Cook County',
    city   = 'Schaumburg'
WHERE store_id = 120;

UPDATE month_end.na_sales_tax_detail
SET state  = 'FL',
    county = 'Palm Beach County',
    city   = 'Boca Raton'
WHERE store_id = 123;

UPDATE month_end.na_sales_tax_detail
SET state  = 'NJ',
    county = 'Bergen County',
    city   = 'Paramus'
WHERE store_id = 124;

-- to include only data from 2015 and on
DELETE
FROM month_end.na_sales_tax_detail
WHERE activity_month < '2015-01-01';

-- snapshot
INSERT INTO month_end.na_sales_tax_detail_snapshot
SELECT country,
       segment,
       business_unit,
       store_type,
       retail_location,
       activity_month,
       store_id,
       order_id,
       credit_type,
       refund_id,
       state,
       county,
       city,
       refund_type,
       transaction_type,
       product_dollars,
       credit_used,
       cash_collected,
       shipping,
       tax,
       total,
       CURRENT_TIMESTAMP AS snapshot_timestamp
FROM month_end.na_sales_tax_detail;

DELETE
FROM month_end.na_sales_tax_detail_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -12, getdate());
