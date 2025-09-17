CREATE OR REPLACE TEMPORARY TABLE _old AS
SELECT order_line_id, SUM(amount) amount
FROM lake_consolidated_view.ultra_merchant.order_line_discount
GROUP BY order_line_id;


CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.nj_mn_accessories_sales_tax_detail AS
-- Online Sales
SELECT reporting_base_prod.shared.udf_correct_state_country(a.state, a.country_code, 'country') AS country,
       'SALES'                                                                                  AS segment,
       IFF(o.store_id = 116, 'PS by JustFab US', st.store_brand || ' ' || st.store_country)     AS business_unit,
       st.store_type,
       CAST(NULL AS VARCHAR(100))                                                               AS retail_location,
       d.month_date                                                                             AS activity_month,
       o.store_id,
       o.order_id,
       ol.product_id,
       NULL                                                                                     AS refund_id,
       reporting_base_prod.shared.udf_correct_state_country(a.state, a.country_code, 'state')   AS state,
       zcs.county,
       COALESCE(zcs.city, a.city)                                                               AS city,
       NULL                                                                                     AS refund_type,
       ol.price_offered_local_amount                                                            AS accessories_subtotal,
       COALESCE(old.amount, uol.unit_discount)                                                  AS accessories_discount,
       o.tax                                                                                    AS accessories_tax,
       o.shipping                                                                               AS shipping_amount,
       ol.price_offered_local_amount -
       COALESCE(old.amount, uol.unit_discount)                                                  AS accessories_product_dollars
FROM lake_consolidated_view.ultra_merchant."ORDER" o
         JOIN edw_prod.data_model.fact_order_line ol ON ol.order_id = o.order_id
         JOIN lake_consolidated_view.ultra_merchant.order_line uol ON ol.order_line_id = uol.order_line_id
         JOIN edw_prod.data_model.dim_product p ON p.product_id = ol.product_id
    AND (p.category ILIKE '%Accessor%' OR p.department ILIKE '%Accessor%' OR p.department = 'Jewelry' OR
         p.department = 'Bags'
        OR p.department = 'Handbags' OR p.category = 'Hair' OR p.department = 'Bag Upsell Sales' OR
         p.department = 'Fuzzies'
        OR p.category = 'Sock' OR p.department = 'Nail Polish' OR p.department = 'Rain Coat' OR p.category = 'Clutches')
         JOIN edw_prod.data_model.dim_date d ON d.full_date = CAST(o.date_shipped AS DATE)
         JOIN edw_prod.data_model.dim_store st ON st.store_id = o.store_id
    AND st.store_type IN ('Online', 'Mobile App')
    AND st.store_country = 'US'
         JOIN lake_consolidated_view.ultra_merchant.address a ON o.shipping_address_id = a.address_id
    AND a.state IN ('MN', 'NJ', 'RI', 'PA', 'MA', 'NY', 'VT')
         LEFT JOIN lake_consolidated_view.ultra_merchant.zip_city_state zcs ON SUBSTRING(a.zip, 1, 5) = zcs.zip
         LEFT JOIN _old old ON old.order_line_id = ol.order_line_id

-------------------------------------------------------------------
-- Online Refunds

UNION ALL

SELECT reporting_base_prod.shared.udf_correct_state_country(a.state, a.country_code, 'country') AS country,
       'REFUNDS'                                                                                AS segment,
       IFF(o.store_id = 116, 'PS by JustFab US', st.store_brand || ' ' || st.store_country)     AS business_unit,
       st.store_type,
       CAST(NULL AS VARCHAR(100))                                                               AS retail_location,
       d.month_date                                                                             AS activity_month,
       o.store_id,
       o.order_id,
       ol.product_id,
       r.refund_id                                                                              AS refund_id,
       reporting_base_prod.shared.udf_correct_state_country(a.state, a.country_code, 'state')   AS state,
       zcs.county,
       COALESCE(zcs.city, a.city)                                                               AS city,
       CASE
           WHEN r.payment_method = 'Check_request' THEN 'Check Request'
           WHEN r.payment_method = 'creditcard' THEN 'Credit Card'
           WHEN r.payment_method = 'moneyorder' THEN 'Money Order'
           WHEN r.payment_method = 'store_credit' THEN 'Store Credit'
           ELSE r.payment_method
           END                                                                                  AS refund_type,
       -ol.price_offered_local_amount                                                           AS accessories_subtotal,
       COALESCE(-old.amount, -uol.unit_discount)                                                AS accessories_discount,
       -ol.tax_local_amount                                                                     AS accessories_tax,
       -ol.shipping_revenue_local_amount                                                        AS shipping_amount,
       -1 * (ol.price_offered_local_amount -
             COALESCE(old.amount, uol.unit_discount))                                           AS accessories_product_dollars
FROM lake_consolidated_view.ultra_merchant."ORDER" o
         JOIN edw_prod.data_model.fact_order_line ol ON ol.order_id = o.order_id
         JOIN edw_prod.data_model.dim_product p ON p.product_id = ol.product_id
    AND (p.category ILIKE '%Accessor%' OR p.department ILIKE '%Accessor%' OR p.department = 'Jewelry' OR
         p.department = 'Bags'
        OR p.department = 'Handbags' OR p.category = 'Hair' OR p.department = 'Bag Upsell Sales' OR
         p.department = 'Fuzzies'
        OR p.category = 'Sock' OR p.department = 'Nail Polish' OR p.department = 'Rain Coat' OR p.category = 'Clutches')
         JOIN lake_consolidated_view.ultra_merchant.refund r ON o.order_id = r.order_id
         JOIN lake_consolidated_view.ultra_merchant.order_line uol ON ol.order_line_id = uol.order_line_id
         JOIN lake_consolidated_view.ultra_merchant.refund_line rl ON r.refund_id = rl.refund_id
    AND rl.order_line_id = uol.order_line_id
         JOIN edw_prod.data_model.dim_date d ON d.full_date = CAST(r.datetime_refunded AS DATE)
         JOIN edw_prod.data_model.dim_store st ON st.store_id = o.store_id
    AND st.store_type IN ('Online', 'Mobile App')
    AND st.store_country = 'US'
         JOIN lake_consolidated_view.ultra_merchant.address a ON o.shipping_address_id = a.address_id
    AND a.state IN ('MN', 'NJ', 'RI', 'PA', 'MA', 'NY', 'VT')
         LEFT JOIN lake_consolidated_view.ultra_merchant.zip_city_state zcs ON SUBSTRING(a.zip, 1, 5) = zcs.zip
         LEFT JOIN _old old ON old.order_line_id = ol.order_line_id
WHERE o.date_shipped IS NOT NULL
  AND r.payment_method <> 'store_credit'
  AND r.datetime_refunded IS NOT NULL
  AND NOT EXISTS(SELECT 1
                 FROM lake_consolidated_view.ultra_merchant.box ps
                 WHERE ps.order_id = o.order_id)

-----------------------------------------------------------------
-- RETAIL SALES

UNION ALL

SELECT reporting_base_prod.shared.udf_correct_state_country(a.state, a.country_code, 'country') AS country,
       'SALES'                                                                                  AS segment,
       IFF(o.store_id = 116, 'PS by JustFab US', st.store_brand || ' ' || st.store_country)     AS business_unit,
       st.store_type,
       CAST(st.store_full_name AS VARCHAR(100))                                                 AS retail_location,
       d.month_date                                                                             AS activity_month,
       o.store_id,
       o.order_id,
       ol.product_id,
       NULL                                                                                     AS refund_id,
       reporting_base_prod.shared.udf_correct_state_country(a.state, a.country_code, 'state')   AS state,
       zcs.county,
       COALESCE(zcs.city, a.city)                                                               AS city,
       NULL                                                                                     AS refund_type,
       ol.price_offered_local_amount                                                            AS accessories_subtotal,
       COALESCE(old.amount, uol.unit_discount)                                                  AS accessories_discount,
       ol.tax_local_amount                                                                      AS accessories_tax,
       ol.shipping_revenue_local_amount                                                         AS shipping_amount,
       ol.price_offered_local_amount -
       COALESCE(old.amount, uol.unit_discount)                                                  AS accessories_product_dollars
FROM lake_consolidated_view.ultra_merchant."ORDER" o
         JOIN edw_prod.data_model.fact_order_line ol ON ol.order_id = o.order_id
         JOIN lake_consolidated_view.ultra_merchant.order_line uol ON ol.order_line_id = uol.order_line_id
         JOIN edw_prod.data_model.dim_product p ON p.product_id = ol.product_id
    AND (p.category ILIKE '%Accessor%' OR p.department ILIKE '%Accessor%' OR p.department = 'Jewelry' OR
         p.department = 'Bags'
        OR p.department = 'Handbags' OR p.category = 'Hair' OR p.department = 'Bag Upsell Sales' OR
         p.department = 'Fuzzies'
        OR p.category = 'Sock' OR p.department = 'Nail Polish' OR p.department = 'Rain Coat' OR p.category = 'Clutches')
         JOIN edw_prod.data_model.dim_date d ON d.full_date = CAST(o.date_placed AS DATE)
         JOIN edw_prod.data_model.dim_store st ON st.store_id = o.store_id
    AND st.store_type = 'Retail'
    AND st.store_country = 'US'
         JOIN lake_consolidated_view.ultra_merchant.address a ON o.shipping_address_id = a.address_id
    AND a.state IN ('MN', 'NJ', 'RI', 'PA', 'MA', 'NY', 'VT')
         LEFT JOIN lake_consolidated_view.ultra_merchant.zip_city_state zcs ON SUBSTRING(a.zip, 1, 5) = zcs.zip
         LEFT JOIN _old old ON old.order_line_id = ol.order_line_id
WHERE o.payment_statuscode >= 2600 -- make sure it's successful

-----------------------------------------------------------------
-- RETAIL REFUNDS

UNION ALL

SELECT reporting_base_prod.shared.udf_correct_state_country(a.state, a.country_code, 'country') AS country,
       'REFUNDS'                                                                                AS segment,
       IFF(o.store_id = 116, 'PS by JustFab US', st.store_brand || ' ' || st.store_country)     AS business_unit,
       st.store_type,
       CAST(st.store_full_name AS VARCHAR(100))                                                 AS retail_location,
       d.month_date                                                                             AS activity_month,
       o.store_id,
       o.order_id,
       ol.product_id,
       r.refund_id                                                                              AS refund_id,
       reporting_base_prod.shared.udf_correct_state_country(a.state, a.country_code, 'state')   AS state,
       zcs.county,
       COALESCE(zcs.city, a.city)                                                               AS city,
       CASE
           WHEN r.payment_method = 'Check_request' THEN 'Check Request'
           WHEN r.payment_method = 'creditcard' THEN 'Credit Card'
           WHEN r.payment_method = 'moneyorder' THEN 'Money Order'
           WHEN r.payment_method = 'store_credit' THEN 'Store Credit'
           ELSE r.payment_method
           END                                                                                  AS refund_type,
       -ol.price_offered_local_amount                                                           AS accessories_subtotal,
       COALESCE(-old.amount, -uol.unit_discount)                                                AS accessories_discount,
       -ol.tax_local_amount                                                                     AS accessories_tax,
       -ol.shipping_revenue_local_amount                                                        AS shipping_amount,
       -1 * (ol.price_offered_local_amount -
             COALESCE(old.amount, uol.unit_discount))                                           AS accessories_product_dollars
FROM lake_consolidated_view.ultra_merchant."ORDER" o
         JOIN edw_prod.data_model.fact_order_line ol ON ol.order_id = o.order_id
         JOIN edw_prod.data_model.dim_product p ON p.product_id = ol.product_id
    AND (p.category ILIKE '%Accessor%' OR p.department ILIKE '%Accessor%' OR p.department = 'Jewelry' OR
         p.department = 'Bags'
        OR p.department = 'Handbags' OR p.category = 'Hair' OR p.department = 'Bag Upsell Sales' OR
         p.department = 'Fuzzies'
        OR p.category = 'Sock' OR p.department = 'Nail Polish' OR p.department = 'Rain Coat' OR p.category = 'Clutches')
         JOIN lake_consolidated_view.ultra_merchant.refund r ON o.order_id = r.order_id
         JOIN lake_consolidated_view.ultra_merchant.order_line uol ON ol.order_line_id = uol.order_line_id
         JOIN lake_consolidated_view.ultra_merchant.refund_line rl ON r.refund_id = rl.refund_id
    AND rl.order_line_id = uol.order_line_id
         JOIN edw_prod.data_model.dim_date d ON d.full_date = CAST(r.datetime_refunded AS DATE)
         JOIN edw_prod.data_model.dim_store st ON st.store_id = o.store_id
    AND st.store_type = 'Retail'
    AND st.store_country = 'US'
         JOIN lake_consolidated_view.ultra_merchant.address a ON o.shipping_address_id = a.address_id
    AND a.state IN ('MN', 'NJ', 'RI', 'PA', 'MA', 'NY', 'VT')
         LEFT JOIN lake_consolidated_view.ultra_merchant.zip_city_state zcs ON SUBSTRING(a.zip, 1, 5) = zcs.zip
         LEFT JOIN _old old ON old.order_line_id = ol.order_line_id
WHERE r.datetime_refunded IS NOT NULL
  AND r.payment_method <> 'store_credit';

ALTER TABLE reporting_base_prod.shared.nj_mn_accessories_sales_tax_detail
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

-- snapshot
INSERT INTO reporting_base_prod.shared.nj_mn_accessories_sales_tax_detail_snapshot
SELECT country,
       segment,
       business_unit,
       store_type,
       retail_location,
       activity_month,
       store_id,
       order_id,
       product_id,
       refund_id,
       state,
       county,
       city,
       refund_type,
       accessories_subtotal,
       accessories_discount,
       accessories_tax,
       shipping_amount,
       accessories_product_dollars,
       CURRENT_TIMESTAMP AS snapshot_timestamp
FROM reporting_base_prod.shared.nj_mn_accessories_sales_tax_detail;

DELETE
FROM reporting_base_prod.shared.nj_mn_accessories_sales_tax_detail_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -12, getdate());
