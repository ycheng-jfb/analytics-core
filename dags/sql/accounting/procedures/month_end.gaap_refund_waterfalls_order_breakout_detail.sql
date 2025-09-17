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
  AND store_full_name NOT IN ('Not Applicable', 'Unknown');


CREATE OR REPLACE TEMPORARY TABLE _order_classifications_re AS
SELECT oc.order_id
FROM lake_consolidated_view.ultra_merchant.order_classification oc
WHERE order_type_id IN (6, 11);


CREATE OR REPLACE TEMPORARY TABLE _order_classifications_act AS
SELECT oc.order_id
FROM lake_consolidated_view.ultra_merchant.order_classification oc
WHERE order_type_id = 23 -- Activating Order
UNION
SELECT -- prior to 2015 we weren't tracking activating explicitly in ecom
       order_id
FROM edw_prod.data_model.fact_order fo
         JOIN edw_prod.data_model.dim_order_membership_classification domc
              ON fo.order_membership_classification_key = domc.order_membership_classification_key
WHERE domc.membership_order_type_l1 = 'Activating VIP'
  AND fo.order_local_datetime < '2015-01-01';

CREATE OR REPLACE TEMPORARY TABLE _gcorder AS
SELECT DISTINCT ol.order_id
FROM lake_consolidated_view.ultra_merchant.order_line ol
WHERE product_type_id = 5;


CREATE OR REPLACE TEMPORARY TABLE _shipped_order_metrics AS
SELECT DISTINCT CASE
                    WHEN o.store_id = 54 THEN 'JustFab Retail'
                    WHEN o.store_id = 116 THEN st.store_name || ' ' || st.store_country
                    WHEN st.store_type = 'Retail' AND st.store_brand = 'Fabletics' THEN 'Fabletics ' || st.store_country || ' Retail'
                    WHEN st.store_type = 'Retail' AND st.store_brand = 'Savage X' THEN 'Savage X Retail'
                    ELSE st.store_brand || ' ' || st.store_country
                    END                                                           AS individual_bu,
                CASE
                    WHEN st.store_id = 54 THEN 26
                    WHEN st.store_type = 'Retail' THEN 100
                    ELSE st.store_id
                    END                                                           AS store_id,
                m.store_id                                                        AS membership_store_id,
                CASE
                    WHEN o.store_id = 54 THEN 'JustFab Retail'
                    WHEN o.store_id = 116 THEN mst.store_name || ' ' || mst.store_country
                    WHEN st.store_type = 'Retail' AND mst.store_brand = 'Fabletics' THEN 'Fabletics ' || st.store_country || ' Retail'
                    WHEN st.store_type = 'Retail' AND mst.store_brand = 'Yitty' THEN 'Yitty Retail'
                    WHEN st.store_type = 'Retail' AND mst.store_brand = 'Savage X' THEN 'Savage X Retail'
                    ELSE mst.store_brand || ' ' || mst.store_country
                    END                                                           AS membership_store,
                CASE
                    WHEN ocr.order_id IS NOT NULL THEN 'Reship/Exchange'
                    WHEN oca.order_id IS NOT NULL THEN 'Shipped Product Order - Activating'
                    ELSE 'Shipped Product Order - Repeat'
                    END                                                           AS order_type,
                d.month_date                                                      AS date_shipped,
                o.order_id,
                1 + zeroifnull(v.rate)                                               rate,
                (o.subtotal - o.discount) / (1 + zeroifnull(v.rate))              AS gross_rev_after_discount,
                (o.subtotal - o.discount + o.shipping) / (1 + zeroifnull(v.rate)) AS total_cash_and_cash_credit,
                o.subtotal / (1 + zeroifnull(v.rate))                                subtotal,
                -1 * (o.discount / (1 + zeroifnull(v.rate)))                         discount,
                o.tax / (1 + zeroifnull(v.rate))                                     tax,
                o.shipping / (1 + zeroifnull(v.rate))                                shipping,
                o.credit / (1 + zeroifnull(v.rate))                                  credit,
                o.date_shipped                                                       actual_ship_date
FROM lake_consolidated_view.ultra_merchant."ORDER" o
         JOIN _store st ON st.store_id = o.store_id
         LEFT JOIN (SELECT DISTINCT customer_id,
                                    store_id,
                                    effective_start_datetime,
                                    effective_end_datetime
                    FROM lake_consolidated_view.ultra_merchant_history.membership) m
                   ON m.customer_id = o.customer_id
                       AND
                      dateadd(SECOND, 900,
                                      iff(st.store_type = 'Retail', o.datetime_added, o.datetime_shipped))
                          BETWEEN m.effective_start_datetime AND m.effective_end_datetime
         LEFT JOIN _store mst ON mst.store_id = m.store_id
         JOIN edw_prod.data_model.dim_date d
              ON d.full_date = CAST(iff(st.store_type = 'Retail', o.date_placed, o.date_shipped) AS DATE)
         LEFT JOIN lake_consolidated_view.ultra_merchant.address a ON o.shipping_address_id = a.address_id
         LEFT JOIN edw_prod.reference.vat_rate_history v
                   ON v.country_code = replace(replace(a.country_code, 'UK', 'GB'), 'EU', 'NL')
                       AND d.full_date BETWEEN v.start_date AND v.expires_date-- Added this join for the VAT rates
         LEFT JOIN _order_classifications_re ocr ON ocr.order_id = o.order_id
         LEFT JOIN _order_classifications_act oca ON oca.order_id = o.order_id
         LEFT JOIN _gcorder gco ON gco.order_id = o.order_id
WHERE d.full_date < $process_to_date
  AND o.datetime_local_transaction IS NOT NULL
  AND gco.order_id IS NULL;
-- exclude items that did not ship at retail, like retail cancelled orders
-- exclude orders that has gift certificates

-- changing Yitty orders to
UPDATE _shipped_order_metrics
SET membership_store    = individual_bu,
    membership_store_id = store_id
WHERE membership_store IS NULL;

CREATE OR REPLACE TRANSIENT TABLE month_end.gaap_refund_waterfalls_order_breakout_detail AS
SELECT *
FROM _shipped_order_metrics;

ALTER TABLE month_end.gaap_refund_waterfalls_order_breakout_detail
    SET DATA_RETENTION_TIME_IN_DAYS = 0;
