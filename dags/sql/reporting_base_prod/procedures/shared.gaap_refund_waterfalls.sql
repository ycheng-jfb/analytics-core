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
                    WHEN st.store_type = 'Retail' AND st.store_brand = 'Fabletics'
                        THEN 'Fabletics ' || st.store_country || ' Retail'
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
                    WHEN st.store_type = 'Retail' AND mst.store_brand = 'Fabletics'
                        THEN 'Fabletics ' || st.store_country || ' Retail'
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
                1 + ZEROIFNULL(v.rate)                                               rate,
                (o.subtotal - o.discount) / (1 + ZEROIFNULL(v.rate))              AS gross_rev_after_discount,
                (o.subtotal - o.discount + o.shipping) / (1 + ZEROIFNULL(v.rate)) AS total_cash_and_cash_credit,
                o.subtotal / (1 + ZEROIFNULL(v.rate))                                subtotal,
                -1 * (o.discount / (1 + ZEROIFNULL(v.rate)))                         discount,
                o.tax / (1 + ZEROIFNULL(v.rate))                                     tax,
                o.shipping / (1 + ZEROIFNULL(v.rate))                                shipping,
                o.credit / (1 + ZEROIFNULL(v.rate))                                  credit,
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
                      DATEADD(SECOND, 900,
                              IFF(st.store_type = 'Retail', o.datetime_added, o.datetime_shipped))
                          BETWEEN m.effective_start_datetime AND m.effective_end_datetime
         LEFT JOIN _store mst ON mst.store_id = m.store_id
         JOIN edw_prod.data_model.dim_date d
              ON d.full_date = CAST(IFF(st.store_type = 'Retail', o.date_placed, o.date_shipped) AS DATE)
         LEFT JOIN lake_consolidated_view.ultra_merchant.address a ON o.shipping_address_id = a.address_id
         LEFT JOIN edw_prod.reference.vat_rate_history v
                   ON v.country_code = REPLACE(REPLACE(a.country_code, 'UK', 'GB'), 'EU', 'NL')
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

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.gaap_refund_waterfalls_order_breakout_detail AS
SELECT *
FROM _shipped_order_metrics;

ALTER TABLE reporting_base_prod.shared.gaap_refund_waterfalls_order_breakout_detail
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

--keeping the structure for the refund part
ALTER TABLE _shipped_order_metrics
    DROP COLUMN individual_bu, STORE_ID, SUBTOTAL, discount, tax, shipping, credit, actual_ship_date;

------------------------------------------------------------------- MAKE SURE ALL REFUND TYPES SHOW UP

CREATE OR REPLACE TEMPORARY TABLE _refund_types
(
    refund_type VARCHAR(20)
);

INSERT INTO _refund_types
VALUES ('Cash Refund'),
       ('Store Credit Refund'),
       ('Token Refund');


CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.gaap_refund_waterfalls_order_detail AS
SELECT DISTINCT s.*, r.*
FROM _shipped_order_metrics s
         LEFT OUTER JOIN (SELECT * FROM _refund_types) AS r;

ALTER TABLE reporting_base_prod.shared.gaap_refund_waterfalls_order_detail
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.gaap_refund_waterfalls_refund_detail AS
SELECT s.*,
       r.refund_id,
       r.payment_method,
       dd.month_date                                                                            AS refund_month,
       CASE
           WHEN r.payment_method = 'store_credit' THEN 'Store Credit Refund'
           WHEN r.payment_method = 'membership_token' THEN 'Token Refund'
           WHEN r.payment_method IN ('creditcard', 'psp', 'cash') THEN 'Cash Refund'
           ELSE 'Unknown'
           END                                                                                  AS refund_type,
       DATEDIFF(MONTH, s.date_shipped, dd.month_date) + 1                                       AS monthoffset,
       CAST(ZEROIFNULL(r.total_refund / (ZEROIFNULL(s.rate))) - r.tax_refund AS DECIMAL(20, 6)) AS refund_total
FROM _shipped_order_metrics s
         JOIN lake_consolidated_view.ultra_merchant.refund r ON s.order_id = r.order_id
    AND r.statuscode = 4570
         JOIN edw_prod.data_model.dim_date dd ON dd.full_date = CAST(r.datetime_refunded AS DATE)
WHERE dd.month_date < $process_to_date
  AND DATEDIFF(MONTH, s.date_shipped, dd.month_date) + 1 >= 0;
-- delete the SD negative offsets

ALTER TABLE reporting_base_prod.shared.gaap_refund_waterfalls_refund_detail
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

-- going into tableau set up for extra column


CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.gaap_refund_waterfalls_full AS
SELECT o.*,
       r.refund_id,
       r.refund_total,
       r.monthoffset,
       r.refund_month,
       r.payment_method,
       CASE
           WHEN ROW_NUMBER() OVER (PARTITION BY o.order_id, o.refund_type ORDER BY o.order_id DESC, r.refund_month DESC, r.refund_id DESC) =
                1 THEN 1
           ELSE 0 END                                                              order_count,
       ZEROIFNULL(CASE
                      WHEN ROW_NUMBER() OVER (PARTITION BY o.order_id, o.refund_type ORDER BY o.order_id DESC, r.refund_month DESC, r.refund_id DESC) =
                           1
                          THEN o.total_cash_and_cash_credit
                      ELSE 0 END)                                                  order_total,
       ZEROIFNULL(rg.refund_group)                                                 refund_group,
       ZEROIFNULL(og.order_total_group)                                            order_total_group,
       ZEROIFNULL(og.order_count_group)                                            order_count_group,
       IFF(order_total_group = 0, 0, ZEROIFNULL(refund_group / order_total_group)) refund_percentage
FROM reporting_base_prod.shared.gaap_refund_waterfalls_order_detail o
         LEFT JOIN reporting_base_prod.shared.gaap_refund_waterfalls_refund_detail r
                   ON r.order_id = o.order_id
                       AND o.refund_type = r.refund_type
         LEFT JOIN (SELECT membership_store, refund_type, order_type, date_shipped, SUM(refund_total) refund_group
                    FROM reporting_base_prod.shared.gaap_refund_waterfalls_refund_detail
                    GROUP BY membership_store, refund_type, order_type, date_shipped) rg
                   ON rg.membership_store = o.membership_store
                       AND rg.refund_type = o.refund_type
                       AND rg.order_type = o.order_type
                       AND rg.date_shipped = o.date_shipped
         LEFT JOIN (SELECT membership_store,
                           refund_type,
                           order_type,
                           date_shipped,
                           SUM(total_cash_and_cash_credit) order_total_group,
                           COUNT(*)                        order_count_group
                    FROM reporting_base_prod.shared.gaap_refund_waterfalls_order_detail
                    GROUP BY membership_store, refund_type, order_type, date_shipped) og
                   ON og.membership_store = o.membership_store
                       AND og.refund_type = o.refund_type
                       AND og.order_type = o.order_type
                       AND og.date_shipped = o.date_shipped;

ALTER TABLE reporting_base_prod.shared.gaap_refund_waterfalls_full
    SET DATA_RETENTION_TIME_IN_DAYS = 0;

INSERT INTO reporting_base_prod.shared.gaap_refund_waterfalls_refund_detail_snapshot
(individual_bu,
 credit_billing_store_id,
 membership_store_id,
 membership_store,
 refund_type,
 order_type,
 order_id,
 refund_id,
 payment_method,
 date_shipped,
 refund_month,
 monthoffset,
 refund_total,
 snapshot_timestamp)
SELECT NULL                 individual_bu,
       NULL                 credit_billing_store_id,
       membership_store_id,
       membership_store,
       refund_type,
       order_type,
       order_id,
       refund_id,
       payment_method,
       date_shipped,
       refund_month,
       monthoffset,
       refund_total,
       CURRENT_TIMESTAMP AS snapshot_timestamp
FROM reporting_base_prod.shared.gaap_refund_waterfalls_refund_detail;

DELETE
FROM reporting_base_prod.shared.gaap_refund_waterfalls_refund_detail_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -18, getdate());

INSERT INTO reporting_base_prod.shared.gaap_refund_waterfalls_order_detail_snapshot
(individual_bu,
 store_id,
 membership_store_id,
 membership_store,
 order_type,
 date_shipped,
 order_id,
 gross_rev_after_discount,
 total_cash_and_cash_credit,
 refund_type,
 snapshot_timestamp)
SELECT DISTINCT NULL                 individual_bu,
                NULL                 store_id,
                membership_store_id,
                membership_store,
                order_type,
                date_shipped,
                order_id,
                gross_rev_after_discount,
                total_cash_and_cash_credit,
                refund_type,
                CURRENT_TIMESTAMP AS snapshot_timestamp
FROM reporting_base_prod.shared.gaap_refund_waterfalls_order_detail;

DELETE
FROM reporting_base_prod.shared.gaap_refund_waterfalls_order_detail_snapshot
WHERE snapshot_timestamp < DATEADD(MONTH, -18, getdate());
