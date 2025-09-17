SET start_date = DATEADD(MM, -1, DATE_TRUNC(MONTH, GETDATE()::DATE));
SET end_date = date_trunc(MONTH, GETDATE()::DATE);


CREATE OR REPLACE TEMPORARY TABLE _retail_attribution AS
SELECT st.store_brand,
       st.store_region,
       IFF(st.store_brand = 'Fabletics', fso.gender, 'F') AS    gender,
       DATE_TRUNC(MONTH, fso.date) AS                           month_date,
       vip.store_full_name AS                                   retail_store,
       SUM(fso.product_order_count) AS                          product_order_count,
       SUM(fso.product_order_unit_count) AS                     product_order_unit_count,
       SUM(fso.product_order_cash_gross_revenue_amount) AS      product_order_cash_gross_revenue,
       SUM(fso.product_order_product_subtotal_amount) AS        product_subtotal,
       SUM(fso.product_order_product_discount_amount) AS        product_discount,
       SUM(fso.product_order_shipping_revenue_amount) AS        shipping_revenue,
       SUM(fso.product_order_shipping_cost_amount) AS           shipping_cost,
       SUM(fso.product_order_landed_product_cost_amount) AS     landed_product_cost,
       SUM(fso.product_order_cash_refund_amount) AS             product_order_cash_refund,
       SUM(fso.product_order_cash_credit_refund_amount) AS      product_order_cash_store_credit_refund,
       SUM(fso.product_order_cash_chargeback_amount) AS         product_order_cash_chargebacks,
       SUM(fso.product_order_noncash_credit_redeemed_amount +
           fso.product_order_cash_credit_redeemed_amount) AS    total_credit_redeemed,
       SUM(fso.product_order_cash_credit_redeemed_amount) AS    cash_credit_redeemed,
       SUM(fso.product_order_noncash_credit_redeemed_amount) AS noncash_credit_redeemed,
       SUM(fso.product_gross_revenue) AS                        product_gross_revenue,
       SUM(fso.product_net_revenue) AS                          product_net_revenue,
       SUM(fso.product_margin_pre_return) AS                    product_margin_pre_return,
       SUM(fso.billed_cash_credit_issued_amount) AS             billed_cash_credit_issued,
       SUM(fso.billed_cash_credit_redeemed_amount) AS           billed_cash_credit_redeemed_online,
       CAST(0 AS DECIMAL(20, 4)) AS                             billed_cash_credit_redeemed_retail,
       SUM(fso.billed_cash_credit_cancelled_amount) AS          billed_cash_credit_cancelled
FROM analytics_base.finance_sales_ops fso
         JOIN data_model.dim_order_membership_classification omc
              ON omc.order_membership_classification_key = fso.order_membership_classification_key
         JOIN data_model.dim_store st ON st.store_id = fso.store_id
         JOIN data_model.dim_store vip ON vip.store_id = fso.vip_store_id
    AND vip.store_type = 'Retail'
WHERE fso.date_object = 'shipped'
  AND fso.date < $end_date
  AND fso.date >= $start_date
  AND currency_type = 'USD'
  AND st.store_type != 'Retail'
  AND membership_order_type_l3 != 'Activating VIP'
  AND fso.activation_key != -1
  AND vip.store_brand IN ('Fabletics', 'Savage X')
GROUP BY st.store_brand,
         st.store_region,
         IFF(st.store_brand = 'Fabletics', fso.gender, 'F'),
         DATE_TRUNC(MONTH, fso.date),
         vip.store_full_name
;


CREATE OR REPLACE TEMPORARY TABLE _retail_attribution_retail_redemptions AS
SELECT DATE_TRUNC(MONTH, fso.date)                        AS month_date,
       vip.store_brand,
       vip.store_region,
       vip.store_full_name                                AS retail_store,
       IFF(st.store_brand = 'Fabletics', fso.gender, 'F') AS gender,
       SUM(fso.billed_cash_credit_redeemed_amount)        AS billed_cash_credit_redeemed_retail
FROM analytics_base.finance_sales_ops fso
         JOIN data_model.dim_order_membership_classification omc
              ON omc.order_membership_classification_key = fso.order_membership_classification_key
         JOIN data_model.dim_store st ON st.store_id = fso.store_id
         JOIN data_model.dim_store vip ON vip.store_id = fso.vip_store_id
    AND vip.store_type = 'Retail'
WHERE fso.date_object = 'shipped'
  AND fso.date < $end_date
  AND fso.date >= $start_date
  AND currency_type = 'USD'
  AND st.store_type = 'Retail' -- Retail Orders
  AND membership_order_type_l3 != 'Activating VIP'
  AND activation_key != -1
  AND vip.store_brand IN ('Fabletics', 'Savage X')
GROUP BY DATE_TRUNC(MONTH, fso.date),
         vip.store_full_name,
         vip.store_brand,
         vip.store_region,
         IFF(st.store_brand = 'Fabletics', fso.gender, 'F');


UPDATE _retail_attribution v2
SET billed_cash_credit_redeemed_retail = rr.billed_cash_credit_redeemed_retail
FROM _retail_attribution_retail_redemptions rr
WHERE rr.store_brand = v2.store_brand
  AND rr.store_region = v2.store_region
  AND rr.retail_store = v2.retail_store
  AND rr.month_date = v2.month_date
  AND rr.gender = v2.gender
;



CREATE OR REPLACE TEMPORARY TABLE _final_data AS
SELECT store_brand,
       store_region,
       gender,
       month_date,
       retail_store,
       product_order_count,
       product_order_unit_count,
       product_order_cash_gross_revenue,
       product_subtotal,
       product_discount,
       shipping_revenue,
       shipping_cost,
       landed_product_cost,
       product_order_cash_refund,
       product_order_cash_store_credit_refund,
       product_order_cash_chargebacks,
       total_credit_redeemed,
       cash_credit_redeemed,
       noncash_credit_redeemed,
       product_gross_revenue,
       product_net_revenue,
       product_margin_pre_return,
       billed_cash_credit_issued,
       billed_cash_credit_redeemed_online,
       billed_cash_credit_redeemed_retail,
       billed_cash_credit_cancelled,
       billed_cash_credit_issued - billed_cash_credit_redeemed_online - billed_cash_credit_redeemed_retail -
       billed_cash_credit_cancelled AS billed_credit_net_billings
FROM _retail_attribution

UNION ALL

SELECT store_brand,
       store_region,
       gender,
       month_date,
       retail_store,
       product_order_count,
       product_order_unit_count,
       product_order_cash_gross_revenue,
       product_subtotal,
       product_discount,
       shipping_revenue,
       shipping_cost,
       landed_product_cost,
       product_order_cash_refund,
       product_order_cash_store_credit_refund,
       product_order_cash_chargebacks,
       total_credit_redeemed,
       cash_credit_redeemed,
       noncash_credit_redeemed,
       product_gross_revenue,
       product_net_revenue,
       product_margin_pre_return,
       billed_cash_credit_issued,
       billed_cash_credit_redeemed_online,
       billed_cash_credit_redeemed_retail,
       billed_cash_credit_cancelled,
       billed_credit_net_billings
FROM reporting.retail_attribution_final_output
;


CREATE OR REPLACE TEMPORARY TABLE _scaffold AS
SELECT DISTINCT fd.store_brand,
                fd.store_region,
                fd.retail_store,
                g.gender,
                md.month_date
FROM _final_data fd
         CROSS JOIN (SELECT DISTINCT gender FROM _final_data) g
         CROSS JOIN (SELECT DISTINCT month_date FROM _final_data) md;


CREATE OR REPLACE TEMPORARY TABLE _final_output AS
SELECT s.store_brand,
       s.store_region,
       s.gender,
       s.month_date,
       s.retail_store,
       COALESCE(fd.product_order_count, 0)                    AS product_order_count,
       COALESCE(fd.product_order_unit_count, 0)               AS product_order_unit_count,
       COALESCE(fd.product_order_cash_gross_revenue, 0)       AS product_order_cash_gross_revenue,
       COALESCE(fd.product_subtotal, 0)                       AS product_subtotal,
       COALESCE(fd.product_discount, 0)                       AS product_discount,
       COALESCE(fd.shipping_revenue, 0)                       AS shipping_revenue,
       COALESCE(fd.shipping_cost, 0)                          AS shipping_cost,
       COALESCE(fd.landed_product_cost, 0)                    AS landed_product_cost,
       COALESCE(fd.product_order_cash_refund, 0)              AS product_order_cash_refund,
       COALESCE(fd.product_order_cash_store_credit_refund, 0) AS product_order_cash_store_credit_refund,
       COALESCE(fd.product_order_cash_chargebacks, 0)         AS product_order_cash_chargebacks,
       COALESCE(fd.total_credit_redeemed, 0)                  AS total_credit_redeemed,
       COALESCE(fd.cash_credit_redeemed, 0)                   AS cash_credit_redeemed,
       COALESCE(fd.noncash_credit_redeemed, 0)                AS noncash_credit_redeemed,
       COALESCE(fd.product_gross_revenue, 0)                  AS product_gross_revenue,
       COALESCE(fd.product_net_revenue, 0)                    AS product_net_revenue,
       COALESCE(fd.product_margin_pre_return, 0)              AS product_margin_pre_return,
       COALESCE(fd.billed_cash_credit_issued, 0)              AS billed_cash_credit_issued,
       COALESCE(fd.billed_cash_credit_redeemed_online, 0)     AS billed_cash_credit_redeemed_online,
       COALESCE(fd.billed_cash_credit_redeemed_retail, 0)     AS billed_cash_credit_redeemed_retail,
       COALESCE(fd.billed_cash_credit_cancelled, 0)           AS billed_cash_credit_cancelled,
       COALESCE(fd.billed_credit_net_billings, 0)             AS billed_credit_net_billings
FROM _scaffold s
         LEFT JOIN _final_data fd ON s.store_brand = fd.store_brand
    AND s.store_region = fd.store_region
    AND s.gender = fd.gender
    AND s.month_date = fd.month_date
    AND s.retail_store = fd.retail_store
;

BEGIN TRANSACTION;

/* Snapshot of this table is available till May, 2023. Table_name: edw_prod.snapshot.retail_attribution_final_output*/
DELETE FROM reporting.retail_attribution_final_output;

INSERT INTO reporting.retail_attribution_final_output
(
    store_brand,
    store_region,
    gender,
    month_date,
    retail_store,
    product_order_count,
    product_order_unit_count,
    product_order_cash_gross_revenue,
    product_subtotal,
    product_discount,
    shipping_revenue,
    shipping_cost,
    landed_product_cost,
    product_order_cash_refund,
    product_order_cash_store_credit_refund,
    product_order_cash_chargebacks,
    total_credit_redeemed,
    cash_credit_redeemed,
    noncash_credit_redeemed,
    product_gross_revenue,
    product_net_revenue,
    product_margin_pre_return,
    billed_cash_credit_issued,
    billed_cash_credit_redeemed_online,
    billed_cash_credit_redeemed_retail,
    billed_cash_credit_cancelled,
    billed_credit_net_billings,
    billed_credit_deferred
)
SELECT
   store_brand,
   store_region,
   gender,
   month_date,
   retail_store,
   product_order_count,
   product_order_unit_count,
   product_order_cash_gross_revenue,
   product_subtotal,
   product_discount,
   shipping_revenue,
   shipping_cost,
   landed_product_cost,
   product_order_cash_refund,
   product_order_cash_store_credit_refund,
   product_order_cash_chargebacks,
   total_credit_redeemed,
   cash_credit_redeemed,
   noncash_credit_redeemed,
   product_gross_revenue,
   product_net_revenue,
   product_margin_pre_return,
   billed_cash_credit_issued,
   billed_cash_credit_redeemed_online,
   billed_cash_credit_redeemed_retail,
   billed_cash_credit_cancelled,
   billed_credit_net_billings,
   SUM(billed_credit_net_billings)
       OVER (PARTITION BY store_brand, store_region, retail_store, gender ORDER BY month_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS billed_credit_deferred
FROM _final_output
;

COMMIT;
