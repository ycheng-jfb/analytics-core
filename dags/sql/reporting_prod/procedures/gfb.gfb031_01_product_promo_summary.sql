SET start_date = DATE_TRUNC(YEAR, DATEADD(YEAR, -2, CURRENT_DATE()));

CREATE OR REPLACE TEMPORARY TABLE _product_promo_summary AS
SELECT pds.business_unit
     , pds.sub_brand
     , pds.region
     , pds.country
     , pds.order_date
     , pds.order_type
     , pds.product_sku
     , pds.promo_order_flag
     , (CASE
            WHEN pds.clearance_flag = 'clearance' THEN 'Clearance'
            WHEN pds.promo_order_flag = 'Promo Orders' THEN 'Promo Order'
            ELSE 'Regular Price' END)        AS product_order_type
     , COUNT(DISTINCT pds.order_id)          AS orders
     , SUM(pds.total_qty_sold)               AS total_qty_sold
     , SUM(pds.total_product_revenue)        AS total_product_revenue
     , SUM(pds.total_cogs)                   AS total_cogs
     , SUM(pds.total_cash_collected)         AS total_cash_collected
     , SUM(pds.total_credit_redeemed_amount) AS total_credit_redeemed_amount
     , SUM(pds.total_subtotal_amount)        AS total_subtotal_amount
     , SUM(pds.total_discount)               AS total_discount
     , SUM(pds.total_shipping_cost)          AS total_shipping_cost
     , SUM(pds.total_shipping_revenue)       AS total_shipping_revenue
FROM gfb.gfb011_promo_data_set pds
WHERE pds.order_date >= $start_date
GROUP BY pds.business_unit
       , pds.sub_brand
       , pds.region
       , pds.country
       , pds.order_date
       , pds.order_type
       , pds.product_sku
       , pds.promo_order_flag
       , (CASE
              WHEN pds.clearance_flag = 'clearance' THEN 'Clearance'
              WHEN pds.promo_order_flag = 'Promo Orders' THEN 'Promo Order'
              ELSE 'Regular Price' END);

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb031_01_product_promo_summary AS
SELECT ppd.*
     , mdp.large_img_url
FROM _product_promo_summary ppd
         LEFT JOIN gfb.merch_dim_product mdp
                   ON mdp.business_unit = ppd.business_unit
                       AND mdp.region = ppd.region
                       AND mdp.country = ppd.country
                       AND mdp.product_sku = ppd.product_sku;
