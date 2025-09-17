SET start_date = DATE_TRUNC(YEAR, DATEADD(YEAR, -2, CURRENT_DATE()));
SET end_date = CURRENT_DATE();

CREATE OR REPLACE TEMPORARY TABLE _points_redemption_detail AS
SELECT lpa.business_unit
     , lpa.region
     , lpa.country
     , lpa.customer_id
     , lpa.activity_date
     , lpa.activity_reason
     , lpa.order_id
     , lpa.promo_id
     , SUM(lpa.points) AS redeemed_points
FROM reporting_prod.gfb.gfb_loyalty_points_activity lpa
WHERE lpa.status = 'Completed'
  AND lpa.type = 'debit'
GROUP BY lpa.business_unit
       , lpa.region
       , lpa.country
       , lpa.customer_id
       , lpa.activity_date
       , lpa.activity_reason
       , lpa.order_id
       , lpa.promo_id
HAVING SUM(lpa.points) != 0;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb037_loyalty_points_redemption AS
SELECT prd.business_unit
     , prd.region
     , prd.country
     , DATE_TRUNC(MONTH, prd.activity_date) AS month_date
     , prd.activity_reason
     , SUM(prd.redeemed_points)             AS redeemed_points
     , COUNT(DISTINCT prd.customer_id)      AS customer_count
FROM _points_redemption_detail prd
WHERE prd.activity_date >= $start_date
  AND prd.activity_date < $end_date
GROUP BY prd.business_unit
       , prd.region
       , prd.country
       , DATE_TRUNC(MONTH, prd.activity_date)
       , prd.activity_reason;


CREATE OR REPLACE TEMPORARY TABLE _loyalty_product_sold_detail AS
SELECT UPPER(st.store_brand)                                                                      AS business_unit
     , UPPER(st.store_region)                                                                     AS region
     , (CASE
            WHEN dc.specialty_country_code = 'GB' THEN 'UK'
            WHEN dc.specialty_country_code != 'Unknown' THEN dc.specialty_country_code
            ELSE st.store_country END)                                                            AS country
     , fol.order_id
     , fol.order_line_id
     , fo.session_id
     , dc.customer_id
     , dp.product_id
     , dp.product_sku
     , dp.sku
     , dp.product_name
     , dp.color                                                                                   AS dp_color
     , LOWER(dp.size)                                                                             AS dp_size
     , CAST(fol.order_local_datetime AS DATE)                                                     AS order_date
     , (CASE
            WHEN domc.membership_order_type_l2 = 'Guest' THEN 'ecom'
            WHEN domc.membership_order_type_l1 = 'Activating VIP' THEN 'vip activating'
            ELSE 'vip repeat' END)                                                                AS order_type
     , fol.item_quantity                                                                          AS total_qty_sold
     , (fol.estimated_landed_cost_local_amount) * COALESCE(fol.order_date_usd_conversion_rate, 1) AS total_cogs
     , SUM(total_qty_sold) OVER (PARTITION BY fol.order_id)                                       AS order_qty_sold
     , prd.redeemed_points                                                                        AS order_redeemed_points
     , order_redeemed_points * (total_qty_sold / order_qty_sold)                                  AS order_line_redeemed_points
     , (fol.subtotal_excl_tariff_local_amount - fol.product_discount_local_amount) *
       COALESCE(fol.order_date_usd_conversion_rate, 1)                                            AS total_product_revenue
FROM edw_prod.data_model_jfb.fact_order_line fol
         JOIN reporting_prod.gfb.vw_store st
              ON st.store_id = fol.store_id
         JOIN edw_prod.data_model_jfb.dim_order_status dos
              ON dos.order_status_key = fol.order_status_key
         JOIN edw_prod.data_model_jfb.dim_order_line_status dols
              ON dols.order_line_status_key = fol.order_line_status_key
         JOIN edw_prod.data_model_jfb.dim_order_membership_classification domc
              ON domc.order_membership_classification_key = fol.order_membership_classification_key
         JOIN edw_prod.data_model_jfb.dim_product dp
              ON dp.product_id = fol.product_id
         JOIN edw_prod.data_model_jfb.dim_product_type dpt
              ON dpt.product_type_key = fol.product_type_key
                  AND dpt.product_type_name = 'Membership Reward Points Item'
                  AND dpt.is_free = 0
         JOIN edw_prod.data_model_jfb.dim_customer dc
              ON dc.customer_id = fol.customer_id
                  AND dc.is_test_customer = 0
         JOIN edw_prod.data_model_jfb.fact_order fo
              ON fo.order_id = fol.order_id
         JOIN edw_prod.data_model_jfb.dim_order_processing_status dops
              ON dops.order_processing_status_key = fo.order_processing_status_key
         JOIN edw_prod.data_model_jfb.dim_order_sales_channel dosc
              ON dosc.order_sales_channel_key = fol.order_sales_channel_key
                  AND dosc.is_border_free_order = 0
                  AND dosc.is_ps_order = 0
                  AND dosc.is_test_order = 0
         JOIN _points_redemption_detail prd
              ON prd.order_id = fol.order_id
WHERE (
            dos.order_status = 'Success'
        OR
            (dos.order_status = 'Pending' AND
             dops.order_processing_status IN ('FulFillment (Batching)', 'FulFillment (In Progress)', 'Placed'))
    )
  AND dols.order_line_status != 'Cancelled'
  AND fol.shipped_local_datetime IS NOT NULL
  AND dosc.order_classification_l1 = 'Product Order'
  AND CAST(fol.order_local_datetime AS DATE) >= $start_date
  AND CAST(fol.order_local_datetime AS DATE) < $end_date;


CREATE OR REPLACE TEMPORARY TABLE _inventory_info AS
SELECT id.business_unit
     , id.region                     AS region
     , id.country                    AS country
     , id.product_sku
     , id.sku
     , id.inventory_date             AS inventory_date
     , SUM(id.qty_onhand)            AS qty_onhand
     , SUM(id.qty_available_to_sell) AS qty_available_to_sell
     , SUM(id.qty_open_to_buy)       AS qty_open_to_buy
FROM reporting_prod.gfb.gfb_inventory_data_set id
WHERE id.inventory_date >= $start_date
GROUP BY id.business_unit
       , id.region
       , id.country
       , id.product_sku
       , id.sku
       , id.inventory_date;


CREATE OR REPLACE TEMPORARY TABLE _sku_size_color_mapping AS
SELECT DISTINCT a.business_unit
              , a.region
              , a.country
              , a.sku
              , a.dp_color AS color
              , a.dp_size  AS size
FROM _loyalty_product_sold_detail a
WHERE a.dp_color != 'Error';


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb037_01_loyalty_item_sold AS
SELECT COALESCE(psd.business_unit, ii.business_unit) AS business_unit
     , COALESCE(psd.region, ii.region)               AS region
     , COALESCE(psd.country, ii.country)             AS country
     , mdp.large_img_url
     , mdp.style_name                                AS product_name
     , mdp.fk_site_name
     , scm.color
     , scm.size
     , COALESCE(psd.product_sku, ii.product_sku)     AS product_sku
     , mdp.gender
     , mdp.landing_cost
     , COALESCE(psd.order_date, ii.inventory_date)   AS date
     , 'Loyalty Product'                             AS metrics_type
     , COALESCE(psd.sku, ii.sku)                     AS sku
     , mdp.current_vip_retail                        AS vip_price
     , COALESCE(psd.total_qty_sold, 0)               AS total_qty_sold
     , COALESCE(psd.total_product_revenue, 0)        AS total_product_revenue
     , COALESCE(psd.total_cogs, 0)                   AS total_cogs
     , COALESCE(ii.qty_available_to_sell, 0)         AS qty_available_to_sell
FROM (
         SELECT psd.business_unit
              , psd.region
              , psd.country
              , psd.product_sku
              , psd.order_date
              , psd.sku
              , SUM(psd.total_qty_sold)        AS total_qty_sold
              , SUM(psd.total_product_revenue) AS total_product_revenue
              , SUM(psd.total_cogs)            AS total_cogs
         FROM _loyalty_product_sold_detail psd
         GROUP BY psd.business_unit
                , psd.region
                , psd.country
                , psd.product_sku
                , psd.order_date
                , psd.sku
     ) psd
         FULL JOIN _inventory_info ii
                   ON ii.business_unit = psd.business_unit
                       AND ii.region = psd.region
                       AND ii.country = psd.country
                       AND ii.sku = psd.sku
                       AND ii.inventory_date = psd.order_date
         LEFT JOIN reporting_prod.gfb.merch_dim_product mdp
                   ON mdp.business_unit = COALESCE(psd.business_unit, ii.business_unit)
                       AND mdp.region = COALESCE(psd.region, ii.region)
                       AND mdp.country = COALESCE(psd.country, ii.country)
                       AND mdp.product_sku = COALESCE(psd.product_sku, ii.product_sku)
         LEFT JOIN _sku_size_color_mapping scm
                   ON scm.business_unit = COALESCE(psd.business_unit, ii.business_unit)
                       AND scm.region = COALESCE(psd.region, ii.region)
                       AND scm.country = COALESCE(psd.country, ii.country)
                       AND scm.sku = COALESCE(psd.sku, ii.sku);


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb037_02_loyalty_promo_redemption AS
SELECT olp.business_unit
     , olp.region
     , olp.country
     , olp.promo_code
     , olp.promo_name
     , olp.order_date
     , olp.order_type
     , olp.total_qty_sold
     , olp.orders
     , olp.total_product_revenue
     , olp.total_discount
     , olp.total_subtotal_amount
     , olp.total_cash_collected
     , olp.total_credit_redeemed_amount
     , olp.total_cogs
     , olp.total_shipping_revenue
     , olp.total_shipping_cost
FROM reporting_prod.gfb.gfb011_01_promo_code_order_data_set olp
WHERE olp.order_date >= $start_date
  AND olp.order_date < $end_date
  AND (
            olp.promo_code LIKE '%LOYALTY%'
        OR olp.promo_code LIKE '%DIAMOND%'
        OR olp.promo_code LIKE '%ELITE%'
        OR olp.promo_code LIKE '%SILVER%'
        OR olp.promo_code LIKE '%ONYX%'
    );
