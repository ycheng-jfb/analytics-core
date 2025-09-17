SET start_date = DATE_TRUNC(YEAR, DATEADD(YEAR, -2, CURRENT_DATE()));

CREATE OR REPLACE TEMPORARY TABLE _product_promo_data AS
SELECT pds.business_unit
     , pds.sub_brand
     , pds.region
     , pds.country
     , pds.order_date
     , pds.order_type
     , pds.promo_order_flag
     , pds.product_sku
     , pds.promo_code_1                                                                          AS promo_code
     , pds.promo_1_offer                                                                         AS promo_offer
     , pds.promo_1_group                                                                         AS promo_group
     , pds.promo_1_goal                                                                          AS promo_goal
     , pds.activating_offers
     , olp.bundle_product_id
     , COUNT(DISTINCT pds.order_id)                                                              AS orders
     , SUM(pds.total_qty_sold)                                                                   AS total_qty_sold
     , SUM(pds.item_only_total_qty_sold)                                                         AS item_only_total_qty_sold
     , SUM(pds.item_only_activating_qty_sold)                                                    AS item_only_activating_qty_sold
     , SUM(pds.item_only_repeat_qty_sold)                                                        AS item_only_repeat_qty_sold
     , SUM(pds.total_product_revenue)                                                            AS total_product_revenue
     , SUM(pds.total_cogs)                                                                       AS total_cogs
     , SUM(pds.total_cash_collected)                                                             AS total_cash_collected
     , SUM(pds.total_credit_redeemed_amount)                                                     AS total_credit_redeemed_amount
     , SUM(pds.total_subtotal_amount)                                                            AS total_subtotal_amount
     , SUM(pds.total_discount)                                                                   AS total_discount
     , COUNT(DISTINCT CASE
                          WHEN olp.bundle_order_line_id != -1 THEN olp.bundle_order_line_id END) AS bundle_unit_sold
     , SUM(pds.total_shipping_cost)                                                              AS total_shipping_cost
     , SUM(pds.total_shipping_revenue)                                                           AS total_shipping_revenue
FROM gfb.gfb011_promo_data_set pds
         JOIN gfb.gfb_order_line_data_set_place_date olp
              ON olp.order_line_id = pds.order_line_id
WHERE pds.promo_code_1 IS NOT NULL
  AND pds.order_date >= $start_date
GROUP BY pds.business_unit
       , pds.sub_brand
       , pds.region
       , pds.country
       , pds.order_date
       , pds.order_type
       , pds.promo_order_flag
       , pds.product_sku
       , pds.promo_code_1
       , pds.promo_1_offer
       , pds.promo_1_group
       , pds.promo_1_goal
       , pds.activating_offers
       , olp.bundle_product_id
UNION
SELECT pds.business_unit
     , pds.sub_brand
     , pds.region
     , pds.country
     , pds.order_date
     , pds.order_type
     , pds.promo_order_flag
     , pds.product_sku
     , pds.promo_code_2                                                                          AS promo_code
     , pds.promo_2_offer                                                                         AS promo_offer
     , pds.promo_2_group                                                                         AS promo_group
     , pds.promo_2_goal                                                                          AS promo_goal
     , pds.activating_offers
     , olp.bundle_product_id
     , COUNT(DISTINCT pds.order_id)                                                              AS orders
     , SUM(pds.total_qty_sold)                                                                   AS total_qty_sold
     , SUM(pds.item_only_total_qty_sold)                                                         AS item_only_total_qty_sold
     , SUM(pds.item_only_activating_qty_sold)                                                    AS item_only_activating_qty_sold
     , SUM(pds.item_only_repeat_qty_sold)                                                        AS item_only_repeat_qty_sold
     , SUM(pds.total_product_revenue)                                                            AS total_product_revenue
     , SUM(pds.total_cogs)                                                                       AS total_cogs
     , SUM(pds.total_cash_collected)                                                             AS total_cash_collected
     , SUM(pds.total_credit_redeemed_amount)                                                     AS total_credit_redeemed_amount
     , SUM(pds.total_subtotal_amount)                                                            AS total_subtotal_amount
     , SUM(pds.total_discount)                                                                   AS total_discount
     , COUNT(DISTINCT CASE
                          WHEN olp.bundle_order_line_id != -1 THEN olp.bundle_order_line_id END) AS bundle_unit_sold
     , SUM(pds.total_shipping_cost)                                                              AS total_shipping_cost
     , SUM(pds.total_shipping_revenue)                                                           AS total_shipping_revenue
FROM gfb.gfb011_promo_data_set pds
         JOIN gfb.gfb_order_line_data_set_place_date olp
              ON olp.order_line_id = pds.order_line_id
WHERE pds.promo_code_2 IS NOT NULL
  AND pds.order_date >= $start_date
GROUP BY pds.business_unit
       , pds.sub_brand
       , pds.region
       , pds.country
       , pds.order_date
       , pds.order_type
       , pds.promo_order_flag
       , pds.product_sku
       , pds.promo_code_2
       , pds.promo_2_offer
       , pds.promo_2_group
       , pds.promo_2_goal
       , pds.activating_offers
       , olp.bundle_product_id
UNION
SELECT pds.business_unit
     , pds.sub_brand
     , pds.region
     , pds.country
     , pds.order_date
     , pds.order_type
     , pds.promo_order_flag
     , pds.product_sku
     , pds.promo_code_2                                                                          AS promo_code
     , pds.promo_2_offer                                                                         AS promo_offer
     , pds.promo_2_group                                                                         AS promo_group
     , pds.promo_2_goal                                                                          AS promo_goal
     , pds.activating_offers
     , olp.bundle_product_id
     , COUNT(DISTINCT pds.order_id)                                                              AS orders
     , SUM(pds.total_qty_sold)                                                                   AS total_qty_sold
     , SUM(pds.item_only_total_qty_sold)                                                         AS item_only_total_qty_sold
     , SUM(pds.item_only_activating_qty_sold)                                                    AS item_only_activating_qty_sold
     , SUM(pds.item_only_repeat_qty_sold)                                                        AS item_only_repeat_qty_sold
     , SUM(pds.total_product_revenue)                                                            AS total_product_revenue
     , SUM(pds.total_cogs)                                                                       AS total_cogs
     , SUM(pds.total_cash_collected)                                                             AS total_cash_collected
     , SUM(pds.total_credit_redeemed_amount)                                                     AS total_credit_redeemed_amount
     , SUM(pds.total_subtotal_amount)                                                            AS total_subtotal_amount
     , SUM(pds.total_discount)                                                                   AS total_discount
     , COUNT(DISTINCT CASE
                          WHEN olp.bundle_order_line_id != -1 THEN olp.bundle_order_line_id END) AS bundle_unit_sold
     , SUM(pds.total_shipping_cost)                                                              AS total_shipping_cost
     , SUM(pds.total_shipping_revenue)                                                           AS total_shipping_revenue
FROM gfb.gfb011_promo_data_set pds
         JOIN gfb.gfb_order_line_data_set_place_date olp
              ON olp.order_line_id = pds.order_line_id
WHERE pds.promo_code_1 IS NULL
  AND pds.promo_code_2 IS NULL
  AND pds.order_date >= $start_date
GROUP BY pds.business_unit
       , pds.sub_brand
       , pds.region
       , pds.country
       , pds.order_date
       , pds.order_type
       , pds.promo_order_flag
       , pds.product_sku
       , pds.promo_code_2
       , pds.promo_2_offer
       , pds.promo_2_group
       , pds.promo_2_goal
       , pds.activating_offers
       , olp.bundle_product_id;

CREATE OR REPLACE TEMPORARY TABLE _final_product_promo_data AS
SELECT business_unit
     , sub_brand
     , region
     , country
     , order_date
     , order_type
     , promo_order_flag
     , product_sku
     , promo_code
     , promo_offer
     , promo_group
     , promo_goal
     , activating_offers
     , bundle_product_id
     , SUM(orders)                        AS orders
     , SUM(total_qty_sold)                AS total_qty_sold
     , SUM(item_only_total_qty_sold)      AS item_only_total_qty_sold
     , SUM(item_only_activating_qty_sold) AS item_only_activating_qty_sold
     , SUM(item_only_repeat_qty_sold)     AS item_only_repeat_qty_sold
     , SUM(total_product_revenue)         AS total_product_revenue
     , SUM(total_cogs)                    AS total_cogs
     , SUM(total_cash_collected)          AS total_cash_collected
     , SUM(total_credit_redeemed_amount)  AS total_credit_redeemed_amount
     , SUM(total_subtotal_amount)         AS total_subtotal_amount
     , SUM(total_discount)                AS total_discount
     , SUM(bundle_unit_sold)              AS bundle_unit_sold
     , SUM(total_shipping_cost)           AS total_shipping_cost
     , SUM(total_shipping_revenue)        AS total_shipping_revenue
FROM _product_promo_data
GROUP BY business_unit, sub_brand, region, country, order_date, order_type, promo_order_flag, product_sku, promo_code,
         promo_offer,
         promo_group, promo_goal, activating_offers, bundle_product_id;

CREATE OR REPLACE TEMPORARY TABLE _include_coeff_for_inventory AS
SELECT *,
       COUNT(ppd.product_sku)
             OVER (PARTITION BY ppd.business_unit, ppd.region, ppd.country, ppd.order_date, ppd.product_sku) AS coeff
FROM _final_product_promo_data ppd;

CREATE OR REPLACE TEMPORARY TABLE _inventory_info AS
SELECT (CASE

            WHEN id.region = 'EU' AND id.main_brand = 'FABKIDS' THEN 'JUSTFAB'
            ELSE id.main_brand END)   AS main_brand
     , id.region                      AS region
     , id.country                     AS country
     , id.product_sku
     , id.inventory_date              AS inventory_date
     , (CASE
            WHEN mcsl.sku IS NOT NULL THEN 'clearance'
            ELSE 'regular' END)       AS clearance_flag
     , mcsl.clearance_group           AS clearance_price
     , (CASE
            WHEN lop.product_sku IS NOT NULL THEN 'lead only'
            ELSE 'not lead only' END) AS lead_only_flag

     , SUM(id.qty_onhand)             AS qty_onhand
     , SUM(id.qty_available_to_sell)  AS qty_available_to_sell
     , SUM(id.qty_open_to_buy)        AS qty_open_to_buy
     , SUM(CASE
               WHEN id.warehouse = 'KENTUCKY'
                   THEN id.qty_available_to_sell
               ELSE 0 END)            AS kentucky_qty_available_to_sell
     , SUM(CASE
               WHEN id.warehouse = 'PERRIS'
                   THEN id.qty_available_to_sell
               ELSE 0 END)            AS perris_qty_available_to_sell
     , SUM(CASE
               WHEN id.warehouse = 'CANADA'
                   THEN id.qty_available_to_sell
               ELSE 0 END)            AS canada_qty_available_to_sell
     , SUM(CASE
               WHEN id.region = 'EU'
                   THEN id.qty_available_to_sell
               ELSE 0 END)            AS eu_qty_available_to_sell
     , SUM(CASE
               WHEN id.warehouse = 'UK'
                   THEN id.qty_available_to_sell
               ELSE 0 END)            AS uk_qty_available_to_sell
     , SUM(id.qty_intransit)          AS qty_intransit
FROM gfb.gfb_inventory_data_set id
        JOIN gfb.merch_dim_product mdp
                   ON mdp.business_unit = (CASE
                                            WHEN id.region = 'EU' AND id.main_brand = 'FABKIDS' THEN 'JUSTFAB'
                                            ELSE id.main_brand END)
                       AND mdp.region = id.region
                       AND mdp.country = id.country
                       AND mdp.product_sku = id.product_sku
         LEFT JOIN lake_view.sharepoint.gfb_merch_clearance_sku_list mcsl
                   ON LOWER(mcsl.business_unit) = LOWER(id.main_brand)
                       AND LOWER(mcsl.region) = LOWER(id.region)
                       AND LOWER(mcsl.sku) = LOWER(id.product_sku)
                       AND
                      (CAST(id.inventory_date AS DATE) BETWEEN mcsl.start_date AND COALESCE(mcsl.end_date, CURRENT_DATE()))
         LEFT JOIN lake_view.sharepoint.gfb_lead_only_product lop
                   ON LOWER(lop.brand) = LOWER(id.main_brand)
                       AND LOWER(lop.region) = LOWER(id.region)
                       AND LOWER(lop.product_sku) = LOWER(id.product_sku)
                       AND
                      (CAST(id.inventory_date AS DATE) BETWEEN lop.date_added AND COALESCE(lop.date_removed, CURRENT_DATE()))
WHERE id.inventory_date >= $start_date
GROUP BY (CASE
              WHEN id.region = 'EU' AND id.main_brand = 'FABKIDS' THEN 'JUSTFAB'
              ELSE id.main_brand END)
       , id.region
       , id.country
       , id.product_sku
       , id.inventory_date
       , (CASE
              WHEN mcsl.sku IS NOT NULL THEN 'clearance'
              ELSE 'regular' END)
       , mcsl.clearance_group
       , (CASE
              WHEN lop.product_sku IS NOT NULL THEN 'lead only'
              ELSE 'not lead only' END);

CREATE OR REPLACE TEMPORARY TABLE _out_of_stock AS
SELECT business_unit
     , region
     , country
     , product_sku
     , SUM(total_inventory)                                 AS total_inventroy
     , COUNT(DISTINCT IFF(total_inventory = 0, size, NULL)) AS count_0_inventory_size
     , COUNT(DISTINCT IFF(total_inventory > 0, size, NULL)) AS sizes_in_stock
     , COUNT(DISTINCT size)                                 AS size_counts
FROM gfb.gfb015_out_of_stock_by_size
GROUP BY business_unit
       , region
       , country
       , product_sku;

CREATE OR REPLACE TEMPORARY TABLE _out_of_stock_hist AS
SELECT business_unit
     , region
     , country
     , product_sku
     , inventory_date
     , SUM(total_inventory)                                 AS total_inventroy
     , COUNT(DISTINCT IFF(total_inventory = 0, size, NULL)) AS count_0_inventory_size
     , COUNT(DISTINCT IFF(total_inventory > 0, size, NULL)) AS sizes_in_stock
     , COUNT(DISTINCT size)                                 AS size_counts
FROM gfb.gfb015_01_out_of_stock_by_size_hist
GROUP BY business_unit
       , region
       , country
       , product_sku
       , inventory_date;

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb031_product_promo_data_set AS
SELECT coalesce(ppd.business_unit,ii.main_brand) as business_unit
     , ppd.sub_brand
     , COALESCE(ppd.region, ii.region)                          AS region
     , COALESCE(ppd.country, ii.country)                        AS country
     , COALESCE(ppd.order_date, ii.inventory_date)              AS order_date
     , ppd.order_type
     , ppd.promo_order_flag
     , CASE
           WHEN lop.product_sku IS NOT NULL THEN 'Lead Only Flag'
           ELSE '' END                                          AS lead_only_flag
     , CASE
           WHEN prp.product_sku IS NOT NULL THEN 'Post Reg Only'
           ELSE '' END                                          AS post_reg_flag
     , COALESCE(ppd.product_sku, ii.product_sku)                AS product_sku
     , ppd.promo_code
     , REGEXP_REPLACE(RIGHT(ppd.promo_code, 2), '[ABCDEFGHIJKLMOPQRSTUVWXYZ]',
                      '')                                       AS price_bucket
     , ppd.promo_offer
     , ppd.promo_group
     , ppd.promo_goal
     , ppd.activating_offers
     , mdp.large_img_url
     , mdp.style_name
     , mdp.color
     , mdp.department
     , mdp.subcategory
     , mdp.subclass
     , (CASE
            WHEN mdp.latest_launch_date > DATE_TRUNC(MONTH, ppd.order_date)
                THEN COALESCE(mdp.previous_launch_date, mdp.latest_launch_date)
            ELSE mdp.latest_launch_date
    END)                                                        AS current_showroom
     , mdp.gender
     , mdp.fk_site_name
     , mdp.latest_launch_date
     , mdp.current_vip_retail

     , oos.count_0_inventory_size                               AS count_0_inventory_size
     , oos.sizes_in_stock                                       AS sizes_in_stock
     , oos.size_counts                                          AS size_counts
     , oosh.count_0_inventory_size                              AS count_0_inventory_size_hist
     , oosh.sizes_in_stock                                      AS sizes_in_stock_hist
     , oosh.size_counts                                         AS size_counts_hist
     , ppd.coeff
     , SUM(ppd.orders)                                          AS orders
     , SUM(ppd.total_qty_sold)                                  AS total_qty_sold
     , SUM(ppd.item_only_total_qty_sold)                        AS item_only_total_qty_sold
     , SUM(ppd.item_only_activating_qty_sold)                   AS item_only_activating_qty_sold
     , SUM(ppd.item_only_repeat_qty_sold)                       AS item_only_repeat_qty_sold
     , SUM(ppd.total_product_revenue)                           AS total_product_revenue
     , SUM(ppd.total_cogs)                                      AS total_cogs
     , SUM(ppd.total_cash_collected)                            AS total_cash_collected
     , SUM(ppd.total_credit_redeemed_amount)                    AS total_credit_redeemed_amount
     , SUM(ppd.total_subtotal_amount)                           AS total_subtotal_amount
     , SUM(ppd.total_discount)                                  AS total_discount
     , SUM(ppd.total_shipping_cost)                             AS total_shipping_cost
     , SUM(ppd.total_shipping_revenue)                          AS total_shipping_revenue
     , SUM(ii.qty_available_to_sell) / IFF(IFNULL(coeff,0) = 0, 1, coeff) AS qty_available_to_sell
FROM _include_coeff_for_inventory ppd
         JOIN gfb.merch_dim_product mdp
                   ON mdp.business_unit = ppd.business_unit
                       AND mdp.region = ppd.region
                       AND mdp.country = ppd.country
                       AND mdp.product_sku = ppd.product_sku
         LEFT JOIN lake_view.sharepoint.gfb_lead_only_product lop
                   ON LOWER(lop.brand) = LOWER(ppd.business_unit)
                       AND LOWER(lop.region) = LOWER(ppd.region)
                       AND LOWER(lop.product_sku) = LOWER(ppd.product_sku)
                       AND
                      (CAST(ppd.order_date AS DATE) >= lop.date_added AND
                       CAST(ppd.order_date AS DATE) <= COALESCE(lop.date_removed, CURRENT_DATE()))
         LEFT JOIN lake_view.sharepoint.gfb_post_reg_product prp
                   ON LOWER(prp.store) = LOWER(ppd.business_unit)
                       AND LOWER(prp.region) = LOWER(ppd.region)
                       AND LOWER(prp.product_sku) = LOWER(ppd.product_sku)
                       AND
                      (ppd.order_date >= prp.date_added AND
                       ppd.order_date <= COALESCE(prp.date_removed, CURRENT_DATE()))
         LEFT JOIN _out_of_stock oos
                   ON oos.business_unit = ppd.business_unit
                       AND oos.country = ppd.country
                       AND oos.product_sku = ppd.product_sku
         LEFT JOIN _out_of_stock_hist oosh
                   ON oosh.business_unit = ppd.business_unit
                       AND oosh.country = ppd.country
                       AND oosh.product_sku = ppd.product_sku
                       AND oosh.inventory_date = ppd.order_date
         FULL JOIN _inventory_info ii
                   ON ii.main_brand = ppd.business_unit
                       AND ii.region = ppd.region
                       AND ii.country = ppd.country
                       AND ii.product_sku = ppd.product_sku
                       AND ii.inventory_date = ppd.order_date

GROUP BY COALESCE(ppd.business_unit, ii.main_brand)
       , ppd.sub_brand
       , COALESCE(ppd.region, ii.region)
       , COALESCE(ppd.country, ii.country)
       , COALESCE(ppd.order_date, ii.inventory_date)
       , ppd.order_type
       , ppd.promo_order_flag
       , COALESCE(ppd.product_sku, ii.product_sku)
       , ppd.promo_code
       , REGEXP_REPLACE(RIGHT(ppd.promo_code, 2), '[ABCDEFGHIJKLMOPQRSTUVWXYZ]', '')
       , (CASE
              WHEN lop.product_sku IS NOT NULL THEN 'Lead Only Flag'
              ELSE '' END)
       , (CASE
              WHEN prp.product_sku IS NOT NULL THEN 'Post Reg Only'
              ELSE '' END)
       , ppd.promo_offer
       , ppd.coeff
       , ppd.promo_group
       , ppd.promo_goal
       , ppd.activating_offers
       , mdp.large_img_url
       , mdp.style_name
       , mdp.color
       , mdp.department
       , mdp.subcategory
       , mdp.subclass
       , (CASE
              WHEN mdp.latest_launch_date > DATE_TRUNC(MONTH, ppd.order_date)
                  THEN COALESCE(mdp.previous_launch_date, mdp.latest_launch_date)
              ELSE mdp.latest_launch_date
    END)
       , mdp.gender
       , mdp.fk_site_name
       , mdp.latest_launch_date
       , mdp.current_vip_retail
       , oos.count_0_inventory_size
       , oos.sizes_in_stock
       , oos.size_counts
       , oosh.count_0_inventory_size
       , oosh.sizes_in_stock
       , oosh.size_counts;
