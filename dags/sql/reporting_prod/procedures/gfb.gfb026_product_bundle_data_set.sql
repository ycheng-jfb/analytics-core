SET start_date = DATEADD(YEAR, -2, DATE_TRUNC(YEAR, CURRENT_DATE()));
SET end_date = CURRENT_DATE();


CREATE OR REPLACE TEMPORARY TABLE _fk_bundle_info AS
SELECT DISTINCT a.business_unit
              , a.region
              , a.country
              , a.bundle_product_id
              , COALESCE(a.fk_bundle_alias, a.bundle_alias)                                                                              AS outfit_alias
              , COALESCE(a.fk_bundle_site_name, a.bundle_name)                                                                           AS site_name
              , a.bundle_name
              , a.fk_vip_retail                                                                                                          AS vip_retail
              , a.fk_type                                                                                                                AS type
              , UPPER(COALESCE(a.fk_gender, a.gender))                                                                                   AS gender
              , a.fk_active_inactive                                                                                                     AS active_inactive
              , a.fk_outfit_vs_box_vs_pack                                                                                               AS outfit_vs_box_vs_pack
              , NTH_VALUE(a.product_sku, 1)
                          OVER (PARTITION BY a.business_unit, a.region, a.country, a.bundle_product_id ORDER BY a.master_product_id ASC) AS item1
              , NTH_VALUE(a.product_sku, 2)
                          OVER (PARTITION BY a.business_unit, a.region, a.country, a.bundle_product_id ORDER BY a.master_product_id ASC) AS item2
              , NTH_VALUE(a.product_sku, 3)
                          OVER (PARTITION BY a.business_unit, a.region, a.country, a.bundle_product_id ORDER BY a.master_product_id ASC) AS item3
              , NTH_VALUE(a.product_sku, 4)
                          OVER (PARTITION BY a.business_unit, a.region, a.country, a.bundle_product_id ORDER BY a.master_product_id ASC) AS item4
              , NTH_VALUE(a.product_sku, 5)
                          OVER (PARTITION BY a.business_unit, a.region, a.country, a.bundle_product_id ORDER BY a.master_product_id ASC) AS item5
              , NTH_VALUE(a.product_sku, 6)
                          OVER (PARTITION BY a.business_unit, a.region, a.country, a.bundle_product_id ORDER BY a.master_product_id ASC) AS item6
              , NTH_VALUE(a.product_sku, 7)
                          OVER (PARTITION BY a.business_unit, a.region, a.country, a.bundle_product_id ORDER BY a.master_product_id ASC) AS item7
              , NTH_VALUE(a.product_sku, 8)
                          OVER (PARTITION BY a.business_unit, a.region, a.country, a.bundle_product_id ORDER BY a.master_product_id ASC) AS item8
              , NTH_VALUE(a.style_name, 1)
                          OVER (PARTITION BY a.business_unit, a.region, a.country, a.bundle_product_id ORDER BY a.master_product_id ASC) AS item1_name
              , NTH_VALUE(a.style_name, 2)
                          OVER (PARTITION BY a.business_unit, a.region, a.country, a.bundle_product_id ORDER BY a.master_product_id ASC) AS item2_name
              , NTH_VALUE(a.style_name, 3)
                          OVER (PARTITION BY a.business_unit, a.region, a.country, a.bundle_product_id ORDER BY a.master_product_id ASC) AS item3_name
              , NTH_VALUE(a.style_name, 4)
                          OVER (PARTITION BY a.business_unit, a.region, a.country, a.bundle_product_id ORDER BY a.master_product_id ASC) AS item4_name
              , NTH_VALUE(a.style_name, 5)
                          OVER (PARTITION BY a.business_unit, a.region, a.country, a.bundle_product_id ORDER BY a.master_product_id ASC) AS item5_name
              , NTH_VALUE(a.style_name, 6)
                          OVER (PARTITION BY a.business_unit, a.region, a.country, a.bundle_product_id ORDER BY a.master_product_id ASC) AS item6_name
              , NTH_VALUE(a.style_name, 7)
                          OVER (PARTITION BY a.business_unit, a.region, a.country, a.bundle_product_id ORDER BY a.master_product_id ASC) AS item7_name
              , NTH_VALUE(a.style_name, 8)
                          OVER (PARTITION BY a.business_unit, a.region, a.country, a.bundle_product_id ORDER BY a.master_product_id ASC) AS item8_name
              , a.fk_total_cost_of_outfit                                                                                                AS total_cost_of_outfit
              , a.fk_total_cost_of_outfit_tariff                                                                                         AS total_cost_of_outfit_tariff
              , a.fk_outfit_imu                                                                                                          AS outfit_imu
              , a.fk_outfit_imu_tariff                                                                                                   AS outfit_imu_tariff
              , a.fk_contains_shoes                                                                                                      AS contains_shoes
              , a.bundle_image_url                                                                                                       AS large_img_url
              , a.is_bundle_active                                                                                                       AS is_active
              , a.current_bundle_vip_retail                                                                                              AS current_vip_retail
              , a.bundle_msrp                                                                                                            AS msrp
              , a.compontents_number
              , cs.current_showroom
              , a.prepack_components
FROM reporting_prod.gfb.gfb_dim_bundle a
         LEFT JOIN
     (
         SELECT business_unit
              , region
              , country
              , bundle_product_id
              , MAX(current_showroom) AS current_showroom
         FROM reporting_prod.gfb.gfb_dim_bundle
         GROUP BY business_unit
                , region
                , country
                , bundle_product_id
     ) cs ON cs.business_unit = a.business_unit
         AND cs.region = a.region
         AND cs.country = a.country
         AND cs.bundle_product_id = a.bundle_product_id
WHERE a.business_unit = 'FABKIDS';

CREATE OR REPLACE TEMPORARY TABLE _sales_info_place_date AS
SELECT ol.business_unit                                         AS business_unit
     , ol.region                                                AS region
     , ol.country                                               AS country
     , ol.bundle_product_id
     , ol.order_date                                            AS order_date

     --Sales
     , COUNT(DISTINCT ol.bundle_order_line_id)                  AS bundle_unit_sold
     , SUM(ol.total_product_revenue)                            AS total_product_revenue
     , SUM(ol.total_cogs)                                       AS total_cogs

     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                      AS activating_product_revenue
     , COUNT(DISTINCT CASE
                          WHEN ol.order_type = 'vip activating'
                              THEN ol.bundle_order_line_id END) AS activating_bundle_unit_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_cogs
               ELSE 0 END)                                      AS activating_cogs

     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                      AS repeat_product_revenue
     , COUNT(DISTINCT CASE
                          WHEN ol.order_type != 'vip activating'
                              THEN ol.bundle_order_line_id
    END)                                                        AS repeat_bundle_unit_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_cogs
               ELSE 0 END)                                      AS repeat_cogs

     , SUM(ol.total_discount)                                   AS total_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_discount
               ELSE 0 END)                                      AS activating_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_discount
               ELSE 0 END)                                      AS repeat_discount

     , SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                      AS lead_product_revenue
     , COUNT(DISTINCT CASE
                          WHEN ol.order_type = 'ecom'
                              THEN ol.bundle_order_line_id
    END)                                                        AS lead_bundle_unit_sold
     , SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_cogs
               ELSE 0 END)                                      AS lead_cogs
     , SUM(ol.tokens_applied)                                   AS tokens_applied
     , COUNT(DISTINCT CASE
                          WHEN ol.tokens_applied > 0
                              THEN ol.bundle_order_line_id
    END)                                                        AS token_bundle_unit_sold
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                      AS token_product_revenue
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                      AS token_cogs
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                      AS token_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.tokens_applied > 0 THEN ol.total_discount
               ELSE 0 END)                                      AS token_discount
     , SUM(ol.two_for_one_applied)                              AS two_for_one_applied
     , COUNT(DISTINCT CASE
                          WHEN ol.two_for_one_applied > 0
                              THEN ol.bundle_order_line_id
    END)                                                        AS two_for_one_bundle_unit_sold
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                      AS two_for_one_product_revenue
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                      AS two_for_one_cogs
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                      AS two_for_one_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0 THEN ol.total_discount
               ELSE 0 END)                                      AS two_for_one_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                      AS activating_token_product_revenue
     , COUNT(DISTINCT CASE
                          WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                              THEN ol.bundle_order_line_id
    END)                                                        AS activating_token_bundle_unit_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                      AS activating_token_cogs
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                      AS activating_token_product_revenue_with_tariff

     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                      AS repeat_token_product_revenue
     , COUNT(DISTINCT CASE
                          WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                              THEN ol.bundle_order_line_id
    END)                                                        AS repeat_token_bundle_unit_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                      AS repeat_token_cogs
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                      AS repeat_token_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                      AS activating_two_for_one_product_revenue
     , COUNT(DISTINCT CASE
                          WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                              THEN ol.bundle_order_line_id
    END)                                                        AS activating_two_for_one_bundle_units_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                      AS activating_two_for_one_cogs
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                      AS activating_two_for_one_product_revenue_with_tariff

     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                      AS repeat_two_for_one_product_revenue
     , COUNT(DISTINCT CASE
                          WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                              THEN ol.bundle_order_line_id
    END)                                                        AS repeat_two_for_one_bundle_units_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                      AS repeat_two_for_one_cogs
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                      AS repeat_two_for_one_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_discount
               ELSE 0 END)                                      AS activating_token_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_discount
               ELSE 0 END)                                      AS repeat_token_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_discount
               ELSE 0 END)                                      AS activating_two_for_one_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_discount
               ELSE 0 END)                                      AS repeat_two_for_one_discount
     , COUNT(DISTINCT CASE
                          WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1'
                              THEN ol.bundle_order_line_id
    END)                                                        AS one_item_bundle_units_sold
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1'
                   THEN ol.tokens_applied
               ELSE 0 END)                                      AS one_item_token_applied
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                      AS one_item_product_revenue
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1'
                   THEN ol.total_cogs
               ELSE 0 END)                                      AS one_item_cogs
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1'
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                      AS one_item_product_revenue_with_tariff
     , COUNT(DISTINCT CASE
                          WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1' AND
                               ol.order_type != 'vip activating'
                              THEN ol.bundle_order_line_id
    END)                                                        AS repeat_one_item_bundle_units_sold
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1' AND ol.order_type != 'vip activating'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                      AS repeat_one_item_product_revenue
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1' AND ol.order_type != 'vip activating'
                   THEN ol.total_cogs
               ELSE 0 END)                                      AS repeat_one_item_cogs
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1' AND ol.order_type != 'vip activating'
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                      AS repeat_one_item_product_revenue_with_tariff
     , COUNT(DISTINCT CASE
                          WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1' AND
                               ol.order_type = 'vip activating'
                              THEN ol.bundle_order_line_id
    END)                                                        AS activating_one_item_bundle_units_sold
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1' AND ol.order_type = 'vip activating'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                      AS activating_one_item_product_revenue
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1' AND ol.order_type = 'vip activating'
                   THEN ol.total_cogs
               ELSE 0 END)                                      AS activating_one_item_cogs
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1' AND ol.order_type = 'vip activating'
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                      AS activating_one_item_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1'
                   THEN ol.total_discount
               ELSE 0 END)                                      AS one_item_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0 AND ol.bundle_product_id = '-1'
                   THEN ol.total_discount
               ELSE 0 END)                                      AS activating_one_item_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0 AND ol.bundle_product_id = '-1'
                   THEN ol.total_discount
               ELSE 0 END)                                      AS repeat_one_item_discount
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356
                   THEN ol.tokens_applied
               ELSE 0 END)                                      AS three_for_one_applied
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356
                   THEN ol.total_qty_sold
               ELSE 0 END)                                      AS three_for_one_qty_sold
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356
                   THEN ol.token_local_amount
               ELSE 0 END)                                      AS three_for_one_local_amount
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356
                   THEN ol.total_product_revenue
               ELSE 0 END)                                      AS three_for_one_product_revenue
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356
                   THEN ol.total_cogs
               ELSE 0 END)                                      AS three_for_one_cogs
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                      AS three_for_one_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356
                   THEN ol.total_discount
               ELSE 0 END)                                      AS three_for_one_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_product_revenue
               ELSE 0 END)                                      AS activating_three_for_one_product_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_qty_sold
               ELSE 0 END)                                      AS activating_three_for_one_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_cogs
               ELSE 0 END)                                      AS activating_three_for_one_cogs
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                      AS activating_three_for_one_product_revenue_with_tariff

     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_product_revenue
               ELSE 0 END)                                      AS repeat_three_for_one_product_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_qty_sold
               ELSE 0 END)                                      AS repeat_three_for_one_qty_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_cogs
               ELSE 0 END)                                      AS repeat_three_for_one_cogs
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                      AS repeat_three_for_one_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND
                    ol.bundle_product_id = 16819356
                   THEN ol.total_discount
               ELSE 0 END)                                      AS activating_three_for_one_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND
                    ol.bundle_product_id = 16819356
                   THEN ol.total_discount
               ELSE 0 END)                                      AS repeat_three_for_one_discount
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356 THEN ol.total_shipping_revenue
               ELSE 0 END)                                      AS three_for_one_shipping_revenue
     , SUM(CASE
               WHEN ol.bundle_product_id = 16819356 THEN ol.total_shipping_cost
               ELSE 0 END)                                      AS three_for_one_shipping_cost
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                      AS activating_three_for_one_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                      AS activating_three_for_one_shipping_cost
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_shipping_revenue
               ELSE 0 END)                                      AS repeat_three_for_one_shipping_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.bundle_product_id = 16819356
                   THEN ol.total_shipping_cost
               ELSE 0 END)                                      AS repeat_three_for_one_shipping_cost
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date ol
WHERE ol.order_classification = 'product order'
  AND ol.order_date >= $start_date
  AND ol.order_date < $end_date
  AND ol.bundle_product_id IS NOT NULL
GROUP BY ol.business_unit
       , ol.region
       , ol.country
       , ol.bundle_product_id
       , ol.order_date;


CREATE OR REPLACE TEMPORARY TABLE _sales_info_ship_date AS
SELECT ol.business_unit                        AS business_unit
     , ol.region                               AS region
     , ol.country                              AS country
     , ol.bundle_product_id
     , ol.ship_date

     --Sales
     , COUNT(DISTINCT ol.bundle_order_line_id) AS bundle_unit_sold
     , SUM(ol.total_product_revenue)           AS total_product_revenue
     , SUM(ol.total_cogs)                      AS total_cogs

     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_product_revenue
               ELSE 0 END)                     AS activating_product_revenue
     , COUNT(DISTINCT CASE
                          WHEN ol.order_type = 'vip activating'
                              THEN ol.bundle_order_line_id
    END)                                       AS activating_bundle_unit_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_cogs
               ELSE 0 END)                     AS activating_cogs

     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_product_revenue
               ELSE 0 END)                     AS repeat_product_revenue
     , COUNT(DISTINCT CASE
                          WHEN ol.order_type != 'vip activating'
                              THEN ol.bundle_order_line_id
    END)                                       AS repeat_bundle_unit_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_cogs
               ELSE 0 END)                     AS repeat_cogs

     , SUM(ol.total_discount)                  AS total_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_discount
               ELSE 0 END)                     AS activating_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_discount
               ELSE 0 END)                     AS repeat_discount

     , SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_product_revenue
               ELSE 0 END)                     AS lead_product_revenue
     , COUNT(DISTINCT CASE
                          WHEN ol.order_type = 'ecom'
                              THEN ol.bundle_order_line_id
    END)                                       AS lead_bundle_unit_sold
     , SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_cogs
               ELSE 0 END)                     AS lead_cogs
FROM reporting_prod.gfb.gfb_order_line_data_set_ship_date ol
WHERE ol.order_classification = 'product order'
  AND ol.ship_date >= $start_date
  AND ol.ship_date < $end_date
  AND ol.bundle_product_id IS NOT NULL
GROUP BY ol.business_unit
       , ol.region
       , ol.country
       , ol.bundle_product_id
       , ol.ship_date;


CREATE OR REPLACE TEMPORARY TABLE _return_info AS
SELECT ol.business_unit                        AS business_unit
     , ol.region                               AS region
     , ol.country                              AS country
     , ol.bundle_product_id
     , ol.return_date                          AS return_date

     , COUNT(DISTINCT ol.bundle_order_line_id) AS total_bundle_return_unit
     , SUM(ol.total_return_dollars)            AS total_return_dollars

     , COUNT(DISTINCT CASE
                          WHEN ol.order_type = 'vip activating'
                              THEN ol.bundle_order_line_id
    END)                                       AS activating_bundle_return_units
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_return_dollars
               ELSE 0 END)                     AS activating_return_dollars

     , COUNT(DISTINCT CASE
                          WHEN ol.order_type != 'vip activating'
                              THEN ol.bundle_order_line_id
    END)                                       AS repeat_bundle_return_units
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_return_dollars
               ELSE 0 END)                     AS repeat_return_dollars
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date ol
WHERE ol.order_classification IN ('product order', 'exchange', 'reship')
  AND ol.return_date >= $start_date
  AND ol.return_date < $end_date
  AND ol.bundle_product_id IS NOT NULL
GROUP BY ol.business_unit
       , ol.region
       , ol.country
       , ol.bundle_product_id
       , ol.return_date;


CREATE OR REPLACE TEMPORARY TABLE _inventory_info AS
SELECT id.business_unit
     , id.region                     AS region
     , id.country                    AS country
     , id.product_sku
     , id.inventory_date             AS inventory_date

     , SUM(id.qty_onhand)            AS qty_onhand
     , SUM(id.qty_available_to_sell) AS qty_available_to_sell
     , SUM(id.qty_open_to_buy)       AS qty_open_to_buy
FROM reporting_prod.gfb.gfb_inventory_data_set id
WHERE id.inventory_date >= $start_date
  AND id.inventory_date < $end_date
GROUP BY id.business_unit
       , id.region
       , id.country
       , id.product_sku
       , id.inventory_date;


CREATE OR REPLACE TEMPORARY TABLE _bundle_component_inventory_1 AS
SELECT bp.business_unit
     , bp.region
     , bp.country
     , bp.bundle_product_id
     , ii.inventory_date

     , COALESCE(ii.qty_onhand, 0)            AS component_qty_onhand
     , COALESCE(ii.qty_available_to_sell, 0) AS component_qty_available_to_sell
FROM _fk_bundle_info bp
         JOIN _inventory_info ii
              ON ii.business_unit = bp.business_unit
                  AND ii.region = bp.region
                  AND ii.country = bp.country
                  AND ii.product_sku = bp.item1;


CREATE OR REPLACE TEMPORARY TABLE _bundle_component_inventory_2 AS
SELECT bp.business_unit
     , bp.region
     , bp.country
     , bp.bundle_product_id
     , ii.inventory_date

     , COALESCE(ii.qty_onhand, 0)            AS component_qty_onhand
     , COALESCE(ii.qty_available_to_sell, 0) AS component_qty_available_to_sell
FROM _fk_bundle_info bp
         JOIN _inventory_info ii
              ON ii.business_unit = bp.business_unit
                  AND ii.region = bp.region
                  AND ii.country = bp.country
                  AND ii.product_sku = bp.item2;


CREATE OR REPLACE TEMPORARY TABLE _bundle_component_inventory_3 AS
SELECT bp.business_unit
     , bp.region
     , bp.country
     , bp.bundle_product_id
     , ii.inventory_date

     , COALESCE(ii.qty_onhand, 0)            AS component_qty_onhand
     , COALESCE(ii.qty_available_to_sell, 0) AS component_qty_available_to_sell
FROM _fk_bundle_info bp
         JOIN _inventory_info ii
              ON ii.business_unit = bp.business_unit
                  AND ii.region = bp.region
                  AND ii.country = bp.country
                  AND ii.product_sku = bp.item3;


CREATE OR REPLACE TEMPORARY TABLE _bundle_component_inventory_4 AS
SELECT bp.business_unit
     , bp.region
     , bp.country
     , bp.bundle_product_id
     , ii.inventory_date

     , COALESCE(ii.qty_onhand, 0)            AS component_qty_onhand
     , COALESCE(ii.qty_available_to_sell, 0) AS component_qty_available_to_sell
FROM _fk_bundle_info bp
         JOIN _inventory_info ii
              ON ii.business_unit = bp.business_unit
                  AND ii.region = bp.region
                  AND ii.country = bp.country
                  AND ii.product_sku = bp.item4;


CREATE OR REPLACE TEMPORARY TABLE _bundle_component_inventory_5 AS
SELECT bp.business_unit
     , bp.region
     , bp.country
     , bp.bundle_product_id
     , ii.inventory_date

     , COALESCE(ii.qty_onhand, 0)            AS component_qty_onhand
     , COALESCE(ii.qty_available_to_sell, 0) AS component_qty_available_to_sell
FROM _fk_bundle_info bp
         JOIN _inventory_info ii
              ON ii.business_unit = bp.business_unit
                  AND ii.region = bp.region
                  AND ii.country = bp.country
                  AND ii.product_sku = bp.item5;


CREATE OR REPLACE TEMPORARY TABLE _bundle_component_inventory_6 AS
SELECT bp.business_unit
     , bp.region
     , bp.country
     , bp.bundle_product_id
     , ii.inventory_date

     , COALESCE(ii.qty_onhand, 0)            AS component_qty_onhand
     , COALESCE(ii.qty_available_to_sell, 0) AS component_qty_available_to_sell
FROM _fk_bundle_info bp
         JOIN _inventory_info ii
              ON ii.business_unit = bp.business_unit
                  AND ii.region = bp.region
                  AND ii.country = bp.country
                  AND ii.product_sku = bp.item6;


CREATE OR REPLACE TEMPORARY TABLE _bundle_component_inventory_7 AS
SELECT bp.business_unit
     , bp.region
     , bp.country
     , bp.bundle_product_id
     , ii.inventory_date

     , COALESCE(ii.qty_onhand, 0)            AS component_qty_onhand
     , COALESCE(ii.qty_available_to_sell, 0) AS component_qty_available_to_sell
FROM _fk_bundle_info bp
         JOIN _inventory_info ii
              ON ii.business_unit = bp.business_unit
                  AND ii.region = bp.region
                  AND ii.country = bp.country
                  AND ii.product_sku = bp.item7;


CREATE OR REPLACE TEMPORARY TABLE _bundle_component_inventory_8 AS
SELECT bp.business_unit
     , bp.region
     , bp.country
     , bp.bundle_product_id
     , ii.inventory_date

     , COALESCE(ii.qty_onhand, 0)            AS component_qty_onhand
     , COALESCE(ii.qty_available_to_sell, 0) AS component_qty_available_to_sell
FROM _fk_bundle_info bp
         JOIN _inventory_info ii
              ON ii.business_unit = bp.business_unit
                  AND ii.region = bp.region
                  AND ii.country = bp.country
                  AND ii.product_sku = bp.item8;


CREATE OR REPLACE TEMPORARY TABLE _bundle_inventory AS
SELECT ci1.business_unit
     , ci1.region
     , ci1.country
     , COALESCE(
    ci1.bundle_product_id
    , ci2.bundle_product_id
    , ci3.bundle_product_id
    , ci4.bundle_product_id
    , ci5.bundle_product_id
    , ci6.bundle_product_id
    , ci7.bundle_product_id
    , ci8.bundle_product_id
    ) AS bundle_product_id
     , COALESCE(
    ci1.inventory_date
    , ci2.inventory_date
    , ci3.inventory_date
    , ci4.inventory_date
    , ci5.inventory_date
    , ci6.inventory_date
    , ci7.inventory_date
    , ci8.inventory_date
    ) AS bundle_inventory_date

-- update from 0 to 9999999 so that any component with null value won't result in least value being selected as 0
     , LEAST(
    COALESCE(ci1.component_qty_onhand, 9999999)
    , COALESCE(ci2.component_qty_onhand, 9999999)
    , COALESCE(ci3.component_qty_onhand, 9999999)
    , COALESCE(ci4.component_qty_onhand, 9999999)
    , COALESCE(ci5.component_qty_onhand, 9999999)
    , COALESCE(ci6.component_qty_onhand, 9999999)
    , COALESCE(ci7.component_qty_onhand, 9999999)
    , COALESCE(ci8.component_qty_onhand, 9999999)
    ) AS bundle_qty_onhand
     , LEAST(
    COALESCE(ci1.component_qty_available_to_sell, 9999999)
    , COALESCE(ci2.component_qty_available_to_sell, 9999999)
    , COALESCE(ci3.component_qty_available_to_sell, 9999999)
    , COALESCE(ci4.component_qty_available_to_sell, 9999999)
    , COALESCE(ci5.component_qty_available_to_sell, 9999999)
    , COALESCE(ci6.component_qty_available_to_sell, 9999999)
    , COALESCE(ci7.component_qty_available_to_sell, 9999999)
    , COALESCE(ci8.component_qty_available_to_sell, 9999999)
    ) AS bundle_qty_available_to_sell

FROM _bundle_component_inventory_1 ci1
         FULL JOIN _bundle_component_inventory_2 ci2
                   ON ci2.business_unit = ci1.business_unit
                       AND ci2.region = ci1.region
                       AND ci2.country = ci1.country
                       AND ci2.inventory_date = ci1.inventory_date
                       AND ci2.bundle_product_id = ci1.bundle_product_id
         FULL JOIN _bundle_component_inventory_3 ci3
                   ON ci3.business_unit = ci1.business_unit
                       AND ci3.region = ci1.region
                       AND ci3.country = ci1.country
                       AND ci3.inventory_date = ci1.inventory_date
                       AND ci3.bundle_product_id = ci1.bundle_product_id
         FULL JOIN _bundle_component_inventory_4 ci4
                   ON ci4.business_unit = ci1.business_unit
                       AND ci4.region = ci1.region
                       AND ci4.country = ci1.country
                       AND ci4.inventory_date = ci1.inventory_date
                       AND ci4.bundle_product_id = ci1.bundle_product_id
         FULL JOIN _bundle_component_inventory_5 ci5
                   ON ci5.business_unit = ci1.business_unit
                       AND ci5.region = ci1.region
                       AND ci5.country = ci1.country
                       AND ci5.inventory_date = ci1.inventory_date
                       AND ci5.bundle_product_id = ci1.bundle_product_id
         FULL JOIN _bundle_component_inventory_6 ci6
                   ON ci6.business_unit = ci1.business_unit
                       AND ci6.region = ci1.region
                       AND ci6.country = ci1.country
                       AND ci6.inventory_date = ci1.inventory_date
                       AND ci6.bundle_product_id = ci1.bundle_product_id
         FULL JOIN _bundle_component_inventory_7 ci7
                   ON ci7.business_unit = ci1.business_unit
                       AND ci7.region = ci1.region
                       AND ci7.country = ci1.country
                       AND ci7.inventory_date = ci1.inventory_date
                       AND ci7.bundle_product_id = ci1.bundle_product_id
         FULL JOIN _bundle_component_inventory_8 ci8
                   ON ci8.business_unit = ci1.business_unit
                       AND ci8.region = ci1.region
                       AND ci8.country = ci1.country
                       AND ci8.inventory_date = ci1.inventory_date
                       AND ci8.bundle_product_id = ci1.bundle_product_id
WHERE bundle_qty_onhand > 0
   OR bundle_qty_available_to_sell > 0;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb026_product_bundle_data_set_place_date AS
SELECT main_data.business_unit_for_join AS business_unit
     , main_data.region_for_join        AS region
     , main_data.country_for_join       AS country
     , main_data.date

     , mdbp.bundle_product_id
     , mdbp.outfit_alias
     , mdbp.site_name
     , mdbp.bundle_name
     , mdbp.vip_retail
     , mdbp.type
     , mdbp.gender
     , mdbp.active_inactive
     , mdbp.is_active
     , mdbp.outfit_vs_box_vs_pack
     , mdbp.compontents_number
     , mdbp.large_img_url
     , mdbp.total_cost_of_outfit
     , mdbp.current_vip_retail
     , mdbp.msrp
     , mdbp.current_showroom
     , mdbp.prepack_components
     , mdbp.item1                       AS bundle_component_product_sku_1
     , mdbp.item2                       AS bundle_component_product_sku_2
     , mdbp.item3                       AS bundle_component_product_sku_3
     , mdbp.item4                       AS bundle_component_product_sku_4
     , mdbp.item5                       AS bundle_component_product_sku_5
     , mdbp.item6                       AS bundle_component_product_sku_6
     , mdbp.item7                       AS bundle_component_product_sku_7
     , mdbp.item8                       AS bundle_component_product_sku_8
     , mdbp.item1_name                  AS bundle_component_sku_name_1
     , mdbp.item2_name                  AS bundle_component_sku_name_2
     , mdbp.item3_name                  AS bundle_component_sku_name_3
     , mdbp.item4_name                  AS bundle_component_sku_name_4
     , mdbp.item5_name                  AS bundle_component_sku_name_5
     , mdbp.item6_name                  AS bundle_component_sku_name_6
     , mdbp.item7_name                  AS bundle_component_sku_name_7
     , mdbp.item8_name                  AS bundle_component_sku_name_8

     , main_data.bundle_unit_sold
     , main_data.total_product_revenue
     , main_data.total_cogs
     , main_data.activating_product_revenue
     , main_data.activating_bundle_unit_sold
     , main_data.activating_cogs
     , main_data.repeat_product_revenue
     , main_data.repeat_bundle_unit_sold
     , main_data.repeat_cogs
     , main_data.total_discount
     , main_data.activating_discount
     , main_data.repeat_discount
     , main_data.lead_bundle_unit_sold
     , main_data.lead_product_revenue
     , main_data.lead_cogs
     , main_data.tokens_applied
     , main_data.token_bundle_units_sold
     , main_data.token_product_revenue
     , main_data.token_product_revenue_with_tariff
     , main_data.token_cogs
     , main_data.token_discount
     , main_data.two_for_one_applied
     , main_data.two_for_one_bundle_units_sold
     , main_data.two_for_one_product_revenue
     , main_data.two_for_one_product_revenue_with_tariff
     , main_data.two_for_one_cogs
     , main_data.two_for_one_discount
     , main_data.activating_token_bundle_units_sold
     , main_data.activating_token_product_revenue
     , main_data.activating_token_product_revenue_with_tariff
     , main_data.activating_token_cogs
     , main_data.activating_token_discount
     , main_data.repeat_token_bundle_units_sold
     , main_data.repeat_token_product_revenue
     , main_data.repeat_token_product_revenue_with_tariff
     , main_data.repeat_token_cogs
     , main_data.repeat_token_discount
     , main_data.activating_two_for_one_bundle_units_sold
     , main_data.activating_two_for_one_product_revenue
     , main_data.activating_two_for_one_product_revenue_with_tariff
     , main_data.activating_two_for_one_cogs
     , main_data.activating_two_for_one_discount
     , main_data.repeat_two_for_one_bundle_units_sold
     , main_data.repeat_two_for_one_product_revenue
     , main_data.repeat_two_for_one_product_revenue_with_tariff
     , main_data.repeat_two_for_one_cogs
     , main_data.repeat_two_for_one_discount
     , main_data.one_item_bundle_units_sold
     , main_data.one_item_cogs
     , main_data.one_item_product_revenue
     , main_data.one_item_product_revenue_with_tariff
     , main_data.one_item_discount
     , main_data.repeat_one_item_bundle_units_sold
     , main_data.repeat_one_item_cogs
     , main_data.repeat_one_item_product_revenue
     , main_data.repeat_one_item_product_revenue_with_tariff
     , main_data.repeat_one_item_discount
     , main_data.activating_one_item_product_revenue_with_tariff
     , main_data.activating_one_item_cogs
     , main_data.activating_one_item_product_revenue
     , main_data.activating_one_item_bundle_units_sold
     , main_data.activating_one_item_discount
     , main_data.three_for_one_applied
     , main_data.three_for_one_qty_sold
     , main_data.three_for_one_local_amount
     , main_data.three_for_one_product_revenue
     , main_data.three_for_one_cogs
     , main_data.three_for_one_product_revenue_with_tariff
     , main_data.three_for_one_discount
     , main_data.activating_three_for_one_product_revenue
     , main_data.activating_three_for_one_qty_sold
     , main_data.activating_three_for_one_cogs
     , main_data.activating_three_for_one_product_revenue_with_tariff
     , main_data.repeat_three_for_one_product_revenue
     , main_data.repeat_three_for_one_qty_sold
     , main_data.repeat_three_for_one_cogs
     , main_data.repeat_three_for_one_product_revenue_with_tariff
     , main_data.activating_three_for_one_discount
     , main_data.repeat_three_for_one_discount
     , main_data.three_for_one_shipping_revenue
     , main_data.three_for_one_shipping_cost
     , main_data.activating_three_for_one_shipping_revenue
     , main_data.activating_three_for_one_shipping_cost
     , main_data.repeat_three_for_one_shipping_revenue
     , main_data.repeat_three_for_one_shipping_cost

     , main_data.total_bundle_return_unit
     , main_data.total_return_dollars
     , main_data.activating_bundle_return_units
     , main_data.activating_return_dollars
     , main_data.repeat_bundle_return_units
     , main_data.repeat_return_dollars

     , main_data.bundle_qty_onhand
     , main_data.bundle_qty_available_to_sell

FROM (
         SELECT COALESCE(si.business_unit, ri.business_unit, ii.business_unit)             AS business_unit_for_join
              , COALESCE(si.region, ri.region, ii.region)                                  AS region_for_join
              , COALESCE(si.country, ri.country, ii.country)                               AS country_for_join
              , COALESCE(si.order_date, ri.return_date, ii.bundle_inventory_date)          AS date
              , COALESCE(si.bundle_product_id, ri.bundle_product_id, ii.bundle_product_id) AS bundle_product_id

              , COALESCE(si.bundle_unit_sold, 0)                                           AS bundle_unit_sold
              , COALESCE(si.total_product_revenue, 0)                                      AS total_product_revenue
              , COALESCE(si.total_cogs, 0)                                                 AS total_cogs
              , COALESCE(si.activating_product_revenue, 0)                                 AS activating_product_revenue
              , COALESCE(si.activating_bundle_unit_sold, 0)                                AS activating_bundle_unit_sold
              , COALESCE(si.activating_cogs, 0)                                            AS activating_cogs
              , COALESCE(si.repeat_product_revenue, 0)                                     AS repeat_product_revenue
              , COALESCE(si.repeat_bundle_unit_sold, 0)                                    AS repeat_bundle_unit_sold
              , COALESCE(si.repeat_cogs, 0)                                                AS repeat_cogs
              , COALESCE(si.total_discount, 0)                                             AS total_discount
              , COALESCE(si.activating_discount, 0)                                        AS activating_discount
              , COALESCE(si.repeat_discount, 0)                                            AS repeat_discount
              , COALESCE(si.lead_bundle_unit_sold, 0)                                      AS lead_bundle_unit_sold
              , COALESCE(si.lead_product_revenue, 0)                                       AS lead_product_revenue
              , COALESCE(si.lead_cogs, 0)                                                  AS lead_cogs
              , COALESCE(si.tokens_applied, 0)                                             AS tokens_applied
              , COALESCE(si.token_bundle_unit_sold, 0)                                     AS token_bundle_units_sold
              , COALESCE(si.token_product_revenue, 0)                                      AS token_product_revenue
              , COALESCE(si.token_product_revenue_with_tariff, 0)                          AS token_product_revenue_with_tariff
              , COALESCE(si.token_cogs, 0)                                                 AS token_cogs
              , COALESCE(si.token_discount, 0)                                             AS token_discount
              , COALESCE(si.two_for_one_applied, 0)                                        AS two_for_one_applied
              , COALESCE(si.two_for_one_bundle_unit_sold, 0)                               AS two_for_one_bundle_units_sold
              , COALESCE(si.two_for_one_product_revenue, 0)                                AS two_for_one_product_revenue
              , COALESCE(si.two_for_one_product_revenue_with_tariff, 0)                    AS two_for_one_product_revenue_with_tariff
              , COALESCE(si.two_for_one_cogs, 0)                                           AS two_for_one_cogs
              , COALESCE(si.two_for_one_discount, 0)                                       AS two_for_one_discount
              , COALESCE(si.activating_token_bundle_unit_sold, 0)                          AS activating_token_bundle_units_sold
              , COALESCE(si.activating_token_product_revenue, 0)                           AS activating_token_product_revenue
              , COALESCE(si.activating_token_product_revenue_with_tariff, 0)               AS activating_token_product_revenue_with_tariff
              , COALESCE(si.activating_token_cogs, 0)                                      AS activating_token_cogs
              , COALESCE(si.activating_token_discount, 0)                                  AS activating_token_discount
              , COALESCE(si.repeat_token_bundle_unit_sold, 0)                              AS repeat_token_bundle_units_sold
              , COALESCE(si.repeat_token_product_revenue, 0)                               AS repeat_token_product_revenue
              , COALESCE(si.repeat_token_product_revenue_with_tariff, 0)                   AS repeat_token_product_revenue_with_tariff
              , COALESCE(si.repeat_token_cogs, 0)                                          AS repeat_token_cogs
              , COALESCE(si.repeat_token_discount, 0)                                      AS repeat_token_discount
              , COALESCE(si.activating_two_for_one_bundle_units_sold, 0)                   AS activating_two_for_one_bundle_units_sold
              , COALESCE(si.activating_two_for_one_product_revenue, 0)                     AS activating_two_for_one_product_revenue
              , COALESCE(si.activating_two_for_one_product_revenue_with_tariff, 0)         AS activating_two_for_one_product_revenue_with_tariff
              , COALESCE(si.activating_two_for_one_cogs, 0)                                AS activating_two_for_one_cogs
              , COALESCE(si.activating_two_for_one_discount, 0)                            AS activating_two_for_one_discount
              , COALESCE(si.repeat_two_for_one_bundle_units_sold, 0)                       AS repeat_two_for_one_bundle_units_sold
              , COALESCE(si.repeat_two_for_one_product_revenue, 0)                         AS repeat_two_for_one_product_revenue
              , COALESCE(si.repeat_two_for_one_product_revenue_with_tariff, 0)             AS repeat_two_for_one_product_revenue_with_tariff
              , COALESCE(si.repeat_two_for_one_cogs, 0)                                    AS repeat_two_for_one_cogs
              , COALESCE(si.repeat_two_for_one_discount, 0)                                AS repeat_two_for_one_discount
              , COALESCE(si.one_item_bundle_units_sold, 0)                                 AS one_item_bundle_units_sold
              , COALESCE(si.one_item_cogs, 0)                                              AS one_item_cogs
              , COALESCE(si.one_item_product_revenue, 0)                                   AS one_item_product_revenue
              , COALESCE(si.one_item_product_revenue_with_tariff, 0)                       AS one_item_product_revenue_with_tariff
              , COALESCE(si.one_item_discount, 0)                                          AS one_item_discount
              , COALESCE(si.repeat_one_item_bundle_units_sold, 0)                          AS repeat_one_item_bundle_units_sold
              , COALESCE(si.repeat_one_item_cogs, 0)                                       AS repeat_one_item_cogs
              , COALESCE(si.repeat_one_item_product_revenue, 0)                            AS repeat_one_item_product_revenue
              , COALESCE(si.repeat_one_item_product_revenue_with_tariff, 0)                AS repeat_one_item_product_revenue_with_tariff
              , COALESCE(si.repeat_one_item_discount, 0)                                   AS repeat_one_item_discount
              , COALESCE(activating_one_item_product_revenue_with_tariff, 0)               AS activating_one_item_product_revenue_with_tariff
              , COALESCE(activating_one_item_cogs, 0)                                      AS activating_one_item_cogs
              , COALESCE(activating_one_item_product_revenue, 0)                           AS activating_one_item_product_revenue
              , COALESCE(activating_one_item_bundle_units_sold, 0)                         AS activating_one_item_bundle_units_sold
              , COALESCE(activating_one_item_discount, 0)                                  AS activating_one_item_discount
              , COALESCE(three_for_one_applied, 0)                                         AS three_for_one_applied
              , COALESCE(three_for_one_qty_sold, 0)                                        AS three_for_one_qty_sold
              , COALESCE(three_for_one_local_amount, 0)                                    AS three_for_one_local_amount
              , COALESCE(three_for_one_product_revenue, 0)                                 AS three_for_one_product_revenue
              , COALESCE(three_for_one_cogs, 0)                                            AS three_for_one_cogs
              , COALESCE(three_for_one_product_revenue_with_tariff, 0)                     AS three_for_one_product_revenue_with_tariff
              , COALESCE(three_for_one_discount, 0)                                        AS three_for_one_discount
              , COALESCE(activating_three_for_one_product_revenue, 0)                      AS activating_three_for_one_product_revenue
              , COALESCE(activating_three_for_one_qty_sold, 0)                             AS activating_three_for_one_qty_sold
              , COALESCE(activating_three_for_one_cogs, 0)                                 AS activating_three_for_one_cogs
              , COALESCE(activating_three_for_one_product_revenue_with_tariff, 0)          AS activating_three_for_one_product_revenue_with_tariff
              , COALESCE(repeat_three_for_one_product_revenue, 0)                          AS repeat_three_for_one_product_revenue
              , COALESCE(repeat_three_for_one_qty_sold, 0)                                 AS repeat_three_for_one_qty_sold
              , COALESCE(repeat_three_for_one_cogs, 0)                                     AS repeat_three_for_one_cogs
              , COALESCE(repeat_three_for_one_product_revenue_with_tariff, 0)              AS repeat_three_for_one_product_revenue_with_tariff
              , COALESCE(activating_three_for_one_discount, 0)                             AS activating_three_for_one_discount
              , COALESCE(repeat_three_for_one_discount, 0)                                 AS repeat_three_for_one_discount
              , COALESCE(three_for_one_shipping_revenue, 0)                                AS three_for_one_shipping_revenue
              , COALESCE(three_for_one_shipping_cost, 0)                                   AS three_for_one_shipping_cost
              , COALESCE(activating_three_for_one_shipping_revenue, 0)                     AS activating_three_for_one_shipping_revenue
              , COALESCE(activating_three_for_one_shipping_cost, 0)                        AS activating_three_for_one_shipping_cost
              , COALESCE(repeat_three_for_one_shipping_revenue, 0)                         AS repeat_three_for_one_shipping_revenue
              , COALESCE(repeat_three_for_one_shipping_cost, 0)                            AS repeat_three_for_one_shipping_cost

              , COALESCE(ri.total_bundle_return_unit, 0)                                   AS total_bundle_return_unit
              , COALESCE(ri.total_return_dollars, 0)                                       AS total_return_dollars
              , COALESCE(ri.activating_bundle_return_units, 0)                             AS activating_bundle_return_units
              , COALESCE(ri.activating_return_dollars, 0)                                  AS activating_return_dollars
              , COALESCE(ri.repeat_bundle_return_units, 0)                                 AS repeat_bundle_return_units
              , COALESCE(ri.repeat_return_dollars, 0)                                      AS repeat_return_dollars

              , COALESCE(ii.bundle_qty_onhand, 0)                                          AS bundle_qty_onhand
              , COALESCE(ii.bundle_qty_available_to_sell, 0)                               AS bundle_qty_available_to_sell
         FROM _sales_info_place_date si
                  FULL JOIN _return_info ri
                            ON ri.business_unit = si.business_unit
                                AND ri.region = si.region
                                AND ri.country = si.country
                                AND ri.return_date = si.order_date
                                AND ri.bundle_product_id = si.bundle_product_id
                  FULL JOIN _bundle_inventory ii
                            ON ii.business_unit = si.business_unit
                                AND ii.region = si.region
                                AND ii.country = si.country
                                AND ii.bundle_product_id = si.bundle_product_id
                                AND ii.bundle_inventory_date = si.order_date
     ) main_data
         JOIN _fk_bundle_info mdbp
              ON mdbp.bundle_product_id = main_data.bundle_product_id
                  AND mdbp.business_unit = main_data.business_unit_for_join
                  AND mdbp.region = main_data.region_for_join
                  AND mdbp.country = main_data.country_for_join;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb026_product_bundle_data_set_shipped_date AS
SELECT main_data.business_unit_for_join AS business_unit
     , main_data.region_for_join        AS region
     , main_data.country_for_join       AS country
     , main_data.date

     , mdbp.bundle_product_id
     , mdbp.outfit_alias
     , mdbp.site_name
     , mdbp.bundle_name
     , mdbp.vip_retail
     , mdbp.type
     , mdbp.gender
     , mdbp.active_inactive
     , mdbp.is_active
     , mdbp.outfit_vs_box_vs_pack
     , mdbp.compontents_number
     , mdbp.large_img_url
     , mdbp.total_cost_of_outfit
     , mdbp.current_vip_retail
     , mdbp.msrp
     , mdbp.current_showroom
     , mdbp.prepack_components
     , mdbp.item1                       AS bundle_component_product_sku_1
     , mdbp.item2                       AS bundle_component_product_sku_2
     , mdbp.item3                       AS bundle_component_product_sku_3
     , mdbp.item4                       AS bundle_component_product_sku_4
     , mdbp.item5                       AS bundle_component_product_sku_5
     , mdbp.item6                       AS bundle_component_product_sku_6
     , mdbp.item7                       AS bundle_component_product_sku_7
     , mdbp.item8                       AS bundle_component_product_sku_8
     , mdbp.item1_name                  AS bundle_component_sku_name_1
     , mdbp.item2_name                  AS bundle_component_sku_name_2
     , mdbp.item3_name                  AS bundle_component_sku_name_3
     , mdbp.item4_name                  AS bundle_component_sku_name_4
     , mdbp.item5_name                  AS bundle_component_sku_name_5
     , mdbp.item6_name                  AS bundle_component_sku_name_6
     , mdbp.item7_name                  AS bundle_component_sku_name_7
     , mdbp.item8_name                  AS bundle_component_sku_name_8

     , main_data.bundle_unit_sold
     , main_data.total_product_revenue
     , main_data.total_cogs
     , main_data.activating_product_revenue
     , main_data.activating_bundle_unit_sold
     , main_data.activating_cogs
     , main_data.repeat_product_revenue
     , main_data.repeat_bundle_unit_sold
     , main_data.repeat_cogs
     , main_data.total_discount
     , main_data.activating_discount
     , main_data.repeat_discount
     , main_data.lead_bundle_unit_sold
     , main_data.lead_product_revenue
     , main_data.lead_cogs

     , main_data.total_bundle_return_unit
     , main_data.total_return_dollars
     , main_data.activating_bundle_return_units
     , main_data.activating_return_dollars
     , main_data.repeat_bundle_return_units
     , main_data.repeat_return_dollars

     , main_data.bundle_qty_onhand
     , main_data.bundle_qty_available_to_sell

FROM (
         SELECT COALESCE(si.business_unit, ri.business_unit, ii.business_unit)             AS business_unit_for_join
              , COALESCE(si.region, ri.region, ii.region)                                  AS region_for_join
              , COALESCE(si.country, ri.country, ii.country)                               AS country_for_join
              , COALESCE(si.ship_date, ri.return_date, ii.bundle_inventory_date)           AS date
              , COALESCE(si.bundle_product_id, ri.bundle_product_id, ii.bundle_product_id) AS bundle_product_id

              , COALESCE(si.bundle_unit_sold, 0)                                           AS bundle_unit_sold
              , COALESCE(si.total_product_revenue, 0)                                      AS total_product_revenue
              , COALESCE(si.total_cogs, 0)                                                 AS total_cogs
              , COALESCE(si.activating_product_revenue, 0)                                 AS activating_product_revenue
              , COALESCE(si.activating_bundle_unit_sold, 0)                                AS activating_bundle_unit_sold
              , COALESCE(si.activating_cogs, 0)                                            AS activating_cogs
              , COALESCE(si.repeat_product_revenue, 0)                                     AS repeat_product_revenue
              , COALESCE(si.repeat_bundle_unit_sold, 0)                                    AS repeat_bundle_unit_sold
              , COALESCE(si.repeat_cogs, 0)                                                AS repeat_cogs
              , COALESCE(si.total_discount, 0)                                             AS total_discount
              , COALESCE(si.activating_discount, 0)                                        AS activating_discount
              , COALESCE(si.repeat_discount, 0)                                            AS repeat_discount
              , COALESCE(si.lead_bundle_unit_sold, 0)                                      AS lead_bundle_unit_sold
              , COALESCE(si.lead_product_revenue, 0)                                       AS lead_product_revenue
              , COALESCE(si.lead_cogs, 0)                                                  AS lead_cogs

              , COALESCE(ri.total_bundle_return_unit, 0)                                   AS total_bundle_return_unit
              , COALESCE(ri.total_return_dollars, 0)                                       AS total_return_dollars
              , COALESCE(ri.activating_bundle_return_units, 0)                             AS activating_bundle_return_units
              , COALESCE(ri.activating_return_dollars, 0)                                  AS activating_return_dollars
              , COALESCE(ri.repeat_bundle_return_units, 0)                                 AS repeat_bundle_return_units
              , COALESCE(ri.repeat_return_dollars, 0)                                      AS repeat_return_dollars

              , COALESCE(ii.bundle_qty_onhand, 0)                                          AS bundle_qty_onhand
              , COALESCE(ii.bundle_qty_available_to_sell, 0)                               AS bundle_qty_available_to_sell
         FROM _sales_info_ship_date si
                  FULL JOIN _return_info ri
                            ON ri.business_unit = si.business_unit
                                AND ri.region = si.region
                                AND ri.country = si.country
                                AND ri.return_date = si.ship_date
                                AND ri.bundle_product_id = si.bundle_product_id
                  FULL JOIN _bundle_inventory ii
                            ON ii.business_unit = si.business_unit
                                AND ii.region = si.region
                                AND ii.country = si.country
                                AND ii.bundle_product_id = si.bundle_product_id
                                AND ii.bundle_inventory_date = si.ship_date
     ) main_data
         JOIN _fk_bundle_info mdbp
              ON mdbp.bundle_product_id = main_data.bundle_product_id
                  AND mdbp.business_unit = main_data.business_unit_for_join
                  AND mdbp.region = main_data.region_for_join
                  AND mdbp.country = main_data.country_for_join;
