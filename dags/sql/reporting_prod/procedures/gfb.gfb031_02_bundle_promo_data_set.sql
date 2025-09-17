SET start_date = DATE_TRUNC(YEAR, DATEADD(YEAR, -2, CURRENT_DATE()));

CREATE OR REPLACE TEMPORARY TABLE _bundle_promo_data AS
SELECT pds.business_unit
     , pds.region
     , pds.country
     , pds.order_date
     , pds.order_type
     , pds.promo_order_flag
     , pds.promo_code_1                                                                          AS promo_code
     , pds.promo_1_offer                                                                         AS promo_offer
     , pds.promo_1_group                                                                         AS promo_group
     , pds.promo_1_goal                                                                          AS promo_goal
     , pds.activating_offers
     , olp.bundle_product_id
     , COUNT(DISTINCT pds.order_id)                                                              AS orders
     , SUM(pds.total_qty_sold)                                                                   AS total_qty_sold
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
       , pds.region
       , pds.country
       , pds.order_date
       , pds.order_type
       , pds.promo_order_flag
       , pds.promo_code_1
       , pds.promo_1_offer
       , pds.promo_1_group
       , pds.promo_1_goal
       , pds.activating_offers
       , olp.bundle_product_id
UNION
SELECT pds.business_unit
     , pds.region
     , pds.country
     , pds.order_date
     , pds.order_type
     , pds.promo_order_flag
     , pds.promo_code_2                                                                          AS promo_code
     , pds.promo_2_offer                                                                         AS promo_offer
     , pds.promo_2_group                                                                         AS promo_group
     , pds.promo_2_goal                                                                          AS promo_goal
     , pds.activating_offers
     , olp.bundle_product_id
     , COUNT(DISTINCT pds.order_id)                                                              AS orders
     , SUM(pds.total_qty_sold)                                                                   AS total_qty_sold
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
       , pds.region
       , pds.country
       , pds.order_date
       , pds.order_type
       , pds.promo_order_flag
       , pds.promo_code_2
       , pds.promo_2_offer
       , pds.promo_2_group
       , pds.promo_2_goal
       , pds.activating_offers
       , olp.bundle_product_id;

CREATE OR REPLACE TEMPORARY TABLE _fk_bundle_info AS
SELECT DISTINCT a.business_unit
              , a.region
              , a.country
              , a.bundle_product_id
              , a.fk_bundle_alias                                                                                                        AS outfit_alias
              , a.fk_bundle_site_name                                                                                                    AS site_name
              , a.fk_vip_retail                                                                                                          AS vip_retail
              , a.fk_type                                                                                                                AS type
              , a.fk_gender                                                                                                              AS gender
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
              , a.fk_total_cost_of_outfit                                                                                                AS total_cost_of_outfit
              , a.fk_total_cost_of_outfit_tariff                                                                                         AS total_cost_of_outfit_tariff
              , a.fk_outfit_imu                                                                                                          AS outfit_imu
              , a.fk_outfit_imu_tariff                                                                                                   AS outfit_imu_tariff
              , a.fk_contains_shoes                                                                                                      AS contains_shoes
              , a.bundle_image_url                                                                                                       AS large_img_url
              , a.is_bundle_active                                                                                                       AS is_active
              , a.current_bundle_vip_retail                                                                                              AS current_vip_retail
              , a.bundle_msrp                                                                                                            AS msrp
FROM gfb.gfb_dim_bundle a
WHERE a.business_unit = 'FABKIDS';

CREATE OR REPLACE TRANSIENT TABLE gfb.gfb031_02_bundle_promo_data_set AS
SELECT ppd.business_unit
     , ppd.region
     , ppd.country
     , ppd.order_date
     , ppd.order_type
     , ppd.promo_order_flag
     , ppd.promo_code
     , ppd.promo_offer
     , ppd.promo_group
     , ppd.promo_goal
     , ppd.activating_offers
     , mdbp.bundle_product_id
     , mdbp.outfit_alias
     , mdbp.site_name
     , mdbp.vip_retail
     , mdbp.type
     , mdbp.gender
     , mdbp.active_inactive
     , mdbp.is_active
     , mdbp.outfit_vs_box_vs_pack
     , mdbp.large_img_url
     , mdbp.total_cost_of_outfit
     , mdbp.current_vip_retail
     , mdbp.msrp
     , mdbp.item1                            AS bundle_component_product_sku_1
     , mdbp.item2                            AS bundle_component_product_sku_2
     , mdbp.item3                            AS bundle_component_product_sku_3
     , mdbp.item4                            AS bundle_component_product_sku_4
     , mdbp.item5                            AS bundle_component_product_sku_5
     , mdbp.item6                            AS bundle_component_product_sku_6
     , mdbp.item7                            AS bundle_component_product_sku_7
     , mdbp.item8                            AS bundle_component_product_sku_8
     , SUM(ppd.orders)                       AS orders
     , SUM(ppd.total_qty_sold)               AS bundle_units_sold
     , SUM(ppd.bundle_unit_sold)             AS bundle_sold
     , SUM(ppd.total_product_revenue)        AS total_product_revenue
     , SUM(ppd.total_cogs)                   AS total_cogs
     , SUM(ppd.total_cash_collected)         AS total_cash_collected
     , SUM(ppd.total_credit_redeemed_amount) AS total_credit_redeemed_amount
     , SUM(ppd.total_subtotal_amount)        AS total_subtotal_amount
     , SUM(ppd.total_discount)               AS total_discount
     , SUM(ppd.total_shipping_cost)          AS total_shipping_cost
     , SUM(ppd.total_shipping_revenue)       AS total_shipping_revenue
FROM _bundle_promo_data ppd
         JOIN _fk_bundle_info mdbp
              ON mdbp.bundle_product_id = ppd.bundle_product_id
                  AND mdbp.business_unit = ppd.business_unit
                  AND mdbp.region = ppd.region
                  AND mdbp.country = ppd.country
WHERE ppd.bundle_product_id != -1
GROUP BY ppd.business_unit
       , ppd.region
       , ppd.country
       , ppd.order_date
       , ppd.order_type
       , ppd.promo_order_flag
       , ppd.promo_code
       , ppd.promo_offer
       , ppd.promo_group
       , ppd.promo_goal
       , ppd.activating_offers
       , mdbp.bundle_product_id
       , mdbp.outfit_alias
       , mdbp.site_name
       , mdbp.vip_retail
       , mdbp.type
       , mdbp.gender
       , mdbp.active_inactive
       , mdbp.is_active
       , mdbp.outfit_vs_box_vs_pack
       , mdbp.large_img_url
       , mdbp.total_cost_of_outfit
       , mdbp.current_vip_retail
       , mdbp.msrp
       , mdbp.item1
       , mdbp.item2
       , mdbp.item3
       , mdbp.item4
       , mdbp.item5
       , mdbp.item6
       , mdbp.item7
       , mdbp.item8;
