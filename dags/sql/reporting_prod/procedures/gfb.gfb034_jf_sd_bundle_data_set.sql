SET start_date = DATEADD(YEAR, -2, DATE_TRUNC(YEAR, CURRENT_DATE()));

CREATE OR REPLACE TEMPORARY TABLE _sales_info_place_date AS
SELECT ol.business_unit                                                                                                                            AS business_unit
     , ol.region                                                                                                                                   AS region
     , ol.country                                                                                                                                  AS country
     , ol.product_sku
     , ol.order_date                                                                                                                               AS order_date
--     ,ol.CLEARANCE_FLAG as clearance_flag
--     ,ol.CLEARANCE_PRICE as clearance_price
--     ,ol.LEAD_ONLY_FLAG as lead_only_flag
     , ol.bundle_product_id_jfsd
     , ol.bundle_product_id
     , ol.credit_order_type
     , COUNT(1)
             OVER (PARTITION BY ol.business_unit,ol.region,ol.country,ol.product_sku,ol.bundle_product_id_jfsd,ol.bundle_product_id,ol.order_date) AS coeff
     --Sales
     , SUM(ol.total_qty_sold)                                                                                                                      AS total_qty_sold
     , SUM(ol.total_product_revenue)                                                                                                               AS total_product_revenue
     , SUM(ol.total_cogs)                                                                                                                          AS total_cogs

     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                                                                                         AS activating_product_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                                                                                         AS activating_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_cogs
               ELSE 0 END)                                                                                                                         AS activating_cogs

     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                                                                                         AS repeat_product_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                                                                                         AS repeat_qty_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_cogs
               ELSE 0 END)                                                                                                                         AS repeat_cogs

     , SUM(ol.total_discount)                                                                                                                      AS total_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_discount
               ELSE 0 END)                                                                                                                         AS activating_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_discount
               ELSE 0 END)                                                                                                                         AS repeat_discount

     , SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                                                                                         AS lead_product_revenue
     , SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                                                                                         AS lead_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_cogs
               ELSE 0 END)                                                                                                                         AS lead_cogs
     , SUM(ol.tokens_applied)                                                                                                                      AS tokens_applied
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                                                                                         AS token_qty_sold
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.token_local_amount
               ELSE 0 END)                                                                                                                         AS token_local_amount
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                                                                                         AS token_product_revenue
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                                                                                         AS token_cogs
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                                                                                         AS token_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.tokens_applied > 0 THEN ol.total_discount
               ELSE 0 END)                                                                                                                         AS token_discount
     , SUM(ol.two_for_one_applied)                                                                                                                 AS two_for_one_applied
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                                                                                         AS two_for_one_qty_sold
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.token_local_amount
               ELSE 0 END)                                                                                                                         AS two_for_one_local_amount
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                                                                                         AS two_for_one_product_revenue
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                                                                                         AS two_for_one_cogs
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                                                                                         AS two_for_one_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0 THEN ol.total_discount
               ELSE 0 END)                                                                                                                         AS two_for_one_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                                                                                         AS activating_token_product_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                                                                                         AS activating_token_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                                                                                         AS activating_token_cogs
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                                                                                         AS activating_token_product_revenue_with_tariff

     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                                                                                         AS repeat_token_product_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                                                                                         AS repeat_token_qty_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                                                                                         AS repeat_token_cogs
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                                                                                         AS repeat_token_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                                                                                         AS activating_two_for_one_product_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                                                                                         AS activating_two_for_one_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                                                                                         AS activating_two_for_one_cogs
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                                                                                         AS activating_two_for_one_product_revenue_with_tariff

     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                                                                                         AS repeat_two_for_one_product_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                                                                                         AS repeat_two_for_one_qty_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                                                                                         AS repeat_two_for_one_cogs
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                                                                                         AS repeat_two_for_one_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag = 'regular' AND ol.tokens_applied > 0
                   THEN ol.total_discount
               ELSE 0 END)                                                                                                                         AS activating_token_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag = 'regular' AND ol.tokens_applied > 0
                   THEN ol.total_discount
               ELSE 0 END)                                                                                                                         AS repeat_token_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag = 'regular' AND ol.two_for_one_applied > 0
                   THEN ol.total_discount
               ELSE 0 END)                                                                                                                         AS activating_two_for_one_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag = 'regular' AND ol.two_for_one_applied > 0
                   THEN ol.total_discount
               ELSE 0 END)                                                                                                                         AS repeat_two_for_one_discount
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                                                                                         AS one_item_qty_sold
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.token_local_amount
               ELSE 0 END)                                                                                                                         AS one_item_local_amount
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                                                                                         AS one_item_product_revenue
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_cogs
               ELSE 0 END)                                                                                                                         AS one_item_cogs
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND bundle_product_id = '-1'
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                                                                                         AS one_item_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND bundle_product_id = '-1' THEN ol.total_discount
               ELSE 0 END)                                                                                                                         AS one_item_discount
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date ol
WHERE ol.order_classification = 'product order'
  AND ol.order_date >= $start_date
  AND (ol.bundle_product_id_jfsd IS NOT NULL OR ol.bundle_product_id IS NOT NULL)
  AND ol.business_unit IN ('JUSTFAB', 'SHOEDAZZLE')
GROUP BY ol.business_unit
       , ol.region
       , ol.country
       , ol.product_sku
       , ol.order_date
--     ,ol.CLEARANCE_FLAG
--     ,ol.CLEARANCE_PRICE
--     ,ol.LEAD_ONLY_FLAG
       , ol.bundle_product_id_jfsd
       , ol.bundle_product_id
       , ol.credit_order_type;

CREATE OR REPLACE TEMPORARY TABLE _inventory_info_product_sku AS
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
GROUP BY id.business_unit
       , id.region
       , id.country
       , id.product_sku
       , id.inventory_date
;

CREATE OR REPLACE TEMPORARY TABLE _current_inventory_product_sku AS
SELECT id.business_unit
     , id.region                     AS region
     , id.country                    AS country
     , id.product_sku

     , SUM(id.qty_onhand)            AS qty_onhand
     , SUM(id.qty_available_to_sell) AS qty_available_to_sell
     , SUM(id.qty_open_to_buy)       AS qty_open_to_buy
FROM reporting_prod.gfb.gfb_inventory_data_set_current id
GROUP BY id.business_unit
       , id.region
       , id.country
       , id.product_sku;

CREATE OR REPLACE TEMPORARY TABLE _broken_size_flag AS
SELECT DISTINCT a.business_unit
              , a.region
              , a.country
              , a.product_sku
              , a.core_size_broken_flag
FROM reporting_prod.gfb.gfb015_out_of_stock_by_size a
;

CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb034_jf_sd_bundle_data_set AS
SELECT dpbj.business_unit
     , dpbj.sub_brand
     , dpbj.region
     , dpbj.country
     , dpbj.product_sku
     , dpbj.mm_product_sku
     , dpbj.product_style_number
     , dpbj.latest_launch_date
     , dpbj.previous_launch_date
     , dpbj.department
     , dpbj.department_detail
     , dpbj.subcategory
     , dpbj.reorder_status
     , dpbj.shared
     , dpbj.style_rank
     , dpbj.description
     , dpbj.ww_wc
     , dpbj.style_name
     , dpbj.color
     , dpbj.master_color
     , dpbj.classification
     , dpbj.vendor
     , dpbj.direct_non_direct
     , dpbj.landing_cost / main_data.coeff             AS landing_cost
     , dpbj.current_vip_retail / main_data.coeff       AS current_vip_retail
     , dpbj.msrp / main_data.coeff                     AS msrp
     , dpbj.heel_height
     , dpbj.heel_type
     , dpbj.coresb_reorder_fashion
     , dpbj.image_url
     , dpbj.is_plussize
     , dpbj.size_13_15
     , dpbj.collection
     , dpbj.qty_pending / main_data.coeff              AS qty_pending
     , dpbj.avg_is_recommended_score / main_data.coeff AS avg_is_recommended_score
     , dpbj.avg_style_score / main_data.coeff          AS avg_style_score
     , dpbj.avg_comfort_score / main_data.coeff        AS avg_comfort_score
     , dpbj.avg_quality_value_score / main_data.coeff  AS avg_quality_value_score
     , dpbj.total_reviews / main_data.coeff            AS total_reviews
     , dpbj.og_retail / main_data.coeff                   og_retail
     , dpbj.priced_down
     , dpbj.subclass
     , dpbj.na_landed_cost / main_data.coeff           AS na_landed_cost
     , dpbj.eu_landed_cost / main_data.coeff           AS eu_landed_cost
     , dpbj.error_free_image_url
     , dpbj.large_img_url
     , dpbj.fk_attribute
     , dpbj.fk_site_name
     , dpbj.fk_new_season_code
     , dpbj.marketing_capsule_name
     , dpbj.season_code
     , dpbj.style_type
     , dpbj.gender
     , dpbj.master_product_id
     , dpbj.fk_size_model
     , dpbj.last_receipt_date
     , dpbj.memory_foam_flag
     , dpbj.cushioned_footbed_flag
     , dpbj.cushioned_heel_flag
     , dpbj.arch_support_flag
     , dpbj.antibacterial_flag
     , dpbj.sweat_wicking_flag
     , dpbj.breathing_holes_flag
     , dpbj.comfort_flag
     , dpbj.distro
     , dpbj.show_room
     , dpbj.current_price / main_data.coeff            AS current_price
     , dpbj.exact_latest_launch_date
     , dpbj.is_base_sku
     , dpbj.boot_shaft_height_console
     , dpbj.color_family_console
     , dpbj.designer_collaboration_console
     , dpbj.fit_console
     , dpbj.heel_shape_console
     , dpbj.heel_size_console
     , dpbj.material_type_console
     , dpbj.occasion_type_console
     , dpbj.toe_type_console
     , dpbj.weather_console
     , dpbj.first_launch_date
     , dpbj.first_clearance_date
     , dpbj.avg_overall_rating / main_data.coeff       AS avg_overall_rating
     , dpbj.img_model_reg
     , dpbj.img_model_plus
     , dpbj.img_type
     , dpbj.product_wait_list_type
     , dpbj.waitlist_with_known_eta_flag
     , dpbj.waitlist_start_datetime
     , dpbj.waitlist_end_datetime
     , dpbj.is_active
     , dpbj.current_status
     , dpbj.current_showroom
     , dpbj.sale_price / main_data.coeff               AS sale_price
     , dpbj.sleeve_lenth_console
     , dpbj.clothing_detail_console
     , dpbj.color_console
     , dpbj.buttom_subclass_console
     , dpbj.shop_category_console
     , dpbj.shoe_style_console
     , dpbj.product_color_from_label
     , dpbj.prepack_flag
     , dpbj.bundle_product_id
     , dpbj.bundle_image_url
     , dpbj.is_bundle_active
     , dpbj.bundle_name
     , dpbj.bundle_alias
     , dpbj.product_type
     , dpbj.group_code
     , dpbj.fk_bundle_alias
     , dpbj.fk_bundle_site_name
     , dpbj.fk_vip_retail
     , dpbj.fk_type
     , dpbj.fk_gender
     , dpbj.fk_active_inactive
     , dpbj.fk_outfit_vs_box_vs_pack
     , dpbj.fk_total_cost_of_outfit
     , dpbj.fk_total_cost_of_outfit_tariff
     , dpbj.fk_outfit_imu
     , dpbj.fk_outfit_imu_tariff
     , dpbj.fk_contains_shoes
     , dpbj.prepack_components
     , dpbj.current_bundle_vip_retail
     , dpbj.bundle_msrp / main_data.coeff              AS bundle_msrp
     , dpbj.compontents_number
     , main_data.date
     , main_data.credit_order_type
     , main_data.total_qty_sold
     , main_data.total_product_revenue
     , main_data.total_cogs
     , main_data.activating_product_revenue
     , main_data.activating_qty_sold
     , main_data.activating_cogs
     , main_data.repeat_product_revenue
     , main_data.repeat_qty_sold
     , main_data.repeat_cogs
     , main_data.total_discount
     , main_data.activating_discount
     , main_data.repeat_discount
     , main_data.lead_qty_sold
     , main_data.lead_product_revenue
     , main_data.lead_cogs
     , main_data.tokens_applied
     , main_data.token_qty_sold
     , main_data.token_local_amount
     , main_data.token_product_revenue
     , main_data.token_product_revenue_with_tariff
     , main_data.token_cogs
     , main_data.token_discount
     , main_data.two_for_one_applied
     , main_data.two_for_one_qty_sold
     , main_data.two_for_one_product_revenue
     , main_data.two_for_one_product_revenue_with_tariff
     , main_data.two_for_one_cogs
     , main_data.two_for_one_discount
     , main_data.activating_token_qty_sold
     , main_data.activating_token_product_revenue
     , main_data.activating_token_product_revenue_with_tariff
     , main_data.activating_token_cogs
     , main_data.activating_token_discount
     , main_data.repeat_token_qty_sold
     , main_data.repeat_token_product_revenue
     , main_data.repeat_token_product_revenue_with_tariff
     , main_data.repeat_token_cogs
     , main_data.repeat_token_discount
     , main_data.activating_two_for_one_qty_sold
     , main_data.activating_two_for_one_product_revenue
     , main_data.activating_two_for_one_product_revenue_with_tariff
     , main_data.activating_two_for_one_cogs
     , main_data.activating_two_for_one_discount
     , main_data.repeat_two_for_one_qty_sold
     , main_data.repeat_two_for_one_product_revenue
     , main_data.repeat_two_for_one_product_revenue_with_tariff
     , main_data.repeat_two_for_one_cogs
     , main_data.repeat_two_for_one_discount
     , main_data.one_item_qty_sold
     , main_data.one_item_cogs
     , main_data.one_item_product_revenue
     , main_data.one_item_product_revenue_with_tariff
     , main_data.one_item_discount
     , (CASE
            WHEN dpbj.latest_launch_date > DATE_TRUNC(MONTH, main_data.date) THEN 'Future Showroom'
    END)                                               AS future_showroom
     , wmp.parent_product_cateogry                     AS site_parent_product_cateogry
     , wmp.product_category                            AS site_product_cateogry
     , bsf.core_size_broken_flag
     , ci.qty_available_to_sell                        AS current_qty_available_to_sell
FROM (SELECT COALESCE(si.business_unit, ii.business_unit)                       AS business_unit_for_join
           , COALESCE(si.region, ii.region)                                     AS region_for_join
           , COALESCE(si.country, ii.country)                                   AS country_for_join
           , COALESCE(si.product_sku, ii.product_sku)                           AS product_sku_for_join
           , COALESCE(si.order_date, ii.inventory_date)                         AS date
           , COALESCE(CAST(si.bundle_product_id_jfsd AS VARCHAR(100)),
                      CAST(si.bundle_product_id AS VARCHAR(100)))               AS bundle_product_id

           , COALESCE(si.total_qty_sold, 0)                                     AS total_qty_sold
           , COALESCE(si.total_product_revenue, 0)                              AS total_product_revenue
           , COALESCE(si.total_cogs, 0)                                         AS total_cogs
           , COALESCE(si.activating_product_revenue, 0)                         AS activating_product_revenue
           , COALESCE(si.activating_qty_sold, 0)                                AS activating_qty_sold
           , COALESCE(si.activating_cogs, 0)                                    AS activating_cogs
           , COALESCE(si.repeat_product_revenue, 0)                             AS repeat_product_revenue
           , COALESCE(si.repeat_qty_sold, 0)                                    AS repeat_qty_sold
           , COALESCE(si.repeat_cogs, 0)                                        AS repeat_cogs
           , COALESCE(si.total_discount, 0)                                     AS total_discount
           , COALESCE(si.activating_discount, 0)                                AS activating_discount
           , COALESCE(si.repeat_discount, 0)                                    AS repeat_discount
           , COALESCE(si.lead_qty_sold, 0)                                      AS lead_qty_sold
           , COALESCE(si.lead_product_revenue, 0)                               AS lead_product_revenue
           , COALESCE(si.lead_cogs, 0)                                          AS lead_cogs
           , COALESCE(si.tokens_applied, 0)                                     AS tokens_applied
           , COALESCE(si.token_local_amount, 0)                                 AS token_local_amount
           , COALESCE(si.token_qty_sold, 0)                                     AS token_qty_sold
           , COALESCE(si.token_product_revenue, 0)                              AS token_product_revenue
           , COALESCE(si.token_product_revenue_with_tariff, 0)                  AS token_product_revenue_with_tariff
           , COALESCE(si.token_cogs, 0)                                         AS token_cogs
           , COALESCE(si.token_discount, 0)                                     AS token_discount
           , COALESCE(si.two_for_one_applied, 0)                                AS two_for_one_applied
           , COALESCE(si.two_for_one_qty_sold, 0)                               AS two_for_one_qty_sold
           , COALESCE(si.two_for_one_product_revenue, 0)                        AS two_for_one_product_revenue
           , COALESCE(si.two_for_one_product_revenue_with_tariff, 0)            AS two_for_one_product_revenue_with_tariff
           , COALESCE(si.two_for_one_cogs, 0)                                   AS two_for_one_cogs
           , COALESCE(si.two_for_one_discount, 0)                               AS two_for_one_discount
           , COALESCE(si.activating_token_qty_sold, 0)                          AS activating_token_qty_sold
           , COALESCE(si.activating_token_product_revenue, 0)                   AS activating_token_product_revenue
           , COALESCE(si.activating_token_product_revenue_with_tariff, 0)       AS activating_token_product_revenue_with_tariff
           , COALESCE(si.activating_token_cogs, 0)                              AS activating_token_cogs
           , COALESCE(si.activating_token_discount, 0)                          AS activating_token_discount
           , COALESCE(si.repeat_token_qty_sold, 0)                              AS repeat_token_qty_sold
           , COALESCE(si.repeat_token_product_revenue, 0)                       AS repeat_token_product_revenue
           , COALESCE(si.repeat_token_product_revenue_with_tariff, 0)           AS repeat_token_product_revenue_with_tariff
           , COALESCE(si.repeat_token_cogs, 0)                                  AS repeat_token_cogs
           , COALESCE(si.repeat_token_discount, 0)                              AS repeat_token_discount
           , COALESCE(si.activating_two_for_one_qty_sold, 0)                    AS activating_two_for_one_qty_sold
           , COALESCE(si.activating_two_for_one_product_revenue, 0)             AS activating_two_for_one_product_revenue
           , COALESCE(si.activating_two_for_one_product_revenue_with_tariff, 0) AS activating_two_for_one_product_revenue_with_tariff
           , COALESCE(si.activating_two_for_one_cogs, 0)                        AS activating_two_for_one_cogs
           , COALESCE(si.activating_two_for_one_discount, 0)                    AS activating_two_for_one_discount
           , COALESCE(si.repeat_two_for_one_qty_sold, 0)                        AS repeat_two_for_one_qty_sold
           , COALESCE(si.repeat_two_for_one_product_revenue, 0)                 AS repeat_two_for_one_product_revenue
           , COALESCE(si.repeat_two_for_one_product_revenue_with_tariff, 0)     AS repeat_two_for_one_product_revenue_with_tariff
           , COALESCE(si.repeat_two_for_one_cogs, 0)                            AS repeat_two_for_one_cogs
           , COALESCE(si.repeat_two_for_one_discount, 0)                        AS repeat_two_for_one_discount
           , COALESCE(si.one_item_qty_sold, 0)                                  AS one_item_qty_sold
           , COALESCE(si.one_item_cogs, 0)                                      AS one_item_cogs
           , COALESCE(si.one_item_product_revenue, 0)                           AS one_item_product_revenue
           , COALESCE(si.one_item_product_revenue_with_tariff, 0)               AS one_item_product_revenue_with_tariff
           , COALESCE(si.one_item_discount, 0)                                  AS one_item_discount


           , COALESCE(ii.qty_available_to_sell, 0)                              AS qty_available_to_sell
           , si.coeff                                                           AS coeff
           , si.credit_order_type                                               AS credit_order_type
      FROM _sales_info_place_date si
               FULL JOIN _inventory_info_product_sku ii
                         ON ii.business_unit = si.business_unit
                             AND ii.region = si.region
                             AND ii.country = si.country
                             AND ii.product_sku = si.product_sku
                             AND ii.inventory_date = si.order_date) main_data
         JOIN reporting_prod.gfb.gfb_dim_bundle dpbj
              ON dpbj.business_unit = main_data.business_unit_for_join
                  AND dpbj.region = main_data.region_for_join
                  AND dpbj.country = main_data.country_for_join
                  AND dpbj.product_sku = main_data.product_sku_for_join
                  AND CAST(dpbj.bundle_product_id AS VARCHAR(100)) = CAST(main_data.bundle_product_id AS VARCHAR(100))
         LEFT JOIN reporting_prod.gfb.dim_web_merch_product wmp
                   ON wmp.business_unit = main_data.business_unit_for_join
                       AND wmp.region = main_data.region_for_join
                       AND wmp.country = main_data.country_for_join
                       AND wmp.product_sku = main_data.product_sku_for_join
         LEFT JOIN _broken_size_flag bsf
                   ON bsf.business_unit = main_data.business_unit_for_join
                       AND bsf.region = main_data.region_for_join
                       AND bsf.country = main_data.country_for_join
                       AND bsf.product_sku = main_data.product_sku_for_join
         LEFT JOIN _current_inventory_product_sku ci
                   ON ci.business_unit = main_data.business_unit_for_join
                       AND ci.region = main_data.region_for_join
                       AND ci.country = main_data.country_for_join
                       AND ci.product_sku = main_data.product_sku_for_join;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb034_01_jf_sd_bundle_order_info AS
SELECT ol.business_unit                                                   AS business_unit
     , ol.region                                                          AS region
     , ol.country                                                         AS country
--     ,ol.PRODUCT_SKU
     , ol.order_date                                                      AS order_date
     , COALESCE(CAST(ol.bundle_product_id AS VARCHAR(100)), CAST(ol.bundle_product_id_jfsd AS VARCHAR(100))
    )                                                                     AS bundle_product_id_jfsd
     , ol.order_id
     , dpbj.department_detail
     , dpbj.subcategory
     , dpbj.gender
     , dpbj.bundle_alias
     , dpbj.group_code
     , dpbj.product_type
     , dpbj.bundle_image_url
     , dpbj.bundle_name
     , dpbj.current_bundle_vip_retail
     , dpbj.is_bundle_active
     , dpbj.current_showroom
     , CASE
           WHEN COALESCE(CAST(ol.bundle_product_id AS VARCHAR(100)),
                         CAST(ol.bundle_product_id_jfsd AS VARCHAR(100))) IS NOT NULL
               THEN 'Bundle'
           ELSE 'Non Bundle'
    END                                                                   AS bundle_flag

     --Sales
     , SUM(ol.total_qty_sold)                                             AS total_qty_sold
     , SUM(ol.total_product_revenue)                                      AS total_product_revenue
     , SUM(ol.total_cogs)                                                 AS total_cogs
     , COUNT(DISTINCT ol.product_sku)                                     AS product_count
     , SUM(ol.total_cash_credit_amount + ol.total_non_cash_credit_amount) AS total_credit_redemption_amount

     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                AS activating_product_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                AS activating_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_cogs
               ELSE 0 END)                                                AS activating_cogs

     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                AS repeat_product_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                AS repeat_qty_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_cogs
               ELSE 0 END)                                                AS repeat_cogs

     , SUM(ol.total_discount)                                             AS total_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating'
                   THEN ol.total_discount
               ELSE 0 END)                                                AS activating_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating'
                   THEN ol.total_discount
               ELSE 0 END)                                                AS repeat_discount

     , SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                AS lead_product_revenue
     , SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                AS lead_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'ecom'
                   THEN ol.total_cogs
               ELSE 0 END)                                                AS lead_cogs
     , SUM(ol.tokens_applied)                                             AS tokens_applied
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                AS token_qty_sold
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.token_local_amount
               ELSE 0 END)                                                AS token_local_amount
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                AS token_product_revenue
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                AS token_cogs
     , SUM(CASE
               WHEN ol.tokens_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                AS token_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.clearance_flag = 'regular' AND ol.tokens_applied > 0 THEN ol.total_discount
               ELSE 0 END)                                                AS token_discount
     , SUM(ol.two_for_one_applied)                                        AS two_for_one_applied
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                AS two_for_one_qty_sold
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.token_local_amount
               ELSE 0 END)                                                AS two_for_one_local_amount
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                AS two_for_one_product_revenue
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                AS two_for_one_cogs
     , SUM(CASE
               WHEN ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                AS two_for_one_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.clearance_flag = 'regular' AND ol.two_for_one_applied > 0 THEN ol.total_discount
               ELSE 0 END)                                                AS two_for_one_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                AS activating_token_product_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                AS activating_token_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                AS activating_token_cogs
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                AS activating_token_product_revenue_with_tariff

     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                AS repeat_token_product_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                AS repeat_token_qty_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                AS repeat_token_cogs
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.tokens_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                AS repeat_token_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                AS activating_two_for_one_product_revenue
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                AS activating_two_for_one_qty_sold
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                AS activating_two_for_one_cogs
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                AS activating_two_for_one_product_revenue_with_tariff

     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                AS repeat_two_for_one_product_revenue
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                AS repeat_two_for_one_qty_sold
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_cogs
               ELSE 0 END)                                                AS repeat_two_for_one_cogs
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.two_for_one_applied > 0
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                AS repeat_two_for_one_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag = 'regular' AND ol.tokens_applied > 0
                   THEN ol.total_discount
               ELSE 0 END)                                                AS activating_token_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag = 'regular' AND ol.tokens_applied > 0
                   THEN ol.total_discount
               ELSE 0 END)                                                AS repeat_token_discount
     , SUM(CASE
               WHEN ol.order_type = 'vip activating' AND ol.clearance_flag = 'regular' AND ol.two_for_one_applied > 0
                   THEN ol.total_discount
               ELSE 0 END)                                                AS activating_two_for_one_discount
     , SUM(CASE
               WHEN ol.order_type != 'vip activating' AND ol.clearance_flag = 'regular' AND ol.two_for_one_applied > 0
                   THEN ol.total_discount
               ELSE 0 END)                                                AS repeat_two_for_one_discount
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1'
                   THEN ol.total_qty_sold
               ELSE 0 END)                                                AS one_item_qty_sold
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1'
                   THEN ol.token_local_amount
               ELSE 0 END)                                                AS one_item_local_amount
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1'
                   THEN ol.total_product_revenue
               ELSE 0 END)                                                AS one_item_product_revenue
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1'
                   THEN ol.total_cogs
               ELSE 0 END)                                                AS one_item_cogs
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1'
                   THEN ol.total_product_revenue_with_tariff
               ELSE 0 END)                                                AS one_item_product_revenue_with_tariff
     , SUM(CASE
               WHEN ol.tokens_applied > 0 AND ol.bundle_product_id = '-1' THEN ol.total_discount
               ELSE 0 END)                                                AS one_item_discount
     , MIN(ii.qty_available_to_sell)                                      AS qty_available_to_sell
     , MAX(cn.compontents_number)                                         AS compontents_number
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date ol
         FULL JOIN _inventory_info_product_sku ii
                   ON ii.business_unit = ol.business_unit
                       AND ii.region = ol.region
                       AND ii.country = ol.country
                       AND ii.product_sku = ol.product_sku
                       AND ii.inventory_date = ol.order_date
         LEFT JOIN reporting_prod.gfb.gfb_dim_bundle dpbj
                   ON dpbj.business_unit = ol.business_unit
                       AND dpbj.region = ol.region
                       AND dpbj.country = ol.country
                       AND dpbj.product_sku = ol.product_sku
                       AND CAST(dpbj.bundle_product_id AS VARCHAR(100)) =
                           COALESCE(CAST(ol.bundle_product_id_jfsd AS VARCHAR(100)),
                                    CAST(ol.bundle_product_id AS VARCHAR(100)))
         LEFT JOIN
     (SELECT DISTINCT a.business_unit
                    , a.region
                    , a.country
                    , a.bundle_product_id
                    , a.compontents_number AS compontents_number
      FROM reporting_prod.gfb.gfb_dim_bundle a) cn ON cn.business_unit = ol.business_unit
         AND cn.region = ol.region
         AND cn.country = ol.country
         AND TRY_CAST(cn.bundle_product_id AS INTEGER) =
             TRY_CAST(COALESCE(CAST(ol.bundle_product_id_jfsd AS VARCHAR(100)),
                               CAST(ol.bundle_product_id AS VARCHAR(100))) AS INTEGER)
WHERE ol.order_classification = 'product order'
  AND ol.order_date >= $start_date
  AND ol.product_sku IN (SELECT product_sku
                         FROM reporting_prod.gfb.gfb_dim_bundle
                         WHERE bundle_product_id IS NOT NULL
                         GROUP BY product_sku)
--    and coalesce(cast(ol.BUNDLE_PRODUCT_ID_JFSD as varchar(100)), cast(ol.BUNDLE_PRODUCT_ID as varchar(100))) is not null
  AND ol.business_unit IN ('JUSTFAB', 'SHOEDAZZLE')
GROUP BY ol.business_unit
       , ol.region
       , ol.country
--     ,ol.PRODUCT_SKU
       , ol.order_date
       , COALESCE(CAST(ol.bundle_product_id AS VARCHAR(100)), CAST(ol.bundle_product_id_jfsd AS VARCHAR(100)))
       , ol.order_id
       , dpbj.group_code
       , dpbj.product_type
       , dpbj.bundle_image_url
       , dpbj.bundle_name
       , dpbj.current_bundle_vip_retail
       , dpbj.is_bundle_active
       , dpbj.current_showroom
       , dpbj.department_detail
       , dpbj.subcategory
       , dpbj.gender
       , dpbj.bundle_alias;
