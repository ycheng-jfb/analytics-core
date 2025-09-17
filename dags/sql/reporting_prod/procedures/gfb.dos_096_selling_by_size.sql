SET end_date = CURRENT_DATE();
SET start_date = DATE_TRUNC(YEAR, DATEADD(YEAR, -2, $end_date));

CREATE OR REPLACE TEMPORARY TABLE _sales_info_place_date AS
SELECT fol.business_unit              AS business_unit
     , fol.region                     AS region
     , fol.country                    AS country
     , fol.product_sku
     , fol.order_date                 AS order_date
     , fol.clearance_flag             AS clearance_flag
     , fol.clearance_price            AS clearance_price
     , fol.lead_only_flag             AS lead_only_flag
     , fol.sku
     , psm.clean_size                 AS size
     , mfd.shared

     --Sales
     , SUM(fol.total_qty_sold)        AS total_qty_sold
     , SUM(fol.total_product_revenue) AS total_product_revenue
     , SUM(fol.total_cogs)            AS total_cogs

     , SUM(CASE
               WHEN fol.order_type = 'vip activating'
                   THEN fol.total_product_revenue
               ELSE 0 END)            AS activating_product_revenue
     , SUM(CASE
               WHEN fol.order_type = 'vip activating'
                   THEN fol.total_qty_sold
               ELSE 0 END)            AS activating_qty_sold
     , SUM(CASE
               WHEN fol.order_type = 'vip activating'
                   THEN fol.total_cogs
               ELSE 0 END)            AS activating_cogs

     , SUM(CASE
               WHEN fol.order_type != 'vip activating'
                   THEN fol.total_product_revenue
               ELSE 0 END)            AS repeat_product_revenue
     , SUM(CASE
               WHEN fol.order_type != 'vip activating'
                   THEN fol.total_qty_sold
               ELSE 0 END)            AS repeat_qty_sold
     , SUM(CASE
               WHEN fol.order_type != 'vip activating'
                   THEN fol.total_cogs
               ELSE 0 END)            AS repeat_cogs

     , SUM(fol.total_discount)        AS total_discount
     , SUM(CASE
               WHEN fol.order_type = 'vip activating'
                   THEN fol.total_discount
               ELSE 0 END)            AS activating_discount
     , SUM(CASE
               WHEN fol.order_type != 'vip activating'
                   THEN fol.total_discount
               ELSE 0 END)            AS repeat_discount
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date fol
         JOIN reporting_prod.gfb.merch_dim_product mfd
              ON LOWER(mfd.business_unit) = LOWER(fol.business_unit)
                  AND LOWER(mfd.region) = LOWER(fol.region)
                  AND LOWER(mfd.country) = LOWER(fol.country)
                  AND mfd.product_sku = fol.product_sku
         LEFT JOIN reporting_prod.gfb.view_product_size_mapping psm
                   ON LOWER(psm.size) = LOWER(fol.dp_size)
                       AND psm.product_sku = mfd.product_sku
                       AND psm.region = fol.region
                       AND psm.country = fol.country
                       AND psm.business_unit = fol.business_unit
WHERE fol.order_classification = 'product order'
  AND fol.order_date >= $start_date
  AND fol.order_date < $end_date
GROUP BY fol.business_unit
       , fol.region
       , fol.country
       , fol.product_sku
       , fol.order_date
       , fol.clearance_flag
       , fol.clearance_price
       , fol.lead_only_flag
       , fol.sku
       , psm.clean_size
       , mfd.shared;


CREATE OR REPLACE TEMPORARY TABLE _inventory_info AS
SELECT id.business_unit
     , id.region                                                                 AS region
     , id.country                                                                AS country
     , mfd.product_sku
     , id.inventory_date                                                         AS inventory_date
     , (CASE
            WHEN id.clearance_price IS NOT NULL THEN 'clearance'
            ELSE 'regular' END)                                                  AS clearance_flag
     , CASE
           WHEN id.clearance_price = '#N/A ()' THEN NULL
           ELSE id.clearance_price END                                           AS clearance_price
     , (CASE
            WHEN lop.product_sku IS NOT NULL THEN 'lead only'
            ELSE 'not lead only' END)                                            AS lead_only_flag
     , id.sku
     , psm.clean_size                                                            AS size

     , SUM(id.qty_onhand)                                                        AS qty_onhand
     , SUM(id.qty_available_to_sell)                                             AS qty_available_to_sell
     , SUM(id.qty_onhand + qty_replen + qty_ri + qty_pick_staging + qty_staging) AS qty_open_to_buy
     , SUM(id.qty_ghost)                                                         AS qty_ghost
     , SUM(id.qty_manual_stock_reserve)                                          AS qty_manual_stock_reserve
     , SUM(id.qty_reserve)                                                       AS qty_reserve
     , SUM(id.qty_pick_staging)                                                  AS qty_pick_staging
     , SUM(id.qty_replen)                                                        AS qty_replen
     , SUM(id.qty_ri)                                                            AS qty_ri
     , SUM(id.qty_special_pick_reserve)                                          AS qty_special_pick_reserve
     , SUM(id.qty_staging)                                                       AS qty_staging
     , SUM(CASE
               WHEN id.warehouse = 'KENTUCKY'
                   THEN id.qty_available_to_sell
               ELSE 0 END)                                                       AS east_coast_qty_available_to_sell
     , SUM(CASE
               WHEN id.warehouse = 'PERRIS'
                   THEN id.qty_available_to_sell
               ELSE 0 END)                                                       AS west_coast_qty_available_to_sell
FROM reporting_prod.gfb.gfb_inventory_data_set id
         JOIN reporting_prod.gfb.merch_dim_product mfd
              ON mfd.product_sku = id.product_sku
                  AND LOWER(mfd.region) = LOWER(id.region)
                  AND LOWER(mfd.country) = LOWER(id.country)
                  AND LOWER(mfd.business_unit) = LOWER(id.business_unit)
         LEFT JOIN lake_view.sharepoint.gfb_lead_only_product lop
                   ON LOWER(lop.brand) = LOWER(id.business_unit)
                       AND LOWER(lop.region) = LOWER(id.region)
                       AND LOWER(lop.product_sku) = LOWER(id.product_sku)
                       AND
                      (CAST(id.inventory_date AS DATE) >= lop.date_added AND
                       CAST(id.inventory_date AS DATE) < COALESCE(lop.date_removed, CURRENT_DATE()))
         LEFT JOIN reporting_prod.gfb.view_product_size_mapping psm
                   ON LOWER(psm.size) = LOWER(id.dp_size)
                       AND psm.product_sku = mfd.product_sku
                       AND psm.region = mfd.region
                       AND psm.country = mfd.country
                       AND psm.business_unit = mfd.business_unit
WHERE id.inventory_date >= $start_date
  AND id.inventory_date < $end_date
GROUP BY id.business_unit
       , id.region
       , id.country
       , mfd.product_sku
       , id.inventory_date
       , (CASE
              WHEN id.clearance_price IS NOT NULL THEN 'clearance'
              ELSE 'regular' END)
       , CASE
             WHEN id.clearance_price = '#N/A ()' THEN NULL
             ELSE id.clearance_price END
       , (CASE
              WHEN lop.product_sku IS NOT NULL THEN 'lead only'
              ELSE 'not lead only' END)
       , id.sku
       , psm.clean_size;


CREATE OR REPLACE TEMPORARY TABLE _product_sales_days AS
SELECT ol.region
     , ol.product_sku
     , MIN(ol.order_date)                                    AS first_sale_date
     , MAX(ol.order_date)                                    AS last_sale_date
     , DATEDIFF(DAY, MIN(ol.order_date), MAX(ol.order_date)) AS days_of_selling
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date ol
WHERE ol.order_classification = 'product order'
GROUP BY ol.region
       , ol.product_sku;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.dos_096_selling_by_size AS
SELECT mdp.business_unit
     , mdp.sub_brand
     , mdp.region
     , mdp.product_sku
     , mdp.image_url
     , mdp.department_detail
     , mdp.subcategory
     , mdp.style_name
     , mdp.latest_launch_date
     , mdp.previous_launch_date
     , mdp.color
     , mdp.shared
     , mdp.country
     , mdp.coresb_reorder_fashion
     , mdp.qty_pending
     , mdp.gender
     , mdp.fk_site_name
     , mdp.fk_attribute
     , mdp.fk_size_model
     , mdp.subclass
     , mdp.color_family_console
     , mdp.ww_wc
     , mdp.weather_console
     , mdp.occasion_type_console
     , mdp.heel_size_console
     , mdp.heel_shape_console
     , mdp.toe_type_console
     , mdp.material_type_console
     , mdp.boot_shaft_height_console
     , mdp.designer_collaboration_console
     , mdp.fit_console
     , mdp.comfort_flag
     , mdp.memory_foam_flag
     , mdp.cushioned_footbed_flag
     , mdp.cushioned_heel_flag
     , mdp.arch_support_flag
     , mdp.antibacterial_flag
     , mdp.sweat_wicking_flag
     , mdp.breathing_holes_flag
     , mdp.current_price
     , mdp.current_vip_retail
     , mdp.msrp
     , mdp.first_launch_date
     , psd.first_sale_date
     , mdp.first_clearance_date
     , mdp.season_code
     , mdp.fk_new_season_code
     , mdp.show_room

     , main_data.*
     , (CASE
            WHEN mdp.latest_launch_date > DATE_TRUNC(MONTH, main_data.date)
                THEN COALESCE(mdp.previous_launch_date, mdp.latest_launch_date)
            ELSE mdp.latest_launch_date
    END) AS current_showroom
FROM (
         SELECT COALESCE(si.business_unit, ii.business_unit)                    AS business_unit_for_join
              , COALESCE(si.region, ii.region)                                  AS region_for_join
              , COALESCE(si.country, ii.country)                                AS country_for_join
              , COALESCE(si.product_sku, ii.product_sku)                        AS product_sku_for_join
              , COALESCE(si.order_date, ii.inventory_date)                      AS date
              , COALESCE(si.clearance_flag, ii.clearance_flag, 'regular')       AS clearance_flag
              , COALESCE(si.clearance_price, ii.clearance_price, 0)             AS clearance_price
              , COALESCE(si.lead_only_flag, ii.lead_only_flag, 'not lead only') AS lead_only_flag
              , COALESCE(si.sku, ii.sku)                                        AS sku
              , COALESCE(si.size, ii.size)                                      AS size

              , COALESCE(si.total_qty_sold, 0)                                  AS total_qty_sold
              , COALESCE(si.total_product_revenue, 0)                           AS total_product_revenue
              , COALESCE(si.total_cogs, 0)                                      AS total_cogs
              , COALESCE(si.activating_product_revenue, 0)                      AS activating_product_revenue
              , COALESCE(si.activating_qty_sold, 0)                             AS activating_qty_sold
              , COALESCE(si.activating_cogs, 0)                                 AS activating_cogs
              , COALESCE(si.repeat_product_revenue, 0)                          AS repeat_product_revenue
              , COALESCE(si.repeat_qty_sold, 0)                                 AS repeat_qty_sold
              , COALESCE(si.repeat_cogs, 0)                                     AS repeat_cogs
              , COALESCE(si.total_discount, 0)                                  AS total_discount
              , COALESCE(si.activating_discount, 0)                             AS activating_discount
              , COALESCE(si.repeat_discount, 0)                                 AS repeat_discount

              , COALESCE(CASE
                             WHEN si.shared = si.business_unit OR si.shared IS NULL
                                 THEN ii.qty_onhand
                             ELSE 0 END, 0)                                     AS qty_onhand
              , COALESCE(CASE
                             WHEN si.shared = si.business_unit OR si.shared IS NULL
                                 THEN ii.qty_available_to_sell
                             ELSE 0 END, 0)                                     AS qty_available_to_sell
              , COALESCE(CASE
                             WHEN si.shared = si.business_unit OR si.shared IS NULL
                                 THEN ii.qty_open_to_buy
                             ELSE 0 END, 0)                                     AS qty_open_to_buy
              , COALESCE(CASE
                             WHEN si.shared = si.business_unit OR si.shared IS NULL
                                 THEN ii.qty_ghost
                             ELSE 0 END, 0)                                     AS qty_ghost
              , COALESCE(CASE
                             WHEN si.shared = si.business_unit OR si.shared IS NULL
                                 THEN ii.qty_manual_stock_reserve
                             ELSE 0 END, 0)                                     AS qty_manual_stock_reserve
              , COALESCE(CASE
                             WHEN si.shared = si.business_unit OR si.shared IS NULL
                                 THEN ii.qty_reserve
                             ELSE 0 END, 0)                                     AS qty_reserve
              , COALESCE(CASE
                             WHEN si.shared = si.business_unit OR si.shared IS NULL
                                 THEN ii.qty_pick_staging
                             ELSE 0 END, 0)                                     AS qty_pick_staging
              , COALESCE(CASE
                             WHEN si.shared = si.business_unit OR si.shared IS NULL
                                 THEN ii.qty_replen
                             ELSE 0 END, 0)                                     AS qty_replen
              , COALESCE(CASE
                             WHEN si.shared = si.business_unit OR si.shared IS NULL
                                 THEN ii.qty_ri
                             ELSE 0 END, 0)                                     AS qty_ri
              , COALESCE(CASE
                             WHEN si.shared = si.business_unit OR si.shared IS NULL
                                 THEN ii.qty_special_pick_reserve
                             ELSE 0 END, 0)                                     AS qty_special_pick_reserve
              , COALESCE(CASE
                             WHEN si.shared = si.business_unit OR si.shared IS NULL
                                 THEN ii.qty_staging
                             ELSE 0 END, 0)                                     AS qty_staging
              , COALESCE(ii.east_coast_qty_available_to_sell, 0)                AS east_coast_qty_available_to_sell
              , COALESCE(ii.west_coast_qty_available_to_sell, 0)                AS west_coast_qty_available_to_sell
         FROM _sales_info_place_date si
                  FULL JOIN _inventory_info ii
                            ON ii.business_unit = si.business_unit
                                AND ii.region = si.region
                                AND ii.country = si.country
                                AND ii.product_sku = si.product_sku
                                AND ii.inventory_date = si.order_date
                                AND ii.sku = si.sku
     ) main_data
         JOIN reporting_prod.gfb.merch_dim_product mdp
              ON mdp.business_unit = main_data.business_unit_for_join
                  AND mdp.region = main_data.region_for_join
                  AND mdp.product_sku = main_data.product_sku_for_join
                  AND mdp.country = main_data.country_for_join
         LEFT JOIN _product_sales_days psd
                   ON psd.region = main_data.region_for_join
                       AND psd.product_sku = main_data.product_sku_for_join;
