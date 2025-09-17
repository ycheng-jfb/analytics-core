CREATE OR REPLACE TEMPORARY TABLE _max_showroom_ubt_with_images AS (
SELECT DISTINCT ubt.*,
            CASE WHEN sub_brand = 'Yitty'
            THEN CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/' || ubt.sku || '/' || 'yitty_'||ubt.sku ||'-1_271x407.jpg')
            ELSE CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/' || ubt.sku || '/' || ubt.sku ||'-1_271x407.jpg') END AS image_url
FROM lake.excel.fl_items_ubt ubt
JOIN (SELECT sku, MAX(current_showroom) AS max_current_showroom
      FROM lake.excel.fl_items_ubt
      WHERE current_showroom <= CURRENT_DATE()
      GROUP BY sku) ms ON ubt.sku = ms.sku
    AND ubt.current_showroom = ms.max_current_showroom
);

CREATE OR REPLACE TEMPORARY TABLE _min_showroom_ubt_with_images AS (
SELECT DISTINCT ubt.*,
CASE WHEN  sub_brand = 'Yitty'
             THEN CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/' || ubt.sku || '/' || 'yitty_'||ubt.sku ||'-1_271x407.jpg')
             ELSE CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/' || ubt.sku || '/' || ubt.sku ||'-1_271x407.jpg') END AS image_url
FROM lake.excel.fl_items_ubt ubt
JOIN (SELECT sku, MIN(current_showroom) AS min_current_showroom
      FROM lake.excel.fl_items_ubt
      GROUP BY sku) ms ON ubt.sku = ms.sku
    AND ubt.current_showroom = ms.min_current_showroom
WHERE current_showroom = DATEADD('month', 1, DATE_TRUNC('month', CURRENT_DATE))
);

CREATE OR REPLACE TEMPORARY TABLE _fl_inventory_by_day AS (
SELECT DISTINCT iw.sku,
    TO_DATE(iw.local_date) AS date,
    iw.product_sku,
    IFF(i.gender ILIKE 'MEN', 'MEN', 'WOMEN') AS gender,
    CASE WHEN UPPER(TRIM(I.size)) ilike any ('OS%','ONE SIZE','ONESIZE') THEN 'OS'
         WHEN UPPER(TRIM(I.size)) ilike '2XL%' THEN 'XXL'
         WHEN UPPER(TRIM(I.size)) ilike '3XL%' THEN 'XXXL'
         WHEN UPPER(TRIM(I.size)) ilike 'XXXL%' THEN 'XXXL'
         WHEN UPPER(TRIM(I.size)) ilike 'XXL/1X%' THEN 'XXL/1X'-- added
         WHEN UPPER(TRIM(I.size)) ilike any ('XXL%','SIZE XXL%') THEN 'XXL' --'XXL/XXXL'?
         WHEN UPPER(TRIM(I.size)) ilike any ('XL%','SIZE XL%') THEN 'XL' --'XL/1X'? and 'XL/XXL'?
         WHEN UPPER(TRIM(I.size)) ilike 'XXS%' THEN 'XXS' --'XXS/XS'?
         WHEN UPPER(TRIM(I.size)) ilike any ('XS%','SIZE XS%') THEN 'XS' --'XS/S'?
         WHEN UPPER(TRIM(I.size)) ilike 'S/M%' THEN 'S/M' --added
         WHEN UPPER(TRIM(I.SIZE)) ilike 'M/L%' THEN 'M/L' --added
         WHEN UPPER(TRIM(I.SIZE)) ilike 'L/XL%' THEN 'L/XL' --added
         WHEN UPPER(TRIM(I.size)) ilike any ('S%','SM%','SIZE S%') THEN 'S'--added S -- 'SM/MD'?
         WHEN UPPER(TRIM(I.size)) ilike any ('M%','MD%','SIZE M%') THEN 'M'--added MD
         WHEN UPPER(TRIM(I.size)) ilike any ('L%','LG%','SIZE L%') THEN 'L'--added LG  --'L/XL'?
         WHEN UPPER(TRIM(I.size)) ilike 'X3%' THEN i.size
         WHEN UPPER(TRIM(I.size)) ilike '1X%' THEN '1X' --1X/2X?
         WHEN UPPER(TRIM(I.size)) ilike '2X%' THEN '2X' --2X/3X?
         WHEN UPPER(TRIM(I.size)) ilike '3X%' THEN '3X' --3X/4X?
         WHEN UPPER(TRIM(I.size)) ilike '4X%' THEN '4X' --4X/5X?
         WHEN UPPER(TRIM(I.size)) ilike '5X%' THEN '5X' --5X/6X?
         WHEN UPPER(TRIM(I.size)) ilike '6X%' THEN '6X'
         WHEN UPPER(TRIM(I.size)) ilike 'Unknown' THEN 'OS'
         WHEN UPPER(TRIM(I.size)) in ('2T','3T') THEN 'OTHER'
         ELSE UPPER(TRIM(I.size)) end AS sizes,
    CASE WHEN w.is_retail = 'TRUE' THEN 'Retail' ELSE 'Online' END AS store_type,
    CASE WHEN iw.region ILIKE 'US%' THEN 'US'
         WHEN UPPER(LEFT(w.warehouse_name, 3)) IN ('RTL', 'SXF') OR wh.region_id = 8 THEN w.country_code
         WHEN w.warehouse_name ILIKE '%UK' THEN 'UK'
         WHEN iw.warehouse_id IN (465, 466) THEN 'US' -- mexico warehouse
         ELSE iw.region END AS country_region,
    SUM(iw.onhand_quantity)                                             AS qty_onhand,
    SUM(iw.replen_quantity)                                             AS qty_replen,
    SUM(iw.ghost_quantity)                                              AS qty_ghost,
    SUM(iw.reserve_quantity)                                            AS qty_order_reserve,
    SUM(iw.special_pick_quantity)                                       AS qty_special_pick_reserve,
    SUM(iw.manual_stock_reserve_quantity)                               AS qty_manual_stock_reserve,
    SUM(iw.receipt_inspection_quantity)                                 AS qty_ri,
    SUM(iw.pick_staging_quantity)                                       AS qty_pick_staging,
    SUM(iw.staging_quantity)                                            AS qty_staging,
    SUM(iw.available_to_sell_quantity)                                  AS qty_available_to_sell --11.28 excluded
FROM edw_prod.data_model_fl.fact_inventory_history  iw
JOIN edw_prod.data_model_fl.dim_warehouse w ON w.warehouse_id = iw.warehouse_id
JOIN lake_view.ultra_warehouse.item i ON iw.item_id = i.item_id
JOIN lake_view.ultra_warehouse.company c ON c.company_id = i.company_id
LEFT JOIN lake_view.ultra_warehouse.item_warehouse it_w ON it_w.item_id = i.item_id
    AND iw.warehouse_id = it_w.warehouse_id
JOIN lake_view.ultra_warehouse.warehouse wh ON wh.warehouse_id = iw.warehouse_id
WHERE (iw.warehouse_id IN (154, 109, 221, 107, 366, 465, 466) OR (wh.region_id = 8 OR UPPER(LEFT(w.warehouse_name, 3)) IN ('RTL', 'SXF'))) --REMOVED PERRIS 231
    AND UPPER(TRIM(c.label)) IN ('FABLETICS' ,'YITTY')
    AND iw.local_date >= DATEADD('Month', -16, CURRENT_DATE())
GROUP BY iw.sku, date, iw.product_sku, gender, sizes, store_type, country_region);


CREATE OR REPLACE TEMPORARY TABLE _fl_sales_by_day AS (
SELECT TO_DATE(fol.order_local_datetime) AS order_date,
    dp.sku,
    dp.product_sku,
    CASE WHEN mc.membership_order_type_l2 ILIKE 'Activating VIP' THEN 'Activating'
         WHEN mc.membership_order_type_l2 ILIKE ANY ('Repeat VIP','Guest') THEN 'Non-Activating' END AS is_activating,
    ds.store_country AS country,
    ds.store_type,
    ds.store_brand AS order_brand,
    CASE WHEN ds.store_type ILIKE 'Retail' THEN ds.store_country
         WHEN ds.store_country ILIKE 'UK' THEN 'UK'
         WHEN ds.store_region ILIKE 'EU' THEN 'EU' ELSE ds.store_country END AS country_region,
    CASE WHEN dpt.product_type_id = 15 THEN 'Part of an Outfit' ELSE 'Pieced Good' END AS outfit_pieced_good_flag,
    CASE WHEN UPPER(TRIM(I.SIZE)) ilike any ('OS%','ONE SIZE','ONESIZE') THEN 'OS'
         WHEN UPPER(TRIM(I.SIZE)) ilike '2XL%' THEN 'XXL'
         WHEN UPPER(TRIM(I.SIZE)) ilike '3XL%' THEN 'XXXL'
         WHEN UPPER(TRIM(I.SIZE)) ilike 'XXXL%' THEN 'XXXL'
         WHEN UPPER(TRIM(I.SIZE)) ilike 'XXL/1X%' THEN 'XXL/1X'
         WHEN UPPER(TRIM(I.SIZE)) ilike any ('XXL%','SIZE XXL%') THEN 'XXL'
         WHEN UPPER(TRIM(I.SIZE)) ilike any ('XL%','SIZE XL%') THEN 'XL'
         WHEN UPPER(TRIM(I.SIZE)) ilike 'XXS%' THEN 'XXS'
         WHEN UPPER(TRIM(I.SIZE)) ilike any ('XS%','SIZE XS%') THEN 'XS'
         WHEN UPPER(TRIM(I.SIZE)) ilike 'S/M%' THEN 'S/M'
         WHEN UPPER(TRIM(I.SIZE)) ilike 'M/L%' THEN 'M/L'
         WHEN UPPER(TRIM(I.SIZE)) ilike 'L/XL%' THEN 'L/XL'
         WHEN UPPER(TRIM(I.SIZE)) ilike any ('S%','SM%','SIZE S%') THEN 'S'
         WHEN UPPER(TRIM(I.SIZE)) ilike any ('M%','MD%','SIZE M%') THEN 'M'
         WHEN UPPER(TRIM(I.SIZE)) ilike any ('L%','LG%','SIZE L%') THEN 'L'
         WHEN UPPER(TRIM(I.SIZE)) ilike 'X3%' THEN i.size
         WHEN UPPER(TRIM(I.SIZE)) ilike '1X%' THEN '1X'
         WHEN UPPER(TRIM(I.SIZE)) ilike '2X%' THEN '2X'
         WHEN UPPER(TRIM(I.SIZE)) ilike '3X%' THEN '3X'
         WHEN UPPER(TRIM(I.SIZE)) ilike '4X%' THEN '4X'
         WHEN UPPER(TRIM(I.SIZE)) ilike '5X%' THEN '5X'
         WHEN UPPER(TRIM(I.SIZE)) ilike '6X%' THEN '6X'
         WHEN UPPER(TRIM(I.SIZE)) ilike 'Unknown' THEN 'OS'
         WHEN UPPER(TRIM(I.SIZE)) in ('2T','3T') THEN 'OTHER' ELSE UPPER(TRIM(I.SIZE)) END AS sizes,
    SUM(IFNULL(fol.price_offered_local_amount,0)*IFNULL(fol.ORDER_DATE_USD_CONVERSION_RATE,1)) AS AIR_VIP_PRICE,
    SUM(IFF(fol.token_count = 0 AND fol.price_offered_local_amount =(fol.product_gross_revenue_excl_shipping_local_amount - fol.tariff_revenue_local_amount),
            fol.item_quantity,0)) AS cash_full_price_unit_sales,
    SUM(fol.item_quantity)                                                                                 AS unit_sales,
    SUM(fol.token_count)                                                                                   AS tokens_applied,
    SUM(IFNULL(fol.subtotal_excl_tariff_local_amount, 0) * IFNULL(fol.order_date_usd_conversion_rate, 1))  AS subtotal_excl_tariff,
    SUM(IFNULL(fol.product_discount_local_amount, 0) * IFNULL(fol.order_date_usd_conversion_rate, 1))      AS discount,
    SUM(IFNULL(fol.product_gross_revenue_excl_shipping_local_amount, 0) * IFNULL(fol.order_date_usd_conversion_rate, 1)) AS product_revenue,
    SUM(IFNULL(pc.reporting_landed_cost_local_amount, 0) * IFNULL(fol.order_date_usd_conversion_rate, 1)) AS product_cost,
    SUM(IFNULL(fol.product_subtotal_local_amount, 0) * IFNULL(fol.order_date_usd_conversion_rate, 1)) AS subtotal,
    SUM(IFNULL(fol.tariff_revenue_local_amount, 0) * IFNULL(fol.order_date_usd_conversion_rate, 1)) AS tariff_revenue,
    SUM(IFF(fol.token_count > 0,fol.item_quantity,0)) AS token_unit_sales,
    SUM(IFF(fol.token_count > 0,(IFNULL(fol.subtotal_excl_tariff_local_amount,0) * IFNULL(fol.order_date_usd_conversion_rate,1)),0)) AS token_subtotal_excl_tariff,
    SUM(IFF(fol.token_count > 0,(IFNULL(fol.product_discount_local_amount,0) * IFNULL(fol.order_date_usd_conversion_rate,1)),0)) AS token_discount,
    SUM(IFF(fol.token_count > 0,(IFNULL(fol.product_gross_revenue_excl_shipping_local_amount, 0) * IFNULL(fol.order_date_usd_conversion_rate, 1)),0)) AS token_product_revenue,
    SUM(IFF(fol.token_count > 0,(IFNULL(pc.reporting_landed_cost_local_amount,0) * IFNULL(fol.order_date_usd_conversion_rate,1)),0)) AS token_product_cost,
    SUM(IFF(fol.token_count > 0,(IFNULL(fol.product_subtotal_local_amount,0) * IFNULL(fol.order_date_usd_conversion_rate,1)),0)) AS token_subtotal,
    SUM(IFF(fol.token_count > 0,(IFNULL(fol.tariff_revenue_local_amount, 0) *IFNULL(fol.order_date_usd_conversion_rate, 1)),0)) AS token_tariff_revenue,
    SUM(IFF(fol.token_count > 0,(IFNULL(fol.price_offered_local_amount,0)*IFNULL(fol.ORDER_DATE_USD_CONVERSION_RATE,1)),0)) AS token_AIR_VIP_price
FROM edw_prod.data_model_fl.fact_order_line fol
LEFT JOIN edw_prod.data_model_fl.fact_order_line_product_cost pc ON pc.order_line_id = fol.order_line_id
JOIN edw_prod.data_model_fl.fact_order fo ON fol.order_id = fo.order_id
JOIN edw_prod.data_model_fl.dim_product dp ON dp.product_id = fol.product_id
LEFT JOIN lake_view.ultra_warehouse.item i on i.item_number = dp.sku
JOIN edw_prod.data_model_fl.dim_store ds ON fol.store_id = ds.store_id
JOIN edw_prod.data_model_fl.dim_product_type dpt ON fol.product_type_key = dpt.product_type_key
JOIN edw_prod.data_model_fl.dim_order_membership_classification mc ON mc.order_membership_classification_key = fol.order_membership_classification_key
JOIN edw_prod.data_model_fl.dim_order_sales_channel doc ON fol.order_sales_channel_key = doc.order_sales_channel_key
JOIN edw_prod.data_model_fl.dim_order_status dos ON fol.order_status_key = dos.order_status_key
JOIN edw_prod.data_model_fl.dim_order_processing_status ops ON ops.order_processing_status_key = fo.order_processing_status_key
JOIN edw_prod.data_model_fl.dim_order_line_status ols ON fol.order_line_status_key = ols.order_line_status_key
WHERE ds.store_brand IN ('Fabletics', 'Yitty')
  AND dpt.is_free = 'false'
  AND dpt.product_type_name IN ('Bundle Component','Normal')
  AND doc.order_classification_l1 IN ('Product Order')
  AND doc.is_ps_order = 'FALSE'
  AND dos.order_status IN ('Success', 'Pending')
  AND order_date >= DATEADD('Month', -16, CURRENT_DATE())
  AND ols.order_line_status <> 'Cancelled'
GROUP BY order_date,dp.sku,dp.product_sku,is_activating,country,ds.store_type,order_brand,country_region,outfit_pieced_good_flag,sizes);

CREATE OR REPLACE TEMPORARY TABLE _sales_and_inv AS  (
SELECT inv.sku,
    inv.product_sku,
    inv.sizes as size,
    CASE WHEN IFF(min_ubt.sku IS NOT NULL, min_ubt.gender, max_ubt.gender) ILIKE 'MEN%' THEN 'MENS'
         ELSE 'WOMENS' END                                                                     AS gender,
    NULL                                                                                       AS is_activating,
    inv.date                                                                                   AS order_date,
    inv.country_region,
    CASE WHEN inv.country_region ILIKE 'EU' THEN NULL
         ELSE inv.country_region END                                                           AS country,
    CASE WHEN inv.country_region IN ('US', 'CA') THEN 'NA' ELSE 'EU' END                       AS region,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.current_name,max_ubt.current_name)                    AS current_name,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.color, max_ubt.color)                                 AS color,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.image_url, max_ubt.image_url)                         AS image_url,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.current_showroom,max_ubt.current_showroom)            AS current_showroom,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.category, max_ubt.category)                           AS category,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.class, max_ubt.class)                                 AS class,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.us_vip_dollar, max_ubt.us_vip_dollar)                 AS vip_price,
    CASE WHEN IFF(min_ubt.sku IS NOT NULL, min_ubt.color_family, max_ubt.color_family) ILIKE 'GRAY' THEN 'GREY'
         ELSE IFF(min_ubt.sku IS NOT NULL, min_ubt.color_family,max_ubt.color_family) END      AS color_family,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.subclass, max_ubt.subclass)                           AS subclass,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.sku_status, max_ubt.sku_status)                       AS sku_status,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.style_status,max_ubt.style_status)                    AS style_status,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.item_status, max_ubt.item_status)                     AS lifecycle,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.fabric, max_ubt.fabric)                               AS fabric,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.initial_launch,max_ubt.initial_launch)                AS special_collection_1,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.factory, max_ubt.factory)                             AS factory,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.design_style_number,max_ubt.design_style_number)      AS design_style_number,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.item_rank, max_ubt.item_rank)                         AS item_rank,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.buy_timing, max_ubt.buy_timing)                       AS buy_timing,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.fit_block, max_ubt.fit_block)                         AS fit_block,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.eco_system, max_ubt.eco_system)                       AS ecosystem,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.end_use, max_ubt.end_use)                             AS end_use,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.color_application,max_ubt.color_application)          AS color_application,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.fit_style, max_ubt.fit_style)                         AS fit_style,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.mainline_capsule,max_ubt.mainline_capsule)            AS mainline_capsule,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.inseams_construction,max_ubt.inseams_construction)    AS inseams_construction,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.channel, max_ubt.channel)                             AS channel,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.size_scale, max_ubt.size_scale)                       AS size_scale,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.size_range, max_ubt.size_range)                       AS size_range,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.editorial_features,max_ubt.editorial_features)        AS editorial_features,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.eco_style, max_ubt.eco_style)                         AS eco_style,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.lined_unlined,max_ubt.lined_unlined)                  AS lined_unlined,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.gbb_pricing,max_ubt.gbb_pricing)                      AS gbb_pricing,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.sub_brand, max_ubt.sub_brand)                         AS sub_brand,
    IFF(min_ubt.sku IS NOT NULL, COALESCE(min_ubt.outfit_alias_1, min_ubt.outfit_alias_2),COALESCE(max_ubt.outfit_alias_1, max_ubt.outfit_alias_2)) AS outfit_alias,
    CASE WHEN min_ubt.sku IS NOT NULL THEN 1
         WHEN max_ubt.sku IS NOT NULL THEN 0
         ELSE NULL END                                                                         AS early_release_flag,
    'Select for Inventory Metrics'                                                             AS outfit_pieced_good_flag,
    CURRENT_TIMESTAMP()                                                                        AS refresh_time,
    inv.store_type,
    'Inventory'                                                                                AS order_brand,
    SUM(inv.qty_onhand)                                                                        AS qty_onhand,
    SUM(inv.qty_replen)                                                                        AS qty_replen,
    SUM(inv.qty_ghost)                                                                         AS qty_ghost,
    SUM(inv.qty_order_reserve)                                                                 AS qty_order_reserve,
    SUM(inv.qty_special_pick_reserve)                                                          AS qty_special_pick_reserve,
    SUM(inv.qty_manual_stock_reserve)                                                          AS qty_manual_stock_reserve,
    SUM(inv.qty_ri)                                                                            AS qty_ri,
    SUM(inv.qty_pick_staging)                                                                  AS qty_pick_staging,
    SUM(inv.qty_staging)                                                                       AS qty_staging,
    SUM(inv.qty_available_to_sell)                                                             AS fc_available_inventory,
    SUM(inv.qty_onhand + inv.qty_replen + inv.qty_ri + inv.qty_pick_staging + inv.qty_staging) AS otb_inventory,
    NULL                                                                                       AS unit_sales,
    NULL                                                                                       AS tokens_applied,
    NULL                                                                                       AS product_revenue,
    NULL                                                                                       AS product_cost,
    NULL                                                                                       AS air_vip_price,
    NULL                                                                                       AS cash_full_price_unit_sales,
    NULL                                                                                       AS token_product_revenue,
    NULL                                                                                       AS token_product_cost,
    NULL                                                                                       AS token_discount,
    NULL                                                                                       AS discount,
    NULL                                                                                       AS subtotal,
    NULL                                                                                       AS token_subtotal,
    NULL                                                                                       AS token_unit_sales,
    NULL                                                                                       AS token_subtotal_excl_tariff,
    NULL                                                                                       AS subtotal_excl_tariff,
    NULL                                                                                       AS tariff_revenue,
    NULL                                                                                       AS token_tariff_revenue,
    NULL                                                                                      AS token_AIR_VIP_price
FROM _fl_inventory_by_day inv
LEFT JOIN _max_showroom_ubt_with_images max_ubt ON inv.product_sku = max_ubt.sku
LEFT JOIN _min_showroom_ubt_with_images min_ubt ON inv.product_sku = min_ubt.sku
    AND min_ubt.current_showroom::DATE = DATEADD(MONTH, 1, DATE_TRUNC('month', inv.date::DATE))
WHERE inv.date >= DATEADD('Month', -16, CURRENT_DATE())
    AND (max_ubt.sku IS NOT NULL OR min_ubt.sku IS NOT NULL)
GROUP BY inv.sku,
    inv.product_sku,
    inv.sizes,
    CASE WHEN IFF(min_ubt.sku IS NOT NULL, min_ubt.gender, max_ubt.gender) ILIKE 'MEN%' THEN 'MENS'
         ELSE 'WOMENS' END,
    inv.date,
    inv.date,
    inv.country_region,
    country,
    region,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.current_name, max_ubt.current_name),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.color, max_ubt.color),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.image_url, max_ubt.image_url),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.current_showroom, max_ubt.current_showroom),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.category, max_ubt.category),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.class, max_ubt.class),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.us_vip_dollar, max_ubt.us_vip_dollar),
    CASE WHEN IFF(min_ubt.sku IS NOT NULL, min_ubt.color_family, max_ubt.color_family) ILIKE 'GRAY' THEN 'GREY'
         ELSE IFF(min_ubt.sku IS NOT NULL, min_ubt.color_family, max_ubt.color_family) END,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.subclass, max_ubt.subclass),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.sku_status, max_ubt.sku_status),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.style_status, max_ubt.style_status),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.item_status, max_ubt.item_status),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.fabric, max_ubt.fabric),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.initial_launch, max_ubt.initial_launch),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.factory, max_ubt.factory),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.design_style_number, max_ubt.design_style_number),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.item_rank, max_ubt.item_rank),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.buy_timing, max_ubt.buy_timing),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.fit_block, max_ubt.fit_block),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.eco_system, max_ubt.eco_system),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.end_use, max_ubt.end_use),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.color_application, max_ubt.color_application),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.fit_style, max_ubt.fit_style),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.mainline_capsule, max_ubt.mainline_capsule),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.inseams_construction, max_ubt.inseams_construction),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.channel, max_ubt.channel),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.size_scale, max_ubt.size_scale),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.size_range, max_ubt.size_range),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.editorial_features, max_ubt.editorial_features),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.eco_style, max_ubt.eco_style),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.lined_unlined, max_ubt.lined_unlined),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.gbb_pricing, max_ubt.gbb_pricing),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.sub_brand, max_ubt.sub_brand),
    IFF(min_ubt.sku IS NOT NULL, COALESCE(min_ubt.outfit_alias_1, min_ubt.outfit_alias_2),
    COALESCE(max_ubt.outfit_alias_1, max_ubt.outfit_alias_2)),
    CASE WHEN min_ubt.sku IS NOT NULL THEN 1 WHEN max_ubt.sku IS NOT NULL THEN 0 ELSE NULL END,
    CURRENT_TIMESTAMP(),
    inv.store_type
UNION ALL
SELECT sales.sku,
    sales.product_sku,
    sales.sizes,
    CASE WHEN IFF(min_ubt.sku IS NOT NULL, min_ubt.gender, max_ubt.gender) ILIKE 'MEN%' THEN 'MENS'
         ELSE 'WOMENS' END                                                                     AS gender,
    sales.is_activating,
    sales.order_date,
    sales.country_region,
    sales.country,
    CASE WHEN sales.country_region IN ('US', 'CA') THEN 'NA' ELSE 'EU' END                     AS region,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.current_name,max_ubt.current_name)                    AS current_name,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.color, max_ubt.color)                                 AS color,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.image_url, max_ubt.image_url)                         AS image_url,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.current_showroom,max_ubt.current_showroom)            AS current_showroom,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.category, max_ubt.category)                           AS category,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.class, max_ubt.class)                                 AS class,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.us_vip_dollar, max_ubt.us_vip_dollar)                 AS vip_price,
    CASE WHEN IFF(min_ubt.sku IS NOT NULL, min_ubt.color_family, max_ubt.color_family) ILIKE 'GRAY' THEN 'GREY'
         ELSE IFF(min_ubt.sku IS NOT NULL, min_ubt.color_family,max_ubt.color_family) END      AS color_family,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.subclass, max_ubt.subclass)                           AS subclass,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.sku_status, max_ubt.sku_status)                       AS sku_status,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.style_status,max_ubt.style_status)                    AS style_status,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.item_status, max_ubt.item_status)                     AS lifecycle,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.fabric, max_ubt.fabric)                               AS fabric,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.initial_launch,max_ubt.initial_launch)                AS special_collection_1,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.factory, max_ubt.factory)                             AS factory,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.design_style_number,max_ubt.design_style_number)      AS design_style_number,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.item_rank, max_ubt.item_rank)                         AS item_rank,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.buy_timing, max_ubt.buy_timing)                       AS buy_timing,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.fit_block, max_ubt.fit_block)                         AS fit_block,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.eco_system, max_ubt.eco_system)                       AS ecosystem,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.end_use, max_ubt.end_use)                             AS end_use,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.color_application,max_ubt.color_application)          AS color_application,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.fit_style, max_ubt.fit_style)                         AS fit_style,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.mainline_capsule,max_ubt.mainline_capsule)            AS mainline_capsule,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.inseams_construction,max_ubt.inseams_construction)    AS inseams_construction,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.channel, max_ubt.channel)                             AS channel,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.size_scale, max_ubt.size_scale)                       AS size_scale,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.size_range, max_ubt.size_range)                       AS size_range,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.editorial_features,max_ubt.editorial_features)        AS editorial_features,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.eco_style, max_ubt.eco_style)                         AS eco_style,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.lined_unlined,max_ubt.lined_unlined)                  AS lined_unlined,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.gbb_pricing,max_ubt.gbb_pricing)                      AS gbb_pricing,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.sub_brand, max_ubt.sub_brand)                         AS sub_brand,
    IFF(min_ubt.sku IS NOT NULL, COALESCE(min_ubt.outfit_alias_1, min_ubt.outfit_alias_2),COALESCE(max_ubt.outfit_alias_1, max_ubt.outfit_alias_2)) AS outfit_alias,
    CASE WHEN min_ubt.sku IS NOT NULL THEN 1
         WHEN max_ubt.sku IS NOT NULL THEN 0
         ELSE NULL END                                                                         AS early_release_flag,
    sales.outfit_pieced_good_flag,
    CURRENT_TIMESTAMP()                                                                        AS refresh_time,
    sales.store_type,
    sales.order_brand,
    NULL                                                                                       AS qty_onhand,
    NULL                                                                                       AS qty_replen,
    NULL                                                                                       AS qty_ghost,
    NULL                                                                                       AS qty_order_reserve,
    NULL                                                                                       AS qty_special_pick_reserve,
    NULL                                                                                       AS qty_manual_stock_reserve,
    NULL                                                                                       AS qty_ri,
    NULL                                                                                       AS qty_pick_staging,
    NULL                                                                                       AS qty_staging,
    NULL                                                                                       AS fc_available_inventory,
    NULL                                                                                       AS otb_inventory,
    SUM(sales.unit_sales)                                                                      AS unit_sales,
    SUM(sales.tokens_applied)                                                                  AS tokens_applied,
    SUM(sales.product_revenue)                                                                 AS product_revenue,
    SUM(sales.product_cost)                                                                    AS product_cost,
    SUM(sales.air_vip_price)                                                                   AS air_vip_price,
    SUM(sales.cash_full_price_unit_sales)                                                      AS cash_full_price_unit_sales,
    SUM(sales.token_product_revenue)                                                           AS token_product_revenue,
    SUM(sales.token_product_cost)                                                              AS token_product_cost,
    SUM(sales.token_discount)                                                                  AS token_discount,
    SUM(sales.discount)                                                                        AS discount,
    SUM(sales.subtotal)                                                                        AS subtotal,
    SUM(sales.token_subtotal)                                                                  AS token_subtotal,
    SUM(sales.token_unit_sales)                                                                AS token_unit_sales,
    SUM(sales.token_subtotal_excl_tariff)                                                      AS token_subtotal_excl_tariff,
    SUM(sales.subtotal_excl_tariff)                                                            AS subtotal_excl_tariff,
    SUM(sales.tariff_revenue)                                                                  AS tariff_revenue,
    SUM(sales.token_tariff_revenue)                                                            AS token_tariff_revenue,
    SUM(sales.token_AIR_VIP_price)                                                             AS token_AIR_VIP_price
FROM _fl_sales_by_day sales
LEFT JOIN _max_showroom_ubt_with_images max_ubt ON sales.product_sku = max_ubt.sku
LEFT JOIN _min_showroom_ubt_with_images min_ubt ON sales.product_sku = min_ubt.sku
    AND min_ubt.current_showroom::DATE = DATEADD(MONTH, 1, DATE_TRUNC('month', sales.order_date::DATE))
WHERE sales.country IS NOT NULL
    AND sales.order_date >= DATEADD('Month', -16, CURRENT_DATE())
    AND (max_ubt.sku IS NOT NULL OR min_ubt.sku IS NOT NULL)
GROUP BY sales.sku,
    sales.product_sku,
    sales.sizes,
    CASE WHEN IFF(min_ubt.sku IS NOT NULL, min_ubt.gender, max_ubt.gender) ILIKE 'MEN%' THEN 'MENS'
         ELSE 'WOMENS' END,
    sales.is_activating,
    sales.order_date,
    sales.country_region,
    sales.country,
    region,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.current_name, max_ubt.current_name),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.color, max_ubt.color),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.image_url, max_ubt.image_url),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.current_showroom, max_ubt.current_showroom),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.category, max_ubt.category),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.class, max_ubt.class),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.us_vip_dollar, max_ubt.us_vip_dollar),
    CASE WHEN IFF(min_ubt.sku IS NOT NULL, min_ubt.color_family, max_ubt.color_family) ILIKE 'GRAY' THEN 'GREY'
         ELSE IFF(min_ubt.sku IS NOT NULL, min_ubt.color_family, max_ubt.color_family) END,
    IFF(min_ubt.sku IS NOT NULL, min_ubt.subclass, max_ubt.subclass),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.sku_status, max_ubt.sku_status),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.style_status, max_ubt.style_status),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.item_status, max_ubt.item_status),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.fabric, max_ubt.fabric),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.initial_launch, max_ubt.initial_launch),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.factory, max_ubt.factory),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.design_style_number, max_ubt.design_style_number),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.item_rank, max_ubt.item_rank),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.buy_timing, max_ubt.buy_timing),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.fit_block, max_ubt.fit_block),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.eco_system, max_ubt.eco_system),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.end_use, max_ubt.end_use),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.color_application, max_ubt.color_application),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.fit_style, max_ubt.fit_style),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.mainline_capsule, max_ubt.mainline_capsule),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.inseams_construction, max_ubt.inseams_construction),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.channel, max_ubt.channel),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.size_scale, max_ubt.size_scale),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.size_range, max_ubt.size_range),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.editorial_features, max_ubt.editorial_features),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.eco_style, max_ubt.eco_style),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.lined_unlined, max_ubt.lined_unlined),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.gbb_pricing, max_ubt.gbb_pricing),
    IFF(min_ubt.sku IS NOT NULL, min_ubt.sub_brand, max_ubt.sub_brand),
    IFF(min_ubt.sku IS NOT NULL, COALESCE(min_ubt.outfit_alias_1, min_ubt.outfit_alias_2),COALESCE(max_ubt.outfit_alias_1, max_ubt.outfit_alias_2)),
    CASE WHEN min_ubt.sku IS NOT NULL THEN 1 WHEN max_ubt.sku IS NOT NULL THEN 0
         ELSE NULL END,
    sales.outfit_pieced_good_flag,
    CURRENT_TIMESTAMP,
    sales.store_type,
    sales.order_brand
);

CREATE OR REPLACE TEMPORARY TABLE _received_qty AS(
SELECT DISTINCT upper(po_number) AS po_number,
    UPPER(ds.sku) AS sku,
    SUM(quantity - quantity_cancelled) AS qty_received,
    SUM(ri.quantity_cancelled) AS qty_cancelled
FROM lake_view.ultra_warehouse.receipt r
JOIN lake_view.ultra_warehouse.receipt_item ri on r.receipt_id = ri.receipt_id
JOIN lake_view.ultra_warehouse.item i on ri.item_id = i.item_id
JOIN edw_prod.data_model_fl.dim_sku ds on i.item_number = ds.sku
WHERE r.label NOT ILIKE '%-C'
    AND r.datetime_received < current_date
GROUP BY 1,2);

CREATE OR REPLACE TEMPORARY TABLE _store_warehouse AS(
SELECT DISTINCT warehouse_id,
    country,
    region,
    is_retail
FROM edw_prod.reference.store_warehouse);

CREATE OR REPLACE TEMPORARY TABLE _po_data AS(
select dd.show_room,
       dd.date_launch,
       dd.sku                                                          AS sku,
       substr(dd.sku, 0, charindex('-', dd.sku, (charindex('-', dd.sku, 1)) + 1) - 1)         AS product_sku,
       IFF(sw.is_retail = 0,'Online','Retail')                         AS store_type,
       country                                                         AS store_country,
       region                                                          AS store_region,
       SUM(dd.qty)                                                     AS qty_ordered,
       SUM(uw.qty_received)                                            AS qty_received,
       SUM(uw.qty_cancelled)                                           AS qty_cancelled
FROM REPORTING_PROD.GSC.PO_DETAIL_DATASET dd
JOIN _store_warehouse sw on sw.warehouse_id = dd.warehouse_id
LEFT JOIN lake.excel.fl_items_ubt ubt ON (CASE WHEN LENGTH(UBT.SKU) = 15 then left(dd.sku, 15) = ubt.sku
                                               WHEN LENGTH(UBT.SKU) = 14 then left(dd.sku, 14) = ubt.sku end) and dd.show_room = left(ubt.current_showroom, 7)
LEFT JOIN _received_qty uw on uw.po_number = dd.po_number and uw.sku = dd.sku
WHERE UPPER(dd.division_id) in ('FABLETICS','YITTY')
    AND dd.line_status not ilike 'Line Canceled'
    AND dd.po_status_text not ilike 'Cancelled'
    AND dd.po_status_id <> 10
    AND dd.qty > 0
GROUP BY 1,2,3,4,5,6,7
HAVING (SUM(dd.qty) - (SUM(uw.qty_received)+ SUM(uw.qty_cancelled)) > (SUM(dd.qty) * .05)));

CREATE OR REPLACE TEMPORARY TABLE _min_date AS(
SELECT sku,
    store_type,
    store_country,
    store_region,
    MIN(date_launch) AS date_launch
FROM _po_data
WHERE date_launch >= CURRENT_DATE()
GROUP BY sku,store_type,store_country,store_region);

CREATE OR REPLACE TEMPORARY TABLE _po_data_final AS (
SELECT po.sku,
    po.store_type,
    po.date_launch,
    po.show_room,
    CASE WHEN po.store_type ILIKE 'Retail' THEN po.store_country
         WHEN po.store_country ILIKE 'UK' THEN 'UK'
         WHEN po.store_region ILIKE 'EU' THEN 'EU'
         ELSE po.store_country END AS country_region,
    SUM(qty_ordered) AS qty_ordered
FROM _po_data po
JOIN _min_date md ON md.sku = po.sku
    AND md.store_type = po.store_type
    AND md.store_country = po.store_country
    AND md.store_region = po.store_region
    AND md.date_launch = po.date_launch
GROUP BY po.sku,
    po.store_type,
    po.date_launch,
    po.show_room,
    country_region);

CREATE OR REPLACE TRANSIENT TABLE reporting_prod.fabletics.merch_master_item AS(
SELECT s.*,
    CASE WHEN ubt.item_status ILIKE '%core%' THEN 'CORE'
         WHEN DATE_PART("MONTH",CURRENT_DATE()) < 7 AND DATE_TRUNC('YEAR', ubt.current_showroom) = DATEADD(YEAR, -1, DATE_TRUNC('YEAR', CURRENT_DATE()))
            THEN 'Q' || EXTRACT('QUARTER',ubt.current_showroom) || ' ' || DATE_PART('YEAR',ubt.current_showroom)
         WHEN ubt.current_showroom <  DATE_TRUNC("YEAR", CURRENT_DATE()) THEN DATE_PART('YEAR', ubt.current_showroom)::STRING
         ELSE ubt.current_showroom::STRING END AS core_current_showroom,
    date_launch AS po_date_launch,
    show_room AS po_showroom,
    qty_ordered AS po_qty_ordered
FROM _sales_and_inv s
LEFT JOIN lake.excel.fl_items_ubt ubt ON s.product_sku = ubt.sku
    AND s.current_showroom = ubt.current_showroom
LEFT JOIN _po_data_final po ON po.sku = s.sku
    AND po.store_type = s.store_type
    AND po.country_region  = s.country_region
    AND order_brand = 'Inventory');
