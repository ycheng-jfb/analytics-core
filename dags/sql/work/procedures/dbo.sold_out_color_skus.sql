CREATE OR REPLACE TEMP TABLE _inv_first_ready AS
SELECT UPPER(product_sku) AS color_sku,
       region,
       MIN(date)          AS first_inventory_ready_date
FROM reporting_prod.sxf.view_base_inventory_dataset i
WHERE qty_available_to_sell > 0
GROUP BY product_sku, region
;
--Scaffold at store, sku,date level
CREATE OR REPLACE TEMP TABLE _sku_date_store AS
SELECT sc.fixed_date       AS date,
       UPPER(sc.sku)       AS sku,
       UPPER(sc.color_sku) AS color_sku,
       sc.store_id,
       sc.store_name,
       sc.region,
       sc.store_type,
       sc.country
FROM reporting_prod.sxf.view_product_sku_date_store sc
         JOIN _inv_first_ready inv ON UPPER(inv.color_sku) = UPPER(sc.color_sku) AND inv.region = sc.region
WHERE sc.fixed_date >= '2020-01-01'
  AND sc.fixed_date <= CURRENT_DATE
  AND sc.store_name NOT IN ('RTLSXF-Las Vegas', 'RTLSXF-Mall of America', 'RTLSXF-SOHO', 'RTLSXF-Valley Fair')
  AND sc.fixed_date >= inv.first_inventory_ready_date
;
--fetch the date sku, store combinations by eliminating Hide from grid skus
CREATE OR REPLACE TEMP TABLE _sellable_skus_scaffold AS
WITH store AS (SELECT DISTINCT store_group_id,
                               store_region,
                               store_id,
                               store_country
               FROM edw_prod.data_model_sxf.dim_store
               WHERE store_brand_abbr = 'SX'
                 AND store_type = 'Online'
                 AND is_core_store = TRUE)
   , _hfg AS (SELECT DISTINCT fpl.label,
                              fp.product_id,
                              fp.datetime_added::DATE                         AS start_date,
                              COALESCE(dl.datetime_added::DATE, CURRENT_DATE) AS delete_date,
                              store.store_country
              FROM lake_consolidated.ultra_merchant.featured_product_location fpl
                       LEFT JOIN lake_consolidated.ultra_merchant.featured_product fp
                                 ON fpl.featured_product_location_id = fp.featured_product_location_id
                       LEFT JOIN lake_sxf_view.ultra_merchant.featured_product_delete_log dl
                                 ON fp.featured_product_id = CONCAT(dl.featured_product_id, 30)
                       JOIN edw_prod.data_model_sxf.dim_product dp ON CONCAT(dp.product_id, 30) = fp.product_id
                       JOIN store ON fpl.store_group_id = store.store_group_id AND dp.store_id = store.store_id
              WHERE 1 = 1
                AND fp.active = 1
                AND LOWER(fpl.label) IN ('hide from grid', 'hide from grid (hfg)'))
   , _hfg_skus AS (SELECT hfg.start_date, hfg.delete_date, UPPER(dp.sku) AS sku, hfg.store_country
                   FROM _hfg hfg
                            JOIN edw_prod.data_model_sxf.dim_product mp ON CONCAT(mp.product_id, 30) = hfg.product_id
                            LEFT JOIN edw_prod.data_model_sxf.dim_product dp ON dp.master_product_id = mp.product_id
                   WHERE mp.is_active = 1
                   GROUP BY 1, 2, 3, 4)
SELECT sc.*
FROM _sku_date_store sc
         LEFT JOIN _hfg_skus hf
                   ON sc.sku = hf.sku AND sc.country = hf.store_country AND sc.date BETWEEN start_date AND delete_date
WHERE hf.sku IS NULL --this eliminates skus in hide from grid
;
--Actual scaffold needed for sold out report
CREATE OR REPLACE TEMP TABLE _scaffold AS
SELECT DISTINCT date
              , IFF(store_type = 'Retail', 'Y', 'N')           AS is_retail
              , region
              , IFF(store_type = 'Retail', store_name, region) AS store_group
              , color_sku                                      AS color_sku
FROM _sellable_skus_scaffold
GROUP BY 1, 2, 3, 4, 5
;
--base inv data
CREATE OR REPLACE TEMPORARY TABLE _hq_data AS
SELECT date::DATE                                     AS date,
       is_retail,
       region,
       IFF(is_retail = 'Y', LOWER(warehouse), region) AS store_group,
       UPPER(color_sku_po)                            AS color_sku,
       SUM(qty_available_to_sell)                     AS qty_available_to_sell
FROM reporting_prod.sxf.view_base_inventory_dataset
WHERE date::DATE >= '2020-01-01'
GROUP BY 1, 2, 3, 4, 5;
-- Local inventory data to adjust the missing combinations in _hq_data
CREATE OR REPLACE TEMPORARY TABLE _local_data AS
SELECT local_date::DATE                AS date,
       w.is_retail,
       w.region_abbr                   AS region,
       w.store_group,
       UPPER(product_sku)              AS color_sku,
       SUM(available_to_sell_quantity) AS qty_available_to_sell
FROM edw_prod.data_model_sxf.fact_inventory_history inv
         JOIN (SELECT DISTINCT IFF(ds.store_type = 'Retail', 'Y', 'N')                            AS is_retail
                             , IFF(store_full_name = 'Savage X UK', 'EU-UK', region)              AS region_abbr
                             , warehouse_id
                             , IFF(ds.store_type = 'Retail', LOWER(store_full_name), region_abbr) AS store_group
               FROM edw_prod.reference.store_warehouse sw
                        JOIN edw_prod.data_model_sxf.dim_store ds ON sw.store_id = ds.store_id
               WHERE ds.store_brand = 'Savage X'
                 AND ds.store_country NOT IN ('CA', 'DL', 'NL', 'SE')
                 AND ds.store_full_name NOT ILIKE '%(DM)%'
                 AND store_full_name NOT IN
                     ('RTLSXF-Las Vegas', 'RTLSXF-Mall of America', 'RTLSXF-SOHO', 'RTLSXF-Valley Fair')) w
              ON inv.warehouse_id = w.warehouse_id
         JOIN reporting_prod.sxf.view_style_master_size sm ON (sm.sku = inv.sku) AND date(local_date) >= '2020-01-01'
GROUP BY 1, 2, 3, 4, 5;
--Scaffold with inventory columns
CREATE OR REPLACE TEMP TABLE _inv AS
SELECT s.date
     , s.is_retail
     , s.region
     , s.store_group
     , s.color_sku
     , COALESCE(hq.qty_available_to_sell, 0) AS qty_available_to_sell
FROM _scaffold s
         LEFT JOIN _hq_data hq ON s.date = hq.date
    AND LOWER(s.store_group) = LOWER(hq.store_group)
    AND s.region = hq.region
    AND s.is_retail = hq.is_retail
    AND s.color_sku = hq.color_sku
;
-- Added Product related columns
CREATE OR REPLACE TRANSIENT TABLE work.dbo.sold_out_color_skus AS
SELECT f.date
     , f.is_retail
     , f.region
     , f.store_group
     , f.color_sku
     , f.qty_available_to_sell
     , 'Y'                                                                      AS is_sellable_color_sku
     , sm.site_name
     , sm.style_name
     , sm.site_color
     , sm.size_range
     , sm.style_number_po                                                       AS style_number
     , sm.sub_department
     , sm.category
     , sm.collection
     , sm.savage_showroom
     , sm.subcategory
     , sm.fabric
     , sm.category_type
     , sm.core_fashion
     , sm.gender
     , sm.latest_showroom
     , CASE
           WHEN sm.active_sets IS NOT NULL AND sm.active_sets <> '' AND sm.vip_box IS NOT NULL AND sm.vip_box <> ''
               THEN CONCAT(sm.active_sets, ' & ', sm.vip_box)
           WHEN sm.active_sets IS NOT NULL AND sm.active_sets <> '' THEN sm.active_sets
           WHEN sm.vip_box IS NOT NULL AND sm.vip_box <> '' THEN sm.vip_box END AS bundle_name
     , sm.image_url
     , CURRENT_TIMESTAMP                                                        AS last_refreshed_date
FROM _inv f
         JOIN reporting_prod.sxf.style_master sm ON sm.color_sku_po = f.color_sku;
