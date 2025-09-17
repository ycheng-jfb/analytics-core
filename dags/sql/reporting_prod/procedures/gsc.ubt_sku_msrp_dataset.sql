SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

TRUNCATE TABLE IF EXISTS gsc.ubt_sku_msrp_dataset;

INSERT INTO gsc.ubt_sku_msrp_dataset(sku, msrp, meta_create_datetime, meta_update_datetime)
WITH _fbl_latest AS (SELECT sku,
                            MAX(current_showroom) AS latest_launch
                     FROM lake.excel.fl_items_ubt
                     GROUP BY sku),
     _gfb_latest AS (SELECT product_sku,
                            MAX(latest_launch_date) AS latest_launch
                     FROM gfb.merch_dim_product
                     GROUP BY product_sku),
     _sxf_latest AS (SELECT color_sku_po,
                            MAX(latest_showroom) AS latest_launch
                     FROM sxf.style_master
                     GROUP BY color_sku_po)
SELECT a.sku,
       a.us_msrp_dollar,
       $execution_start_time AS meta_create_datetime,
       $execution_start_time AS meta_update_datetime
FROM lake.excel.fl_items_ubt a
         JOIN _fbl_latest b ON a.sku = b.sku AND a.current_showroom = b.latest_launch
UNION ALL
SELECT a.product_sku,
       a.msrp,
       $execution_start_time AS meta_create_datetime,
       $execution_start_time AS meta_update_datetime
FROM gfb.merch_dim_product a
         JOIN _gfb_latest b ON a.product_sku = b.product_sku AND a.latest_launch_date = b.latest_launch
WHERE a.region = 'NA'
  AND a.country = 'US'
UNION ALL
SELECT a.color_sku_po,
       a.msrp,
       $execution_start_time AS meta_create_datetime,
       $execution_start_time AS meta_update_datetime
FROM sxf.style_master a
         JOIN _sxf_latest b ON a.color_sku_po = b.color_sku_po AND a.latest_showroom = b.latest_launch
;
