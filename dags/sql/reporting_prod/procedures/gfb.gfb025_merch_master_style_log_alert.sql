CREATE OR REPLACE TEMPORARY TABLE _product AS
SELECT a.*
     , (CASE
            WHEN dp.product_status = 'Active' THEN 1
            ELSE 0 END) AS is_active
FROM (
         SELECT DISTINCT UPPER(st.store_brand)                                                                                                  AS business_unit
                       , UPPER(st.store_region)                                                                                                 AS region
                       , dp.product_sku
                       , FIRST_VALUE(dp.master_product_id)
                                     OVER (PARTITION BY dp.product_sku, st.store_brand, st.store_region ORDER BY dp.current_showroom_date DESC) AS master_product_id
                       , FIRST_VALUE(dp.product_name)
                                     OVER (PARTITION BY dp.product_sku, st.store_brand, st.store_region ORDER BY dp.current_showroom_date DESC) AS product_name
                       , FIRST_VALUE(dp.color)
                                     OVER (PARTITION BY dp.product_sku, st.store_brand, st.store_region ORDER BY dp.current_showroom_date DESC) AS color
         FROM edw_prod.data_model_jfb.dim_product dp
                  JOIN reporting_prod.gfb.vw_store st
                       ON st.store_id = dp.store_id
         WHERE dp.master_product_id > 0
           AND dp.retail_unit_price != 0
           AND dp.product_name != 'DO NOT USE'
           AND st.store_country IN ('US', 'FR')
     ) a
         JOIN edw_prod.data_model_jfb.dim_product dp
              ON dp.product_id = a.master_product_id;


CREATE OR REPLACE TEMPORARY TABLE _sales_product_sku AS
SELECT olp.business_unit
     , olp.region
     , olp.product_sku
     , SUM(olp.total_qty_sold) AS total_qty_sold_last_3_month
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date olp
WHERE olp.order_classification = 'product order'
  AND olp.order_date >= DATEADD(MONTH, -3, CURRENT_DATE())
GROUP BY olp.business_unit
       , olp.region
       , olp.product_sku;


CREATE OR REPLACE TEMPORARY TABLE _inventory_product_sku AS
SELECT ic.business_unit
     , ic.region
     , ic.product_sku
     , SUM(ic.qty_onhand)            AS qty_onhand_current
     , SUM(ic.qty_available_to_sell) AS qty_available_to_sell_current
FROM reporting_prod.gfb.gfb_inventory_data_set_current ic
GROUP BY ic.business_unit
       , ic.region
       , ic.product_sku;


CREATE OR REPLACE TEMPORARY TABLE _sales_inventory_product_sku AS
SELECT COALESCE(sps.business_unit, ips.business_unit) AS business_unit
     , COALESCE(sps.region, ips.region)               AS region
     , COALESCE(sps.product_sku, ips.product_sku)     AS product_sku
     , COALESCE(sps.total_qty_sold_last_3_month, 0)   AS total_qty_sold_last_3_month
     , COALESCE(ips.qty_onhand_current, 0)            AS qty_onhand_current
     , COALESCE(ips.qty_available_to_sell_current, 0) AS qty_available_to_sell_current
FROM _sales_product_sku sps
         FULL JOIN _inventory_product_sku ips
                   ON ips.business_unit = sps.business_unit
                       AND ips.region = sps.region
                       AND ips.product_sku = sps.product_sku;


CREATE OR REPLACE TEMPORARY TABLE _merch_dim_product AS
SELECT DISTINCT mdp.business_unit
              , mdp.region
              , mdp.product_sku
FROM reporting_prod.gfb.merch_dim_product mdp;


CREATE OR REPLACE TEMPORARY TABLE _po_launch_date AS
SELECT pds.region
     , pds.product_sku
     , MAX(pds.launch_date) AS latest_launch_date
FROM reporting_prod.gfb.gfb_po_data_set pds
GROUP BY pds.region
       , pds.product_sku;


CREATE OR REPLACE TEMPORARY TABLE _first_sale_date AS
SELECT olp.business_unit
     , olp.region
     , olp.product_sku
     , MIN(olp.order_date) AS first_sale_date
FROM reporting_prod.gfb.gfb_order_line_data_set_place_date olp
WHERE olp.order_classification = 'product order'
GROUP BY olp.business_unit
       , olp.region
       , olp.product_sku;


CREATE OR REPLACE TEMPORARY TABLE _first_inventory_date AS
SELECT id.business_unit
     , id.region
     , id.product_sku
     , MIN(id.inventory_date) AS first_inventory_date
FROM reporting_prod.gfb.gfb_inventory_data_set id
GROUP BY id.business_unit
       , id.region
       , id.product_sku;


CREATE OR REPLACE TEMPORARY TABLE _po_department AS
SELECT DISTINCT pds.division_id                                                                                                                                                                AS business_unit
              , pds.region_id                                                                                                                                                                  AS region
              , ARRAY_TO_STRING(ARRAY_SLICE(SPLIT(pds.sku, '-'), 0, -1), '-')                                                                                                                  AS product_sku
              , FIRST_VALUE(UPPER(pds.department))
                            OVER (PARTITION BY pds.division_id, pds.region_id, ARRAY_TO_STRING(ARRAY_SLICE(SPLIT(pds.sku, '-'), 0, -1), '-') ORDER BY DATE_TRUNC(MONTH, pds.date_launch) DESC) AS po_department
FROM reporting_prod.gsc.po_detail_dataset pds
WHERE pds.style_name != 'TBD'
  AND UPPER(pds.po_status_text) NOT LIKE '%CANCEL%'
  AND UPPER(pds.line_status) NOT LIKE '%CANCEL%'
  AND UPPER(pds.region_id) IN ('US', 'CA', 'EU')
  AND UPPER(pds.division_id) IN ('JUSTFAB', 'SHOEDAZZLE', 'FABKIDS');

CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb025_merch_master_style_log_alert AS
SELECT sips.*
     , pld.latest_launch_date
     , fsd.first_sale_date
     , fid.first_inventory_date
     , pd.po_department
     , p.product_name
     , p.color
     , (CASE
            WHEN p.is_active = 1 THEN 'Active'
            WHEN p.is_active = 0 THEN 'Inactive'
            ELSE 'Not In Console' END) AS is_active
FROM _sales_inventory_product_sku sips
         LEFT JOIN _merch_dim_product mdp
                   ON mdp.business_unit = sips.business_unit
                       AND mdp.region = sips.region
                       AND mdp.product_sku = sips.product_sku
         LEFT JOIN _po_launch_date pld
                   ON pld.region = sips.region
                       AND pld.product_sku = sips.product_sku
         LEFT JOIN _first_sale_date fsd
                   ON fsd.business_unit = sips.business_unit
                       AND fsd.region = sips.region
                       AND fsd.product_sku = sips.product_sku
         LEFT JOIN _first_inventory_date fid
                   ON fid.business_unit = sips.business_unit
                       AND fid.region = sips.region
                       AND fid.product_sku = sips.product_sku
         LEFT JOIN _po_department pd
                   ON pd.business_unit = sips.business_unit
                       AND pd.region = sips.region
                       AND pd.product_sku = sips.product_sku
         LEFT JOIN _product p
                   ON p.business_unit = sips.business_unit
                       AND p.region = sips.region
                       AND p.product_sku = sips.product_sku
WHERE mdp.product_sku IS NULL
  AND (
            sips.total_qty_sold_last_3_month > 0
        OR
            sips.qty_onhand_current > 0
    )

;
select * from _po_department;

select * from reporting_prod.gfb.gfb025_merch_master_style_log_alert;
