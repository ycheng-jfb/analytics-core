CREATE OR REPLACE TEMPORARY TABLE _fk_on_jf AS
SELECT UPPER(mml.business_unit)                                                                                                                               AS business_unit
     , 'JFB'                                                                                                                                                  AS sub_brand
     , UPPER(mml.region)                                                                                                                                      AS region
     , UPPER(REPLACE(mml.product_sku, ' ', ''))                                                                                                            AS product_sku
     , FIRST_VALUE(mml.launch_date)
                   OVER (PARTITION BY UPPER(mml.region), UPPER(REPLACE(mml.product_sku, '   ', '')) ORDER BY mml.launch_date DESC)                           AS latest_launch_date
     , UPPER(SPLIT(REPLACE(mml.product_sku, '   ', ''), '-')[0])                                                                                             AS product_style_number
     , FIRST_VALUE(CASE
                       WHEN UPPER(mml.region) = 'EU' THEN TRIM(
                           UPPER(COALESCE(mml.department, mml.planning_department)), ' ')
                       ELSE TRIM(UPPER(mml.planning_department), ' ') END)
                   OVER (PARTITION BY UPPER(mml.business_unit), UPPER(mml.region), UPPER(REPLACE(mml.product_sku, ' ', '')) ORDER BY mml.launch_date DESC) AS department
     , FIRST_VALUE(CASE
                       WHEN UPPER(mml.region) = 'EU'
                           THEN TRIM(UPPER(COALESCE(mml.category, mml.planning_category)), ' ')
                       WHEN UPPER(mml.region) = 'NA' AND
                            TRIM(UPPER(mml.planning_department), ' ') IN ('JEWELRY', 'ACCESSORIES')
                           THEN TRIM(UPPER(mml.jewelry_accessory_sub_category), ' ')
                       ELSE TRIM(UPPER(mml.planning_category), ' ') END)
                   OVER (PARTITION BY UPPER(mml.business_unit), UPPER(mml.region), UPPER(REPLACE(mml.product_sku, ' ', '')) ORDER BY mml.launch_date DESC) AS subcategory
     , FIRST_VALUE(UPPER(mml.brand))
                   OVER (PARTITION BY UPPER(mml.business_unit), UPPER(mml.region), UPPER(REPLACE(mml.product_sku, ' ', '')) ORDER BY mml.launch_date DESC) AS shared
     , FIRST_VALUE(TRIM(UPPER(mml.planning_category), ' '))
                   OVER (PARTITION BY UPPER(mml.business_unit), UPPER(mml.region), UPPER(REPLACE(mml.product_sku, ' ', '')) ORDER BY mml.launch_date DESC) AS planning_category
     , CASE
           WHEN mml.department ILIKE '%FABKID%' OR mml.planning_department ILIKE '%FABKIDS%' THEN NULL
           ELSE 'WOMENS' END                                                                                                                                  AS gender
     , NULL                                                                                                                                                   AS distro
FROM lake_view.sharepoint.jfb_master_style_log mml
UNION
SELECT UPPER(mml.business_unit)                                                                                                                               AS business_unit
     , 'JFB'                                                                                                                                                  AS sub_brand
     , UPPER(mml.region)                                                                                                                                      AS region
     , UPPER(REPLACE(mml.product_sku, ' ', ''))                                                                                                            AS product_sku
     , FIRST_VALUE(mml.launch_date)
                   OVER (PARTITION BY UPPER(mml.region), UPPER(REPLACE(mml.product_sku, '   ', '')) ORDER BY mml.launch_date DESC)                           AS latest_launch_date
     , UPPER(SPLIT(REPLACE(mml.product_sku, '   ', ''), '-')[0])                                                                                             AS product_style_number
     , FIRST_VALUE(CASE
                       WHEN UPPER(mml.region) = 'EU' THEN TRIM(
                           UPPER(COALESCE(mml.department, mml.planning_department)), ' ')
                       ELSE TRIM(UPPER(mml.planning_department), ' ') END)
                   OVER (PARTITION BY UPPER(mml.business_unit), UPPER(mml.region), UPPER(REPLACE(mml.product_sku, ' ', '')) ORDER BY mml.launch_date DESC) AS department
     , FIRST_VALUE(CASE
                       WHEN UPPER(mml.region) = 'EU'
                           THEN TRIM(UPPER(COALESCE(mml.category, mml.planning_category)), ' ')
                       WHEN UPPER(mml.region) = 'NA' AND
                            TRIM(UPPER(mml.planning_department), ' ') IN ('JEWELRY', 'ACCESSORIES')
                           THEN TRIM(UPPER(mml.jewelry_accessory_sub_category), ' ')
                       ELSE TRIM(UPPER(mml.planning_category), ' ') END)
                   OVER (PARTITION BY UPPER(mml.business_unit), UPPER(mml.region), UPPER(REPLACE(mml.product_sku, ' ', '')) ORDER BY mml.launch_date DESC) AS subcategory
     , FIRST_VALUE(UPPER(mml.brand))
                   OVER (PARTITION BY UPPER(mml.business_unit), UPPER(mml.region), UPPER(REPLACE(mml.product_sku, ' ', '')) ORDER BY mml.launch_date DESC) AS shared
     , FIRST_VALUE(TRIM(UPPER(mml.planning_category), ' '))
                   OVER (PARTITION BY UPPER(mml.business_unit), UPPER(mml.region), UPPER(REPLACE(mml.product_sku, ' ', '')) ORDER BY mml.launch_date DESC) AS planning_category
     , CASE
           WHEN mml.department ILIKE '%FABKID%' OR mml.planning_department ILIKE '%FABKIDS%' THEN NULL
           ELSE 'WOMENS' END                                                                                                                                  AS gender
     , NULL                                                                                                                                                   AS distro
FROM reporting_prod.gfb.merch_master_log mml
         LEFT JOIN lake_view.sharepoint.jfb_master_style_log mdt
                   ON UPPER(REPLACE(mml.product_sku, '  ', '')) = UPPER(REPLACE(mdt.product_sku, '  ', ''))
                       AND UPPER(mml.region) = UPPER(mdt.region)
WHERE UPPER(REPLACE(mdt.product_sku, '  ', '')) IS NULL
UNION
SELECT UPPER(s.store_brand)                                                                                                                                           AS business_unit
     , 'JFB'                                                                                                                                                          AS sub_brand
     , s.store_region                                                                                                                                                 AS region
     , dm.product_sku
     , FIRST_VALUE(dm.current_showroom_date)
                   OVER (PARTITION BY UPPER(s.store_region), UPPER(REPLACE(dm.product_sku, '    ', '')) ORDER BY dm.current_showroom_date DESC)                       AS latest_launch_date
     , UPPER(SPLIT(REPLACE(dm.product_sku, '    ', ''), '-')[0])                                                                                                      AS product_style_number
     , FIRST_VALUE(CONCAT(dm.category, ' - ', dm.product_category))
                   OVER (PARTITION BY UPPER(business_unit), UPPER(s.store_region), UPPER(REPLACE(dm.product_sku, '  ', '')) ORDER BY dm.current_showroom_date DESC) AS department
     , UPPER(fk.class)                                                                                                                                                AS subcategory
     , NULL                                                                                                                                                           AS shared
     , FIRST_VALUE(dm.category)
                   OVER (PARTITION BY UPPER(business_unit), UPPER(s.store_region), UPPER(REPLACE(dm.product_sku, '  ', '')) ORDER BY dm.current_showroom_date DESC) AS planning_category
     , NULL                                                                                                                                                           AS gender
     , NULL                                                                                                                                                           AS distro
FROM edw_prod.data_model_jfb.dim_product dm
         JOIN edw_prod.data_model_jfb.dim_store s
              ON dm.store_id = s.store_id
         LEFT JOIN lake_view.sharepoint.gfb_fk_merch_item_attributes fk
                   ON fk.style_color = dm.product_sku
         JOIN (SELECT DISTINCT product_sku
               FROM edw_prod.data_model_jfb.dim_product
               WHERE store_id = 46
                 AND product_sku != 'Unknown') mdp
              ON mdp.product_sku = dm.product_sku
WHERE s.store_id = 26
  AND dm.membership_brand_id NOT IN (10, 11, 12, 13)
  AND dm.product_sku != 'Unknown'
  AND NOT dm.product_name ILIKE ANY ('%do not use%', '%delete%')
  AND NOT dm.product_alias ILIKE ANY ('%do not use%', '%delete%')
  AND CONCAT(dm.product_sku, s.store_region, UPPER(s.store_brand)) NOT IN
      (SELECT DISTINCT CONCAT(UPPER(REPLACE(product_sku, '  ', '')), UPPER(region), UPPER(business_unit))
       FROM reporting_prod.gfb.merch_master_log
       WHERE UPPER(department) LIKE '%FABKIDS'
          OR planning_department ILIKE '%FABKIDS%')
UNION
SELECT UPPER(s.store_brand)                                                                                                                                   AS business_unit
     , b.sub_brand
     , s.store_region                                                                                                                                         AS region
     , product_sku
     , FIRST_VALUE(dm.current_showroom_date)
                   OVER (PARTITION BY UPPER(s.store_region), UPPER(REPLACE(dm.product_sku, '    ', '')) ORDER BY dm.current_showroom_date DESC)               AS latest_launch_date
     , UPPER(SPLIT(REPLACE(dm.product_sku, '    ', ''), '-')[0])                                                                                              AS product_style_number
     , FIRST_VALUE(CONCAT(dm.department, ' - ', dm.category))
                   OVER (PARTITION BY UPPER(brand), UPPER(s.store_region), UPPER(REPLACE(dm.product_sku, '  ', '')) ORDER BY dm.current_showroom_date DESC) AS department
     , UPPER(dm.subcategory)                                                                                                                                  AS subcategory
     , NULL                                                                                                                                                   AS shared
     , FIRST_VALUE(dm.category)
                   OVER (PARTITION BY UPPER(brand), UPPER(s.store_region), UPPER(REPLACE(dm.product_sku, '  ', '')) ORDER BY dm.current_showroom_date DESC) AS planning_category
     , CASE
           WHEN dm.membership_brand_id = 10 THEN 'WOMENS'
           WHEN dm.membership_brand_id = 11 THEN 'MENS'
    END                                                                                                                                                       AS gender
     , NULL                                                                                                                                                   AS distro
FROM edw_prod.data_model_jfb.dim_product dm
         JOIN edw_prod.data_model_jfb.dim_store s
              ON dm.store_id = s.store_id
         JOIN lake_view.sharepoint.gfb_third_party_brands b
              ON dm.membership_brand_id = b.membership_brand_id
WHERE product_sku NOT ILIKE '%unknown%'
  AND NOT dm.product_name ILIKE ANY ('%do not use%', '%delete%', '%remove%')
;


CREATE OR REPLACE TEMPORARY TABLE _miracle_mile_product_attributes AS
SELECT DISTINCT UPPER(s.store_brand)                                                                                                  AS business_unit
              , s.store_region                                                                                                        AS region
              , s.store_country                                                                                                       AS country
              , b.sub_brand
              , product_sku
              , product_name
              , FIRST_VALUE(color)
                            OVER (PARTITION BY s.store_brand_abbr,s.store_country,product_sku ORDER BY dm.current_showroom_date DESC) AS color
              , vip_unit_price
FROM edw_prod.data_model_jfb.dim_product dm
         JOIN edw_prod.data_model_jfb.dim_store s
              ON dm.store_id = s.store_id
         JOIN lake_view.sharepoint.gfb_third_party_brands b
              ON dm.membership_brand_id = b.membership_brand_id
WHERE product_sku NOT ILIKE '%unknown%'
  AND color NOT ILIKE '%do not use%';

CREATE OR REPLACE TEMPORARY TABLE _main_brand AS
SELECT DISTINCT product_sku                                                                          AS product_sku
              , FIRST_VALUE(main_brand) OVER (PARTITION BY product_sku ORDER BY inventory_date DESC) AS main_brand
FROM gfb.gfb_inventory_data_set
WHERE UPPER(business_unit) IN ('JUSTFAB', 'SHOEDAZZLE', 'FABKIDS');

--merch master log file
CREATE OR REPLACE TEMPORARY TABLE _merch_master_log_file AS
SELECT DISTINCT business_unit
              , CASE
                    WHEN sub_brand NOT IN ('JUSTFAB', 'SHOEDAZZLE', 'FABKIDS') THEN sub_brand
                    WHEN mb.main_brand IS NULL THEN sub_brand
                    ELSE 'JFB' END                                                                                     AS sub_brand
              , region
              , fk.product_sku
              , FIRST_VALUE(latest_launch_date)
                            OVER (PARTITION BY business_unit, region, fk.product_sku ORDER BY latest_launch_date DESC) AS latest_launch_date
              , product_style_number
              , CASE
                    WHEN department = 'APPAREL – FABKIDS' AND planning_category LIKE '%BOY%' THEN 'BOYS APPAREL'
                    WHEN department = 'APPAREL – FABKIDS' AND planning_category LIKE '%GIRL%' THEN 'GIRLS APPAREL'
                    WHEN department = 'FOOTWEAR – FABKIDS' AND planning_category LIKE '%BOY%' THEN 'BOYS SHOES'
                    WHEN department = 'FOOTWEAR – FABKIDS' AND planning_category LIKE '%GIRL%' THEN 'GIRLS SHOES'
                    WHEN department = 'Girls - Shoes' THEN 'GIRLS FOOTWEAR'
                    WHEN department = 'Boys - Shoes' THEN 'BOYS FOOTWEAR'
                    WHEN department = 'Boys - Clothing' THEN 'BOYS APPAREL'
                    WHEN department = 'Girls - Clothing' THEN 'GIRLS APPAREL'
                    WHEN department = 'BOYS - ACCESSORIES' THEN 'BOYS ACCESSORY'
                    WHEN department = 'GIRLS - ACCESSORIES' THEN 'GIRLS ACCESSORY'
                    ELSE UPPER(department) END                                                                         AS department
              , FIRST_VALUE(subcategory)
                            OVER (PARTITION BY business_unit, region, fk.product_sku ORDER BY latest_launch_date DESC) AS subcategory
              , shared
              , FIRST_VALUE(planning_category)
                            OVER (PARTITION BY business_unit, region, fk.product_sku ORDER BY latest_launch_date DESC) AS planning_category
              , gender
              , distro
FROM _fk_on_jf fk
         LEFT JOIN _main_brand mb ON fk.product_sku = mb.product_sku
UNION
SELECT DISTINCT 'FABKIDS'                                                                     AS business_unit
              , 'JFB'                                                                         AS sub_brand
              , 'NA'                                                                          AS region
              , UPPER(fk.style_color)                                                         AS product_sku
              , FIRST_VALUE(IFNULL(fk.launch_date, fk.initial_showroom))
                            OVER (PARTITION BY UPPER(fk.style_color) ORDER BY fk.launch_date) AS latest_launch_date
              , UPPER(SPLIT(fk.style_color, '-')[0])                                          AS product_style_number
              , UPPER(fk.category)                                                            AS department
              , IFF(class = 'Shoes', UPPER(fk.subclass), UPPER(fk.class))                     AS subcategory
              , 'FABKIDS'                                                                     AS shared
              , NULL                                                                          AS planning_category
              , fk.gender
              , fk.distro
FROM lake_view.sharepoint.gfb_fk_merch_item_attributes fk
WHERE fk.style_color IS NOT NULL;


--merch reference sheet
CREATE OR REPLACE TEMPORARY TABLE _merch_reference_sheet AS
SELECT DISTINCT UPPER(mrs.color_sku)                                                        AS product_sku
              , FIRST_VALUE(UPPER(REPLACE(mrs.direct_non_direct, '-', ' ')))
                            OVER (PARTITION BY mrs.color_sku ORDER BY mrs.launch_date DESC) AS direct_non_direct
              , FIRST_VALUE(UPPER(mrs.heel_height))
                            OVER (PARTITION BY mrs.color_sku ORDER BY mrs.launch_date DESC) AS heel_height
              , FIRST_VALUE(UPPER(mrs.heel_type))
                            OVER (PARTITION BY mrs.color_sku ORDER BY mrs.launch_date DESC) AS heel_type
              , FIRST_VALUE(UPPER(mrs.coresb_reorder_fashion))
                            OVER (PARTITION BY mrs.color_sku ORDER BY mrs.launch_date DESC) AS coresb_reorder_fashion
              , FIRST_VALUE(UPPER(mrs.size_13_15))
                            OVER (PARTITION BY mrs.color_sku ORDER BY mrs.launch_date DESC) AS size_13_15
              , FIRST_VALUE(UPPER(mrs.collection) IGNORE NULLS)
                            OVER (PARTITION BY mrs.color_sku ORDER BY mrs.launch_date DESC) AS collection
              , FIRST_VALUE(
    IFF(CONTAINS(CAST(mrs.na_retail_vip AS STRING), '-'), 0, mrs.na_retail_vip))
    OVER (PARTITION BY mrs.color_sku ORDER BY mrs.launch_date)                              AS og_retail
              , FIRST_VALUE(
    IFF(CONTAINS(CAST(mrs.na_landed_cost AS STRING), '-'), 0, mrs.na_landed_cost))
    OVER (PARTITION BY mrs.color_sku ORDER BY mrs.launch_date DESC)                         AS na_landed_cost
              , FIRST_VALUE((CASE
                                 WHEN CONTAINS(CAST(mrs.eu_landed_cost AS STRING), '-') THEN 0
                                 WHEN CONTAINS(CAST(mrs.eu_landed_cost AS STRING), 'D') THEN 0
                                 ELSE mrs.eu_landed_cost END))
                            OVER (PARTITION BY mrs.color_sku ORDER BY mrs.launch_date DESC) AS eu_landed_cost
              , FIRST_VALUE(UPPER(mrs.season_code))
                            OVER (PARTITION BY mrs.color_sku ORDER BY mrs.launch_date DESC) AS season_code
              , FIRST_VALUE(UPPER(mrs.style_type))
                            OVER (PARTITION BY mrs.color_sku ORDER BY mrs.launch_date DESC) AS style_type
              , FIRST_VALUE(UPPER(mrs.memory_foam))
                            OVER (PARTITION BY mrs.color_sku ORDER BY mrs.launch_date DESC) AS memory_foam_flag
              , FIRST_VALUE(UPPER(mrs.cushioned_footbed))
                            OVER (PARTITION BY mrs.color_sku ORDER BY mrs.launch_date DESC) AS cushioned_footbed_flag
              , FIRST_VALUE(UPPER(mrs.cushioned_heel))
                            OVER (PARTITION BY mrs.color_sku ORDER BY mrs.launch_date DESC) AS cushioned_heel_flag
              , FIRST_VALUE(UPPER(mrs.arch_support))
                            OVER (PARTITION BY mrs.color_sku ORDER BY mrs.launch_date DESC) AS arch_support_flag
              , FIRST_VALUE(UPPER(mrs.antibacterial))
                            OVER (PARTITION BY mrs.color_sku ORDER BY mrs.launch_date DESC) AS antibacterial_flag
              , FIRST_VALUE(UPPER(mrs.sweat_wicking))
                            OVER (PARTITION BY mrs.color_sku ORDER BY mrs.launch_date DESC) AS sweat_wicking_flag
              , FIRST_VALUE(UPPER(mrs.breathing_holes))
                            OVER (PARTITION BY mrs.color_sku ORDER BY mrs.launch_date DESC) AS breathing_holes_flag
FROM reporting_prod.gfb.merch_reference_sheet mrs
UNION
SELECT DISTINCT UPPER(fk.style_color)                                                              AS product_sku
              , NULL                                                                               AS direct_non_direct
              , NULL                                                                               AS heel_height
              , NULL                                                                               AS heel_type
              , NULL                                                                               AS coresb_reorder_fashion
              , NULL                                                                               AS size_13_15
              , NULL                                                                               AS collection
              , fk.retail                                                                          AS og_retail
              , FIRST_VALUE(fk.true_cost)
                            OVER (PARTITION BY UPPER(fk.style_color) ORDER BY fk.launch_date DESC) AS na_landed_cost
              , NULL                                                                               AS eu_landed_cost
              , NULL                                                                               AS season_code
              , NULL                                                                               AS style_type
              , NULL                                                                               AS memory_foam_flag
              , NULL                                                                               AS cushioned_footbed_flag
              , NULL                                                                               AS cushioned_heel_flag
              , NULL                                                                               AS arch_support_flag
              , NULL                                                                               AS antibacterial_flag
              , NULL                                                                               AS sweat_wicking_flag
              , NULL                                                                               AS breathing_holes_flag
FROM lake_view.sharepoint.gfb_fk_merch_item_attributes fk
WHERE UPPER(fk.style_color) NOT IN (
    SELECT DISTINCT UPPER(mrs.color_sku)
    FROM reporting_prod.gfb.merch_reference_sheet mrs
)
  AND fk.style_color IS NOT NULL;


--PO info
CREATE OR REPLACE TEMPORARY TABLE _po_detail AS
SELECT a.*
     , RANK() OVER (PARTITION BY a.business_unit, a.region, a.product_sku, a.launch_date ORDER BY a.qty DESC) AS vendor_qty_rank
FROM (
         SELECT UPPER(jpe.division_id)                                                           AS business_unit
              , (CASE
                     WHEN UPPER(jpe.region_id) IN ('US', 'CA') THEN 'NA'
                     ELSE 'EU' END)                                                              AS region
              , UPPER(SPLIT(jpe.sku, '-')[0] || '-' || SPLIT(jpe.sku, '-')[1])                   AS product_sku
              , UPPER(LEFT(jpe.gp_description, REGEXP_INSTR(jpe.gp_description, ',', 1, 3) - 1)) AS description
              , UPPER(jpe.style_rank)                                                            AS style_rank
              , DATE_TRUNC(MONTH, jpe.date_launch)                                               AS launch_date
              , UPPER(jpe.vend_name)                                                             AS vendor
              , UPPER(jpe.style_name)                                                            AS style_name
              , UPPER(jpe.style_type)                                                            AS style_type
              , UPPER(jpe.gender)                                                                AS gender
              , UPPER(jpe.color)                                                                 AS color
              , UPPER(jpe.subclass)                                                              AS subclass
              , DATE_FROM_PARTS(SPLIT(jpe.show_room, '-')[0], SPLIT(jpe.show_room, '-')[1], 1)   AS show_room

              , AVG(jpe.reporting_landed_cost)                                                   AS landed_cost
              , SUM(jpe.qty)                                                                     AS qty
              , MAX(jpe.date_launch)                                                             AS exact_launch_date
         FROM reporting_prod.gsc.po_detail_dataset jpe
         WHERE jpe.style_name != 'TBD'
           AND UPPER(jpe.po_status_text) NOT LIKE '%CANCEL%'
           AND UPPER(jpe.line_status) NOT LIKE '%CANCEL%'
           AND UPPER(jpe.division_id) IN ('JUSTFAB', 'SHOEDAZZLE', 'FABKIDS')
           AND DATE_TRUNC(MONTH, jpe.date_launch) <=
               DATEADD(MONTH, 1, DATE_TRUNC(MONTH, CURRENT_DATE())) -- Exclude far future products
         GROUP BY UPPER(jpe.division_id)
                , (CASE
                       WHEN UPPER(jpe.region_id) IN ('US', 'CA') THEN 'NA'
                       ELSE 'EU' END)
                , UPPER(SPLIT(jpe.sku, '-')[0] || '-' || SPLIT(jpe.sku, '-')[1])
                , UPPER(LEFT(jpe.gp_description, REGEXP_INSTR(jpe.gp_description, ',', 1, 3) - 1))
                , UPPER(jpe.style_rank)
                , DATE_TRUNC(MONTH, jpe.date_launch)
                , UPPER(jpe.vend_name)
                , UPPER(jpe.style_name)
                , UPPER(jpe.style_type)
                , UPPER(jpe.gender)
                , UPPER(jpe.color)
                , UPPER(jpe.subclass)
                , DATE_FROM_PARTS(SPLIT(jpe.show_room, '-')[0], SPLIT(jpe.show_room, '-')[1], 1)
     ) a
;


CREATE OR REPLACE TEMPORARY TABLE _po_info AS
SELECT DISTINCT po.business_unit
              , po.region
              , po.product_sku
              , FIRST_VALUE(po.description)
                            OVER (PARTITION BY po.region, po.product_sku ORDER BY po.launch_date DESC) AS description
              , FIRST_VALUE(po.style_rank)
                            OVER (PARTITION BY po.region, po.product_sku ORDER BY po.launch_date DESC) AS style_rank
              , NTH_VALUE(po.launch_date, 1)
                          OVER (PARTITION BY po.region, po.product_sku ORDER BY po.launch_date DESC)   AS latest_launch_date
              , NTH_VALUE(po.launch_date, 2)
                          OVER (PARTITION BY po.region, po.product_sku ORDER BY po.launch_date DESC)   AS previous_launch_date
              , LAST_VALUE(po.launch_date)
                           OVER (PARTITION BY po.region, po.product_sku ORDER BY po.launch_date DESC)  AS first_launch_date
              , FIRST_VALUE(po.vendor)
                            OVER (PARTITION BY po.region, po.product_sku ORDER BY po.launch_date DESC) AS vendor
              , FIRST_VALUE(po.style_name)
                            OVER (PARTITION BY po.region, po.product_sku ORDER BY po.launch_date DESC) AS style_name
              , FIRST_VALUE(po.style_type)
                            OVER (PARTITION BY po.region, po.product_sku ORDER BY po.launch_date DESC) AS style_type
              , FIRST_VALUE(po.landed_cost)
                            OVER (PARTITION BY po.region, po.product_sku ORDER BY po.launch_date DESC) AS landing_cost
              , FIRST_VALUE(po.color)
                            OVER (PARTITION BY po.region, po.product_sku ORDER BY po.launch_date DESC) AS color
              , FIRST_VALUE(po.subclass)
                            OVER (PARTITION BY po.region, po.product_sku ORDER BY po.launch_date DESC) AS subclass
              , FIRST_VALUE(po.show_room)
                            OVER (PARTITION BY po.region, po.product_sku ORDER BY po.launch_date DESC) AS show_room
              , NTH_VALUE(po.exact_launch_date, 1)
                          OVER (PARTITION BY po.region, po.product_sku ORDER BY po.launch_date DESC)   AS exact_latest_launch_date
FROM _po_detail po
         JOIN _merch_master_log_file mml
              ON mml.business_unit = po.business_unit
                  AND mml.region = po.region
                  AND mml.product_sku = po.product_sku
WHERE po.vendor_qty_rank = 1;


CREATE OR REPLACE TEMPORARY TABLE _product_info AS
SELECT DISTINCT dp.product_sku
              , mml.business_unit
              , mml.region
              , FIRST_VALUE(dp.vip_unit_price  IGNORE NULLS)
                            OVER (PARTITION BY dp.product_sku, ds.store_brand, ds.store_region ORDER BY dp.current_showroom_date DESC) AS current_vip_retail
              , FIRST_VALUE(dp.retail_unit_price  IGNORE NULLS)
                            OVER (PARTITION BY dp.product_sku, ds.store_brand, ds.store_region ORDER BY dp.current_showroom_date DESC) AS current_retail
              , FIRST_VALUE(dp.image_url  IGNORE NULLS)
                            OVER (PARTITION BY dp.product_sku ORDER BY dp.current_showroom_date DESC)                                  AS image_url
              , FIRST_VALUE(dp.is_plus_size  IGNORE NULLS)
                            OVER (PARTITION BY dp.product_sku ORDER BY dp.current_showroom_date DESC)                                  AS is_plussize
              , FIRST_VALUE(dp.product_name  IGNORE NULLS)
                            OVER (PARTITION BY dp.product_sku ORDER BY dp.current_showroom_date DESC)                                  AS product_name
FROM edw_prod.data_model_jfb.dim_product dp
         JOIN edw_prod.data_model_jfb.dim_store ds
              ON ds.store_id = dp.store_id
         JOIN _merch_master_log_file mml
              ON LOWER(mml.business_unit) = LOWER(ds.store_brand)
                  AND LOWER(mml.region) = LOWER(ds.store_region)
                  AND mml.product_sku = dp.product_sku
WHERE dp.retail_unit_price != 0
  AND ds.store_region = 'NA'
  AND NOT dp.product_name ILIKE ANY ('%DO NOT USE%', '%remove%', '%delete%', '%foundry%')
  AND NOT dp.image_url ILIKE ANY ('%/OLD_%', '%DELETE%', '%/TEST%', '%static%')

UNION

--To solve the currency difference, we only use FR data
SELECT DISTINCT dp.product_sku
              , mml.business_unit
              , mml.region
              , FIRST_VALUE(dp.vip_unit_price)
                            OVER (PARTITION BY dp.product_sku, ds.store_brand, ds.store_region ORDER BY dp.current_showroom_date DESC) AS current_vip_retail
              , FIRST_VALUE(dp.retail_unit_price)
                            OVER (PARTITION BY dp.product_sku, ds.store_brand, ds.store_region ORDER BY dp.current_showroom_date DESC) AS current_retail
              , FIRST_VALUE(dp.image_url)
                            OVER (PARTITION BY dp.product_sku ORDER BY dp.current_showroom_date DESC)                                  AS image_url
              , FIRST_VALUE(dp.is_plus_size)
                            OVER (PARTITION BY dp.product_sku ORDER BY dp.current_showroom_date DESC)                                  AS is_plussize
              , COALESCE(sn.product_name, FIRST_VALUE(dp.product_name)
                                                      OVER (PARTITION BY dp.product_sku ORDER BY dp.current_showroom_date DESC))       AS product_name
FROM edw_prod.data_model_jfb.dim_product dp
         JOIN edw_prod.data_model_jfb.dim_store ds
              ON ds.store_id = dp.store_id
         JOIN _merch_master_log_file mml
              ON LOWER(mml.business_unit) = LOWER(ds.store_brand)
                  AND LOWER(mml.region) = LOWER(ds.store_region)
                  AND mml.product_sku = dp.product_sku
         LEFT JOIN (
    SELECT DISTINCT dp.product_sku
                  , mml.business_unit
                  , mml.region
                  , FIRST_VALUE(dp.product_name)
                                OVER (PARTITION BY dp.product_sku ORDER BY dp.current_showroom_date DESC) AS product_name
    FROM edw_prod.data_model_jfb.dim_product dp
             JOIN edw_prod.data_model_jfb.dim_store ds
                  ON ds.store_id = dp.store_id
             JOIN _merch_master_log_file mml
                  ON LOWER(mml.business_unit) = LOWER(ds.store_brand)
                      AND LOWER(mml.region) = LOWER(ds.store_region)
                      AND mml.product_sku = dp.product_sku
             JOIN edw_prod.data_model_jfb.dim_product dpm
                  ON dpm.product_id = dp.master_product_id
    WHERE dp.retail_unit_price != 0
      AND ds.store_region = 'EU'
      AND ds.store_country = 'UK'
      AND mml.business_unit = 'JUSTFAB'
      AND mml.department LIKE '%FABKID%'
      AND NOT dp.product_name ILIKE ANY ('%DO NOT USE%', '%remove%', '%delete%', '%foundry%')
      AND NOT dp.image_url ILIKE ANY ('%/OLD_%', '%DELETE%', '%/TEST%', '%static3%')) AS sn
                   ON LOWER(sn.business_unit) = LOWER(mml.business_unit)
                       AND sn.product_sku = dp.product_sku
WHERE dp.retail_unit_price != 0
  AND ds.store_region = 'EU'
  AND ds.store_country = 'FR'
  AND NOT dp.product_name ILIKE ANY ('%DO NOT USE%', '%remove%', '%delete%', '%foundry%')
  AND NOT dp.image_url ILIKE ANY ('%/OLD_%', '%DELETE%', '%/TEST%', '%static3%')
;

--fixing issue with some FK products on JF
INSERT INTO _product_info
SELECT DISTINCT mml.product_sku
              , mml.business_unit
              , mml.region
              , FIRST_VALUE(dp.vip_unit_price)
                            OVER (PARTITION BY dp.product_sku, ds.store_brand, ds.store_region ORDER BY dp.current_showroom_date DESC) AS current_vip_retail
              , FIRST_VALUE(dp.retail_unit_price)
                            OVER (PARTITION BY dp.product_sku, ds.store_brand, ds.store_region ORDER BY dp.current_showroom_date DESC) AS current_retail
              , FIRST_VALUE(dp.image_url)
                            OVER (PARTITION BY dp.product_sku, ds.store_brand ORDER BY dp.current_showroom_date DESC)                  AS image_url
              , FIRST_VALUE(dp.is_plus_size)
                            OVER (PARTITION BY dp.product_sku ORDER BY dp.current_showroom_date DESC)                                  AS is_plussize
              , FIRST_VALUE(dp.product_name)
                            OVER (PARTITION BY dp.product_sku ORDER BY dp.current_showroom_date DESC)                                  AS product_name
FROM _merch_master_log_file mml
         JOIN edw_prod.data_model_jfb.dim_product dp
              ON mml.product_sku = dp.product_sku
         JOIN edw_prod.data_model_jfb.dim_store ds
              ON ds.store_id = dp.store_id
                  AND LOWER(ds.store_brand) = 'fabkids'
                  AND LOWER(mml.region) = LOWER(ds.store_region)
WHERE mml.business_unit IN ('JUSTFAB', 'SHOEDAZZLE')
  AND mml.department ILIKE '%FABKIDS%'
  AND mml.product_sku NOT IN (
    SELECT DISTINCT a.product_sku
    FROM _product_info a
    WHERE a.image_url IS NOT NULL
      AND a.business_unit != 'FABKIDS'
);


CREATE OR REPLACE TEMPORARY TABLE _pendin_qty AS
SELECT UPPER(jpe.division_id)                                  AS business_unit
     , (CASE
            WHEN jpe.warehouse_id = 107 THEN 'NA'
            WHEN jpe.warehouse_id = 109 THEN 'NA'
            WHEN jpe.warehouse_id = 154 THEN 'NA'
            WHEN jpe.warehouse_id = 231 THEN 'NA'
            WHEN jpe.warehouse_id = 421 THEN 'NA'
            WHEN jpe.warehouse_id = 221 THEN 'EU'
            WHEN jpe.warehouse_id = 366 THEN 'EU'
            WHEN jpe.warehouse_id = 466 THEN 'NA'
    END)                                                       AS region
     , (CASE
            WHEN jpe.warehouse_id = 107 THEN 'US'
            WHEN jpe.warehouse_id = 109 THEN 'CA'
            WHEN jpe.warehouse_id = 154 THEN 'US'
            WHEN jpe.warehouse_id = 231 THEN 'US'
            WHEN jpe.warehouse_id = 421 THEN 'US'
            WHEN jpe.warehouse_id = 221 THEN 'FR'
            WHEN jpe.warehouse_id = 366 THEN 'UK'
            WHEN jpe.warehouse_id = 466 THEN 'US'
    END)                                                       AS country
     , SPLIT(jpe.sku, '-')[0] || '-' || SPLIT(jpe.sku, '-')[1] AS product_sku
     , SUM(jpe.qty)                                            AS qty_pending
FROM reporting_prod.gsc.po_detail_dataset jpe
         JOIN edw_prod.data_model_jfb.dim_date dd
              ON dd.full_date = jpe.date_launch
WHERE jpe.style_name != 'TBD'
  AND jpe.po_status_id IN (3, 8, 12)
  AND UPPER(jpe.line_status) != 'CANCELLED'
  AND UPPER(jpe.division_id) IN ('JUSTFAB', 'SHOEDAZZLE', 'FABKIDS')
  AND TO_DATE(jpe.show_room, 'YYYY-MM') >= DATE_TRUNC(MONTH, DATEADD(DAY, -1, CURRENT_DATE()))
GROUP BY UPPER(jpe.division_id)
       , (CASE
              WHEN jpe.warehouse_id = 107 THEN 'NA'
              WHEN jpe.warehouse_id = 109 THEN 'NA'
              WHEN jpe.warehouse_id = 154 THEN 'NA'
              WHEN jpe.warehouse_id = 231 THEN 'NA'
              WHEN jpe.warehouse_id = 421 THEN 'NA'
              WHEN jpe.warehouse_id = 221 THEN 'EU'
              WHEN jpe.warehouse_id = 366 THEN 'EU'
              WHEN jpe.warehouse_id = 466 THEN 'NA'
    END)
       , (CASE
              WHEN jpe.warehouse_id = 107 THEN 'US'
              WHEN jpe.warehouse_id = 109 THEN 'CA'
              WHEN jpe.warehouse_id = 154 THEN 'US'
              WHEN jpe.warehouse_id = 231 THEN 'US'
              WHEN jpe.warehouse_id = 421 THEN 'US'
              WHEN jpe.warehouse_id = 221 THEN 'FR'
              WHEN jpe.warehouse_id = 366 THEN 'UK'
              WHEN jpe.warehouse_id = 466 THEN 'US'
    END)
       , SPLIT(jpe.sku, '-')[0] || '-' || SPLIT(jpe.sku, '-')[1];


CREATE OR REPLACE TEMPORARY TABLE _product_reviews AS
SELECT UPPER(pr.business_unit)               AS business_unit
     , UPPER(pr.region)                      AS region
     , pr.product_sku
     , COUNT(DISTINCT pr.review_id)          AS total_reviews
     , AVG(recommended)                      AS avg_is_recommended_score
     , AVG(style_review_field_score)         AS avg_style_score
     , AVG(comfort_review_field_score)       AS avg_comfort_score
     , AVG(quality_value_review_field_score) AS avg_quality_value_score
     , AVG(overall_review_field_score)       AS avg_overall_rating
FROM reporting_prod.gfb.gfb_product_review_data_set pr
WHERE pr.business_unit IN ('SHOEDAZZLE', 'JUSTFAB', 'FABKIDS')
  AND pr.status = 'Accepted'
GROUP BY UPPER(pr.business_unit)
       , UPPER(pr.region)
       , pr.product_sku;


CREATE OR REPLACE TEMPORARY TABLE _store AS
SELECT DISTINCT ds.store_brand
              , ds.store_region
              , ds.store_country
FROM edw_prod.data_model_jfb.dim_store ds
WHERE ds.store_brand_abbr IN ('JF', 'SD', 'FK')
  AND ds.store_full_name NOT LIKE '%(DM)%'
  AND ds.store_full_name NOT LIKE '%Heels.com%'
  AND ds.store_full_name NOT LIKE '%G-SWAG%'
  AND ds.store_full_name NOT LIKE '%Retail%'
  AND ds.store_full_name NOT LIKE '%PS%'
  AND ds.store_full_name NOT LIKE '%Wholesale%'
  AND ds.store_full_name NOT LIKE '%Sample%'
  AND ds.store_region IN ('NA', 'EU');


--New dim_store does not have special stores
INSERT INTO _store
VALUES ('JUSTFAB', 'EU', 'AT')
     , ('FABKIDS', 'NA', 'CA')
     , ('JUSTFAB', 'EU', 'BE')
     , ('SHOEDAZZLE', 'NA', 'CA');

CREATE OR REPLACE TEMPORARY TABLE _current_status AS
SELECT DISTINCT FIRST_VALUE(case when is_active = 1 then 'Active' else 'Inactive' end)
                            OVER (PARTITION BY dp.store_id,dp.product_sku ORDER BY is_active DESC) AS product_status,
                REGEXP_REPLACE(dp.sku, '-[0-9]+$', '') AS product_sku,
                dp.store_id
FROM edw_prod.new_stg.dim_product AS dp;

CREATE OR REPLACE TEMPORARY TABLE _master_product_id AS
SELECT a.*
     , dp.product_status
     , REPLACE(SPLIT(SPLIT(dp.product_alias, '(')[1], ')')[0], '"', '') AS product_color_from_label
FROM (
         SELECT DISTINCT st.store_brand
                       , st.store_region
                       , st.store_country
                       , mml.product_sku
                       , cs.product_status AS current_status
                       , FIRST_VALUE(CASE
                                         WHEN dp.master_product_id = -1 THEN dp.product_id
                                         ELSE dp.master_product_id END)
                                     OVER (PARTITION BY dp.product_sku, st.store_brand, st.store_region, st.store_country ORDER BY dp.current_showroom_date DESC) AS master_product_id
         FROM _merch_master_log_file mml
                  JOIN _store st
                       ON LOWER(st.store_brand) = LOWER(mml.business_unit)
                           AND LOWER(st.store_region) = LOWER(mml.region)
                  JOIN edw_prod.data_model_jfb.dim_product dp
                       ON dp.product_sku = mml.product_sku
                           AND dp.product_alias NOT ILIKE '%TEST%'
                           AND dp.product_alias NOT ILIKE '%DELETE%'
                           AND dp.product_alias NOT ILIKE '%REMOVE%'
                  JOIN edw_prod.data_model_jfb.dim_store ds
                       ON ds.store_id = dp.store_id
                           AND ds.store_brand = st.store_brand
                           AND ds.store_region = st.store_region
                           AND ds.store_country = st.store_country
                  JOIN _current_status cs
                       ON cs.product_sku = dp.product_sku
                           AND cs.store_id = dp.store_id
     ) a
         JOIN edw_prod.data_model_jfb.dim_product dp
              ON dp.product_id = a.master_product_id;


CREATE OR REPLACE TEMPORARY TABLE _product_label_color AS
SELECT DISTINCT mpi.product_sku
              , mpi.store_region
              , FIRST_VALUE(mpi.product_color_from_label)
                            OVER (PARTITION BY mpi.product_sku ORDER BY mpi.master_product_id DESC) AS product_color_from_label
FROM _master_product_id mpi;


CREATE OR REPLACE TEMPORARY TABLE _product_console_attribute AS
SELECT a.store_brand
     , a.store_region
     , a.store_country
     , a.product_sku
     , a.master_product_id
     , a.product_attribute_category
     , LISTAGG(a.product_attribute, ', ') AS product_attribute
FROM (
         SELECT DISTINCT mpi.store_brand
                       , mpi.store_region
                       , mpi.store_country
                       , mpi.product_sku
                       , mpi.master_product_id
                       , pt.label AS product_attribute_category
                       , t.label  AS product_attribute
         FROM _master_product_id mpi
                  JOIN lake_jfb_view.ultra_merchant.product_tag prt
                       ON prt.product_id = mpi.master_product_id
                  JOIN lake_jfb_view.ultra_merchant.tag t
                       ON t.tag_id = prt.tag_id
                  JOIN lake_jfb_view.ultra_merchant.tag pt
                       ON pt.tag_id = t.parent_tag_id
                  LEFT JOIN lake_jfb_view.ultra_merchant.tag_archive ta
                            ON ta.tag_id = t.tag_id
                                AND ta.tag_archive_datetime_deleted IS NULL
         WHERE ta.tag_id IS NULL
     ) a
GROUP BY a.store_brand
       , a.store_region
       , a.store_country
       , a.product_sku
       , a.master_product_id
       , a.product_attribute_category;


CREATE OR REPLACE TEMPORARY TABLE _product_console_attribute_flat AS
SELECT DISTINCT mpi.store_brand
              , mpi.store_region
              , mpi.store_country
              , mpi.product_sku
              , pca1.product_attribute  AS boot_shaft_height_console
              , pca2.product_attribute  AS color_family_console
              , pca3.product_attribute  AS designer_collaboration_console
              , pca4.product_attribute  AS fit_console
              , pca5.product_attribute  AS heel_shape_console
              , pca6.product_attribute  AS heel_size_console
              , pca7.product_attribute  AS material_type_console
              , pca8.product_attribute  AS occasion_type_console
              , pca9.product_attribute  AS toe_type_console
              , pca10.product_attribute AS weather_console
              , pca11.product_attribute AS img_model_plus
              , pca12.product_attribute AS img_model_reg
              , pca15.product_attribute AS img_type
              , pca16.product_attribute AS sleeve_lenth_console
              , pca17.product_attribute AS clothing_detail_console
              , pca18.product_attribute AS color_console
              , pca19.product_attribute AS buttom_subclass_console
              , pca20.product_attribute AS shop_category_console
              , pca21.product_attribute AS shoe_style_console
              , pca22.product_attribute AS gender
FROM _master_product_id mpi
         LEFT JOIN _product_console_attribute pca1
                   ON pca1.store_brand = mpi.store_brand
                       AND pca1.store_region = mpi.store_region
                       AND pca1.store_country = mpi.store_country
                       AND pca1.product_sku = mpi.product_sku
                       AND pca1.product_attribute_category = 'Boot Shaft Height'
         LEFT JOIN _product_console_attribute pca2
                   ON pca2.store_brand = mpi.store_brand
                       AND pca2.store_region = mpi.store_region
                       AND pca2.store_country = mpi.store_country
                       AND pca2.product_sku = mpi.product_sku
                       AND pca2.product_attribute_category = 'Color Family'
         LEFT JOIN _product_console_attribute pca3
                   ON pca3.store_brand = mpi.store_brand
                       AND pca3.store_region = mpi.store_region
                       AND pca3.store_country = mpi.store_country
                       AND pca3.product_sku = mpi.product_sku
                       AND pca3.product_attribute_category = 'Designer Collaboration'
         LEFT JOIN _product_console_attribute pca4
                   ON pca4.store_brand = mpi.store_brand
                       AND pca4.store_region = mpi.store_region
                       AND pca4.store_country = mpi.store_country
                       AND pca4.product_sku = mpi.product_sku
                       AND pca4.product_attribute_category = 'Fit'
         LEFT JOIN _product_console_attribute pca5
                   ON pca5.store_brand = mpi.store_brand
                       AND pca5.store_region = mpi.store_region
                       AND pca5.store_country = mpi.store_country
                       AND pca5.product_sku = mpi.product_sku
                       AND pca5.product_attribute_category = 'Heel Shape'
         LEFT JOIN _product_console_attribute pca6
                   ON pca6.store_brand = mpi.store_brand
                       AND pca6.store_region = mpi.store_region
                       AND pca6.store_country = mpi.store_country
                       AND pca6.product_sku = mpi.product_sku
                       AND pca6.product_attribute_category = 'Heel Size (Deprecated)'
         LEFT JOIN _product_console_attribute pca7
                   ON pca7.store_brand = mpi.store_brand
                       AND pca7.store_region = mpi.store_region
                       AND pca7.store_country = mpi.store_country
                       AND pca7.product_sku = mpi.product_sku
                       AND pca7.product_attribute_category = 'Material Type'
         LEFT JOIN _product_console_attribute pca8
                   ON pca8.store_brand = mpi.store_brand
                       AND pca8.store_region = mpi.store_region
                       AND pca8.store_country = mpi.store_country
                       AND pca8.product_sku = mpi.product_sku
                       AND pca8.product_attribute_category = 'Occasion Type'
         LEFT JOIN _product_console_attribute pca9
                   ON pca9.store_brand = mpi.store_brand
                       AND pca9.store_region = mpi.store_region
                       AND pca9.store_country = mpi.store_country
                       AND pca9.product_sku = mpi.product_sku
                       AND pca9.product_attribute_category = 'Toe Type'
         LEFT JOIN _product_console_attribute pca10
                   ON pca10.store_brand = mpi.store_brand
                       AND pca10.store_region = mpi.store_region
                       AND pca10.store_country = mpi.store_country
                       AND pca10.product_sku = mpi.product_sku
                       AND pca10.product_attribute_category = 'Weather'
         LEFT JOIN _product_console_attribute pca11
                   ON pca11.store_brand = mpi.store_brand
                       AND pca11.store_region = mpi.store_region
                       AND pca11.store_country = mpi.store_country
                       AND pca11.product_sku = mpi.product_sku
                       AND pca11.product_attribute_category = 'Img Model - Plus'
         LEFT JOIN _product_console_attribute pca12
                   ON pca12.store_brand = mpi.store_brand
                       AND pca12.store_region = mpi.store_region
                       AND pca12.store_country = mpi.store_country
                       AND pca12.product_sku = mpi.product_sku
                       AND pca12.product_attribute_category = 'Img Model - Reg'
         LEFT JOIN _product_console_attribute pca15
                   ON pca15.store_brand = mpi.store_brand
                       AND pca15.store_region = mpi.store_region
                       AND pca15.store_country = mpi.store_country
                       AND pca15.product_sku = mpi.product_sku
                       AND pca15.product_attribute_category = 'Img Type'
         LEFT JOIN _product_console_attribute pca16
                   ON pca16.store_brand = mpi.store_brand
                       AND pca16.store_region = mpi.store_region
                       AND pca16.store_country = mpi.store_country
                       AND pca16.product_sku = mpi.product_sku
                       AND pca16.product_attribute_category = 'Sleeve Length'
         LEFT JOIN _product_console_attribute pca17
                   ON pca17.store_brand = mpi.store_brand
                       AND pca17.store_region = mpi.store_region
                       AND pca17.store_country = mpi.store_country
                       AND pca17.product_sku = mpi.product_sku
                       AND pca17.product_attribute_category = 'Clothing Detail'
         LEFT JOIN _product_console_attribute pca18
                   ON pca18.store_brand = mpi.store_brand
                       AND pca18.store_region = mpi.store_region
                       AND pca18.store_country = mpi.store_country
                       AND pca18.product_sku = mpi.product_sku
                       AND pca18.product_attribute_category = 'Color'
         LEFT JOIN _product_console_attribute pca19
                   ON pca19.store_brand = mpi.store_brand
                       AND pca19.store_region = mpi.store_region
                       AND pca19.store_country = mpi.store_country
                       AND pca19.product_sku = mpi.product_sku
                       AND pca19.product_attribute_category = 'Bottoms Sub Class'
         LEFT JOIN _product_console_attribute pca20
                   ON pca20.store_brand = mpi.store_brand
                       AND pca20.store_region = mpi.store_region
                       AND pca20.store_country = mpi.store_country
                       AND pca20.product_sku = mpi.product_sku
                       AND pca20.product_attribute_category = 'Shop Category'
         LEFT JOIN _product_console_attribute pca21
                   ON pca21.store_brand = mpi.store_brand
                       AND pca21.store_region = mpi.store_region
                       AND pca21.store_country = mpi.store_country
                       AND pca21.product_sku = mpi.product_sku
                       AND pca21.product_attribute_category = 'Shoe Style'
         LEFT JOIN _product_console_attribute pca22
                   ON pca22.store_brand = mpi.store_brand
                       AND pca22.store_region = mpi.store_region
                       AND pca22.store_country = mpi.store_country
                       AND pca22.product_sku = mpi.product_sku
                       AND pca22.product_attribute_category = 'Gender';


CREATE OR REPLACE TEMPORARY TABLE _latest_receipt AS
SELECT po.business_unit
     , po.region
     , po.product_sku
     , MAX(po.date_received) AS last_receipt_date
FROM reporting_prod.gfb.gfb_po_data_set po
WHERE po.date_received IS NOT NULL
GROUP BY po.business_unit
       , po.region
       , po.product_sku;


CREATE OR REPLACE TEMPORARY TABLE _clearance_price AS
SELECT DISTINCT UPPER(mcsl.business_unit) AS business_unit
              , UPPER(mcsl.region)        AS region
              , mcsl.sku                  AS product_sku
              , mcsl.clearance_group      AS clearance_price
FROM lake_view.sharepoint.gfb_merch_clearance_sku_list mcsl
WHERE mcsl.start_date <= CURRENT_DATE()
  AND mcsl.end_date >= CURRENT_DATE();


CREATE OR REPLACE TEMPORARY TABLE _merch_dim_product AS
SELECT DISTINCT mml.business_unit
              , mml.sub_brand
              , mml.region
              , UPPER(st.store_country)                                                                   AS country
              , mml.product_sku
              , mml.product_style_number
              , COALESCE(mml.latest_launch_date, pi.latest_launch_date)                                   AS latest_launch_date
              , pi.previous_launch_date
              , mml.department
              , IFF(mml.department = 'APPAREL' AND prod.is_plussize = 1, 'APPAREL PLUS',
                    mml.department)                                                                       AS department_detail
              , CASE
                    WHEN mml.department = 'JEWELRY' AND mml.subcategory LIKE '%EARRING%' THEN 'EARRING'
                    WHEN mml.department = 'JEWELRY' AND mml.subcategory LIKE '%BODY%' THEN 'BODY JEWELRY'
                    WHEN mml.department = 'HANDBAGS' AND mml.subcategory LIKE '%CROSS%' THEN 'CROSSBODY BAGS'
                    WHEN mml.department = 'ACCESSORIES' AND mml.subcategory LIKE '%HAT%' THEN 'HAT'
                    WHEN mml.department = 'ACCESSORIES' AND mml.subcategory LIKE '%KEY%' THEN 'KEYCHAIN'
                    WHEN mml.department = 'ACCESSORIES' AND mml.subcategory LIKE '%MASK%' THEN 'MASK'
                    WHEN mml.department = 'ACCESSORIES' AND mml.subcategory LIKE '%SCAR%' THEN 'SCARF'
                    WHEN mml.department = 'HANDBAGS' AND mml.subcategory LIKE '%SHOULDER%' THEN 'SHOULDER BAGS'
                    ELSE mml.subcategory END                                                              AS subcategory
              , CASE
                    WHEN COALESCE(pi.latest_launch_date, mml.latest_launch_date) IS NULL THEN NULL
                    WHEN COALESCE(pi.latest_launch_date, mml.latest_launch_date) > pi.previous_launch_date
                        THEN 'RE-ORDER'
                    ELSE 'NEW' END                                                                        AS reorder_status
              , IFF(mml.business_unit = 'JUSTFAB' AND mml.department LIKE '%FABKIDS%', 'JUSTFAB',
                    mml.shared)                                                                           AS shared
              , pi.style_rank
              , pi.description
              , CASE
                    WHEN mml.department = 'FOOTWEAR' AND mml.business_unit != 'FABKIDS' AND
                         CONTAINS(pi.description, 'WC') AND CONTAINS(pi.description, 'WW') THEN 'WC & WW'
                    WHEN mml.department = 'FOOTWEAR' AND mml.business_unit != 'FABKIDS' AND
                         CONTAINS(pi.description, 'WC') THEN 'WC'
                    WHEN mml.department = 'FOOTWEAR' AND mml.business_unit != 'FABKIDS' AND
                         CONTAINS(pi.description, 'WW') THEN 'WW'
                    WHEN mml.department = 'FOOTWEAR' AND mml.business_unit != 'FABKIDS' AND
                         CONTAINS(pi.description, '-E') THEN 'E'
                    WHEN mml.department = 'FOOTWEAR' AND mml.business_unit != 'FABKIDS' AND
                         CONTAINS(pi.description, '-D') THEN 'D'
    END                                                                                                   AS ww_wc
              , COALESCE(
    FIRST_VALUE(prod.product_name IGNORE NULLS) OVER (PARTITION BY mml.product_sku ORDER BY mml.region DESC),
    FIRST_VALUE(pi.style_name IGNORE NULLS) OVER (PARTITION BY mml.product_sku ORDER BY mml.region DESC)) AS style_name
              , COALESCE(fk_info.fk_color, pi.color)                                                      AS color
              , UPPER(mcm.color_attribute)                                                                AS master_color
              , IFF(CONTAINS(mml.planning_category, 'BOOTS'), 'BOOTS', 'SHOES')                           AS classification
              , pi.vendor
              , mrs.direct_non_direct
              , pi.landing_cost
              , FIRST_VALUE(IFF(prod.current_vip_retail = 0, CAST(ROUND(prod.current_retail, 2) AS VARCHAR(20)),
                                CAST(ROUND(prod.current_vip_retail, 2) AS VARCHAR(20))) IGNORE NULLS)
                            OVER (PARTITION BY mml.product_sku ORDER BY mml.region DESC)                  AS current_vip_retail
              , FIRST_VALUE(prod.current_retail IGNORE NULLS)
                            OVER (PARTITION BY mml.product_sku ORDER BY mml.region DESC)                  AS msrp
              , mrs.heel_height
              , mrs.heel_type
              , mrs.coresb_reorder_fashion
              , prod.image_url
              , IFF(prod.is_plussize = 1, 'Y', 'N')                                                       AS is_plussize
              , mrs.size_13_15
              , COALESCE(mrs.collection, cat.collection, ma.collection)                                   AS collection
              , CASE
                    WHEN mml.shared = mml.business_unit OR mml.shared IS NULL
                        THEN pq.qty_pending
    END                                                                                                   AS qty_pending
              , pr.avg_is_recommended_score
              , pr.avg_style_score
              , pr.avg_comfort_score
              , pr.avg_quality_value_score
              , pr.total_reviews
              , mrs.og_retail
              , IFF(prod.current_vip_retail < mrs.og_retail, 'Price Down', 'X')                           AS priced_down
              , COALESCE(fk_info.fk_subclass, pi.subclass)                                                AS subclass
              , mrs.na_landed_cost
              , mrs.eu_landed_cost
              , IFF(CONTAINS(prod.image_url, 'fabkid') AND CONTAINS(prod.image_url, 'Error'),
                    REPLACE(prod.image_url, 'Error', mml.product_sku),
                    prod.image_url)                                                                       AS error_free_image_url
              , CASE
                    WHEN CONTAINS(error_free_image_url, 'fabkid')
                        THEN REPLACE(error_free_image_url, '_main_thumb', '_main_large')
                    WHEN CONTAINS(error_free_image_url, 'justfab')
                        THEN REPLACE(error_free_image_url, '48x70', '589x860')
                    WHEN CONTAINS(error_free_image_url, 'sd-assets')
                        THEN REPLACE(error_free_image_url, 'mini', 'original')
    END                                                                                                   AS large_img_url
              , fk_info.fk_attribute
              , fk_info.fk_site_name
              , fk_info.fk_new_season_code
              , UPPER(pmc.marketing_capsule_name)                                                         AS marketing_capsule_name
              , COALESCE(mrs.season_code, ma.season_code)                                                 AS season_code
              , COALESCE(mrs.style_type, ma.style_type)                                                   AS style_type
              , COALESCE(mml.gender, pca.gender,
                         IFF(mml.sub_brand = 'DREAM PAIRS KIDS' AND mml.department ILIKE '%boy%',
                             'BOYS', IFF(mml.sub_brand = 'DREAM PAIRS KIDS'
                                             AND mml.department NOT ILIKE '%boy%', 'GIRLS',
                                         NULL)))                                                          AS gender
              , IFF(mpi.master_product_id IS NULL, FIRST_VALUE(mpi.master_product_id)
                                                               OVER (PARTITION BY mml.product_sku, mml.business_unit, mml.region ORDER BY mpi.master_product_id ASC),
                    mpi.master_product_id)                                                                AS master_product_id
              , fk_info.fk_size_model
              , lr.last_receipt_date
              , mrs.memory_foam_flag
              , mrs.cushioned_footbed_flag
              , mrs.cushioned_heel_flag
              , mrs.arch_support_flag
              , mrs.antibacterial_flag
              , mrs.sweat_wicking_flag
              , mrs.breathing_holes_flag
              , IFF(mrs.memory_foam_flag = 'Y'
                        OR mrs.cushioned_footbed_flag = 'Y'
                        OR mrs.cushioned_heel_flag = 'Y'
                        OR mrs.arch_support_flag = 'Y'
                        OR mrs.antibacterial_flag = 'Y'
                        OR mrs.sweat_wicking_flag = 'Y'
                        OR mrs.breathing_holes_flag = 'Y', 'Y',
                    'N')                                                                                  AS comfort_flag
              , mml.distro
              , pi.show_room
              , IFF(mpi.product_status = 'Active', 1, 0)                                                  AS is_active
              , IFF(mpi.current_status = 'Active', 1, 0)                                                  AS current_status
              , pi.exact_latest_launch_date
              , IFF(pca.boot_shaft_height_console IS NULL, FIRST_VALUE(pca.boot_shaft_height_console)
                                                                       OVER (PARTITION BY mml.product_sku, mml.business_unit, mml.region ORDER BY mpi.master_product_id ASC),
                    pca.boot_shaft_height_console)                                                        AS boot_shaft_height_console
              , IFF(pca.color_family_console IS NULL, FIRST_VALUE(pca.color_family_console)
                                                                  OVER (PARTITION BY mml.product_sku, mml.business_unit, mml.region ORDER BY mpi.master_product_id ASC),
                    pca.color_family_console)                                                             AS color_family_console
              , IFF(pca.designer_collaboration_console IS NULL, FIRST_VALUE(pca.designer_collaboration_console)
                                                                            OVER (PARTITION BY mml.product_sku, mml.business_unit, mml.region ORDER BY mpi.master_product_id ASC),
                    pca.designer_collaboration_console)                                                   AS designer_collaboration_console
              , IFF(pca.fit_console IS NULL, FIRST_VALUE(pca.fit_console)
                                                         OVER (PARTITION BY mml.product_sku, mml.business_unit, mml.region ORDER BY mpi.master_product_id ASC),
                    pca.fit_console)                                                                      AS fit_console
              , IFF(pca.heel_shape_console IS NULL, FIRST_VALUE(pca.heel_shape_console)
                                                                OVER (PARTITION BY mml.product_sku, mml.business_unit, mml.region ORDER BY mpi.master_product_id ASC),
                    pca.heel_shape_console)                                                               AS heel_shape_console
              , IFF(pca.heel_size_console IS NULL, FIRST_VALUE(pca.heel_size_console)
                                                               OVER (PARTITION BY mml.product_sku, mml.business_unit, mml.region ORDER BY mpi.master_product_id ASC),
                    pca.heel_size_console)                                                                AS heel_size_console
              , IFF(pca.material_type_console IS NULL, FIRST_VALUE(pca.material_type_console)
                                                                   OVER (PARTITION BY mml.product_sku, mml.business_unit, mml.region ORDER BY mpi.master_product_id ASC),
                    pca.material_type_console)                                                            AS material_type_console
              , IFF(pca.occasion_type_console IS NULL, FIRST_VALUE(pca.occasion_type_console)
                                                                   OVER (PARTITION BY mml.product_sku, mml.business_unit, mml.region ORDER BY mpi.master_product_id ASC),
                    pca.occasion_type_console)                                                            AS occasion_type_console
              , IFF(pca.toe_type_console IS NULL, FIRST_VALUE(pca.toe_type_console)
                                                              OVER (PARTITION BY mml.product_sku, mml.business_unit, mml.region ORDER BY mpi.master_product_id ASC),
                    pca.toe_type_console)                                                                 AS toe_type_console
              , IFF(pca.weather_console IS NULL, FIRST_VALUE(pca.weather_console)
                                                             OVER (PARTITION BY mml.product_sku, mml.business_unit, mml.region ORDER BY mpi.master_product_id ASC),
                    pca.weather_console)                                                                  AS weather_console
              , pi.first_launch_date
              , pr.avg_overall_rating
              , IFF(pca.img_model_plus IS NULL, FIRST_VALUE(pca.img_model_plus)
                                                            OVER (PARTITION BY mml.product_sku, mml.business_unit, mml.region ORDER BY mpi.master_product_id ASC),
                    pca.img_model_plus)                                                                   AS img_model_plus
              , IFF(pca.img_model_reg IS NULL, FIRST_VALUE(pca.img_model_reg)
                                                           OVER (PARTITION BY mml.product_sku, mml.business_unit, mml.region ORDER BY mpi.master_product_id ASC),
                    pca.img_model_reg)                                                                    AS img_model_reg
              , IFF(pca.img_type IS NULL, FIRST_VALUE(pca.img_type)
                                                      OVER (PARTITION BY mml.product_sku, mml.business_unit, mml.region ORDER BY mpi.master_product_id ASC),
                    pca.img_type)                                                                         AS img_type
              , IFF(pca.sleeve_lenth_console IS NULL, FIRST_VALUE(pca.sleeve_lenth_console)
                                                                  OVER (PARTITION BY mml.product_sku, mml.business_unit, mml.region ORDER BY mpi.master_product_id ASC),
                    pca.sleeve_lenth_console)                                                             AS sleeve_lenth_console
              , IFF(pca.clothing_detail_console IS NULL, FIRST_VALUE(pca.clothing_detail_console)
                                                                     OVER (PARTITION BY mml.product_sku, mml.business_unit, mml.region ORDER BY mpi.master_product_id ASC),
                    pca.clothing_detail_console)                                                          AS clothing_detail_console
              , IFF(pca.color_console IS NULL, FIRST_VALUE(pca.color_console)
                                                           OVER (PARTITION BY mml.product_sku, mml.business_unit, mml.region ORDER BY mpi.master_product_id ASC),
                    pca.color_console)                                                                    AS color_console
              , IFF(pca.buttom_subclass_console IS NULL, FIRST_VALUE(pca.buttom_subclass_console)
                                                                     OVER (PARTITION BY mml.product_sku, mml.business_unit, mml.region ORDER BY mpi.master_product_id ASC),
                    pca.buttom_subclass_console)                                                          AS buttom_subclass_console
              , IFF(pca.shop_category_console IS NULL, FIRST_VALUE(pca.shop_category_console)
                                                                   OVER (PARTITION BY mml.product_sku, mml.business_unit, mml.region ORDER BY mpi.master_product_id ASC),
                    pca.shop_category_console)                                                            AS shop_category_console
              , IFF(pca.shoe_style_console IS NULL, FIRST_VALUE(pca.shoe_style_console)
                                                                OVER (PARTITION BY mml.product_sku, mml.business_unit, mml.region ORDER BY mpi.master_product_id ASC),
                    pca.shoe_style_console)                                                               AS shoe_style_console
              , mpi.product_color_from_label
FROM _merch_master_log_file mml
         JOIN _store st
              ON LOWER(st.store_brand) = LOWER(mml.business_unit)
                  AND LOWER(st.store_region) = LOWER(mml.region)
         LEFT JOIN _po_info pi
                   ON pi.business_unit = mml.business_unit
                       AND pi.region = mml.region
                       AND pi.product_sku = mml.product_sku
         LEFT JOIN lake.merch.merch_color_mapping mcm
                   ON LOWER(mcm.color) = LOWER(pi.color)
                       AND LOWER(mcm.business_unit) = LOWER(mml.business_unit)
         LEFT JOIN _merch_reference_sheet mrs
                   ON mrs.product_sku = mml.product_sku
         LEFT JOIN _product_info prod
                   ON prod.business_unit = mml.business_unit
                       AND prod.region = mml.region
                       AND prod.product_sku = mml.product_sku
         LEFT JOIN _pendin_qty pq
                   ON pq.business_unit = mml.business_unit
                       AND pq.region = mml.region
                       AND LOWER(pq.country) = LOWER(st.store_country)
                       AND pq.product_sku = mml.product_sku
         LEFT JOIN _product_reviews pr
                   ON pr.business_unit = mml.business_unit
                       AND pr.region = mml.region
                       AND pr.product_sku = mml.product_sku
         LEFT JOIN lake_view.sharepoint.gfb_collection_apparel_tracker cat
                   ON LOWER(mml.business_unit) = LOWER(cat.business_unit)
                       AND mml.product_sku = cat.product_sku
         LEFT JOIN
     (
         SELECT DISTINCT 'FABKIDS'                                                                          AS business_unit
                       , 'NA'                                                                               AS region
                       , UPPER(fk.style_color)                                                              AS product_sku
                       , UPPER(fk.subclass)                                                                 AS fk_subclass
                       , UPPER(fk.attribute)                                                                AS fk_attribute
                       , FIRST_VALUE(UPPER(fk.site_name))
                                     OVER (PARTITION BY UPPER(fk.style_color) ORDER BY fk.launch_date DESC) AS fk_site_name
                       , UPPER(fk.color)                                                                    AS fk_color
                       , UPPER(fk.new_season_code)                                                          AS fk_new_season_code
                       , UPPER(fk.size_model)                                                               AS fk_size_model
         FROM lake_view.sharepoint.gfb_fk_merch_item_attributes fk
     ) fk_info ON fk_info.business_unit = mml.business_unit
         AND fk_info.region = mml.region
         AND fk_info.product_sku = mml.product_sku
         LEFT JOIN lake_view.sharepoint.gfb_product_marketing_capsule pmc
                   ON LOWER(pmc.brand) = LOWER(mml.business_unit)
                       AND LOWER(pmc.region) = LOWER(mml.region)
                       AND LOWER(pmc.product_sku) = LOWER(mml.product_sku)
         LEFT JOIN _master_product_id mpi
                   ON LOWER(mpi.store_brand) = LOWER(mml.business_unit)
                       AND LOWER(mpi.store_region) = LOWER(mml.region)
                       AND LOWER(mpi.store_country) = LOWER(st.store_country)
                       AND mpi.product_sku = mml.product_sku
         LEFT JOIN lake_view.sharepoint.gfb_merch_attributes ma
                   ON LOWER(ma.business_unit) = LOWER(mml.business_unit)
                       AND mml.product_sku = ma.product_sku
                       AND mml.department = 'APPAREL'
         LEFT JOIN _latest_receipt lr
                   ON LOWER(lr.business_unit) = LOWER(mml.business_unit)
                       AND LOWER(lr.region) = LOWER(mml.region)
                       AND lr.product_sku = mml.product_sku
         LEFT JOIN _product_console_attribute_flat pca
                   ON LOWER(pca.store_brand) = LOWER(mml.business_unit)
                       AND LOWER(pca.store_region) = LOWER(mml.region)
                       AND LOWER(pca.store_country) = LOWER(st.store_country)
                       AND pca.product_sku = mml.product_sku;


CREATE OR REPLACE TEMPORARY TABLE _product_attr AS
SELECT DISTINCT a.product_sku
              , a.region
              , FIRST_VALUE(a.product_style_number)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.product_style_number ASC)   AS product_style_number
              , FIRST_VALUE(a.reorder_status)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.reorder_status ASC)         AS reorder_status
              , FIRST_VALUE(a.style_rank)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.style_rank ASC)             AS style_rank
              , FIRST_VALUE(a.description)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.description ASC)            AS description
              , FIRST_VALUE(a.ww_wc)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.ww_wc ASC)                  AS ww_wc
              , FIRST_VALUE(a.style_name)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.style_name ASC)             AS style_name
              , FIRST_VALUE(a.color)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.color ASC)                  AS color
              , FIRST_VALUE(a.master_color)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.master_color ASC)           AS master_color
              , FIRST_VALUE(a.classification)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.classification ASC)         AS classification
              , FIRST_VALUE(a.vendor)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.vendor ASC)                 AS vendor
              , FIRST_VALUE(a.direct_non_direct)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.direct_non_direct ASC)      AS direct_non_direct
              , FIRST_VALUE(a.landing_cost)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.landing_cost ASC)           AS landing_cost
              , FIRST_VALUE(a.heel_height)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.heel_height ASC)            AS heel_height
              , FIRST_VALUE(a.heel_type)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.heel_type ASC)              AS heel_type
              , FIRST_VALUE(a.coresb_reorder_fashion)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.coresb_reorder_fashion ASC) AS coresb_reorder_fashion
              , FIRST_VALUE(a.image_url)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.image_url ASC)              AS image_url
              , FIRST_VALUE(a.is_plussize)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.is_plussize ASC)            AS is_plussize
              , FIRST_VALUE(a.size_13_15)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.size_13_15 ASC)             AS size_13_15
              , FIRST_VALUE(a.collection)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.collection ASC)             AS collection
              , FIRST_VALUE(a.qty_pending)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.qty_pending ASC)            AS qty_pending
              , FIRST_VALUE(a.og_retail)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.og_retail ASC)              AS og_retail
              , FIRST_VALUE(a.priced_down)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.priced_down ASC)            AS priced_down
              , FIRST_VALUE(a.subclass)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.subclass ASC)               AS subclass
              , FIRST_VALUE(a.na_landed_cost)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.na_landed_cost ASC)         AS na_landed_cost
              , FIRST_VALUE(a.eu_landed_cost)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.eu_landed_cost ASC)         AS eu_landed_cost
              , FIRST_VALUE(a.error_free_image_url)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.error_free_image_url ASC)   AS error_free_image_url
              , FIRST_VALUE(a.large_img_url)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.large_img_url ASC)          AS large_img_url
              , FIRST_VALUE(a.fk_attribute)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.fk_attribute ASC)           AS fk_attribute
              , FIRST_VALUE(a.fk_site_name)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.fk_site_name ASC)           AS fk_site_name
              , FIRST_VALUE(a.fk_new_season_code)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.fk_new_season_code ASC)     AS fk_new_season_code
              , FIRST_VALUE(a.marketing_capsule_name)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.marketing_capsule_name ASC) AS marketing_capsule_name
              , FIRST_VALUE(a.season_code)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.season_code ASC)            AS season_code
              , FIRST_VALUE(a.style_type)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.style_type ASC)             AS style_type
              , FIRST_VALUE(a.gender IGNORE NULLS)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.business_unit ASC)          AS gender
              , FIRST_VALUE(a.fk_size_model)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.fk_size_model ASC)          AS fk_size_model
              , FIRST_VALUE(a.last_receipt_date)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.last_receipt_date ASC)      AS last_receipt_date
              , FIRST_VALUE(a.memory_foam_flag)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.memory_foam_flag ASC)       AS memory_foam_flag
              , FIRST_VALUE(a.cushioned_footbed_flag)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.cushioned_footbed_flag ASC) AS cushioned_footbed_flag
              , FIRST_VALUE(a.cushioned_heel_flag)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.cushioned_heel_flag ASC)    AS cushioned_heel_flag
              , FIRST_VALUE(a.arch_support_flag)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.arch_support_flag ASC)      AS arch_support_flag
              , FIRST_VALUE(a.antibacterial_flag)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.antibacterial_flag ASC)     AS antibacterial_flag
              , FIRST_VALUE(a.sweat_wicking_flag)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.sweat_wicking_flag ASC)     AS sweat_wicking_flag
              , FIRST_VALUE(a.breathing_holes_flag)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.breathing_holes_flag ASC)   AS breathing_holes_flag
              , FIRST_VALUE(a.comfort_flag)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.comfort_flag ASC)           AS comfort_flag
              , FIRST_VALUE(a.distro)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.distro ASC)                 AS distro
              , FIRST_VALUE(a.is_active)
                            OVER (PARTITION BY a.product_sku, a.region ORDER BY a.distro ASC)                 AS is_active
FROM _merch_dim_product a;


CREATE OR REPLACE TEMPORARY TABLE _first_clearance_date AS
SELECT UPPER(a.business_unit) AS business_unit
     , UPPER(a.region)        AS region
     , a.sku                  AS product_sku
     , MIN(a.start_date)      AS first_clearance_date
FROM lake_view.sharepoint.gfb_merch_clearance_sku_list a
GROUP BY UPPER(a.business_unit)
       , UPPER(a.region)
       , a.sku;

CREATE OR REPLACE TEMPORARY TABLE _console_product_waitlist_type AS
SELECT UPPER(st.store_brand)   AS business_unit
     , UPPER(st.store_region)  AS region
     , UPPER(st.store_country) AS country
     , dp.product_sku
     , pwlt.code
     , pwlt.label              AS product_wait_list_type
     , MAX(pwl.start_datetime) AS start_datetime
     , MAX(pwl.end_datetime)   AS end_datetime
FROM lake_jfb_view.ultra_merchant.product_wait_list pwl
         JOIN lake_jfb_view.ultra_merchant.product_wait_list_type pwlt
              ON pwlt.product_wait_list_type_id = pwl.product_wait_list_type_id
         JOIN edw_prod.data_model_jfb.dim_product dp
              ON dp.master_product_id = pwl.product_id
         JOIN edw_prod.data_model_jfb.dim_store st
              ON st.store_id = dp.store_id
                  AND st.store_brand_abbr IN ('JF', 'SD', 'FK')
                  AND st.store_full_name NOT LIKE '%(DM)%'
                  AND st.store_full_name NOT LIKE '%Wholesale%'
                  AND st.store_full_name NOT LIKE '%Heels.com%'
                  AND st.store_full_name NOT LIKE '%Retail%'
                  AND st.store_full_name NOT LIKE '%Sample%'
                  AND st.store_full_name NOT LIKE '%SWAG%'
                  AND st.store_full_name NOT LIKE '%PS%'
GROUP BY UPPER(st.store_brand)
       , UPPER(st.store_region)
       , UPPER(st.store_country)
       , dp.product_sku
       , pwlt.code
       , pwlt.label;


CREATE OR REPLACE TEMPORARY TABLE _fk_on_brands AS
SELECT DISTINCT mdp.product_sku
              , FIRST_VALUE(mdp.current_vip_retail IGNORE NULLS)
                            OVER (PARTITION BY mdp.product_sku ORDER BY mdp.latest_launch_date DESC) AS current_vip_retail
FROM _merch_dim_product mdp
WHERE mdp.business_unit = 'FABKIDS'
  AND mdp.current_vip_retail != 0
  AND mdp.current_vip_retail IS NOT NULL;

CREATE OR REPLACE TEMPORARY TABLE _fk_prepacks AS
SELECT outfit_vs_box_vs_pack
     , item1
     , 'FABKIDS' AS business_unit
     , 'NA'      AS region
FROM lake_view.sharepoint.gfb_fk_merch_outfit_attributes
WHERE outfit_vs_box_vs_pack = 'Prepack';

CREATE OR REPLACE TEMPORARY TABLE _jf_fk_check_for_images AS
SELECT product_sku, COUNT(DISTINCT business_unit) AS brand_count
FROM _merch_dim_product
WHERE business_unit IN ('JUSTFAB', 'FABKIDS')
  AND region = 'NA'
GROUP BY product_sku
HAVING COUNT(DISTINCT business_unit) > 1;


CREATE OR REPLACE TEMPORARY TABLE _mm_department_subcategory AS
SELECT DISTINCT s.tfg_colorway_sku                                                                        AS product_sku,
                FIRST_VALUE(CASE
                                WHEN ec2.node_name = 'BOOTIES' THEN 'ANKLE BOOTS'
                                WHEN ec2.node_name = 'SANDALS-FLAT' THEN 'FLAT SANDALS'
                                WHEN ec2.node_name = 'SANDALS-DRESSY' THEN 'HEELED SANDALS'
                                WHEN ec2.node_name = 'STREET' THEN 'SNEAKERS'
                                ELSE ec2.node_name END)
                            OVER (PARTITION BY product_sku ORDER BY product_sku)                          AS subcategory,
                FIRST_VALUE(UPPER(c3.node_name)) OVER (PARTITION BY product_sku ORDER BY product_sku ASC) AS subclass
FROM lake_view.centric.ed_sku s
         LEFT JOIN lake_view.centric.ed_style d ON s.the_parent_id = d.id
         LEFT JOIN lake_view.centric.ed_classifier2 ec2 ON ec2.id = d.tfg_classifier2
         LEFT JOIN lake_view.centric.ed_classifier3 c3 ON c3.id = d.classifier3
         JOIN (SELECT DISTINCT product_sku
               FROM reporting_prod.gfb.merch_dim_product
               WHERE sub_brand NOT IN ('JFB')) mdp
              ON s.tfg_colorway_sku = mdp.product_sku;

CREATE OR REPLACE TEMP TABLE _mm_version_sku AS
SELECT DISTINCT tfg_colorway_sku,
                FIRST_VALUE(SUBSTRING(tfg_vendor_sku, 1, REGEXP_INSTR(tfg_vendor_sku, '-', 1, 2) - 1))
                            OVER (PARTITION BY tfg_colorway_sku ORDER BY SUBSTRING(tfg_vendor_sku, 1,
                                                                                   REGEXP_INSTR(tfg_vendor_sku, '-', 1, 2) -
                                                                                   1)) AS mm_sku
FROM lake_view.centric.ed_sku
WHERE tfg_division = 'tfgDivision:MM'
  AND tfg_vendor_sku IS NOT NULL;

CREATE OR REPLACE TEMPORARY TABLE _department_alignment AS
SELECT mdp.business_unit,
       mdp.product_sku,
       a.gender,
       a.department,
       a.department_detail,
       a.subcategory,
       p.subclass
FROM _merch_dim_product mdp
         JOIN (SELECT business_unit,
                      product_sku,
                      department,
                      department_detail,
                      gender,
                      subcategory,
                      ROW_NUMBER() OVER (PARTITION BY product_sku ORDER BY business_unit ASC,region DESC) AS row_num
               FROM _merch_dim_product) a
              ON mdp.product_sku = a.product_sku
         LEFT JOIN(SELECT DISTINCT product_sku,
                                   FIRST_VALUE(subclass)
                                               OVER (PARTITION BY product_sku ORDER BY region DESC) AS subclass
                   FROM _product_attr) p
                  ON mdp.product_sku = p.product_sku
WHERE a.row_num = 1;

CREATE OR REPLACE TRANSIENT TABLE gfb.merch_dim_product AS
SELECT DISTINCT a.business_unit
              , a.sub_brand
              , a.region
              , a.country
              , a.product_sku
              , mvs.mm_sku                                                                                                                AS mm_product_sku
              , a.product_style_number
              , a.latest_launch_date
              , COALESCE(a.previous_launch_date, FIRST_VALUE(a.previous_launch_date)
                                                             OVER (PARTITION BY a.region, a.product_sku ORDER BY a.previous_launch_date)) AS previous_launch_date
              , CASE
                    WHEN da.department_detail = 'SHOES - UNKNOWN' AND
                         (da.gender NOT ILIKE '%boy%' OR b.gender NOT ILIKE '%girl%') THEN 'FOOTWEAR'
                    WHEN da.subcategory = 'SHOES' AND b.gender ILIKE '%boy%' THEN 'BOYS SHOES'
                    WHEN (da.subcategory IN ('SLEEPWEAR', 'BOTTOM', 'TOP', 'CLOTHING') OR
                          da.department ILIKE '%clothing%') AND b.gender ILIKE '%boy%'
                        THEN 'BOYS APPAREL'
                    WHEN (da.subcategory IN ('SLEEPWEAR', 'BOTTOM', 'TOP', 'CLOTHING') OR
                          da.department ILIKE '%clothing%') AND b.gender ILIKE '%girl%'
                        THEN 'GIRLS APPAREL'
                    WHEN da.department_detail ILIKE '%shoe%' AND b.gender ILIKE '%boy%'
                        THEN 'BOYS SHOES'
                    WHEN da.department_detail ILIKE '%footwear%' AND b.gender ILIKE '%boy%' THEN 'BOYS SHOES'
                    WHEN da.department_detail ILIKE '%shoe%' AND b.gender ILIKE '%girl%'
                        THEN 'GIRLS SHOES'
                    WHEN da.department_detail = 'SNEAKERS - SHOES' AND b.gender ILIKE '%girl%' THEN 'GIRLS SHOES'
                    WHEN da.department_detail ILIKE '%footwear%' AND b.gender ILIKE '%girl%'
                        THEN 'GIRLS SHOES'
                    WHEN da.subcategory ILIKE ('%ACCESSORY%') AND b.gender ILIKE '%boy%' THEN 'BOYS ACCESSORY'
                    WHEN da.subcategory ILIKE ('%ACCESSORY%') AND b.gender ILIKE '%girl%' THEN 'GIRLS ACCESSORY'
                    WHEN da.department = 'APPAREL – FABKIDS' AND b.gender = 'Girls' THEN 'GIRLS APPAREL'
                    WHEN da.department = 'APPAREL - FABKIDS' AND b.gender = 'Boys' THEN 'BOYS APPAREL'
                    WHEN da.department = 'BOYS - ACCESSORIES' THEN 'BOYS ACCESSORY'
                    WHEN da.department = 'GIRLS - ACCESSORIES' THEN 'GIRLS ACCESSORY'
                    WHEN da.department = 'GIRLS FOOTWEAR' THEN 'GIRLS SHOES'
                    WHEN da.department = 'BOYS FOOTWEAR' THEN 'BOYS SHOES'
                    WHEN da.department ILIKE '%BAGS & ACCESSORIES%' THEN 'BAGS & ACCESSORIES'
                    ELSE da.department
    END                                                                                                                                   AS department
              , CASE
                    WHEN da.department_detail = 'SHOES - UNKNOWN' AND
                         (da.gender NOT ILIKE '%boy%' OR b.gender NOT ILIKE '%girl%') THEN 'FOOTWEAR'
                    WHEN (da.subcategory IN ('SLEEPWEAR', 'BOTTOM', 'TOP', 'CLOTHING') OR
                          da.department ILIKE '%clothing%') AND b.gender ILIKE '%boy%'
                        THEN 'BOYS APPAREL'
                    WHEN (da.subcategory IN ('SLEEPWEAR', 'BOTTOM', 'TOP', 'CLOTHING') OR
                          da.department ILIKE '%clothing%') AND b.gender ILIKE '%girl%'
                        THEN 'GIRLS APPAREL'
                    WHEN da.department_detail ILIKE '%shoe%' AND b.gender ILIKE '%boy%'
                        THEN 'BOYS SHOES'
                    WHEN da.department_detail ILIKE '%footwear%' AND b.gender ILIKE '%boy%' THEN 'BOYS SHOES'
                    WHEN da.department_detail ILIKE '%shoe%' AND b.gender ILIKE '%girl%'
                        THEN 'GIRLS SHOES'
                    WHEN da.department_detail = 'SNEAKERS - SHOES' AND b.gender ILIKE '%girl%' THEN 'GIRLS SHOES'
                    WHEN da.department_detail ILIKE '%footwear%' AND b.gender ILIKE '%girl%'
                        THEN 'GIRLS SHOES'
                    WHEN da.subcategory ILIKE ('%ACCESSORY%') AND b.gender ILIKE '%boy%' THEN 'BOYS ACCESSORY'
                    WHEN da.subcategory ILIKE ('%ACCESSORY%') AND b.gender ILIKE '%girl%' THEN 'GIRLS ACCESSORY'
                    WHEN da.department_detail = 'APPAREL – FABKIDS' AND b.gender = 'Girls' THEN 'GIRLS APPAREL'
                    WHEN da.department_detail = 'APPAREL - FABKIDS' AND b.gender = 'Boys' THEN 'BOYS APPAREL'
                    WHEN da.department_detail = 'BOYS - ACCESSORIES' THEN 'BOYS ACCESSORY'
                    WHEN da.department_detail = 'GIRLS - ACCESSORIES' THEN 'GIRLS ACCESSORY'
                    WHEN da.department_detail = 'GIRLS FOOTWEAR' THEN 'GIRLS SHOES'
                    WHEN da.department_detail = 'BOYS FOOTWEAR' THEN 'BOYS SHOES'
                    WHEN da.department_detail ILIKE '%BAGS & ACCESSORIES%' THEN 'BAGS & ACCESSORIES'
                    ELSE da.department_detail
    END                                                                                                                                   AS department_detail
              , CASE
                    WHEN mds.subcategory IS NOT NULL THEN mds.subcategory
                    WHEN da.subcategory = 'BOOTIES' THEN 'ANKLE BOOTS'
                    WHEN da.subcategory = 'SANDALS-FLAT' THEN 'FLAT SANDALS'
                    WHEN da.subcategory = 'SANDALS-DRESSY' THEN 'HEELED SANDALS'
                    WHEN da.subcategory = 'STREET' THEN 'SNEAKERS'
                    WHEN da.subcategory = 'BOOT' THEN 'BOOTS'
                    WHEN da.subcategory ILIKE '%BELT%' THEN 'BELTS'
                    WHEN da.subcategory ILIKE '%BOTTOM%' THEN 'BOTTOMS'
                    WHEN da.subcategory = 'TOP' OR da.subcategory IN ('TOPS - BOYS', 'TOPS - GIRLS') THEN 'TOPS'
                    WHEN da.subcategory ILIKE '%OUTERWEAR%' THEN 'OUTERWEAR'
                    WHEN da.subcategory ILIKE '%JACKET%' THEN 'JACKETS'
                    WHEN da.subcategory ILIKE '%swimwears%' THEN 'SWIMWEAR'
                    WHEN da.subcategory = 'GIRLS APPAREL' AND da.subcategory = 'DRESS' THEN 'DRESSES'
                    WHEN CONTAINS(da.subcategory, '-') THEN LEFT(da.subcategory, POSITION('-' IN da.subcategory) - 2)
                    ELSE da.subcategory
    END                                                                                                                                   AS subcategory
              , b.reorder_status
              , a.shared
              , b.style_rank
              , b.description
              , b.ww_wc
              , COALESCE(b.style_name, mmpa.product_name)                                                                                 AS style_name
              , COALESCE(b.color, mmpa.color)                                                                                             AS color
              , b.master_color
              , b.classification
              , b.vendor
              , b.direct_non_direct
              , b.landing_cost
              , COALESCE(a.current_vip_retail, fob.current_vip_retail)                                                                    AS current_vip_retail
              , a.msrp
              , b.heel_height
              , b.heel_type
              , b.coresb_reorder_fashion
              , CASE
                    WHEN (a.business_unit = 'FABKIDS' AND jfk.product_sku IS NOT NULL) OR
                         (a.business_unit IN ('JUSTFAB', 'SHOEDAZZLE') AND
                          (a.department_detail ILIKE '%boy%' OR a.department_detail ILIKE '%girl%'))
                        THEN 'http://us-cdn.justfab.com/media/images/products/' ||
                             a.product_sku || '/' || a.product_sku ||
                             '-1_48x70.jpg'
                    ELSE b.image_url END                                                                                                  AS image_url
              , b.is_plussize
              , b.size_13_15
              , b.collection
              , b.qty_pending
              , a.avg_is_recommended_score
              , a.avg_style_score
              , a.avg_comfort_score
              , a.avg_quality_value_score
              , a.total_reviews
              , b.og_retail
              , b.priced_down
              , CASE
                    WHEN a.sub_brand NOT IN ('JFB') THEN mds.subclass
                    ELSE da.subclass END                                                                                                  AS subclass
              , b.na_landed_cost
              , b.eu_landed_cost
              , b.error_free_image_url
              , CASE
                    WHEN (a.business_unit = 'FABKIDS' AND jfk.product_sku IS NOT NULL) OR
                         (a.business_unit IN ('JUSTFAB', 'SHOEDAZZLE') AND
                          (a.department_detail ILIKE '%boy%' OR a.department_detail ILIKE '%girl%'))
                        THEN 'http://us-cdn.justfab.com/media/images/products/' ||
                             a.product_sku || '/' || a.product_sku ||
                             '-1_589x860.jpg'
                    ELSE b.large_img_url END                                                                                              AS large_img_url
              , b.fk_attribute
              , b.fk_site_name
              , b.fk_new_season_code
              , b.marketing_capsule_name
              , b.season_code
              , b.style_type
              , CASE
                    WHEN UPPER(b.gender) = 'MEN' THEN 'MENS'
                    WHEN UPPER(b.gender) = 'WOMEN' THEN 'WOMENS'
                    WHEN UPPER(b.gender) IN ('GIRLS, BOYS', 'BOYS, GIRLS')
                        AND a.department_detail ILIKE '%BOY%' THEN 'BOYS'
                    WHEN UPPER(b.gender) IN ('GIRLS, BOYS', 'BOYS, GIRLS')
                        AND a.department_detail ILIKE '%GIRL%' THEN 'GIRLS'
                    WHEN b.gender IS NULL AND a.department_detail ILIKE '%GIRL%'
                        THEN 'GIRLS'
                    WHEN b.gender IS NULL AND a.department_detail ILIKE '%BOY%'
                        THEN 'BOYS'
                    WHEN b.gender ILIKE ANY ('%WOMEN, MEN%', '%MEN, WOMEN%')
                        THEN 'WOMENS'
                    WHEN b.gender ILIKE ANY ('%BOYS, GIRLS%', '%GIRLS, BOYS%')
                        THEN 'GIRLS'
                    ELSE UPPER(b.gender) END                                                                                              AS gender
              , a.master_product_id
              , b.fk_size_model
              , b.last_receipt_date
              , b.memory_foam_flag
              , b.cushioned_footbed_flag
              , b.cushioned_heel_flag
              , b.arch_support_flag
              , b.antibacterial_flag
              , b.sweat_wicking_flag
              , b.breathing_holes_flag
              , b.comfort_flag
              , b.distro
              , CASE
                    WHEN a.sub_brand NOT IN ('JFB') OR
                         a.product_sku IN ('BM2252888-8954', 'BM2252890-3589', 'BM2252890-11504', 'OW2459161-11505',
                                           'TP2251875-11253', 'BM2251844-8893', 'BM2251858-8980', 'BM2252829-11851',
                                           'BM2458693-3854', 'DR2354478-8171', 'DR2457891-8993', 'OW2252773-11849',
                                           'TP2252767-11279', 'TP2354514-8886', 'TP2457907-8893')
                        THEN a.latest_launch_date
                    ELSE IFF(TO_NUMBER(DATEDIFF('day', IFNULL(a.show_room, a.latest_launch_date),
                                                LAST_DAY(IFNULL(a.show_room, a.latest_launch_date)))) <= 4,
                             DATE_TRUNC('month', DATEADD('month', 1, IFNULL(a.show_room, a.latest_launch_date))),
                             IFNULL(a.show_room, a.latest_launch_date)) END                                                               AS show_room
              , COALESCE(cp.clearance_price, a.current_vip_retail,
                         mmpa.vip_unit_price)                                                                                             AS current_price
              , a.exact_latest_launch_date
              , (CASE
                     WHEN bs.product_sku IS NULL THEN 0
                     ELSE 1 END)                                                                                                          AS is_base_sku
              , a.boot_shaft_height_console
              , a.color_family_console
              , a.designer_collaboration_console
              , a.fit_console
              , a.heel_shape_console
              , a.heel_size_console
              , a.material_type_console
              , a.occasion_type_console
              , a.toe_type_console
              , a.weather_console
              , a.first_launch_date
              , fcd.first_clearance_date
              , a.avg_overall_rating
              , a.img_model_reg
              , a.img_model_plus
              , a.img_type
              , (CASE
                     WHEN cpwt.product_sku IS NULL THEN 'No Waitlist Type'
                     ELSE cpwt.product_wait_list_type END)                                                                                AS product_wait_list_type
              , (CASE
                     WHEN cpwt.product_sku IS NOT NULL AND CURRENT_DATE() <= cpwt.end_datetime
                         THEN 'Waitlist Item'
                     ELSE 'Not Waitlist Item' END)                                                                                        AS waitlist_with_known_eta_flag
              , cpwt.start_datetime                                                                                                       AS waitlist_start_datetime
              , cpwt.end_datetime                                                                                                         AS waitlist_end_datetime
              , CASE
                    WHEN a.is_active = 1 AND a.sub_brand NOT IN ('JUSTFAB', 'SHOEDAZZLE', 'FABKIDS') THEN 1
                    WHEN a.is_active = 1 AND ci.qty_available_to_sell > 0 THEN 1
                    WHEN a.is_active = 1 AND ci.qty_available_to_sell = 0 AND
                         waitlist_with_known_eta_flag = 'Waitlist Item' THEN 1
                    ELSE 0 END                                                                                                            AS is_active
              , CASE
                    WHEN a.current_status = 1 AND a.sub_brand NOT IN ('JUSTFAB', 'SHOEDAZZLE', 'FABKIDS') THEN 1
                    WHEN a.current_status = 1 AND ci.qty_available_to_sell > 0 THEN 1
                    WHEN a.current_status = 1 AND ci.qty_available_to_sell = 0 AND
                         waitlist_with_known_eta_flag = 'Waitlist Item' THEN 1
                    ELSE 0 END                                                                                                            AS current_status
              , (CASE
                     WHEN a.latest_launch_date > DATE_TRUNC(MONTH, CURRENT_DATE())
                         THEN COALESCE(COALESCE(a.previous_launch_date, FIRST_VALUE(a.previous_launch_date)
                                                                                    OVER (PARTITION BY a.region, a.product_sku ORDER BY a.previous_launch_date)),
                                       a.latest_launch_date)
                     ELSE a.latest_launch_date
    END)                                                                                                                                  AS current_showroom
              , po.unit_price                                                                                                             AS sale_price
              , a.sleeve_lenth_console
              , a.clothing_detail_console
              , a.color_console
              , a.buttom_subclass_console
              , a.shop_category_console
              , a.shoe_style_console
              , plc.product_color_from_label
              , fp.outfit_vs_box_vs_pack                                                                                                  AS prepack_flag
FROM _merch_dim_product a
         JOIN _product_attr b
              ON a.product_sku = b.product_sku
                  AND a.region = b.region
         JOIN _department_alignment da
              ON a.business_unit = da.business_unit
                  AND a.product_sku = da.product_sku
         LEFT JOIN _clearance_price cp
                   ON cp.business_unit = a.business_unit
                       AND cp.region = a.region
                       AND cp.product_sku = a.product_sku
         LEFT JOIN lake_view.sharepoint.gfb_merch_base_sku bs
                   ON LOWER(bs.business_unit) = LOWER(a.business_unit)
                       AND LOWER(bs.region) = LOWER(a.region)
                       AND bs.product_sku = a.product_sku
         LEFT JOIN _first_clearance_date fcd
                   ON LOWER(fcd.business_unit) = LOWER(a.business_unit)
                       AND LOWER(fcd.region) = LOWER(a.region)
                       AND fcd.product_sku = a.product_sku
         LEFT JOIN
     (
         SELECT a.business_unit
              , a.region
              , a.country
              , a.product_sku
              , SUM(a.qty_available_to_sell) AS qty_available_to_sell
         FROM reporting_prod.gfb.gfb_inventory_data_set_current a
         GROUP BY a.business_unit
                , a.region
                , a.country
                , a.product_sku
     ) ci ON ci.business_unit = a.business_unit
         AND ci.region = a.region
         AND ci.country = a.country
         AND ci.product_sku = a.product_sku
         LEFT JOIN _console_product_waitlist_type cpwt
                   ON cpwt.business_unit = a.business_unit
                       AND cpwt.region = a.region
                       AND cpwt.country = a.country
                       AND cpwt.product_sku = a.product_sku
         LEFT JOIN lake_jfb_view.ultra_merchant.product p
                   ON p.product_id = a.master_product_id
         LEFT JOIN lake_jfb_view.ultra_merchant.pricing_option po
                   ON po.pricing_id = p.sale_pricing_id
         LEFT JOIN _product_label_color plc
                   ON plc.product_sku = a.product_sku
                       AND plc.store_region = a.region
         LEFT JOIN _fk_on_brands fob
                   ON fob.product_sku = a.product_sku
         LEFT JOIN _fk_prepacks fp
                   ON a.business_unit = fp.business_unit
                       AND a.product_sku = fp.item1
         LEFT JOIN _jf_fk_check_for_images jfk
                   ON jfk.product_sku = a.product_sku
         LEFT JOIN _miracle_mile_product_attributes mmpa
                   ON mmpa.product_sku = a.product_sku
         LEFT JOIN _mm_department_subcategory mds
                   ON mds.product_sku = a.product_sku
         LEFT JOIN _mm_version_sku mvs
                   ON mvs.tfg_colorway_sku = a.product_sku;

