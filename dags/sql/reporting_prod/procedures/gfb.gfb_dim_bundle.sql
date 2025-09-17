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


CREATE OR REPLACE TEMPORARY TABLE _bundle_product AS
SELECT DISTINCT st.store_brand
              , st.store_region
              , pbc.bundle_product_id
              , pbc.component_product_id
              , UPPER(dp.product_alias) AS bundle_alias
              , dp.image_url            AS bundle_image_url
              , (CASE
                     WHEN dp.product_status = 'Active' THEN 1
                     ELSE 0 END)        AS is_bundle_active
              , dp.product_type
              , dp.group_code
              , dp.base_sku

FROM edw_prod.data_model_jfb.dim_product dp
         JOIN edw_prod.data_model_jfb.dim_store ds
              ON ds.store_id = dp.store_id
         JOIN _store st
              ON ds.store_brand = st.store_brand
                  AND ds.store_region = st.store_region
                  AND ds.store_country = st.store_country
         JOIN lake_jfb_view.ultra_merchant.product_bundle_component pbc
              ON pbc.bundle_product_id = dp.product_id
         JOIN (SELECT DISTINCT product_id FROM lake_view.sharepoint.gfb_fk_merch_outfit_attributes) fk
              ON pbc.bundle_product_id = fk.product_id
WHERE st.store_brand IN ('FabKids')
  AND dp.product_type = 'Bundle'
UNION
SELECT DISTINCT st.store_brand
              , st.store_region
              , pbc.bundle_product_id
              , pbc.component_product_id
              , UPPER(dp.product_alias)                AS bundle_alias
              , dp.image_url                           AS bundle_image_url
              , (CASE
                     WHEN dp.product_status = 'Active' THEN 1
                     ELSE 0 END)                       AS is_bundle_active
              , dp.product_type
              , dp.group_code
              , COALESCE(dp_1.base_sku, dp_2.base_sku) AS base_sku
FROM edw_prod.data_model_jfb.dim_product dp
         JOIN edw_prod.data_model_jfb.dim_store ds
              ON ds.store_id = dp.store_id
         JOIN _store st
              ON ds.store_brand = st.store_brand
                  AND ds.store_region = st.store_region
                  AND ds.store_country = st.store_country
         JOIN lake_jfb_view.ultra_merchant.product_bundle_component pbc
              ON pbc.bundle_product_id = dp.product_id
         LEFT JOIN edw_prod.data_model_jfb.dim_product dp_1 -- product with multiple color and size
                   ON dp_1.master_product_id = pbc.component_product_id
         LEFT JOIN edw_prod.data_model_jfb.dim_product dp_2 -- product with only one option
                   ON dp_2.product_id = pbc.component_product_id
                       AND dp_2.master_product_id = -1
                       AND dp_2.base_sku != 'Unknown'
WHERE st.store_brand IN ('JustFab', 'ShoeDazzle')
  AND dp.product_type = 'Bundle';


CREATE OR REPLACE TEMPORARY TABLE _bundle_alias_name AS
SELECT DISTINCT b.bundle_product_id
              , b.bundle_name
              , b.bundle_alias
FROM (
         SELECT a.*
              , RANK() OVER (PARTITION BY a.bundle_product_id ORDER BY a.bundle_component_history_key DESC) AS bundle_rank
         FROM (
                  SELECT bch.bundle_product_id
                       , bch.bundle_name
                       , bch.bundle_alias
                       , bch.bundle_component_history_key
                  FROM edw_prod.data_model_jfb.dim_bundle_component_history bch
                  WHERE LOWER(bch.bundle_alias) != 'error'
              ) a
     ) b
WHERE b.bundle_rank = 1;


CREATE OR REPLACE TEMPORARY TABLE _fk_bundle_info AS
SELECT DISTINCT 'FABKIDS'              AS business_unit
              , 'NA'                   AS region
              , moa.product_id         AS bundle_product_id
              , TRIM(moa.outfit_alias) AS outfit_alias
              , moa.site_name
              , moa.vip_retail
              , moa.type
              , moa.gender
              , moa.active_inactive
              , moa.outfit_vs_box_vs_pack
              , moa.total_cost_of_outfit
              , moa.total_cost_of_outfit_tariff
              , moa.outfit_imu
              , moa.outfit_imu_tariff
              , moa.contains_shoes
              , moa.compontents_number
FROM lake_view.sharepoint.gfb_fk_merch_outfit_attributes moa;


CREATE OR REPLACE TEMPORARY TABLE _final_bundle_info AS
SELECT DISTINCT UPPER(bp.store_brand)                                                                                                                                                                 AS business_unit
              , UPPER(bp.store_region)                                                                                                                                                                AS region
              , UPPER(st.store_country)                                                                                                                                                               AS country
              , bp.bundle_product_id
              , bp.component_product_id
              , (CASE
                     WHEN bp.store_brand IN ('JustFab', 'FabKids') THEN
                             'https://jf-na-cdn.justfab.com/image/product/' ||
                             ban.bundle_alias || '/' || ban.bundle_alias ||
                             '-1_589x860.jpg'
                     ELSE bp.bundle_image_url END)                                                                                                                                                    AS bundle_image_url
              , bp.is_bundle_active
              , UPPER(bp.product_type)                                                                                                                                                                AS product_type
              , UPPER(bp.group_code)                                                                                                                                                                  AS group_code
              , bp.base_sku

              , UPPER(ban.bundle_name)                                                                                                                                                                AS bundle_name
              , UPPER(ban.bundle_alias)                                                                                                                                                               AS bundle_alias
              , fbi.compontents_number

              , FIRST_VALUE(UPPER(fbi.outfit_alias))
                            OVER (PARTITION BY UPPER(bp.store_brand), UPPER(bp.store_region), UPPER(st.store_country), UPPER(ban.bundle_alias),bp.bundle_product_id ORDER BY UPPER(fbi.outfit_alias)) AS fk_bundle_alias
              , FIRST_VALUE(UPPER(fbi.site_name))
                            OVER (PARTITION BY UPPER(bp.store_brand), UPPER(bp.store_region), UPPER(st.store_country), UPPER(ban.bundle_alias),bp.bundle_product_id ORDER BY UPPER(fbi.outfit_alias)) AS fk_bundle_site_name
              , FIRST_VALUE(UPPER(fbi.vip_retail))
                            OVER (PARTITION BY UPPER(bp.store_brand), UPPER(bp.store_region), UPPER(st.store_country), UPPER(ban.bundle_alias),bp.bundle_product_id ORDER BY UPPER(fbi.outfit_alias)) AS fk_vip_retail
              , FIRST_VALUE(UPPER(fbi.type))
                            OVER (PARTITION BY UPPER(bp.store_brand), UPPER(bp.store_region), UPPER(st.store_country), UPPER(ban.bundle_alias),bp.bundle_product_id ORDER BY UPPER(fbi.outfit_alias)) AS fk_type
              , FIRST_VALUE(UPPER(fbi.gender))
                            OVER (PARTITION BY UPPER(bp.store_brand), UPPER(bp.store_region), UPPER(st.store_country), UPPER(ban.bundle_alias),bp.bundle_product_id ORDER BY UPPER(fbi.outfit_alias)) AS fk_gender
              , FIRST_VALUE(UPPER(fbi.active_inactive))
                            OVER (PARTITION BY UPPER(bp.store_brand), UPPER(bp.store_region), UPPER(st.store_country), UPPER(ban.bundle_alias),bp.bundle_product_id ORDER BY UPPER(fbi.outfit_alias)) AS fk_active_inactive
              , FIRST_VALUE(UPPER(fbi.outfit_vs_box_vs_pack))
                            OVER (PARTITION BY UPPER(bp.store_brand), UPPER(bp.store_region), UPPER(st.store_country), UPPER(ban.bundle_alias),bp.bundle_product_id ORDER BY UPPER(fbi.outfit_alias)) AS fk_outfit_vs_box_vs_pack
              , FIRST_VALUE(UPPER(fbi.total_cost_of_outfit))
                            OVER (PARTITION BY UPPER(bp.store_brand), UPPER(bp.store_region), UPPER(st.store_country), UPPER(ban.bundle_alias),bp.bundle_product_id ORDER BY UPPER(fbi.outfit_alias)) AS fk_total_cost_of_outfit
              , FIRST_VALUE(UPPER(fbi.total_cost_of_outfit_tariff))
                            OVER (PARTITION BY UPPER(bp.store_brand), UPPER(bp.store_region), UPPER(st.store_country), UPPER(ban.bundle_alias),bp.bundle_product_id ORDER BY UPPER(fbi.outfit_alias)) AS fk_total_cost_of_outfit_tariff
              , FIRST_VALUE(UPPER(fbi.outfit_imu))
                            OVER (PARTITION BY UPPER(bp.store_brand), UPPER(bp.store_region), UPPER(st.store_country), UPPER(ban.bundle_alias),bp.bundle_product_id ORDER BY UPPER(fbi.outfit_alias)) AS fk_outfit_imu
              , FIRST_VALUE(UPPER(fbi.outfit_imu_tariff))
                            OVER (PARTITION BY UPPER(bp.store_brand), UPPER(bp.store_region), UPPER(st.store_country), UPPER(ban.bundle_alias),bp.bundle_product_id ORDER BY UPPER(fbi.outfit_alias)) AS fk_outfit_imu_tariff
              , FIRST_VALUE(UPPER(fbi.contains_shoes))
                            OVER (PARTITION BY UPPER(bp.store_brand), UPPER(bp.store_region), UPPER(st.store_country), UPPER(ban.bundle_alias),bp.bundle_product_id ORDER BY UPPER(fbi.outfit_alias)) AS fk_contains_shoes
FROM _bundle_product bp
         JOIN _store st
              ON UPPER(st.store_brand) = UPPER(bp.store_brand)
                  AND st.store_region = bp.store_region
         LEFT JOIN _bundle_alias_name ban
                   ON ban.bundle_product_id = bp.bundle_product_id
         LEFT JOIN _fk_bundle_info fbi
                   ON fbi.bundle_product_id = bp.bundle_product_id;


CREATE OR REPLACE TRANSIENT TABLE reporting_prod.gfb.gfb_dim_bundle AS
SELECT DISTINCT a.*
              , fbi.bundle_product_id
              , fbi.bundle_image_url
              , fbi.is_bundle_active

              , fbi.bundle_name
              , fbi.bundle_alias
              , fbi.product_type
              , fbi.group_code

              , fbi.fk_bundle_alias
              , fbi.fk_bundle_site_name
              , fbi.fk_vip_retail
              , fbi.fk_type
              , fbi.fk_gender
              , fbi.fk_active_inactive
              , fbi.fk_outfit_vs_box_vs_pack
              , fbi.fk_total_cost_of_outfit
              , fbi.fk_total_cost_of_outfit_tariff
              , fbi.fk_outfit_imu
              , fbi.fk_outfit_imu_tariff
              , fbi.fk_contains_shoes
              , fbi.compontents_number AS prepack_components

              , dp.vip_unit_price      AS current_bundle_vip_retail
              , dp.retail_unit_price   AS bundle_msrp
              , cn.compontents_number
FROM reporting_prod.gfb.merch_dim_product a
         JOIN _final_bundle_info fbi
              ON fbi.business_unit = a.business_unit
                  AND fbi.region = a.region
                  AND fbi.country = a.country
                  AND fbi.component_product_id = a.master_product_id
         LEFT JOIN edw_prod.data_model_jfb.dim_product dp
                   ON dp.product_id = fbi.bundle_product_id
         LEFT JOIN
     (
         SELECT a.business_unit
              , a.region
              , a.country
              , a.bundle_product_id
              , COUNT(DISTINCT a.component_product_id) AS compontents_number
         FROM _final_bundle_info a
         GROUP BY a.business_unit
                , a.region
                , a.country
                , a.bundle_product_id
     ) cn ON cn.business_unit = fbi.business_unit
         AND cn.region = fbi.region
         AND cn.country = fbi.country
         AND cn.bundle_product_id = fbi.bundle_product_id
WHERE a.business_unit IN ('FABKIDS')
UNION
SELECT DISTINCT a.*
              , fbi.bundle_product_id
              , fbi.bundle_image_url
              , fbi.is_bundle_active

              , fbi.bundle_name
              , fbi.bundle_alias
              , fbi.product_type
              , fbi.group_code

              , fbi.fk_bundle_alias
              , fbi.fk_bundle_site_name
              , fbi.fk_vip_retail
              , fbi.fk_type
              , fbi.fk_gender
              , fbi.fk_active_inactive
              , fbi.fk_outfit_vs_box_vs_pack
              , fbi.fk_total_cost_of_outfit
              , fbi.fk_total_cost_of_outfit_tariff
              , fbi.fk_outfit_imu
              , fbi.fk_outfit_imu_tariff
              , fbi.fk_contains_shoes
              , fbi.compontents_number AS prepack_components

              , dp.vip_unit_price      AS current_bundle_vip_retail
              , dp.retail_unit_price   AS bundle_msrp
              , cn.compontents_number
FROM reporting_prod.gfb.merch_dim_product a
         JOIN _final_bundle_info fbi
              ON fbi.business_unit = a.business_unit
                  AND fbi.region = a.region
                  AND fbi.country = a.country
                  AND fbi.base_sku = a.product_style_number
         LEFT JOIN edw_prod.data_model_jfb.dim_product dp
                   ON dp.product_id = fbi.bundle_product_id
         LEFT JOIN
     (
         SELECT a.business_unit
              , a.region
              , a.country
              , a.bundle_product_id
              , COUNT(DISTINCT a.component_product_id) AS compontents_number
         FROM _final_bundle_info a
         GROUP BY a.business_unit
                , a.region
                , a.country
                , a.bundle_product_id
     ) cn ON cn.business_unit = fbi.business_unit
         AND cn.region = fbi.region
         AND cn.country = fbi.country
         AND cn.bundle_product_id = fbi.bundle_product_id
WHERE a.business_unit IN ('JUSTFAB', 'SHOEDAZZLE');
