SET execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMPORARY TABLE _order_line AS (
SELECT DISTINCT ol.order_id,
  ol.order_line_id,
  DATE(ol.order_hq_date) order_hq_date,
  ol.order_store_full_name,
  ol.order_store_type,
  ol.store_country,
  ol.store_region_abbr,
  ol.is_ecomm_detailed AS order_classification,
  ol.bundle_group_key,
  ol.set_box_bundle_name,
  ol.sku,
  ol.product_sku,
  ol.item_quantity,
  sms.site_name,
  sms.savage_showroom,
  sms.category,
  sms.subcategory,
  sms.category_type,
  sms.sub_department,
  sms.collection,
  sms.site_color,
  sms.color_family,
  ol.membership_type_time_of_order,
  ol.num_credits_redeemed_on_order,
  ol.token_count,
  IFF(UPPER(sms.sub_department) LIKE 'WOMEN''S LOUNGEWEAR',1,0) AS is_womens_loungewear_subdepartment,
  IFF(UPPER(sms.sub_department) LIKE 'WOMEN''S INTIMATES',1,0) AS is_womens_intimates_subdepartment,
  IFF(UPPER(sms.sub_department) LIKE 'WOMEN''S SPORTSWEAR',1,0) AS is_womens_sportswear_subdepartment,
  IFF(UPPER(sms.sub_department) LIKE 'MEN''S INTIMATES',1,0) AS is_mens_intimates_subdepartment,
  IFF(UPPER(sms.sub_department) LIKE 'MEN''S SPORTSWEAR',1,0) AS is_mens_sportswear_subdepartment,
  IFF(UPPER(sms.sub_department) LIKE 'MEN''S LOUNGEWEAR',1,0) AS is_mens_loungewear_subdepartment,
  IFF(UPPER(sms.sub_department) IS NULL,1,0) AS is_null_subdepartment,
  IFF(LOWER(sms.core_fashion)='core basic',1,0) AS is_core_basic,
  IFF(LOWER(sms.core_fashion)='core fashion',1,0) AS is_core_fashion,
  IFF(LOWER(sms.core_fashion)='fashion',1,0) AS is_fashion,
  IFF(LOWER(sms.category)='bra',1,0) AS is_bra,
  IFF(LOWER(sms.category)='bralette',1,0) AS is_bralette,
  IFF(LOWER(sms.category)='undie',1,0) AS is_undie,
  IFF(LOWER(sms.category)='lingerie',1,0) AS is_lingerie,
  IFF(LOWER(sms.category)='sleepwear',1,0) AS is_sleepwear,
  IFF(LOWER(sms.category)='accessories',1,0) AS is_accessories,
  IFF(LOWER(sms.category)='collectibles',1,0) AS is_collectibles,
  IFF(LOWER(sms.site_name) LIKE '%onesie%',1,0) AS is_onesie,
  IFF(LOWER(sms.category_type)='top',1,0) AS is_top,
  IFF(LOWER(sms.category_type)='bottom',1,0) AS is_bottom,
  IFF(LOWER(sms.category_type)='all over',1,0) AS is_allover,
  IFF(LOWER(sms.category_type)='n/a',1,0) AS is_not_applicable_category_type,
  IFF(LOWER(sms.category_type)='new - merch, add to top/bottom Key',1,0) AS is_unassigned_category_type,
  IFF(LOWER(sms.gender)='women',1,0) AS is_women,
  IFF(LOWER(sms.gender)='men',1,0) AS is_men,
  IFF(LOWER(sms.gender)='unisex',1,0) AS is_unisex,
  ol.is_byo_set,
  ol.is_bundle,
  ol.is_vip_box,
  ol.is_pre_made_set,
  ol.is_individual_item,
  sms.size_range,
  ol.is_marked_down
FROM REPORTING_PROD.SXF.VIEW_ORDER_LINE_RECOGNIZED_DATASET ol
JOIN REPORTING_PROD.SXF.VIEW_STYLE_MASTER_SIZE sms ON sms.sku = ol.sku
);

CREATE OR REPLACE TEMPORARY TABLE _order_level AS (
SELECT order_id,
  SUM(item_quantity) AS upt_order_level,
  SUM(is_top) AS tops_order_level,
  SUM(is_bottom) AS bottoms_order_level,
  COUNT(DISTINCT category_type) AS category_types_order_level
FROM _order_line
GROUP BY order_id
);

CREATE OR REPLACE TEMPORARY TABLE _bundle_level AS (
SELECT order_id,
  bundle_group_key,
  SUM(item_quantity) AS upt_bundle_level,
  SUM(is_top) AS tops_bundle_level,
  SUM(is_bottom) AS bottoms_bundle_level,
  COUNT(DISTINCT category_type) AS category_types_bundle_level
FROM _order_line
GROUP BY order_id,
  bundle_group_key
);

CREATE OR REPLACE TEMPORARY TABLE _sd_level AS (
SELECT order_id,
  sub_department,
  SUM(item_quantity) AS upt_subdepartment_level,
  SUM(is_top) AS tops_subdepartment_level,
  SUM(is_bottom) AS bottoms_subdepartment_level,
  COUNT(DISTINCT category_type) AS category_types_subdepartment_level
FROM _order_line
GROUP BY order_id,
  sub_department
);

CREATE OR REPLACE TEMPORARY TABLE _site_color_collection_match_units_sub_d AS (
SELECT order_id,
  sub_department,
  collection,
  site_color,
  SUM(is_top) AS top,
  SUM(is_bottom) AS bottom,
  COUNT(DISTINCT category_type) AS category_type_count
FROM _order_line
WHERE category NOT IN ('COLLECTIBLES')
GROUP BY order_id,
  collection,
  order_hq_date,
  order_classification,
  site_color,
  sub_department
);

CREATE OR REPLACE TEMPORARY TABLE _collection_match_units_sub_d AS (
SELECT order_id,
    sub_department,
    collection,
    SUM(is_top) AS top,
    SUM(is_bottom) AS bottom,
    COUNT(DISTINCT category_type) AS category_type_count
FROM _order_line
WHERE category NOT IN ('COLLECTIBLES')
GROUP BY order_id,
  collection,
  order_hq_date,
  order_classification,
  sub_department
);

CREATE OR REPLACE TEMPORARY TABLE _color_family_match_units_sub_d AS (
SELECT order_id,
  sub_department,
  color_family,
  SUM(is_top) AS top,
  SUM(is_bottom) AS bottom,
  COUNT(DISTINCT category_type) AS category_type_count
FROM _order_line
WHERE category NOT IN ('COLLECTIBLES')
GROUP BY order_id,
  color_family,
  order_hq_date,
  order_classification,
  sub_department
);

CREATE OR REPLACE TEMPORARY TABLE _site_color_collection_match_units_bg AS (
SELECT order_id,
  bundle_group_key,
  collection,
  site_color,
  SUM(is_top) AS top,
  SUM(is_bottom) AS bottom,
  COUNT(DISTINCT category_type) AS category_type_count
FROM _order_line
WHERE category NOT IN ('COLLECTIBLES')
  AND (is_byo_set =1 OR is_pre_made_set =1 OR is_vip_box =1)
GROUP BY order_id,
  collection,
  order_hq_date,
  order_classification,
  site_color,
  bundle_group_key
);

CREATE OR REPLACE TEMPORARY TABLE _collection_match_units_bg AS (
SELECT order_id,
  bundle_group_key,
  collection,
  SUM(is_top) AS top,
  SUM(is_bottom) AS bottom,
  COUNT(DISTINCT category_type) AS category_type_count
FROM _order_line
WHERE category NOT IN ('COLLECTIBLES')
  AND (is_byo_set = 1 OR is_pre_made_set = 1 OR is_vip_box = 1)
GROUP BY order_id,
  collection,
  order_hq_date,
  order_classification,
  bundle_group_key
);

CREATE OR REPLACE TEMPORARY TABLE _color_family_match_units_bg AS (
SELECT order_id,
  bundle_group_key,
  color_family,
  SUM(is_top) AS top,
  SUM(is_bottom) AS bottom,
  COUNT(DISTINCT category_type) AS category_type_count
FROM _order_line
WHERE category NOT IN ('COLLECTIBLES')
  AND (is_byo_set = 1 OR is_pre_made_set = 1 OR is_vip_box = 1)
GROUP BY order_id,
  color_family,
  order_hq_date,
  order_classification,
  bundle_group_key
);

CREATE OR REPLACE TEMPORARY TABLE _site_color_collection_match_units AS (
SELECT order_id,
  collection,
  site_color,
  SUM(is_top) AS top,
  SUM(is_bottom) AS bottom,
  COUNT(DISTINCT category_type) AS category_type_count
FROM _order_line
WHERE category NOT IN ('COLLECTIBLES')
GROUP BY order_id,
  collection,
  order_hq_date,
  order_classification,
  site_color
);

CREATE OR REPLACE TEMPORARY TABLE _collection_match_units AS (
SELECT order_id,
  collection,
  SUM(is_top) AS top,
  SUM(is_bottom) AS bottom,
  COUNT(DISTINCT category_type) AS category_type_count
FROM _order_line
WHERE category NOT IN ('COLLECTIBLES')
GROUP BY order_id,
  collection,
  order_hq_date,
  order_classification
);

CREATE OR REPLACE TEMPORARY TABLE _color_family_match_units AS (
SELECT order_id,
  color_family,
  SUM(is_top) AS top,
  SUM(is_bottom) AS bottom,
  COUNT(DISTINCT category_type) AS category_type_count
FROM _order_line
WHERE category NOT IN ('COLLECTIBLES')
GROUP BY order_id,
  color_family,
  order_hq_date,
  order_classification
);

TRUNCATE TABLE REPORTING_PROD.SXF.ORDER_LINE_MATCHINESS;

INSERT INTO REPORTING_PROD.SXF.ORDER_LINE_MATCHINESS
(
    order_id
    ,order_line_id
    ,order_hq_date
    ,order_store_full_name
    ,order_store_type
    ,store_country
    ,store_region_abbr
    ,order_classification
    ,bundle_group_key
    ,set_box_bundle_name
    ,sku
    ,product_sku
    ,item_quantity
    ,site_name
    ,savage_showroom
    ,category
    ,subcategory
    ,category_type
    ,sub_department
    ,collection
    ,site_color
    ,color_family
    ,membership_type_time_of_order
    ,num_credits_redeemed_on_order
    ,token_count
    ,is_womens_loungewear_subdepartment
    ,is_womens_intimates_subdepartment
    ,is_womens_sportswear_subdepartment
    ,is_mens_intimates_subdepartment
    ,is_mens_sportswear_subdepartment
    ,is_mens_loungewear_subdepartment
    ,is_null_subdepartment
    ,is_core_basic
    ,is_core_fashion
    ,is_fashion
    ,is_bra
    ,is_bralette
    ,is_undie
    ,is_lingerie
    ,is_sleepwear
    ,is_accessories
    ,is_collectibles
    ,is_onesie
    ,is_top
    ,is_bottom
    ,is_allover
    ,is_not_applicable_category_type
    ,is_unassigned_category_type
    ,is_women
    ,is_men
    ,is_unisex
    ,is_byo_set
    ,is_bundle
    ,is_vip_box
    ,is_pre_made_set
    ,is_individual_item
    ,size_range
    ,is_marked_down
    ,upt_order_level
    ,tops_order_leveL
    ,bottoms_order_level
    ,category_types_order_level
    ,upt_bundle_level
    ,tops_bundle_level
    ,bottoms_bundle_level
    ,category_types_bundle_level
    ,upt_subdepartment_level
    ,tops_subdepartment_level
    ,bottoms_subdepartment_level
    ,category_types_subdepartment_level
    ,filter_categories
    ,t4_wi_units_matched_gen
    ,t3_wi_units_matched_gen
    ,t1_wi_units_matched_gen
    ,t4_mi_units_matched_gen
    ,t3_mi_units_matched_gen
    ,t1_mi_units_matched_gen
    ,t4_wl_units_matched_gen
    ,t3_wl_units_matched_gen
    ,t1_wl_units_matched_gen
    ,t4_ws_units_matched_gen
    ,t3_ws_units_matched_gen
    ,t1_ws_units_matched_gen
    ,t4_bg_units_matched_gen
    ,t3_bg_units_matched_gen
    ,t1_bg_units_matched_gen
    ,t4_ttl_units_matched_gen
    ,t3_ttl_units_matched_gen
    ,t1_ttl_units_matched_gen
    ,t4_wi_units_matched_topbtm
    ,t3_wi_units_matched_topbtm
    ,t1_wi_units_matched_topbtm
    ,t4_mi_units_matched_topbtm
    ,t3_mi_units_matched_topbtm
    ,t1_mi_units_matched_topbtm
    ,t4_wl_units_matched_topbtm
    ,t3_wl_units_matched_topbtm
    ,t1_wl_units_matched_topbtm
    ,t4_ws_units_matched_topbtm
    ,t3_ws_units_matched_topbtm
    ,t1_ws_units_matched_topbtm
    ,t4_bg_units_matched_topbtm
    ,t3_bg_units_matched_topbtm
    ,t1_bg_units_matched_topbtm
    ,t4_ttl_units_matched_topbtm
    ,t3_ttl_units_matched_topbtm
    ,t1_ttl_units_matched_topbtm
    ,meta_create_datetime
    ,meta_update_datetime
)
SELECT ba.*
  ,o.UPT_order_level
  ,o.tops_order_level
  ,o.bottoms_order_level
  ,o.category_types_order_level
  ,bg.upt_bundle_level
  ,bg.tops_bundle_level
  ,bg.bottoms_bundle_level
  ,bg.category_types_bundle_level
  ,sd.upt_subdepartment_level
  ,sd.tops_subdepartment_level
  ,sd.bottoms_subdepartment_level
  ,sd.category_types_subdepartment_level
  ,iff(category NOT IN ('COLLECTIBLES'),True,False) AS filter_categories
    ------General Bundles
  ,iff(t4m_sd.order_id IS NOT NULL AND t4m_sd.category_type_count > 1 AND filter_categories AND ba.is_womens_intimates_subdepartment,1,0) AS t4_wi_units_matched_gen
  ,iff(t3m_sd.order_id IS NOT NULL AND t3m_sd.category_type_count > 1 AND filter_categories AND ba.is_womens_intimates_subdepartment,1,0) AS t3_wi_units_matched_gen
  ,iff(t1m_sd.order_id IS NOT NULL AND t1m_sd.category_type_count > 1 AND filter_categories AND ba.is_womens_intimates_subdepartment,1,0) AS t1_wi_units_matched_gen
  ,iff(t4m_sd.order_id IS NOT NULL AND t4m_sd.category_type_count > 1 AND filter_categories AND ba.is_mens_intimates_subdepartment,1,0) AS t4_mi_units_matched_gen
  ,iff(t3m_sd.order_id IS NOT NULL AND t3m_sd.category_type_count > 1 AND filter_categories AND ba.is_mens_intimates_subdepartment,1,0) AS t3_mi_units_matched_gen
  ,iff(t1m_sd.order_id IS NOT NULL AND t1m_sd.category_type_count > 1 AND filter_categories AND ba.is_mens_intimates_subdepartment,1,0) AS t1_mi_units_matched_gen
  ,iff(t4m_sd.order_id IS NOT NULL AND t4m_sd.category_type_count > 1 AND filter_categories AND ba.is_womens_loungewear_subdepartment,1,0) AS t4_wl_units_matched_gen
  ,iff(t3m_sd.order_id IS NOT NULL AND t3m_sd.category_type_count > 1 AND filter_categories AND ba.is_womens_loungewear_subdepartment,1,0) AS t3_wl_units_matched_gen
  ,iff(t1m_sd.order_id IS NOT NULL AND t1m_sd.category_type_count > 1 AND filter_categories AND ba.is_womens_loungewear_subdepartment,1,0) AS t1_wl_units_matched_gen
  ,iff(t4m_sd.order_id IS NOT NULL AND t4m_sd.category_type_count > 1 AND filter_categories AND ba.is_womens_sportswear_subdepartment,1,0) AS t4_ws_units_matched_gen
  ,iff(t3m_sd.order_id IS NOT NULL AND t3m_sd.category_type_count > 1 AND filter_categories AND ba.is_womens_sportswear_subdepartment,1,0) AS t3_ws_units_matched_gen
  ,iff(t1m_sd.order_id IS NOT NULL AND t1m_sd.category_type_count > 1 AND filter_categories AND ba.is_womens_sportswear_subdepartment,1,0) AS t1_ws_units_matched_gen
  ,iff(t4m_bg.order_id IS NOT NULL AND t4m_bg.category_type_count > 1 AND filter_categories,1,0) AS t4_bg_units_matched_gen
  ,iff(t3m_bg.order_id IS NOT NULL AND t3m_bg.category_type_count > 1 AND filter_categories,1,0) AS t3_bg_units_matched_gen
  ,iff(t1m_bg.order_id IS NOT NULL AND t1m_bg.category_type_count > 1 AND filter_categories,1,0) AS t1_bg_units_matched_gen
  ,iff(t4m.order_id IS NOT NULL AND t4m.category_type_count > 1 AND filter_categories,1,0) AS t4_ttl_units_matched_gen
  ,iff(t3m.order_id IS NOT NULL AND t3m.category_type_count > 1 AND filter_categories,1,0) AS t3_ttl_units_matched_gen
  ,iff(t1m.order_id IS NOT NULL AND t1m.category_type_count > 1 AND filter_categories,1,0) AS t1_ttl_units_matched_gen
  ----------------Top and Bottom Bundles
  ,iff(t4m_sd.order_id IS NOT NULL AND t4m_sd.top >= 1 AND t4m_sd.bottom >= 1 AND filter_categories AND ba.is_womens_intimates_subdepartment,1,0) AS t4_wi_units_matched_topbtm
  ,iff(t3m_sd.order_id IS NOT NULL AND t3m_sd.top >= 1 AND t3m_sd.bottom >= 1 AND filter_categories AND ba.is_womens_intimates_subdepartment,1,0) AS t3_wi_units_matched_topbtm
  ,iff(t1m_sd.order_id IS NOT NULL AND t1m_sd.top >= 1 AND t1m_sd.bottom >= 1 AND filter_categories AND ba.is_womens_intimates_subdepartment,1,0) AS t1_wi_units_matched_topbtm
  ,iff(t4m_sd.order_id IS NOT NULL AND t4m_sd.top >= 1 AND t4m_sd.bottom >= 1 AND filter_categories AND ba.is_mens_intimates_subdepartment,1,0) AS t4_mi_units_matched_topbtm
  ,iff(t3m_sd.order_id IS NOT NULL AND t3m_sd.top >= 1 AND t3m_sd.bottom >= 1 AND filter_categories AND ba.is_mens_intimates_subdepartment,1,0) AS t3_mi_units_matched_topbtm
  ,iff(t1m_sd.order_id IS NOT NULL AND t1m_sd.top >= 1 AND t1m_sd.bottom >= 1 AND filter_categories AND ba.is_mens_intimates_subdepartment,1,0) AS t1_mi_units_matched_topbtm
  ,iff(t4m_sd.order_id IS NOT NULL AND t4m_sd.top >= 1 AND t4m_sd.bottom >= 1 AND filter_categories AND ba.is_womens_loungewear_subdepartment,1,0) AS t4_wl_units_matched_topbtm
  ,iff(t3m_sd.order_id IS NOT NULL AND t3m_sd.top >= 1 AND t3m_sd.bottom >= 1 AND filter_categories AND ba.is_womens_loungewear_subdepartment,1,0) AS t3_wl_units_matched_topbtm
  ,iff(t1m_sd.order_id IS NOT NULL AND t1m_sd.top >= 1 AND t1m_sd.bottom >= 1 AND filter_categories AND ba.is_womens_loungewear_subdepartment,1,0) AS t1_wl_units_matched_topbtm
  ,iff(t4m_sd.order_id IS NOT NULL AND t4m_sd.top >= 1 AND t4m_sd.bottom >= 1 AND filter_categories AND ba.is_womens_sportswear_subdepartment,1,0) AS t4_ws_units_matched_topbtm
  ,iff(t3m_sd.order_id IS NOT NULL AND t3m_sd.top >= 1 AND t3m_sd.bottom >= 1 AND filter_categories AND ba.is_womens_sportswear_subdepartment,1,0) AS t3_ws_units_matched_topbtm
  ,iff(t1m_sd.order_id IS NOT NULL AND t1m_sd.top >= 1 AND t1m_sd.bottom >= 1 AND filter_categories and ba.is_womens_sportswear_subdepartment,1,0) AS t1_ws_units_matched_topbtm
  ,iff(t4m_bg.order_id IS NOT NULL AND t4m_bg.top >= 1 AND t4m_bg.bottom >= 1 AND filter_categories,1,0) AS t4_bg_units_matched_topbtm
  ,iff(t3m_bg.order_id IS NOT NULL AND t3m_bg.top >= 1 AND t3m_bg.bottom >= 1 AND filter_categories,1,0) AS t3_bg_units_matched_topbtm
  ,iff(t1m_bg.order_id IS NOT NULL AND t1m_bg.top >= 1 AND t1m_bg.bottom >= 1 AND filter_categories,1,0) AS t1_bg_units_matched_topbtm
  ,iff(t4m.order_id IS NOT NULL AND t4m.top >= 1 AND t4m.bottom >= 1 AND filter_categories,1,0) AS t4_ttl_units_matched_topbtm
  ,iff(t3m.order_id IS NOT NULL AND t3m.top >= 1 AND t3m.bottom >= 1 AND filter_categories,1,0) AS t3_ttl_units_matched_topbtm
  ,iff(t1m.order_id IS NOT NULL AND t1m.top >= 1 AND t1m.bottom >= 1 AND filter_categories,1,0) AS t1_ttl_units_matched_topbtm
  ,$execution_start_time AS meta_update_time
  ,$execution_start_time AS meta_create_time
FROM _order_line ba
LEFT JOIN _order_level o ON o.order_id = ba.order_id
LEFT JOIN _bundle_level bg ON bg.order_id = ba.order_id
  AND bg.bundle_group_key = ba.bundle_group_key
LEFT JOIN _sd_level sd ON sd.order_id = ba.order_id
  AND sd.sub_department = ba.sub_department
LEFT JOIN _site_color_collection_match_units_sub_d t4m_sd ON ba.order_id = t4m_sd.order_id
  AND ba.sub_department = t4m_sd.sub_department
  AND ba.site_color = t4m_sd.site_color
  AND ba.collection = t4m_sd.collection
LEFT JOIN _collection_match_units_sub_d t3m_sd ON ba.order_id = t3m_sd.order_id
  AND ba.sub_department = t3m_sd.sub_department
  AND ba.collection = t3m_sd.collection
LEFT JOIN _color_family_match_units_sub_d t1m_sd ON ba.order_id = t1m_sd.order_id
  AND ba.sub_department = t1m_sd.sub_department
  AND ba.color_family = t1m_sd.color_family
LEFT JOIN _site_color_collection_match_units_bg t4m_bg ON ba.order_id = t4m_bg.order_id
  AND ba.bundle_group_key = t4m_bg.bundle_group_key
  AND ba.site_color = t4m_bg.site_color
  AND ba.collection = t4m_bg.collection
LEFT JOIN _collection_match_units_bg t3m_bg ON ba.order_id = t3m_bg.order_id
  AND ba.bundle_group_key = t3m_bg.bundle_group_key
  AND ba.collection = t3m_bg.collection
LEFT JOIN _color_family_match_units_bg t1m_bg ON ba.order_id = t1m_bg.order_id
  AND ba.bundle_group_key = t1m_bg.bundle_group_key
  AND ba.color_family = t1m_bg.color_family
LEFT JOIN _site_color_collection_match_units t4m ON ba.order_id = t4m.order_id
  AND ba.site_color = t4m.site_color
  AND ba.collection = t4m.collection
LEFT JOIN _collection_match_units t3m ON ba.order_id = t3m.order_id
  AND ba.collection = t3m.collection
LEFT JOIN _color_family_match_units t1m ON ba.order_id = t1m.order_id
  AND ba.color_family = t1m.color_family;
