
-- dim_item_price
truncate table EDW_PROD.NEW_STG.DIM_ITEM_PRICE;
insert into EDW_PROD.NEW_STG.DIM_ITEM_PRICE
with metafield_jf as (
SELECT
    t2.owner_id,
    MAX(CASE WHEN t2.key = 'skc' THEN t2.value END) AS skc_value,
    MAX(CASE WHEN t2.key = 'membershipBrand' THEN t2.value END) AS membershipBrand_value,
    MAX(CASE WHEN t2.key = 'material' THEN t2.value END) AS material_value,
    MAX(CASE WHEN t2.key = 'heelHeight' THEN t2.value END) AS heelHeight_value,
    MAX(CASE WHEN t2.key = 'occasion' THEN t2.value END) AS occasion_value,
    MAX(CASE WHEN t2.key = 'regPrice' THEN REPLACE(parse_json(t2.value):"amount",'"','')::NUMBER(18,2) END) AS regPrice_value,
    MAX(CASE WHEN t2.key = 'groupCode' THEN t2.value END) AS groupCode_value
FROM
    LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.metafield t2
WHERE
    t2.key IN ('skc', 'membershipBrand', 'material', 'heelHeight', 'occasion', 'regPrice', 'groupCode')
GROUP BY
    t2.owner_id
)
,metafield as (
SELECT
    t2.owner_id,
    MAX(CASE WHEN t2.key = 'skc' THEN t2.value END) AS skc_value,
    MAX(CASE WHEN t2.key = 'membershipBrand' THEN t2.value END) AS membershipBrand_value,
    MAX(CASE WHEN t2.key = 'material' THEN t2.value END) AS material_value,
    MAX(CASE WHEN t2.key = 'heelHeight' THEN t2.value END) AS heelHeight_value,
    MAX(CASE WHEN t2.key = 'occasion' THEN t2.value END) AS occasion_value,
    MAX(CASE WHEN t2.key = 'regPrice' THEN REPLACE(parse_json(t2.value):"amount",'"','')::NUMBER(18,2) END) AS regPrice_value,
    MAX(CASE WHEN t2.key = 'groupCode' THEN t2.value END) AS groupCode_value
FROM
    LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.metafield t2
WHERE
    t2.key IN ('skc', 'membershipBrand', 'material', 'heelHeight', 'occasion', 'regPrice', 'groupCode')
GROUP BY
    t2.owner_id
)

-- 新增：针对 mmos_membership_marketing_fabkids 的元数据处理
,metafield_fabkids as (
SELECT
    t2.owner_id,
    MAX(CASE WHEN t2.key = 'skc' THEN t2.value END) AS skc_value,
    MAX(CASE WHEN t2.key = 'membershipBrand' THEN t2.value END) AS membershipBrand_value,
    MAX(CASE WHEN t2.key = 'material' THEN t2.value END) AS material_value,
    MAX(CASE WHEN t2.key = 'heelHeight' THEN t2.value END) AS heelHeight_value,
    MAX(CASE WHEN t2.key = 'occasion' THEN t2.value END) AS occasion_value,
    MAX(CASE WHEN t2.key = 'regPrice' THEN REPLACE(parse_json(t2.value):"amount",'"','')::NUMBER(18,2) END) AS regPrice_value,
    MAX(CASE WHEN t2.key = 'groupCode' THEN t2.value END) AS groupCode_value
FROM
    LAKE_MMOS.SHOPIFY_FABKIDS_PROD.metafield t2
WHERE
    t2.key IN ('skc', 'membershipBrand', 'material', 'heelHeight', 'occasion', 'regPrice', 'groupCode')
GROUP BY
    t2.owner_id
)
SELECT
     null item_price_key
    ,t1.id product_id
    ,t1.inventory_item_id AS item_id
    ,t1.product_id as master_product_id
    ,55 AS store_id
    ,t1.sku
    ,t2.skc_value AS product_sku
    ,SPLIT_PART(t2.skc_value, '-', 1) AS base_sku
    ,t3.title AS product_name
    ,t1.PRICE vip_unit_price
    ,tt2.regPrice_value AS retail_unit_price
    ,dim.membership_brand_id AS brand_label
    ,t1.available_for_sale as is_active
    ,t1.created_at::TIMESTAMP_LTZ(9) meta_create_datetime
    ,current_date meta_update_datetime
FROM LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.product_variant t1
LEFT JOIN metafield t2 ON t1.product_id = t2.owner_id
LEFT JOIN metafield tt2 ON t1.id = tt2.owner_id
LEFT JOIN LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.product t3 ON t1.product_id = t3.id
LEFT JOIN edw_prod.new_stg.dim_membership_brand dim
    ON t2.membershipBrand_value= dim.lable

union all

SELECT
     null item_price_key
    ,t1.id product_id
    ,t1.inventory_item_id AS item_id
    ,t1.product_id as master_product_id
    ,26 AS store_id
    ,t1.sku
    ,t2.skc_value AS product_sku
    ,SPLIT_PART(t2.skc_value, '-', 1) AS base_sku
    ,t3.title AS product_name
    ,t1.PRICE vip_unit_price
    ,tt2.regPrice_value AS retail_unit_price
    ,dim.membership_brand_id AS brand_label
    ,t1.available_for_sale as is_active
    ,t1.created_at::TIMESTAMP_LTZ(9) meta_create_datetime
    ,current_date meta_update_datetime
FROM LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.product_variant t1
LEFT JOIN metafield_jf t2 ON t1.product_id = t2.owner_id
LEFT JOIN metafield_jf tt2 ON t1.id = tt2.owner_id
LEFT JOIN LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.product t3 ON t1.product_id = t3.id
LEFT JOIN edw_prod.new_stg.dim_membership_brand dim
    ON t2.membershipBrand_value= dim.lable

union all

SELECT
     null item_price_key
    ,t1.id product_id
    ,t1.inventory_item_id AS item_id
    ,t1.product_id as master_product_id
    ,46 AS store_id  -- 请替换为实际的 store_id
    ,t1.sku
    ,t2.skc_value AS product_sku
    ,SPLIT_PART(t2.skc_value, '-', 1) AS base_sku
    ,t3.title AS product_name
    ,t1.PRICE vip_unit_price
    ,tt2.regPrice_value AS retail_unit_price
    ,dim.membership_brand_id AS brand_label
    ,t1.available_for_sale as is_active
    ,t1.created_at::TIMESTAMP_LTZ(9) meta_create_datetime
    ,current_date meta_update_datetime
FROM LAKE_MMOS.SHOPIFY_FABKIDS_PROD.product_variant t1
LEFT JOIN metafield_fabkids t2 ON t1.product_id = t2.owner_id
LEFT JOIN metafield_fabkids tt2 ON t1.id = tt2.owner_id
LEFT JOIN LAKE_MMOS.SHOPIFY_FABKIDS_PROD.product t3 ON t1.product_id = t3.id
LEFT JOIN edw_prod.new_stg.dim_membership_brand dim
    ON t2.membershipBrand_value= dim.lable
;





