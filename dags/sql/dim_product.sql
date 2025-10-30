truncate table EDW_PROD.NEW_STG.DIM_PRODUCT;
insert into EDW_PROD.NEW_STG.DIM_PRODUCT
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
),
    metafield_fk as (
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
select t1.id as PRODUCT_ID,
       t1.product_id as MASTER_PRODUCT_ID,
       26 as store_id,
       t1.sku,
       t2.skc_value as product_sku,
       SPLIT_PART(t2.skc_value, '-', 1) base_sku,
       t3.title product_name,
       concat(case when t2.skc_value is not null then t2.skc_value else '' end,'(',color_t.name,')') product_alias,
       null as product_type,
       t2.membershipBrand_value as brand,
       t3.vendor manufacturer,
       t3.PUBLISHED_AT current_showroom_date,
       null product_category_id,
       t3.product_type as PRODUCT_TYPE_SOURCE,
       SPLIT_PART(product_type, '>>', ARRAY_SIZE(SPLIT(product_type, '>>'))) AS product_category,
       SPLIT_PART(t3.product_type, '>>',1) as  department,
       SPLIT_PART(t3.product_type, '>>',2) as  category,
       SPLIT_PART(t3.product_type, '>>',3) as  subcategory,
       SPLIT_PART(t3.product_type, '>>',4) as  class,
       color_t.name as color,
       size_t.name as size,
       null is_plus_size,
       null inseam_type,
       null inseam_size,
       t2.material_value material,
       t2.heelHeight_value as heel_height,
       t2.occasion_value as occasion,
       t1.PRICE vip_unit_price ,
       tt2.regPrice_value as retail_unit_price,
       null sale_price,
       null short_description,
       null medium_description,
       t3.description,
       t1.available_for_sale is_active,
       null is_free,
       null image_url,
       null product_status_code,
       null product_status,
       null wms_class,
       t2.groupCode_value group_code,
       null product_id_object_type,
       null master_product_last_update_datetime,
       -- t2.membershipBrand_value as membership_brand_id,
       null as membership_brand_id,
       null is_endowment_eligible,
       null is_warehouse_product,
       null warehouse_unit_price,
       t1.created_at::TIMESTAMP_LTZ(9) meta_create_datetime,
       current_date::TIMESTAMP_LTZ(9) meta_update_datetime
from LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.product_variant t1
left join metafield_jf t2 on t1.product_id = t2.owner_id
left join metafield_jf tt2 on t1.id = tt2.owner_id
left join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.product t3 on t1.product_id = t3.id
-- left join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.product_image t4 on t3.id = t4.product_id
LEFT JOIN edw_prod.new_stg.dim_membership_brand dim
    ON t2.membershipBrand_value= dim.lable
-- 添加color字段逻辑
LEFT JOIN (
    with t as (
        select a.VARIANT_ID, b.*, c.name as name_type
        from LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.PRODUCT_VARIANT_OPTION_VALUE a
        left join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.PRODUCT_OPTION_VALUE b
            on a.option_value_id = b.id
        left join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.PRODUCT_OPTION c
            on b.product_option_id = c.id
    )
    select VARIANT_ID, name
    from t
    where name_type = 'Color'
) color_t on t1.id = color_t.VARIANT_ID
-- 添加size字段逻辑
LEFT JOIN (
    with t as (
        select a.VARIANT_ID, b.*, c.name as name_type
        from LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.PRODUCT_VARIANT_OPTION_VALUE a
        left join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.PRODUCT_OPTION_VALUE b
            on a.option_value_id = b.id
        left join LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.PRODUCT_OPTION c
            on b.product_option_id = c.id
    )
    select VARIANT_ID, name
    from t
    where name_type = 'Size'
) size_t on t1.id = size_t.VARIANT_ID

union all

select t1.id as PRODUCT_ID,
       t1.product_id as MASTER_PRODUCT_ID,
       55 as store_id,
       t1.sku,
       t2.skc_value as product_sku,
       SPLIT_PART(t2.skc_value, '-', 1) base_sku,
       t3.title product_name,
       concat(case when t2.skc_value is not null then t2.skc_value else '' end,'(',color_t.name,')') product_alias,
       null as product_type,
       t2.membershipBrand_value as brand,
       t3.vendor manufacturer,
       t3.PUBLISHED_AT current_showroom_date,
       null product_category_id,
       t3.product_type as PRODUCT_TYPE_SOURCE,
       SPLIT_PART(product_type, '>>', ARRAY_SIZE(SPLIT(product_type, '>>'))) AS product_category,
       SPLIT_PART(t3.product_type, '>>',1) as  department,
       SPLIT_PART(t3.product_type, '>>',2) as  category,
       SPLIT_PART(t3.product_type, '>>',3) as  subcategory,
       SPLIT_PART(t3.product_type, '>>',4) as  class,
       color_t.name as color,
       size_t.name as size,
       null is_plus_size,
       null inseam_type,
       null inseam_size,
       t2.material_value material,
       t2.heelHeight_value as heel_height,
       t2.occasion_value as occasion,
       t1.PRICE vip_unit_price ,
       tt2.regPrice_value as retail_unit_price,
       null sale_price,
       null short_description,
       null medium_description,
       t3.description,
       t1.available_for_sale is_active,
       null is_free,
       null image_url,
       null product_status_code,
       null product_status,
       null wms_class,
       t2.groupCode_value group_code,
       null product_id_object_type,
       null master_product_last_update_datetime,
       -- t2.membershipBrand_value as membership_brand_id,
       null as membership_brand_id,
       null is_endowment_eligible,
       null is_warehouse_product,
       null warehouse_unit_price,
       t1.created_at::TIMESTAMP_LTZ(9) meta_create_datetime,
       current_date::TIMESTAMP_LTZ(9) meta_update_datetime
from LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.product_variant t1
left join metafield t2 on t1.product_id = t2.owner_id
left join metafield tt2 on t1.id = tt2.owner_id
left join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.product t3 on t1.product_id = t3.id
-- left join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.product_image t4 on t3.id = t4.product_id
LEFT JOIN edw_prod.new_stg.dim_membership_brand dim
    ON t2.membershipBrand_value= dim.lable
-- 添加color字段逻辑
LEFT JOIN (
    with t as (
        select a.VARIANT_ID, b.*, c.name as name_type
        from LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.PRODUCT_VARIANT_OPTION_VALUE a
        left join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.PRODUCT_OPTION_VALUE b
            on a.option_value_id = b.id
        left join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.PRODUCT_OPTION c
            on b.product_option_id = c.id
    )
    select VARIANT_ID, name
    from t
    where name_type = 'Color'
) color_t on t1.id = color_t.VARIANT_ID
-- 添加size字段逻辑
LEFT JOIN (
    with t as (
        select a.VARIANT_ID, b.*, c.name as name_type
        from LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.PRODUCT_VARIANT_OPTION_VALUE a
        left join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.PRODUCT_OPTION_VALUE b
            on a.option_value_id = b.id
        left join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.PRODUCT_OPTION c
            on b.product_option_id = c.id
    )
    select VARIANT_ID, name
    from t
    where name_type = 'Size'
) size_t on t1.id = size_t.VARIANT_ID


union all

select t1.id as PRODUCT_ID,
       t1.product_id as MASTER_PRODUCT_ID,
       46 as store_id,
       t1.sku,
       t2.skc_value as product_sku,
       SPLIT_PART(t2.skc_value, '-', 1) base_sku,
       t3.title product_name,
       concat(case when t2.skc_value is not null then t2.skc_value else '' end,'(',color_t.name,')') product_alias,
       null as product_type,
       t2.membershipBrand_value as brand,
       t3.vendor manufacturer,
       t3.PUBLISHED_AT current_showroom_date,
       null product_category_id,
       t3.product_type as PRODUCT_TYPE_SOURCE,
       SPLIT_PART(product_type, '>>', ARRAY_SIZE(SPLIT(product_type, '>>'))) AS product_category,
       SPLIT_PART(t3.product_type, '>>',1) as  department,
       SPLIT_PART(t3.product_type, '>>',2) as  category,
       SPLIT_PART(t3.product_type, '>>',3) as  subcategory,
       SPLIT_PART(t3.product_type, '>>',4) as  class,
       color_t.name as color,
       size_t.name as size,
       null is_plus_size,
       null inseam_type,
       null inseam_size,
       t2.material_value material,
       t2.heelHeight_value as heel_height,
       t2.occasion_value as occasion,
       t1.PRICE vip_unit_price ,
       tt2.regPrice_value as retail_unit_price,
       null sale_price,
       null short_description,
       null medium_description,
       t3.description,
       t1.available_for_sale is_active,
       null is_free,
       null image_url,
       null product_status_code,
       null product_status,
       null wms_class,
       t2.groupCode_value group_code,
       null product_id_object_type,
       null master_product_last_update_datetime,
       -- t2.membershipBrand_value as membership_brand_id,
       null as membership_brand_id,
       null is_endowment_eligible,
       null is_warehouse_product,
       null warehouse_unit_price,
       t1.created_at::TIMESTAMP_LTZ(9) meta_create_datetime,
       current_date::TIMESTAMP_LTZ(9) meta_update_datetime
from LAKE_MMOS.SHOPIFY_FABKIDS_PROD.product_variant t1
left join metafield_fk t2 on t1.product_id = t2.owner_id
left join metafield_fk tt2 on t1.id = tt2.owner_id
left join LAKE_MMOS.SHOPIFY_FABKIDS_PROD.product t3 on t1.product_id = t3.id
-- left join LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.product_image t4 on t3.id = t4.product_id
LEFT JOIN edw_prod.new_stg.dim_membership_brand dim
    ON t2.membershipBrand_value= dim.lable
-- 添加color字段逻辑
LEFT JOIN (
    with t as (
        select a.VARIANT_ID, b.*, c.name as name_type
        from LAKE_MMOS.SHOPIFY_FABKIDS_PROD.PRODUCT_VARIANT_OPTION_VALUE a
        left join LAKE_MMOS.SHOPIFY_FABKIDS_PROD.PRODUCT_OPTION_VALUE b
            on a.option_value_id = b.id
        left join LAKE_MMOS.SHOPIFY_FABKIDS_PROD.PRODUCT_OPTION c
            on b.product_option_id = c.id
    )
    select VARIANT_ID, name
    from t
    where name_type = 'Color'
) color_t on t1.id = color_t.VARIANT_ID
-- 添加size字段逻辑
LEFT JOIN (
    with t as (
        select a.VARIANT_ID, b.*, c.name as name_type
        from LAKE_MMOS.SHOPIFY_FABKIDS_PROD.PRODUCT_VARIANT_OPTION_VALUE a
        left join LAKE_MMOS.SHOPIFY_FABKIDS_PROD.PRODUCT_OPTION_VALUE b
            on a.option_value_id = b.id
        left join LAKE_MMOS.SHOPIFY_FABKIDS_PROD.PRODUCT_OPTION c
            on b.product_option_id = c.id
    )
    select VARIANT_ID, name
    from t
    where name_type = 'Size'
) size_t on t1.id = size_t.VARIANT_ID