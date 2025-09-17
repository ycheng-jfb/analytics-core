create or replace temporary table _product_categories_feed as
select distinct
    upper(st.STORE_BRAND) as business_unit
    ,upper(st.STORE_REGION) as region
    ,pc.PRODUCT_CATEGORY_ID
    ,pc.ACTIVE
    ,pc.ARCHIVE
    ,pc.PRODUCT_CATEGORY_TYPE
    ,ppc_2.LABEL as parent_parent_product_cateogry
    ,ppc_1.LABEL as parent_product_cateogry
    ,pc.LABEL as product_category
    ,dp.PRODUCT_ID as MASTER_PRODUCT_ID
from LAKE_JFB_VIEW.ULTRA_MERCHANT.PRODUCT_PRODUCT_CATEGORY a
join LAKE_JFB_VIEW.ULTRA_MERCHANT.PRODUCT_CATEGORY pc
    on pc.PRODUCT_CATEGORY_ID = a.PRODUCT_CATEGORY_ID
join EDW_PROD.DATA_MODEL_JFB.DIM_PRODUCT dp
    on dp.PRODUCT_ID = a.PRODUCT_ID
left join LAKE_JFB_VIEW.ULTRA_MERCHANT.PRODUCT_CATEGORY ppc_1
    on ppc_1.PRODUCT_CATEGORY_ID = pc.PARENT_PRODUCT_CATEGORY_ID
left join LAKE_JFB_VIEW.ULTRA_MERCHANT.PRODUCT_CATEGORY ppc_2
    on ppc_2.PRODUCT_CATEGORY_ID = ppc_1.PARENT_PRODUCT_CATEGORY_ID
join reporting_prod.gfb.vw_store st
    on st.STORE_ID = dp.STORE_ID
where
    pc.PRODUCT_CATEGORY_TYPE = 'Product Data Feed'
    and pc.ARCHIVE = 0;


create or replace temporary table _product_categories_generic as
select distinct
    upper(st.STORE_BRAND) as business_unit
    ,upper(st.STORE_REGION) as region
    ,pc.PRODUCT_CATEGORY_ID
    ,pc.ACTIVE
    ,pc.ARCHIVE
    ,'Generic' as PRODUCT_CATEGORY_TYPE
    ,ppc_2.LABEL as parent_parent_product_cateogry
    ,ppc_1.LABEL as parent_product_cateogry
    ,pc.LABEL as product_category
    ,dp.PRODUCT_ID as MASTER_PRODUCT_ID
from LAKE_JFB_VIEW.ULTRA_MERCHANT.PRODUCT_PRODUCT_CATEGORY a
join LAKE_JFB_VIEW.ULTRA_MERCHANT.PRODUCT_CATEGORY pc
    on pc.PRODUCT_CATEGORY_ID = a.PRODUCT_CATEGORY_ID
join EDW_PROD.DATA_MODEL_JFB.DIM_PRODUCT dp
    on dp.PRODUCT_ID = a.PRODUCT_ID
left join LAKE_JFB_VIEW.ULTRA_MERCHANT.PRODUCT_CATEGORY ppc_1
    on ppc_1.PRODUCT_CATEGORY_ID = pc.PARENT_PRODUCT_CATEGORY_ID
left join LAKE_JFB_VIEW.ULTRA_MERCHANT.PRODUCT_CATEGORY ppc_2
    on ppc_2.PRODUCT_CATEGORY_ID = ppc_1.PARENT_PRODUCT_CATEGORY_ID
join reporting_prod.gfb.vw_store st
    on st.STORE_ID = dp.STORE_ID
left join _product_categories_feed pcf
    on pcf.business_unit = upper(st.STORE_BRAND)
    and pcf.region = upper(st.STORE_REGION)
    and pcf.MASTER_PRODUCT_ID = dp.MASTER_PRODUCT_ID
where
    pc.PRODUCT_CATEGORY_TYPE is null
    and pc.ARCHIVE = 0
    and pcf.MASTER_PRODUCT_ID is null;


create or replace temporary table _all_product_categories as
select
    pcf.*
from _product_categories_feed pcf

union

select
    pcg.*
from _product_categories_generic pcg;


--New dim_store does not have special stores
create or replace temporary table _store_country as
select distinct
    ds.STORE_BRAND
    ,ds.STORE_REGION
    ,ds.STORE_COUNTRY
from EDW_PROD.DATA_MODEL_JFB.DIM_STORE ds
where
    ds.STORE_BRAND_ABBR in ('JF', 'SD', 'FK')
    and ds.STORE_FULL_NAME not like '%(DM)%'
    and ds.STORE_FULL_NAME not like '%Heels.com%'
    and ds.STORE_FULL_NAME not like '%G-SWAG%'
    and ds.STORE_FULL_NAME not like '%Retail%'
    and ds.STORE_FULL_NAME not like '%PS%'
    and ds.STORE_FULL_NAME not like '%Wholesale%'
    and ds.STORE_FULL_NAME not like '%Sample%'
    and ds.STORE_REGION in ('NA', 'EU');


insert into _store_country
values
('JUSTFAB', 'EU', 'AT')
,('FABKIDS', 'NA', 'CA')
,('JUSTFAB', 'EU', 'BE')
,('SHOEDAZZLE', 'NA', 'CA');


create or replace transient table REPORTING_PROD.GFB.dim_web_merch_product as
select distinct
    a.business_unit
    ,a.region
    ,upper(st.STORE_COUNTRY) as country
    ,mdp.PRODUCT_SKU
    ,a.PRODUCT_CATEGORY_ID
    ,a.ACTIVE
    ,a.ARCHIVE
    ,a.PRODUCT_CATEGORY_TYPE
    ,a.parent_parent_product_cateogry
    ,a.parent_product_cateogry
    ,a.product_category
    ,a.MASTER_PRODUCT_ID
    ,mdp.IMG_MODEL_PLUS
    ,mdp.IMG_MODEL_REG
    ,mdp.IMG_TYPE
    ,mdp.MSRP
    ,mdp.CURRENT_VIP_RETAIL
    ,mdp.SALE_PRICE
    ,mdp.LARGE_IMG_URL
    ,mdp.STYLE_NAME
    ,mdp.Boot_Shaft_Height_console
    ,mdp.color_family_console
    ,mdp.designer_collaboration_console
    ,mdp.fit_console
    ,mdp.heel_shape_console
    ,mdp.heel_size_console
    ,mdp.material_type_console
    ,mdp.occasion_type_console
    ,mdp.toe_type_console
    ,mdp.weather_console
    ,mdp.sleeve_lenth_console
    ,mdp.clothing_detail_console
    ,mdp.color_console
    ,mdp.buttom_subclass_console
    ,mdp.shop_category_console
    ,mdp.shoe_style_console
    ,mdp.PRODUCT_COLOR_FROM_LABEL
from
(
    select
        apc.business_unit
        ,apc.region
        ,apc.PRODUCT_CATEGORY_ID
        ,apc.ACTIVE
        ,apc.ARCHIVE
        ,apc.PRODUCT_CATEGORY_TYPE
        ,apc.parent_parent_product_cateogry
        ,apc.parent_product_cateogry
        ,apc.product_category
        ,apc.MASTER_PRODUCT_ID
        ,rank() over (partition by apc.business_unit, apc.region, apc.MASTER_PRODUCT_ID order by apc.PRODUCT_CATEGORY_ID desc) product_category_rank
    from _all_product_categories apc
) a
join _store_country st
    on upper(st.STORE_BRAND) = a.business_unit
    and upper(st.STORE_REGION) = a.region
join REPORTING_PROD.GFB.MERCH_DIM_PRODUCT mdp
    on mdp.BUSINESS_UNIT = a.business_unit
    and mdp.REGION = a.region
    and mdp.COUNTRY = upper(st.STORE_COUNTRY)
    and mdp.MASTER_PRODUCT_ID = a.MASTER_PRODUCT_ID
where
    a.product_category_rank = 1
    and a.MASTER_PRODUCT_ID != -1;
