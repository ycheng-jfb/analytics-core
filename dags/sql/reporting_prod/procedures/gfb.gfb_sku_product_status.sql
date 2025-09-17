create or replace transient table REPORTING_PROD.GFB.gfb_sku_product_status as
select distinct
    upper(st.STORE_BRAND) as business_unit
    ,upper(st.STORE_REGION) as region
    ,dp.SKU
    ,dp.PRODUCT_SKU
    ,first_value(mdp.PRODUCT_STATUS) over (partition by business_unit, region, dp.SKU order by dp.MASTER_PRODUCT_ID desc) as master_product_status
    ,first_value(dp.PRODUCT_STATUS) over (partition by business_unit, region, dp.SKU order by dp.MASTER_PRODUCT_ID desc) as sku_product_status
    ,(case
        when master_product_status = 'Inactive' then 'Inactive'
        else sku_product_status end) as is_active
from EDW_PROD.DATA_MODEL_JFB.DIM_PRODUCT dp
join reporting_prod.gfb.vw_store st
    on st.STORE_ID = dp.STORE_ID
join EDW_PROD.DATA_MODEL_JFB.DIM_PRODUCT mdp
    on mdp.PRODUCT_ID = dp.MASTER_PRODUCT_ID
where
    dp.PRODUCT_NAME not like '%DO NOT USE%'
    and dp.PRODUCT_STATUS in ('Active', 'Inactive')
    and dp.SKU != 'Unknown'
    and st.STORE_COUNTRY in ('US', 'FR');
