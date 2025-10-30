-- dim_bundle_component_history
-- 查出目前所有的捆绑产品
CREATE OR REPLACE TEMPORARY TABLE EDW_PROD.NEW_STG._DIM_BUNDLE_COMPONENT_HISTORY as 
with shopify_metafield as (
SELECT distinct
    t2.owner_id                                           -- 相当于：product_variant_id
   ,f.value:product_id::NUMBER(38,0) AS master_product_id -- 相当于：master_product_id
   ,55 store_id
FROM 
    LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.METAFIELD t2,
    LATERAL FLATTEN(input => PARSE_JSON(t2.value)) f  -- 展开JSON数组
WHERE 
    t2.namespace = 'simple_bundles'
    AND t2.key = 'bundled_variants'

union all

SELECT distinct
    t2.owner_id                                           -- 相当于：product_variant_id
   ,f.value:product_id::NUMBER(38,0) AS master_product_id -- 相当于：master_product_id
   ,26 store_id
FROM 
    LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.METAFIELD t2,
    LATERAL FLATTEN(input => PARSE_JSON(t2.value)) f  -- 展开JSON数组
WHERE 
    t2.namespace = 'simple_bundles'
    AND t2.key = 'bundled_variants'

union all

 SELECT distinct
    t2.owner_id                                           -- 相当于：product_variant_id
   ,f.value:product_id::NUMBER(38,0) AS master_product_id -- 相当于：master_product_id
   ,46 store_id
FROM
    LAKE_MMOS.SHOPIFY_FABKIDS_PROD.METAFIELD t2,
    LATERAL FLATTEN(input => PARSE_JSON(t2.value)) f  -- 展开JSON数组
WHERE
    t2.namespace = 'simple_bundles'
    AND t2.key = 'bundled_variants'

)
, master_product as (
select 
 master_product_id
,product_category
,color
,is_free
,product_name
,retail_unit_price
,vip_unit_price
,store_id
,ROW_NUMBER() over(partition by master_product_id order by meta_update_datetime,meta_create_datetime desc) rk
from EDW_PROD.NEW_STG.DIM_PRODUCT p 
QUALIFY rk = 1
)
SELECT
 -- null bundle_component_history_key
 null product_bundle_component_id                 -- 没用
,p.product_id bundle_product_id                    -- 相当于：product_variant_id
,t2.master_product_id bundle_component_product_id -- 相当于：master_product_id
,p.product_name  bundle_name  -- 对应 product_id 的 product_name
,p.product_alias bundle_alias -- 对应 product_id 的 product_alias
,p.product_category as bundle_default_product_category -- 对应 product_id 的 product_category
,mp.product_category as bundle_component_default_product_category -- 对应  master_product_id 的 product_category
,mp.color as bundle_component_color -- 对应  master_product_id 的 color
,mp.product_name as  bundle_component_name    -- 对应  master_product_id 的 PRODUCT_NAME
-- 对应  master_product_id 的 RETAIL_UNIT_PRICE 按照RETAIL_UNIT_PRICE比例进行分摊保留4位小数
,ROUND(
    iff((mp.retail_unit_price/sum(mp.retail_unit_price) over(partition by p.product_id)) = 0
        ,0,mp.retail_unit_price/sum(mp.retail_unit_price) over(partition by p.product_id))
    ,4) bundle_price_contribution_percent   
-- bundle_price_contribution_percent *   product_id 的 vip_unit_price            
,ROUND(bundle_price_contribution_percent * p.vip_unit_price,4)    as bundle_component_vip_unit_price    
-- bundle_price_contribution_percent *   product_id 的 retail_unit_price 
,ROUND(bundle_price_contribution_percent * p.retail_unit_price,4) as bundle_component_retail_unit_price 
,mp.is_free as bundle_is_free                                       -- 对应  master_product_id 的 is_free
,p.meta_create_datetime as effective_start_datetime
,'9999-12-31'::TIMESTAMP as effective_end_datetime
,1 as is_current
,p.meta_create_datetime as meta_create_datetime
,current_date as meta_update_datetime
FROM EDW_PROD.NEW_STG.DIM_PRODUCT p 
inner join shopify_metafield t2 on t2.owner_id = p.product_id and p.store_id = t2.store_id
left join master_product mp on t2.master_product_id = mp.master_product_id and mp.store_id = t2.store_id
;
-- 修改过期的数据
MERGE INTO EDW_PROD.NEW_STG.DIM_BUNDLE_COMPONENT_HISTORY AS tgt          
USING (
  select
   t2.bundle_component_history_key
  ,t1.PRODUCT_BUNDLE_COMPONENT_ID
  ,t1.BUNDLE_PRODUCT_ID
  ,t1.BUNDLE_COMPONENT_PRODUCT_ID
  ,t1.BUNDLE_NAME
  ,t1.BUNDLE_ALIAS
  ,t1.BUNDLE_DEFAULT_PRODUCT_CATEGORY
  ,t1.BUNDLE_COMPONENT_DEFAULT_PRODUCT_CATEGORY
  ,t1.BUNDLE_COMPONENT_COLOR
  ,t1.BUNDLE_COMPONENT_NAME
  ,t1.BUNDLE_PRICE_CONTRIBUTION_PERCENT
  ,t1.BUNDLE_COMPONENT_VIP_UNIT_PRICE
  ,t1.BUNDLE_COMPONENT_RETAIL_UNIT_PRICE
  ,t1.BUNDLE_IS_FREE
  ,current_date EFFECTIVE_START_DATETIME
  ,'9999-12-31'::TIMESTAMP as EFFECTIVE_END_DATETIME
  ,1 IS_CURRENT
  ,current_date META_CREATE_DATETIME
  ,current_date META_UPDATE_DATETIME
  from EDW_PROD.NEW_STG._DIM_BUNDLE_COMPONENT_HISTORY t1
  left join EDW_PROD.NEW_STG.DIM_BUNDLE_COMPONENT_HISTORY t2 
      on  t2.is_current =1
          and iff(t1.BUNDLE_PRODUCT_ID is null and t2.BUNDLE_PRODUCT_ID is null ,true,t1.BUNDLE_PRODUCT_ID = t2.BUNDLE_PRODUCT_ID )
          and iff(t1.BUNDLE_COMPONENT_PRODUCT_ID is null and t2.BUNDLE_COMPONENT_PRODUCT_ID is null ,true,t1.BUNDLE_COMPONENT_PRODUCT_ID = t2.BUNDLE_COMPONENT_PRODUCT_ID)
          and iff(t1.BUNDLE_NAME is null and t2.BUNDLE_NAME is null ,true,t1.BUNDLE_NAME = t2.BUNDLE_NAME)
          and iff(t1.BUNDLE_ALIAS is null and t2.BUNDLE_ALIAS is null ,true,t1.BUNDLE_ALIAS = t2.BUNDLE_ALIAS)
          and iff(t1.BUNDLE_DEFAULT_PRODUCT_CATEGORY is null and t2.BUNDLE_DEFAULT_PRODUCT_CATEGORY is null ,true,t1.BUNDLE_DEFAULT_PRODUCT_CATEGORY = t2.BUNDLE_DEFAULT_PRODUCT_CATEGORY)
          and iff(t1.BUNDLE_COMPONENT_DEFAULT_PRODUCT_CATEGORY is null and t2.BUNDLE_COMPONENT_DEFAULT_PRODUCT_CATEGORY is null ,true,t1.BUNDLE_COMPONENT_DEFAULT_PRODUCT_CATEGORY = t2.BUNDLE_COMPONENT_DEFAULT_PRODUCT_CATEGORY)
          and iff(t1.BUNDLE_COMPONENT_COLOR is null and t2.BUNDLE_COMPONENT_COLOR is null ,true,t1.BUNDLE_COMPONENT_COLOR = t2.BUNDLE_COMPONENT_COLOR)
          and iff(t1.BUNDLE_COMPONENT_NAME is null and t2.BUNDLE_COMPONENT_NAME is null ,true,t1.BUNDLE_COMPONENT_NAME = t2.BUNDLE_COMPONENT_NAME)
          and iff(t1.BUNDLE_PRICE_CONTRIBUTION_PERCENT is null and t2.BUNDLE_PRICE_CONTRIBUTION_PERCENT is null ,true,t1.BUNDLE_PRICE_CONTRIBUTION_PERCENT = t2.BUNDLE_PRICE_CONTRIBUTION_PERCENT)
          and iff(t1.BUNDLE_COMPONENT_VIP_UNIT_PRICE is null and t2.BUNDLE_COMPONENT_VIP_UNIT_PRICE is null ,true,t1.BUNDLE_COMPONENT_VIP_UNIT_PRICE = t2.BUNDLE_COMPONENT_VIP_UNIT_PRICE)
          and iff(t1.BUNDLE_COMPONENT_RETAIL_UNIT_PRICE is null and t2.BUNDLE_COMPONENT_RETAIL_UNIT_PRICE is null ,true,t1.BUNDLE_COMPONENT_RETAIL_UNIT_PRICE = t2.BUNDLE_COMPONENT_RETAIL_UNIT_PRICE)
          and iff(t1.BUNDLE_IS_FREE is null and t2.BUNDLE_IS_FREE is null ,true,t1.BUNDLE_IS_FREE = t2.BUNDLE_IS_FREE)
  where t2.bundle_component_history_key is null
) AS src 
    ON  tgt.BUNDLE_PRODUCT_ID = src.BUNDLE_PRODUCT_ID 
    and tgt.BUNDLE_COMPONENT_PRODUCT_ID = src.BUNDLE_COMPONENT_PRODUCT_ID
WHEN MATCHED THEN
 UPDATE SET
    tgt.EFFECTIVE_END_DATETIME = current_date
   ,tgt.IS_CURRENT = 0
   ,tgt.META_UPDATE_DATETIME = current_date
;

-- 插入新数据
INSERT INTO EDW_PROD.NEW_STG.DIM_BUNDLE_COMPONENT_HISTORY (
    PRODUCT_BUNDLE_COMPONENT_ID,
    BUNDLE_PRODUCT_ID,
    BUNDLE_COMPONENT_PRODUCT_ID,
    BUNDLE_NAME,
    BUNDLE_ALIAS,
    BUNDLE_DEFAULT_PRODUCT_CATEGORY,
    BUNDLE_COMPONENT_DEFAULT_PRODUCT_CATEGORY,
    BUNDLE_COMPONENT_COLOR,
    BUNDLE_COMPONENT_NAME,
    BUNDLE_PRICE_CONTRIBUTION_PERCENT,
    BUNDLE_COMPONENT_VIP_UNIT_PRICE,
    BUNDLE_COMPONENT_RETAIL_UNIT_PRICE,
    BUNDLE_IS_FREE,
    EFFECTIVE_START_DATETIME,
    EFFECTIVE_END_DATETIME,
    IS_CURRENT,
    META_CREATE_DATETIME,
    META_UPDATE_DATETIME
)
  select
   t1.PRODUCT_BUNDLE_COMPONENT_ID
  ,t1.BUNDLE_PRODUCT_ID
  ,t1.BUNDLE_COMPONENT_PRODUCT_ID
  ,t1.BUNDLE_NAME
  ,t1.BUNDLE_ALIAS
  ,t1.BUNDLE_DEFAULT_PRODUCT_CATEGORY
  ,t1.BUNDLE_COMPONENT_DEFAULT_PRODUCT_CATEGORY
  ,t1.BUNDLE_COMPONENT_COLOR
  ,t1.BUNDLE_COMPONENT_NAME
  ,t1.BUNDLE_PRICE_CONTRIBUTION_PERCENT
  ,t1.BUNDLE_COMPONENT_VIP_UNIT_PRICE
  ,t1.BUNDLE_COMPONENT_RETAIL_UNIT_PRICE
  ,t1.BUNDLE_IS_FREE
  ,current_date EFFECTIVE_START_DATETIME
  ,'9999-12-31'::TIMESTAMP as EFFECTIVE_END_DATETIME
  ,1 IS_CURRENT
  ,current_date META_CREATE_DATETIME
  ,current_date META_UPDATE_DATETIME
  from EDW_PROD.NEW_STG._DIM_BUNDLE_COMPONENT_HISTORY t1
  left join EDW_PROD.NEW_STG.DIM_BUNDLE_COMPONENT_HISTORY t2 
      on  t2.is_current =1
          and iff(t1.BUNDLE_PRODUCT_ID is null and t2.BUNDLE_PRODUCT_ID is null ,true,t1.BUNDLE_PRODUCT_ID = t2.BUNDLE_PRODUCT_ID )
          and iff(t1.BUNDLE_COMPONENT_PRODUCT_ID is null and t2.BUNDLE_COMPONENT_PRODUCT_ID is null ,true,t1.BUNDLE_COMPONENT_PRODUCT_ID = t2.BUNDLE_COMPONENT_PRODUCT_ID)
          and iff(t1.BUNDLE_NAME is null and t2.BUNDLE_NAME is null ,true,t1.BUNDLE_NAME = t2.BUNDLE_NAME)
          and iff(t1.BUNDLE_ALIAS is null and t2.BUNDLE_ALIAS is null ,true,t1.BUNDLE_ALIAS = t2.BUNDLE_ALIAS)
          and iff(t1.BUNDLE_DEFAULT_PRODUCT_CATEGORY is null and t2.BUNDLE_DEFAULT_PRODUCT_CATEGORY is null ,true,t1.BUNDLE_DEFAULT_PRODUCT_CATEGORY = t2.BUNDLE_DEFAULT_PRODUCT_CATEGORY)
          and iff(t1.BUNDLE_COMPONENT_DEFAULT_PRODUCT_CATEGORY is null and t2.BUNDLE_COMPONENT_DEFAULT_PRODUCT_CATEGORY is null ,true,t1.BUNDLE_COMPONENT_DEFAULT_PRODUCT_CATEGORY = t2.BUNDLE_COMPONENT_DEFAULT_PRODUCT_CATEGORY)
          and iff(t1.BUNDLE_COMPONENT_COLOR is null and t2.BUNDLE_COMPONENT_COLOR is null ,true,t1.BUNDLE_COMPONENT_COLOR = t2.BUNDLE_COMPONENT_COLOR)
          and iff(t1.BUNDLE_COMPONENT_NAME is null and t2.BUNDLE_COMPONENT_NAME is null ,true,t1.BUNDLE_COMPONENT_NAME = t2.BUNDLE_COMPONENT_NAME)
          and iff(t1.BUNDLE_PRICE_CONTRIBUTION_PERCENT is null and t2.BUNDLE_PRICE_CONTRIBUTION_PERCENT is null ,true,t1.BUNDLE_PRICE_CONTRIBUTION_PERCENT = t2.BUNDLE_PRICE_CONTRIBUTION_PERCENT)
          and iff(t1.BUNDLE_COMPONENT_VIP_UNIT_PRICE is null and t2.BUNDLE_COMPONENT_VIP_UNIT_PRICE is null ,true,t1.BUNDLE_COMPONENT_VIP_UNIT_PRICE = t2.BUNDLE_COMPONENT_VIP_UNIT_PRICE)
          and iff(t1.BUNDLE_COMPONENT_RETAIL_UNIT_PRICE is null and t2.BUNDLE_COMPONENT_RETAIL_UNIT_PRICE is null ,true,t1.BUNDLE_COMPONENT_RETAIL_UNIT_PRICE = t2.BUNDLE_COMPONENT_RETAIL_UNIT_PRICE)
          and iff(t1.BUNDLE_IS_FREE is null and t2.BUNDLE_IS_FREE is null ,true,t1.BUNDLE_IS_FREE = t2.BUNDLE_IS_FREE)
  where t2.bundle_component_history_key is null
;
