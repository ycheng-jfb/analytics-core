CREATE TABLE IF NOT EXISTS EDW_PROD.NEW_STG.DIM_DISCOUNT (
    DISCOUNT_ID NUMBER(38,0) NOT NULL,
    META_ORIGINAL_DISCOUNT_ID NUMBER(38,0),
    DISCOUNT_APPLIED_TO VARCHAR(15),
    DISCOUNT_CALCULATION_METHOD VARCHAR(25),
    DISCOUNT_LABEL VARCHAR(50),
    DISCOUNT_PERCENTAGE NUMBER(18,6),
    DISCOUNT_RATE NUMBER(19,4),
    DISCOUNT_DATE_EXPIRES TIMESTAMP_NTZ(0),
    DISCOUNT_STATUS_CODE NUMBER(38,0),
    EFFECTIVE_START_DATETIME TIMESTAMP_LTZ(9),
    EFFECTIVE_END_DATETIME TIMESTAMP_LTZ(9),
    IS_CURRENT BOOLEAN,
    META_ROW_HASH NUMBER(38,0),
    META_CREATE_DATETIME TIMESTAMP_LTZ(9),
    META_UPDATE_DATETIME TIMESTAMP_LTZ(9),
    primary key (DISCOUNT_ID)
);

CREATE OR REPLACE TEMP TABLE EDW_PROD.NEW_STG._promotion_rule_product AS (
    select
         t1."id"                 id
        ,t1."promotion_gid"      promotion_gid
        ,t2.discount_type       discount_type
        ,t2.discount_percentage discount_percentage
        ,t2.discount_rate       discount_rate
    from SHOPIFY_TEST."mmos_membership_marketing_us"."promotion" t1
    inner join 
    (
        select
        "promotion_id"                                             promotion_id
        ,case when "rule_type" in (1,3) then 'flat_price'
              when "rule_type" in (2,4) then 'percentage'
              else cast("rule_type"as VARCHAR(256)) end                  discount_type
        ,iff( "rule_type" in (1,3),"discount_value",null)       discount_percentage
        ,iff("rule_type" in (2,4)      ,"discount_value",null)  discount_rate
        from SHOPIFY_TEST."mmos_membership_marketing_us"."promotion_rule_cart"

        union all
        select
        "promotion_id"                                        promotion_id
        ,case when "discount_type"=1 then 'flat_price'
              when "discount_type"=2 then 'percentage'
              when "discount_type"=3 then 'fixed_price'
              else cast("discount_type"as VARCHAR(256)) end  discount_type
        ,iff("discount_type" in (1,3) ,"discount_value",null)  discount_percentage
        ,iff("discount_type" = 2      ,"discount_value",null)  discount_rate
        from SHOPIFY_TEST."mmos_membership_marketing_us"."promotion_rule_product"

        union all

        select
        "promotion_id" promotion_id
        ,null          discount_type
        ,null          discount_percentage
        ,null          discount_rate
        from SHOPIFY_TEST."mmos_membership_marketing_us"."promotion_rule_shipping"
    ) t2  on t1."id" = t2.promotion_id
    where t1."promotion_gid"<>''
)
;




-- ,t1 as (
INSERT OVERWRITE INTO EDW_PROD.NEW_STG.DIM_DISCOUNT
select 
     t1.id                                     as discount_id
     ,null as META_ORIGINAL_DISCOUNT_ID
    ,t1.app_discount_type_discount_class       as discount_applied_to
    ,cast(t2.discount_type as VARCHAR(256))                         as discount_calculation_method
    ,t1.title                                  as discount_label
    ,t2.discount_percentage                    as discount_percentage
    ,t2.discount_rate                          as discount_rate
    -- ,t1.ends_at                                as discount_date_expires
    ,null
    -- ,t1.status                                 as discount_status_code
    ,null
,null as EFFECTIVE_START_DATETIME
,null as EFFECTIVE_END_DATETIME
,null as IS_CURRENT
,null as META_ROW_HASH
    ,null                                      as meta_create_datetime
    ,null                                      as meta_update_datetime
from SHOPIFY_TEST.SHOPIFY.DISCOUNT_AUTOMATIC_APP t1 
left join EDW_PROD.NEW_STG._promotion_rule_product t2 on cast(t1.id as VARCHAR(50)) = t2.promotion_gid

union all 

select 
     t1.id                                     as discount_id
     ,null as META_ORIGINAL_DISCOUNT_ID
    ,t1.discount_class                        as discount_applied_to
    ,case when customer_gets_value_percentage is null 
          and customer_gets_value_amount_amount is not null then 'flat_price'
     when customer_gets_value_percentage is not null 
              and customer_gets_value_amount_amount is null then 'percentage'
     else null end                             as discount_calculation_method
    ,t1.title                                  as discount_label
    ,t1.customer_gets_value_percentage         as discount_percentage
    ,t1.customer_gets_value_amount_amount      as discount_rate
    -- ,t1.ends_at                                as discount_date_expires
    ,null
    -- ,t1.status                                 as discount_status_code
    ,null
,null as EFFECTIVE_START_DATETIME
,null as EFFECTIVE_END_DATETIME
,null as IS_CURRENT
,null as META_ROW_HASH
    ,null                                      as meta_create_datetime
    ,null                                      as meta_update_datetime
from SHOPIFY_TEST.SHOPIFY.DISCOUNT_AUTOMATIC_BASIC t1 


union all 

select 
     t1.id                                     as discount_id
     ,null as META_ORIGINAL_DISCOUNT_ID
    ,t1.discount_class                         as discount_applied_to
    ,null                                      as discount_calculation_method
    ,t1.title                                  as discount_label
    ,null                                      as discount_percentage
    ,null                                      as discount_rate
    -- ,t1.ends_at                                as discount_date_expires
    ,null
    -- ,t1.status                                 as discount_status_code
    ,null
,null as EFFECTIVE_START_DATETIME
,null as EFFECTIVE_END_DATETIME
,null as IS_CURRENT
,null as META_ROW_HASH
    ,null                                      as meta_create_datetime
    ,null                                      as meta_update_datetime
from SHOPIFY_TEST.SHOPIFY.DISCOUNT_AUTOMATIC_BXGY t1 

union all 

select 
     t1.id                                     as discount_id
     ,null as META_ORIGINAL_DISCOUNT_ID
    ,t1.discount_class                         as discount_applied_to
    ,null                                      as discount_calculation_method
    ,t1.title                                  as discount_label
    ,null                                      as discount_percentage
    ,null                                      as discount_rate
    -- ,t1.ends_at                                as discount_date_expires
    ,null
    -- ,t1.status                                 as discount_status_code
    ,null
,null as EFFECTIVE_START_DATETIME
,null as EFFECTIVE_END_DATETIME
,null as IS_CURRENT
,null as META_ROW_HASH
    ,null                                      as meta_create_datetime
    ,null                                      as meta_update_datetime
from SHOPIFY_TEST.SHOPIFY.DISCOUNT_AUTOMATIC_FREE_SHIPPING t1 

union all 

select 
     t1.id                                     as discount_id
     ,null as META_ORIGINAL_DISCOUNT_ID
    ,t2.target_type                            as discount_applied_to
    ,cast(t2.value_type as VARCHAR(256))                             as discount_calculation_method
    ,t2.title                                  as discount_label
    ,t2.value                                  as discount_percentage
    ,t2.value                                  as discount_rate
    -- ,t2.ends_at                                as discount_date_expires
    ,null
    -- ,null                                      as discount_status_code
    ,null
,null as EFFECTIVE_START_DATETIME
,null as EFFECTIVE_END_DATETIME
,null as IS_CURRENT
,null as META_ROW_HASH
    ,null                                      as meta_create_datetime
    ,null                                      as meta_update_datetime
from SHOPIFY_TEST.SHOPIFY.DISCOUNT_CODE t1 
left join SHOPIFY_TEST.SHOPIFY.price_rule t2 on t1.price_rule_id = t2.id

union all 

-- 观测生产数据，测试数据没有找到对应价格规则
select 
     t1.id                                     as discount_id
     ,null as META_ORIGINAL_DISCOUNT_ID
    ,t1.app_discount_type_discount_class       as discount_applied_to
    ,cast(t2.discount_type as VARCHAR(256))    as discount_calculation_method
    ,t1.title                                  as discount_label
    ,t2.discount_percentage                    as discount_percentage
    ,t2.discount_rate                          as discount_rate
    -- ,t1.ends_at                                as discount_date_expires
    ,null
    -- ,t1.status                                 as discount_status_code
    ,null
,null as EFFECTIVE_START_DATETIME
,null as EFFECTIVE_END_DATETIME
,null as IS_CURRENT
,null as META_ROW_HASH
    , null                                     as meta_create_datetime
    ,null                                      as meta_update_datetime
from SHOPIFY_TEST.SHOPIFY.DISCOUNT_CODE_APP t1 
left join EDW_PROD.NEW_STG._promotion_rule_product t2 on cast(t1.id as VARCHAR(50)) = t2.promotion_gid

union all 

select 
     t1.id                                     as discount_id
     ,null as META_ORIGINAL_DISCOUNT_ID
    ,t1.discount_class                        as discount_applied_to
    ,case when customer_gets_value_percentage is null 
          and customer_gets_value_amount_amount is not null then 'percentage'
     when customer_gets_value_percentage is not null 
              and customer_gets_value_amount_amount is null then 'fixed_amount'
     else null end                             as discount_calculation_method
    ,t1.title                                  as discount_label
    ,t1.customer_gets_value_percentage         as discount_percentage
    ,t1.customer_gets_value_amount_amount      as discount_rate
    -- ,t1.ends_at                                as discount_date_expires
    ,null
    -- ,t1.status                                 as discount_status_code
    ,null
,null as EFFECTIVE_START_DATETIME
,null as EFFECTIVE_END_DATETIME
,null as IS_CURRENT
,null as META_ROW_HASH
    ,null                                      as meta_create_datetime
    ,null                                      as meta_update_datetime
from SHOPIFY_TEST.SHOPIFY.DISCOUNT_CODE_BASIC t1 

union all 

select 
     t1.id                                     as discount_id
     ,null as META_ORIGINAL_DISCOUNT_ID
    ,t1.discount_class                         as discount_applied_to
    ,null                                      as discount_calculation_method
    ,t1.title                                  as discount_label
    ,null                                      as discount_percentage
    ,null                                      as discount_rate
    -- ,t1.ends_at                                as discount_date_expires
    ,null
    -- ,t1.status                                 as discount_status_code
    ,null
,null as EFFECTIVE_START_DATETIME
,null as EFFECTIVE_END_DATETIME
,null as IS_CURRENT
,null as META_ROW_HASH
    ,null                                      as meta_create_datetime
    ,null                                      as meta_update_datetime
from SHOPIFY_TEST.SHOPIFY.DISCOUNT_CODE_BXGY t1 

union all 

select 
     t1.id                                     as discount_id
,null as META_ORIGINAL_DISCOUNT_ID
    ,t1.discount_class                         as discount_applied_to
    ,null                                      as discount_calculation_method
    ,t1.title                                  as discount_label
    ,null                                      as discount_percentage
    ,null                                      as discount_rate
    -- ,t1.ends_at                                as discount_date_expires
    ,null
    -- ,t1.status                                 as discount_status_code
    ,null
,null as EFFECTIVE_START_DATETIME
,null as EFFECTIVE_END_DATETIME
,null as IS_CURRENT
,null as META_ROW_HASH
    ,null                                      as meta_create_datetime
    ,null                                      as meta_update_datetime
from SHOPIFY_TEST.SHOPIFY.DISCOUNT_CODE_FREE_SHIPPING t1 
-- )

-- select  
-- discount_applied_to,count(1)
-- from t1
-- group by discount_applied_to
;

