CREATE OR REPLACE TEMP TABLE EDW_PROD.NEW_STG._promotion_rule_product_discount AS 
    with promotion_rule_cart as (
        select *,55 store_id,CONCAT('550000000',"promotion_id","id")::NUMBER(38,0) promo_id
        from LAKE_MMOS."mmos_membership_marketing_shoedazzle"."promotion_rule_cart"
        union all 
        select *,26 store_id,CONCAT('260000000',"promotion_id","id")::NUMBER(38,0) promo_id
        from LAKE_MMOS."mmos_membership_marketing_us"."promotion_rule_cart"
        union all 
        select *,46 store_id,CONCAT('460000000',"promotion_id","id")::NUMBER(38,0) promo_id
        from LAKE_MMOS."mmos_membership_marketing_fabkids"."promotion_rule_cart"
    )
    ,promotion_rule_product as (
        select *,55 store_id,CONCAT('550000000',"promotion_id","id")::NUMBER(38,0) promo_id
        from LAKE_MMOS."mmos_membership_marketing_shoedazzle"."promotion_rule_product"
        union all 
        select *,26 store_id,CONCAT('260000000',"promotion_id","id")::NUMBER(38,0) promo_id
        from LAKE_MMOS."mmos_membership_marketing_us"."promotion_rule_product"
        union all 
        select *,46 store_id,CONCAT('460000000',"promotion_id","id")::NUMBER(38,0) promo_id
        from LAKE_MMOS."mmos_membership_marketing_fabkids"."promotion_rule_product"
    )
    ,promotion_rule_shipping as (
        select *,55 store_id,CONCAT('550000000',"promotion_id","id")::NUMBER(38,0) promo_id
        from LAKE_MMOS."mmos_membership_marketing_shoedazzle"."promotion_rule_shipping"
        union all 
        select *,26 store_id,CONCAT('260000000',"promotion_id","id")::NUMBER(38,0) promo_id
        from LAKE_MMOS."mmos_membership_marketing_us"."promotion_rule_shipping"
        union all 
        select *,46 store_id,CONCAT('460000000',"promotion_id","id")::NUMBER(38,0) promo_id
        from LAKE_MMOS."mmos_membership_marketing_fabkids"."promotion_rule_shipping"
    )
    ,promotion as (
        select *,55 store_id
        from LAKE_MMOS."mmos_membership_marketing_shoedazzle"."promotion"
        union all 
        select *,26 store_id
        from LAKE_MMOS."mmos_membership_marketing_us"."promotion"
        union all 
        select *,46 store_id
        from LAKE_MMOS."mmos_membership_marketing_fabkids"."promotion"
    )
    select
         t1."id"                promotion_id
        ,t1."promotion_gid"      promotion_gid
        ,t2.discount_type       discount_type
        ,t2.discount_percentage discount_percentage
        ,t2.discount_rate       discount_rate
        ,t1."promotion_code"    promotion_code
        ,t1.store_id 
        ,t2.promo_id            promo_id
    from promotion t1
    inner join 
    (
        select
        "promotion_id"                                             promotion_id
        ,case when "rule_type" in (1,3) then 'flat_price'
              when "rule_type" in (2,4) then 'percentage'
              else cast("rule_type"as VARCHAR(256)) end                  discount_type
        ,iff( "rule_type" in (2,4),"discount_value",null)       discount_percentage
        ,iff("rule_type" in (1,3)   ,"discount_value",null)  discount_rate
        ,store_id
        ,promo_id
        from promotion_rule_cart
        where "_fivetran_deleted" = false

        union all
        select
        "promotion_id"                                        promotion_id
        ,case when "discount_type"=1 then 'flat_price'
              when "discount_type"=2 then 'percentage'
              when "discount_type"=3 then 'fixed_price'
              else cast("discount_type"as VARCHAR(256)) end  discount_type
        ,iff("discount_type" = 2 ,"discount_value",null)  discount_percentage
        ,iff("discount_type"  in (1,3)     ,"discount_value",null)  discount_rate
        ,store_id
        ,promo_id
        from promotion_rule_product
        where "_fivetran_deleted" = false

        union all

        select
        "promotion_id" promotion_id
        ,null          discount_type
        ,null          discount_percentage
        ,null          discount_rate
        ,store_id
        ,promo_id
        from promotion_rule_shipping
        where "_fivetran_deleted" = false
    ) t2  on t1."id" = t2.promotion_id and t1.store_id = t2.store_id
    where t1."promotion_gid"<>''
;


CREATE OR REPLACE TEMP TABLE EDW_PROD.NEW_STG._DIM_DISCOUNT AS 
with DISCOUNT_AUTOMATIC_APP as (
    select *,55 as store_id 
    from LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.DISCOUNT_AUTOMATIC_APP
    union all 
    select *,26 as store_id 
    from LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.DISCOUNT_AUTOMATIC_APP
    union all 
    select *,46 as store_id 
    from LAKE_MMOS.SHOPIFY_FABKIDS_PROD.DISCOUNT_AUTOMATIC_APP
)
,DISCOUNT_AUTOMATIC_BASIC as (
    select *,55 as store_id 
    from LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.DISCOUNT_AUTOMATIC_BASIC
    -- union all 
    -- select *,55 as store_id 
    -- from LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.DISCOUNT_AUTOMATIC_BASIC
)
,DISCOUNT_CODE_BASIC as (
    select *,55 as store_id 
    from LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.DISCOUNT_CODE_BASIC
    union all 
    select *,26 as store_id 
    from LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.DISCOUNT_CODE_BASIC
    union all 
    select *,46 as store_id 
    from LAKE_MMOS.SHOPIFY_FABKIDS_PROD.DISCOUNT_CODE_BASIC
)
,DISCOUNT_CODE_APP as (
    select *,55 as store_id 
    from LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.DISCOUNT_CODE_APP
    union all 
    select *,26 as store_id 
    from LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.DISCOUNT_CODE_APP
    union all 
    select *,46 as store_id 
    from LAKE_MMOS.SHOPIFY_FABKIDS_PROD.DISCOUNT_CODE_APP
)
,DISCOUNT_CODE_BXGY as (
    select *,55 as store_id 
    from LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.DISCOUNT_CODE_BXGY
    union all 
    select *,26 as store_id 
    from LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.DISCOUNT_CODE_BXGY
    union all 
    select *,46 as store_id 
    from LAKE_MMOS.SHOPIFY_FABKIDS_PROD.DISCOUNT_CODE_BXGY
)
,DISCOUNT_CODE_FREE_SHIPPING as (
    select *,55 as store_id 
    from LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.DISCOUNT_CODE_FREE_SHIPPING
    union all 
    select *,26 as store_id 
    from LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.DISCOUNT_CODE_FREE_SHIPPING
    union all 
    select *,46 as store_id 
    from LAKE_MMOS.SHOPIFY_FABKIDS_PROD.DISCOUNT_CODE_FREE_SHIPPING
)

select 
     CONCAT(t1.store_id,t1.id)::NUMBER(38,0)   as discount_id
    ,t1.app_discount_type_discount_class       as discount_applied_to
    ,cast(t2.discount_type as VARCHAR(256))                         as discount_calculation_method
    ,t1.title                                  as discount_label
    ,t2.discount_percentage                    as discount_percentage
    ,t2.discount_rate                          as discount_rate
    ,t1.ends_at::TIMESTAMP_NTZ(0)                                as discount_date_expires
    ,case when t1.status='ACTIVE' then 1700
          when t1.status='EXPIRED' then 3673  
          else null end as discount_status_code
    ,current_date                                      as meta_create_datetime
    ,current_date                                      as meta_update_datetime
    ,t1.store_id
    ,t2.promotion_code
    ,t2.promo_id as promotion_id
from DISCOUNT_AUTOMATIC_APP t1 
left join EDW_PROD.NEW_STG._promotion_rule_product_discount t2 
    on cast(t1.discount_id as VARCHAR(50)) = t2.promotion_gid and t1.store_id = t2.store_id
where _fivetran_deleted = false

union all 

select 
     CONCAT(t1.store_id,t1.id)::NUMBER(38,0)   as discount_id
    ,t1.discount_class                        as discount_applied_to
    ,case when customer_gets_value_percentage is null 
          and customer_gets_value_amount_amount is not null then 'flat_price'
     when customer_gets_value_percentage is not null 
              and customer_gets_value_amount_amount is null then 'percentage'
     else null end                             as discount_calculation_method
    ,t1.title                                  as discount_label
    ,t1.customer_gets_value_percentage         as discount_percentage
    ,t1.customer_gets_value_amount_amount      as discount_rate
    ,t1.ends_at::TIMESTAMP_NTZ(0)                                as discount_date_expires
    ,case when t1.status='ACTIVE' then 1700
          when t1.status='EXPIRED' then 3673  
          else null end as discount_status_code
    ,current_date                                      as meta_create_datetime
    ,current_date                                      as meta_update_datetime
    ,t1.store_id
    ,t2.promotion_code
    ,t2.promo_id as promotion_id
from DISCOUNT_AUTOMATIC_BASIC t1 
left join EDW_PROD.NEW_STG._promotion_rule_product_discount t2 
    on cast(t1.id as VARCHAR(50)) = t2.promotion_gid and t1.store_id = t2.store_id
where _fivetran_deleted = false

union all 

select 
     CONCAT(t1.store_id,t1.id)::NUMBER(38,0)   as discount_id
    ,t1.app_discount_type_discount_class       as discount_applied_to
    ,cast(t2.discount_type as VARCHAR(256))                         as discount_calculation_method
    ,t1.title                                  as discount_label
    ,t2.discount_percentage                    as discount_percentage
    ,t2.discount_rate                          as discount_rate
    ,t1.ends_at::TIMESTAMP_NTZ(0)                                as discount_date_expires
    ,case when t1.status='ACTIVE' then 1700
          when t1.status='EXPIRED' then 3673  
          else null end as discount_status_code
    ,current_date                                      as meta_create_datetime
    ,current_date                                      as meta_update_datetime
    ,t1.store_id
    ,t2.promotion_code
    ,t2.promo_id as promotion_id
from DISCOUNT_CODE_APP t1 
left join EDW_PROD.NEW_STG._promotion_rule_product_discount t2 
    on cast(t1.id as VARCHAR(50)) = t2.promotion_gid and t1.store_id = t2.store_id
where _fivetran_deleted = false and  APP_DISCOUNT_TYPE_DESCRIPTION <> 'vip-credit-function'

union all 

-- select 
--      t1.id                                     as discount_id
--      ,null as META_ORIGINAL_DISCOUNT_ID
--     ,t1.discount_class                         as discount_applied_to
--     ,null                                      as discount_calculation_method
--     ,t1.title                                  as discount_label
--     ,null                                      as discount_percentage
--     ,null                                      as discount_rate
--     -- ,t1.ends_at                                as discount_date_expires
--     ,null
--     -- ,t1.status                                 as discount_status_code
--     ,null
-- ,null as EFFECTIVE_START_DATETIME
-- ,null as EFFECTIVE_END_DATETIME
-- ,null as IS_CURRENT
-- ,null as META_ROW_HASH
--     ,null                                      as meta_create_datetime
--     ,null                                      as meta_update_datetime
-- from SHOPIFY_TEST.SHOPIFY.DISCOUNT_AUTOMATIC_BXGY t1 
-- where _fivetran_deleted = false

-- union all 

-- select 
--      t1.id                                     as discount_id
--      ,null as META_ORIGINAL_DISCOUNT_ID
--     ,t1.discount_class                         as discount_applied_to
--     ,null                                      as discount_calculation_method
--     ,t1.title                                  as discount_label
--     ,null                                      as discount_percentage
--     ,null                                      as discount_rate
--     -- ,t1.ends_at                                as discount_date_expires
--     ,null
--     -- ,t1.status                                 as discount_status_code
--     ,null
-- ,null as EFFECTIVE_START_DATETIME
-- ,null as EFFECTIVE_END_DATETIME
-- ,null as IS_CURRENT
-- ,null as META_ROW_HASH
--     ,null                                      as meta_create_datetime
--     ,null                                      as meta_update_datetime
-- from SHOPIFY_TEST.SHOPIFY.DISCOUNT_AUTOMATIC_FREE_SHIPPING t1 
-- where _fivetran_deleted = false
 


-- union all 

select 
     CONCAT(t1.store_id,t1.id)::NUMBER(38,0)   as discount_id
    ,t1.discount_class                        as discount_applied_to
    ,case when customer_gets_value_percentage is null 
          and customer_gets_value_amount_amount is not null then 'fixed_amount'
     when customer_gets_value_percentage is not null 
              and customer_gets_value_amount_amount is null then 'percentage'
     else null end                             as discount_calculation_method
    ,t1.title                                  as discount_label
    ,t1.customer_gets_value_percentage         as discount_percentage
    ,t1.customer_gets_value_amount_amount      as discount_rate
    ,t1.ends_at::TIMESTAMP_NTZ(0)                                as discount_date_expires
    ,case when t1.status='ACTIVE' then 1700
          when t1.status='EXPIRED' then 3673  
          else null end as discount_status_code
    ,current_date                                      as meta_create_datetime
    ,current_date                                      as meta_update_datetime
    ,t1.store_id
    ,t2.promotion_code
    ,t2.promo_id as promotion_id
from DISCOUNT_CODE_BASIC t1 
left join EDW_PROD.NEW_STG._promotion_rule_product_discount t2 
    on cast(t1.id as VARCHAR(50)) = t2.promotion_gid and t1.store_id = t2.store_id
where _fivetran_deleted = false

union all 

select 
     CONCAT(t1.store_id,t1.id)::NUMBER(38,0)   as discount_id
    ,t1.discount_class                         as discount_applied_to
    -- ,null                                      as discount_calculation_method
     ,case when customer_gets_value_percentage is null 
          and customer_gets_value_amount_amount is not null then 'fixed_amount'
     when customer_gets_value_percentage is not null 
              and customer_gets_value_amount_amount is null then 'percentage'
     else null end                             as discount_calculation_method
    ,t1.title                                  as discount_label
    ,t1.customer_gets_value_percentage         as discount_percentage
    ,t1.customer_gets_value_amount_amount      as discount_rate
    ,t1.ends_at::TIMESTAMP_NTZ(0)                                as discount_date_expires
    ,case when t1.status='ACTIVE' then 1700
          when t1.status='EXPIRED' then 3673  
          else null end as discount_status_code
    ,current_date                                      as meta_create_datetime
    ,current_date                                      as meta_update_datetime
    ,t1.store_id
    ,t2.promotion_code
    ,t2.promo_id as promotion_id
from DISCOUNT_CODE_BXGY t1 
left join EDW_PROD.NEW_STG._promotion_rule_product_discount t2 
    on cast(t1.id as VARCHAR(50)) = t2.promotion_gid and t1.store_id = t2.store_id
where _fivetran_deleted = false

union all 

select 
     CONCAT(t1.store_id,t1.id)::NUMBER(38,0)   as discount_id
    ,t1.discount_class                         as discount_applied_to
    ,'fixed_amount'                              as discount_calculation_method
    ,t1.title                                  as discount_label
    ,null                                      as discount_percentage
    ,0                                      as discount_rate
    ,t1.ends_at::TIMESTAMP_NTZ(0)                               as discount_date_expires
    ,case when t1.status='ACTIVE' then 1700
          when t1.status='EXPIRED' then 3673  
          else null end as discount_status_code
    ,current_date                                      as meta_create_datetime
    ,current_date                                      as meta_update_datetime
    ,t1.store_id
    ,t2.promotion_code
    ,t2.promo_id as promotion_id
from DISCOUNT_CODE_FREE_SHIPPING t1 
left join EDW_PROD.NEW_STG._promotion_rule_product_discount t2 
    on cast(t1.id as VARCHAR(50)) = t2.promotion_gid and t1.store_id = t2.store_id
where _fivetran_deleted = false
;


truncate table EDW_PROD.NEW_STG.DIM_DISCOUNT;
insert into EDW_PROD.NEW_STG.DIM_DISCOUNT
select 
 iff(promotion_id is not null,promotion_id,discount_id) discount_id
,discount_applied_to
,discount_calculation_method
,discount_label
,discount_percentage
,discount_rate
,discount_date_expires
,discount_status_code
,meta_create_datetime
,meta_update_datetime
,store_id
,promotion_code
,promotion_id
from EDW_PROD.NEW_STG._DIM_DISCOUNT
;