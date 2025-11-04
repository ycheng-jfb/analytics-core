-- 代码暂时不可重跑，有时间在改进
-- dim_promo_history
-- 需要更新的数据 或者插入的数据
CREATE OR REPLACE TEMP TABLE EDW_PROD.NEW_STG._dim_promo_history AS 
with promotion_rule_cart as (
    select *,55 site_id
    ,LEAST(
        CONVERT_TIMEZONE('UTC','America/Los_Angeles', TO_TIMESTAMP_NTZ("updated_at")),
        CONVERT_TIMEZONE('America/Los_Angeles', "_fivetran_synced")
    ) AS last_updated
    ,CONCAT("promotion_id","id",'55')::NUMBER(38,0) promo_id
    from LAKE_MMOS."mmos_membership_marketing_shoedazzle"."promotion_rule_cart"
    where "_fivetran_deleted" = false
    union all 
    select *,26 site_id
    ,LEAST(
        CONVERT_TIMEZONE('UTC','America/Los_Angeles', TO_TIMESTAMP_NTZ("updated_at")),
        CONVERT_TIMEZONE('America/Los_Angeles', "_fivetran_synced")
    ) AS last_updated
    ,CONCAT("promotion_id","id",'26')::NUMBER(38,0) promo_id
    from LAKE_MMOS."mmos_membership_marketing_us"."promotion_rule_cart"
    where "_fivetran_deleted" = false
    union all 
    select *,46 site_id
    ,LEAST(
        CONVERT_TIMEZONE('UTC','America/Los_Angeles', TO_TIMESTAMP_NTZ("updated_at")),
        CONVERT_TIMEZONE('America/Los_Angeles', "_fivetran_synced")
    ) AS last_updated
    ,CONCAT("promotion_id","id",'46')::NUMBER(38,0) promo_id
    from LAKE_MMOS."mmos_membership_marketing_fabkids"."promotion_rule_cart"
    where "_fivetran_deleted" = false
    union all 
    select *,2000 site_id
    ,LEAST(
        CONVERT_TIMEZONE('UTC','America/Los_Angeles', TO_TIMESTAMP_NTZ("updated_at")),
        CONVERT_TIMEZONE('America/Los_Angeles', "_fivetran_synced")
    ) AS last_updated
    ,CONCAT("promotion_id","id",'2000')::NUMBER(38,0) promo_id
    from LAKE_MMOS."mmos_membership_marketing_eu"."promotion_rule_cart"
    where "_fivetran_deleted" = false
)
,promotion_rule_product as (
    select 
        "id"
    ,"promotion_id"
    ,CONCAT("promotion_id","id",'55')::NUMBER(38,0) promo_id
    ,"rule_type"
    ,"discount_type"
    ,"discount_value"
    ,"variant_condition"
    ,"min_quantity"
    ,"max_quantity"
    ,"is_first_order_only"
    ,"created_at"
    ,"updated_at"
    ,"sort"
    ,"country_code"
    ,"_fivetran_deleted"
    ,"_fivetran_synced"
    ,55 site_id
    ,LEAST(
        CONVERT_TIMEZONE('UTC','America/Los_Angeles', TO_TIMESTAMP_NTZ("updated_at")),
        CONVERT_TIMEZONE('America/Los_Angeles', "_fivetran_synced")
    ) AS last_updated
    from LAKE_MMOS."mmos_membership_marketing_shoedazzle"."promotion_rule_product"
    where "_fivetran_deleted" = false
    union all 
    select 
        "id"
    ,"promotion_id"
    ,CONCAT("promotion_id","id",'26')::NUMBER(38,0) promo_id
    ,"rule_type"
    ,"discount_type"
    ,"discount_value"
    ,"variant_condition"
    ,"min_quantity"
    ,"max_quantity"
    ,"is_first_order_only"
    ,"created_at"
    ,"updated_at"
    ,"sort"
    ,"country_code"
    ,"_fivetran_deleted"
    ,"_fivetran_synced"
    ,26 site_id
    ,LEAST(
        CONVERT_TIMEZONE('UTC','America/Los_Angeles', TO_TIMESTAMP_NTZ("updated_at")),
        CONVERT_TIMEZONE('America/Los_Angeles', "_fivetran_synced")
    ) AS last_updated
    from LAKE_MMOS."mmos_membership_marketing_us"."promotion_rule_product"
    where "_fivetran_deleted" = false
    union all 
    select 
        "id"
    ,"promotion_id"
    ,CONCAT("promotion_id","id",'46')::NUMBER(38,0) promo_id
    ,"rule_type"
    ,"discount_type"
    ,"discount_value"
    ,"variant_condition"
    ,"min_quantity"
    ,"max_quantity"
    ,"is_first_order_only"
    ,"created_at"
    ,"updated_at"
    ,"sort"
    ,"country_code"
    ,"_fivetran_deleted"
    ,"_fivetran_synced"
    ,46 site_id
    ,LEAST(
        CONVERT_TIMEZONE('UTC','America/Los_Angeles', TO_TIMESTAMP_NTZ("updated_at")),
        CONVERT_TIMEZONE('America/Los_Angeles', "_fivetran_synced")
    ) AS last_updated
    from LAKE_MMOS."mmos_membership_marketing_fabkids"."promotion_rule_product"
    where "_fivetran_deleted" = false
    union all 
    select 
        "id"
    ,"promotion_id"
    ,CONCAT("promotion_id","id",'2000')::NUMBER(38,0) promo_id
    ,"rule_type"
    ,"discount_type"
    ,"discount_value"
    ,"variant_condition"
    ,"min_quantity"
    ,"max_quantity"
    ,"is_first_order_only"
    ,"created_at"
    ,"updated_at"
    ,"sort"
    ,"country_code"
    ,"_fivetran_deleted"
    ,"_fivetran_synced"
    ,2000 site_id
    ,LEAST(
        CONVERT_TIMEZONE('UTC','America/Los_Angeles', TO_TIMESTAMP_NTZ("updated_at")),
        CONVERT_TIMEZONE('America/Los_Angeles', "_fivetran_synced")
    ) AS last_updated
    from LAKE_MMOS."mmos_membership_marketing_eu"."promotion_rule_product"
    where "_fivetran_deleted" = false
)
,promotion_rule_shipping as (
    select *,55 site_id
    ,LEAST(
        CONVERT_TIMEZONE('UTC','America/Los_Angeles', TO_TIMESTAMP_NTZ("updated_at")),
        CONVERT_TIMEZONE('America/Los_Angeles', "_fivetran_synced")
    ) AS last_updated
    ,CONCAT("promotion_id","id",'55')::NUMBER(38,0) promo_id
    from LAKE_MMOS."mmos_membership_marketing_shoedazzle"."promotion_rule_shipping"
    where "_fivetran_deleted" = false
    union all 
    select *,26 site_id
    ,LEAST(
        CONVERT_TIMEZONE('UTC','America/Los_Angeles', TO_TIMESTAMP_NTZ("updated_at")),
        CONVERT_TIMEZONE('America/Los_Angeles', "_fivetran_synced")
    ) AS last_updated
    ,CONCAT("promotion_id","id",'26')::NUMBER(38,0) promo_id
    from LAKE_MMOS."mmos_membership_marketing_us"."promotion_rule_shipping"
    where "_fivetran_deleted" = false
    union all 
    select *,46 site_id
    ,LEAST(
        CONVERT_TIMEZONE('UTC','America/Los_Angeles', TO_TIMESTAMP_NTZ("updated_at")),
        CONVERT_TIMEZONE('America/Los_Angeles', "_fivetran_synced")
    ) AS last_updated
    ,CONCAT("promotion_id","id",'46')::NUMBER(38,0) promo_id
    from LAKE_MMOS."mmos_membership_marketing_fabkids"."promotion_rule_shipping"
    where "_fivetran_deleted" = false
    union all 
    select *,2000 site_id
    ,LEAST(
        CONVERT_TIMEZONE('UTC','America/Los_Angeles', TO_TIMESTAMP_NTZ("updated_at")),
        CONVERT_TIMEZONE('America/Los_Angeles', "_fivetran_synced")
    ) AS last_updated
    ,CONCAT("promotion_id","id",'2000')::NUMBER(38,0) promo_id
    from LAKE_MMOS."mmos_membership_marketing_eu"."promotion_rule_shipping"
    where "_fivetran_deleted" = false
)
,promotion as (
    select *,55 site_id
    ,LEAST(
        CONVERT_TIMEZONE('UTC','America/Los_Angeles', TO_TIMESTAMP_NTZ("updated_at")),
        CONVERT_TIMEZONE('America/Los_Angeles', "_fivetran_synced")
    ) AS last_updated
    from LAKE_MMOS."mmos_membership_marketing_shoedazzle"."promotion"
    where "_fivetran_deleted" = false
    union all 
    select *,26 site_id
    ,LEAST(
        CONVERT_TIMEZONE('UTC','America/Los_Angeles', TO_TIMESTAMP_NTZ("updated_at")),
        CONVERT_TIMEZONE('America/Los_Angeles', "_fivetran_synced")
    ) AS last_updated
    from LAKE_MMOS."mmos_membership_marketing_us"."promotion"
    where "_fivetran_deleted" = false
    union all 
    select *,46 site_id
    ,LEAST(
        CONVERT_TIMEZONE('UTC','America/Los_Angeles', TO_TIMESTAMP_NTZ("updated_at")),
        CONVERT_TIMEZONE('America/Los_Angeles', "_fivetran_synced")
    ) AS last_updated
    from LAKE_MMOS."mmos_membership_marketing_fabkids"."promotion"
    where "_fivetran_deleted" = false
    union all 
    select *,2000 site_id
    ,LEAST(
        CONVERT_TIMEZONE('UTC','America/Los_Angeles', TO_TIMESTAMP_NTZ("updated_at")),
        CONVERT_TIMEZONE('America/Los_Angeles', "_fivetran_synced")
    ) AS last_updated
    from LAKE_MMOS."mmos_membership_marketing_eu"."promotion"
    where "_fivetran_deleted" = false
)
,promotion_user_group as (
    select *,55 site_id
    ,CONVERT_TIMEZONE('America/Los_Angeles', "_fivetran_synced")  AS last_updated
    from LAKE_MMOS."mmos_membership_marketing_shoedazzle"."promotion_user_group"
    where "_fivetran_deleted" = false
    union all 
    select *,26 site_id
    ,CONVERT_TIMEZONE('America/Los_Angeles', "_fivetran_synced")  AS last_updated
    from LAKE_MMOS."mmos_membership_marketing_us"."promotion_user_group"
    where "_fivetran_deleted" = false
    union all 
    select *,46 site_id
    ,CONVERT_TIMEZONE('America/Los_Angeles', "_fivetran_synced")  AS last_updated
    from LAKE_MMOS."mmos_membership_marketing_fabkids"."promotion_user_group"
    where "_fivetran_deleted" = false
    union all 
    select *,2000 site_id
    ,CONVERT_TIMEZONE('America/Los_Angeles', "_fivetran_synced")  AS last_updated
    from LAKE_MMOS."mmos_membership_marketing_eu"."promotion_user_group"
    where "_fivetran_deleted" = false
)
,promotion_rule as(
select
"promotion_id"                                             promotion_id
,case when "rule_type" in (1,3) then 'flat_price'
      when "rule_type" in (2,4) then 'percentage'
      else cast("rule_type"as VARCHAR(256)) end                  discount_type
,iff( "rule_type" in (1,3),"discount_value",null)       discount_percentage
,iff("rule_type" in (2,4)      ,"discount_value",null)  discount_rate
,site_id
,last_updated
,promo_id promo_id
from promotion_rule_cart
where "_fivetran_deleted" = false

union all
select
"promotion_id"                                        promotion_id
,case when "discount_type"=1 then 'flat_price'
      when "discount_type"=2 then 'percentage'
      when "discount_type"=3 then 'fixed_price'
      else cast("discount_type"as VARCHAR(256)) end  discount_type
,iff("discount_type" in (1,3) ,"discount_value",null)  discount_percentage
,iff("discount_type" = 2      ,"discount_value",null)  discount_rate
,site_id
,last_updated
,promo_id promo_id
from promotion_rule_product
where "_fivetran_deleted" = false

union all

select
"promotion_id" promotion_id
,null          discount_type
,null          discount_percentage
,null          discount_rate
,site_id
,last_updated
,promo_id promo_id
from promotion_rule_shipping
where "_fivetran_deleted" = false
)
select 
 t2.promo_id as promo_id          -- 促销活动 ID
,t1."promotion_name"    as promo_name        -- 促销活动名称
,t1."promotion_code"    as first_promo_code  -- 首个促销代码
,t1."promotion_code"    as promo_code        -- 促销代码
,t1."description"       as promo_description -- 促销活动描述
,t1."promotion_subtype" as promo_type        -- 促销类型
,null                   as promo_cms_type    -- 促销内容管理系统类型
,null                   as promo_cms_description -- 促销内容管理系统描述
,null                   as promo_classification -- 促销分类
,t2.discount_type       as discount_method -- 折扣方式
,iff("promotion_type" in (1,2) ,t1."label",'Unknown') as subtotal_discount  -- 
,iff("promotion_type" in (3) ,t1."label",'Unknown') as shipping_discount  -- 
,iff(DATEDIFF(second, '1970-01-01', DATEADD(day, -1, CURRENT_DATE())) BETWEEN t1."start_time" AND t1."end_time" or t1."end_time" =0,4750,4755 )           as promo_status_code -- 与促销状态关联的状态码
,iff(DATEDIFF(second, '1970-01-01', DATEADD(day, -1, CURRENT_DATE())) BETWEEN t1."start_time" AND t1."end_time" or t1."end_time" =0,'Active','Inactive' ) as promo_status -- 1 草稿 2 未发布 3 发布成功 4 发布失败
,CONVERT_TIMEZONE('UTC','America/Los_Angeles',TO_TIMESTAMP_NTZ(t1."start_time")) as promo_start_datetime -- 促销活动开始日期时间
,CONVERT_TIMEZONE('UTC','America/Los_Angeles',TO_TIMESTAMP_NTZ(t1."end_time")) as promo_end_datetime -- 促销活动结束日期时间
,null                   as promo_store_applied   -- 应用促销的渠道
,null                   as required_product_quantity   -- 所需产品数量
,null                   as min_product_quantity   -- 最小产品数量
,null as fpl_included -- 是否包含 FPL（需结合业务确定 FPL 含义）
,null as fpl_excluded -- 是否排除 FPL（需结合业务确定 FPL 含义）
,null as fpl_required -- 是否需要 FPL（需结合业务确定 FPL 含义）
,null as category_included -- 包含的类别
,null as category_excluded -- 排除的类别
,null as category_required -- 必需的类别
,null as promo_name_cms    -- 内容管理系统中的促销活动名称
,case t3."limit_user_type" when 1 then '不限制' 
                           when 2 then 'shopify用户集合' 
                           when 3 then '平台用户id集合' 
                           else null end as membership_choice -- 会员选择
,null as registration_date_option -- 注册日期选项
,null as registration_exact_date -- 注册确切日期
,null as registration_start_date -- 注册开始日期
,null as registration_end_date -- 注册结束日期
,null as registration_day -- 注册日
,null as registration_day_start -- 注册日开始时间
,null as registration_day_end -- 注册日结束时间
,null as gender_selected -- 选择的性别
,null as membership_reward_multiplier -- 会员奖励倍数
,null as is_deleted -- 是否已删除
,iff(dph.promo_id is not null,CURRENT_DATE(),
    CONVERT_TIMEZONE('UTC','America/Los_Angeles',TO_TIMESTAMP_NTZ(t1."created_at"))) as effective_start_datetime -- 生效开始日期时间
,'9999-12-31' as effective_end_datetime -- 生效结束日期时间
,1 as is_current -- 是否为当前有效
,CURRENT_DATE() as meta_create_datetime -- 元数据创建日期时间
,CURRENT_DATE() as meta_update_datetime -- 元数据更新日期时间
,t1.site_id site_id 
from promotion t1 
inner join promotion_rule t2  on t1."id" = t2.promotion_id and t1.site_id =t2.site_id
left join promotion_user_group  t3
    on t1."id" = t3."promotion_id" and t1.site_id =t3.site_id
left join EDW_PROD.NEW_STG.dim_promo_history  dph on dph.promo_id = t2.promo_id
where LEAST(
    COALESCE(t1.last_updated, '1900-01-01'::TIMESTAMP_NTZ),
    COALESCE(t2.last_updated, '1900-01-01'::TIMESTAMP_NTZ),
    COALESCE(t3.last_updated, '1900-01-01'::TIMESTAMP_NTZ)
    ) >= DATEADD(day, -1, CURRENT_DATE())
and LEAST(
    COALESCE(t1.last_updated, '1900-01-01'::TIMESTAMP_NTZ),
    COALESCE(t2.last_updated, '1900-01-01'::TIMESTAMP_NTZ),
    COALESCE(t3.last_updated, '1900-01-01'::TIMESTAMP_NTZ)
    ) < CURRENT_DATE()
;

-- 跟新历史数据状态 dim_product_price_history_update
MERGE INTO EDW_PROD.NEW_STG.DIM_PROMO_HISTORY tgt
USING EDW_PROD.NEW_STG._DIM_PROMO_HISTORY  src
    ON tgt.promo_id = src.promo_id and tgt.is_current = 1
WHEN MATCHED THEN UPDATE SET
    tgt.effective_end_datetime = current_date,
    tgt.is_current = 0,
    tgt.meta_update_datetime=current_date
;


insert into EDW_PROD.NEW_STG.dim_promo_history (
PROMO_ID,
PROMO_NAME,
FIRST_PROMO_CODE,
PROMO_CODE,
PROMO_DESCRIPTION,
PROMO_TYPE,
PROMO_CMS_TYPE,
PROMO_CMS_DESCRIPTION,
PROMO_CLASSIFICATION,
DISCOUNT_METHOD,
SUBTOTAL_DISCOUNT,
SHIPPING_DISCOUNT,
PROMO_STATUS_CODE,
PROMO_STATUS,
PROMO_START_DATETIME,
PROMO_END_DATETIME,
IS_DELETED,
PROMO_STORE_APPLIED,
REQUIRED_PRODUCT_QUANTITY,
MIN_PRODUCT_QUANTITY,
FPL_INCLUDED,
FPL_EXCLUDED,
FPL_REQUIRED,
CATEGORY_INCLUDED,
CATEGORY_EXCLUDED,
CATEGORY_REQUIRED,
PROMO_NAME_CMS,
MEMBERSHIP_CHOICE,
REGISTRATION_DATE_OPTION,
REGISTRATION_EXACT_DATE,
REGISTRATION_START_DATE,
REGISTRATION_END_DATE,
REGISTRATION_DAY,
REGISTRATION_DAY_START,
REGISTRATION_DAY_END,
GENDER_SELECTED,
MEMBERSHIP_REWARD_MULTIPLIER,
EFFECTIVE_START_DATETIME,
EFFECTIVE_END_DATETIME,
IS_CURRENT,
META_CREATE_DATETIME,
META_UPDATE_DATETIME,
site_id
) 
select 
*
from EDW_PROD.NEW_STG._DIM_PROMO_HISTORY
;