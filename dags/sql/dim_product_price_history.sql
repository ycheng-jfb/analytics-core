CREATE OR REPLACE TEMP TABLE EDW_PROD.NEW_STG.DIM_PRODUCT_PRICE_HISTORY_TEMP AS 
    with vip_credit_activity as (
    select * ,26 store_id
    from LAKE_MMOS."mmos_membership_marketing_us"."vip_credit_activity"
    where "_fivetran_deleted"=false 
    union all
    select * ,55 store_id
    from LAKE_MMOS."mmos_membership_marketing_shoedazzle"."vip_credit_activity"
    where "_fivetran_deleted"=false
    )
    , COLLECTION_PRODUCT as (
    select * ,26 store_id
    from LAKE_MMOS.SHOPIFY_JUSTFAB_PROD.COLLECTION_PRODUCT
    union all
    select * ,55 store_id
    from LAKE_MMOS.SHOPIFY_SHOEDAZZLE_PROD.COLLECTION_PRODUCT
    )
    ,is_two_for_one as ( 
    select distinct 
    b.product_id master_product_id,b.store_id
    from vip_credit_activity a 
    join COLLECTION_PRODUCT b 
    on a."collection_gid"=b.collection_id and a.store_id = b.store_id
    where a."product_num"=2 and "credit_num"=1
    )
    select 
     t2.product_id
    ,t2.master_product_id
    ,t2.store_id
    ,t2.vip_unit_price
    ,t2.retail_unit_price
    ,t2.sale_price
    ,t2.meta_create_datetime as effective_start_datetime
    -- ,'9999-12-31' effective_end_datetime
    -- ,1 as is_current
    -- ,current_date as meta_create_datetime
    -- ,current_date as meta_update_datetime
    ,t1.product_price_history_key
    ,iff(t3.master_product_id is not null,1,0) is_two_for_one
    from EDW_PROD.NEW_STG.DIM_PRODUCT t2
    left join EDW_PROD.NEW_STG.DIM_PRODUCT_PRICE_HISTORY t1 on t1.product_id = t2.product_id and t1.store_id = t2.store_id and t1.is_current = 1 
    left join is_two_for_one t3 on t3.master_product_id = t2.master_product_id and t3.store_id = t2.store_id
    where t1.vip_unit_price <> t2.vip_unit_price or t1.retail_unit_price <> t2.retail_unit_price or t1.product_price_history_key is null
;

-- 跟新历史数据状态 dim_product_price_history_update
MERGE INTO EDW_PROD.NEW_STG.DIM_PRODUCT_PRICE_HISTORY tgt
USING EDW_PROD.NEW_STG.DIM_PRODUCT_PRICE_HISTORY_TEMP src
    ON tgt.product_price_history_key = src.product_price_history_key and src.product_price_history_key is not null
WHEN MATCHED THEN UPDATE SET
    tgt.effective_end_datetime = current_date,
    tgt.is_current = 0,
    tgt.meta_update_datetime=current_date
;


-- 初始化代码
-- INSERT INTO EDW_PROD.NEW_STG.dim_product_price_history (
-- product_id,
-- master_product_id,
-- store_id,
-- vip_unit_price,
-- retail_unit_price,
-- sale_price,
-- effective_start_datetime,
-- effective_end_datetime,
-- is_current,
-- meta_create_datetime,
-- meta_update_datetime
-- )
-- select 
--  t1.product_id
-- ,t1.master_product_id
-- ,t1.store_id
-- ,t1.vip_unit_price
-- ,t1.retail_unit_price
-- ,t1.sale_price
-- ,t1.meta_create_datetime as effective_start_datetime
-- ,'9999-12-31' effective_end_datetime
-- ,1 as is_current
-- ,current_date as meta_create_datetime
-- ,current_date as meta_update_datetime
-- from EDW_PROD.NEW_STG.DIM_PRODUCT t1
-- 


-- create or replace TABLE EDW_PROD.NEW_STG.DIM_PRODUCT_PRICE_HISTORY (
--     product_price_history_key number(38,0) autoincrement start 10000000000 increment 1 order,
--     product_id number(38,0) not null,
--     master_product_id number(38,0),
--     store_id number(38,0),
--     vip_unit_price number(19,4),
--     retail_unit_price number(19,4),
--     sale_price number(19,4),
--     effective_start_datetime timestamp_ltz(9),
--     effective_end_datetime timestamp_ltz(9),
--     is_current boolean,
--     meta_create_datetime timestamp_ltz(9),
--     meta_update_datetime timestamp_ltz(9),
--     primary key (product_id),
--     unique (product_price_history_key)
-- )



-- 需要更新的数据和插入的数据
-- dim_product_price_history_temp.sql

-- 跟新历史数据状态
-- dim_product_price_history_update.sql


-- 插入新数据
INSERT INTO EDW_PROD.NEW_STG.DIM_PRODUCT_PRICE_HISTORY (
product_id,
master_product_id,
store_id,
vip_unit_price,
retail_unit_price,
sale_price,
effective_start_datetime,
effective_end_datetime,
is_current,
meta_create_datetime,
meta_update_datetime,
is_two_for_one
)
select 
 t1.product_id
,t1.master_product_id
,t1.store_id
,t1.vip_unit_price
,t1.retail_unit_price
,t1.sale_price
,iff(t1.product_price_history_key is not null,current_date,t1.effective_start_datetime) as effective_start_datetime
,'9999-12-31' effective_end_datetime
,1 as is_current
,current_date as meta_create_datetime
,current_date as meta_update_datetime
,is_two_for_one
from EDW_PROD.NEW_STG.DIM_PRODUCT_PRICE_HISTORY_TEMP t1
;
