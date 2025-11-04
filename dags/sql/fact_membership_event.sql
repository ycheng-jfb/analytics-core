truncate table EDW_PROD.NEW_STG.FACT_MEMBERSHIP_EVENT;
insert into EDW_PROD.NEW_STG.FACT_MEMBERSHIP_EVENT
with t1 as (
SELECT
    fme.membership_event_key,
    EDW_PROD.stg.udf_unconcat_brand(fme.customer_id) AS customer_id,
    fme.store_id,
    EDW_PROD.stg.udf_unconcat_brand(fme.order_id) AS order_id,
    EDW_PROD.stg.udf_unconcat_brand(fme.session_id) AS session_id,
    fme.membership_event_type_key,
    fme.membership_event_type,
    fme.membership_type_detail,
    fme.membership_state,
    fme.event_start_local_datetime,
    fme.event_end_local_datetime,
    fme.recent_activation_local_datetime,
    fme.is_scrubs_customer,
    fme.is_current,
    fme.meta_create_datetime,
    fme.meta_update_datetime
FROM EDW_PROD.stg.fact_membership_event AS fme
    LEFT JOIN EDW_PROD.stg.dim_store AS ds
        ON fme.store_id = ds.store_id
WHERE NOT fme.is_deleted
    AND NOT NVL(fme.is_test_customer, FALSE)
    AND ds.store_brand NOT IN ('Legacy')
    AND fme.membership_event_type not in ('Hard Cancellation from E-Comm')
    AND substring(fme.customer_id, -2) = '10'
    and fme.store_id in (55,26,46)

union all 

select
    t1."id"::NUMBER(38,0) membership_event_key,
    t1."user_id"::NUMBER(38,0) CUSTOMER_ID,
    55 as STORE_ID,
    iff(t1."shopify_order_id"='',null,t1."shopify_order_id"::NUMBER(38,0)) as ORDER_ID,
    null as session_id,
    case when t1."event" in('create_subscription','reactivation_subscription')  then 3
         when t1."event" = 'cancel_subscription' then 5
    else null end as membership_event_type_key,
    case when t1."event" in('create_subscription','reactivation_subscription')  then 'Activation'
         when t1."event" = 'cancel_subscription' then 'Cancellation'
    else null end as membership_event_type,
     case when t1."event" in('create_subscription','reactivation_subscription')  then 'Monthly'
         when t1."event" = 'cancel_subscription' then 'Soft'
    else null end as membership_type_detail,
    case membership_event_type when 'Activation'   then 'VIP'
                           when 'Cancellation' then 'Cancelled'
                           else null end  as membership_state,
    TO_TIMESTAMP_TZ(t1."created_at") as event_start_local_datetime,
    lead(TO_TIMESTAMP_TZ(t1."created_at"),1,'9999-12-31'::TIMESTAMP) over (partition by t1."user_id" order by t1."created_at") as event_end_local_datetime,
    TO_TIMESTAMP_TZ(t2."last_join_vip_time") as recent_activation_local_datetime,
    null as is_scrubs_customer,
    iff(event_end_local_datetime = '9999-12-31' ,1,0) as is_current,
    current_date as meta_create_datetime,
    current_date as meta_update_datetime
from LAKE_MMOS."mmos_membership_marketing_shoedazzle"."user_subscription_actions_shard_all" t1
left join LAKE_MMOS."mmos_membership_marketing_shoedazzle"."user_shard_all" t2
    on t1."user_id" = t2."id"
where t1."is_done" = 1
  and t1."event" in ('create_subscription','reactivation_subscription','cancel_subscription')
  and t1."source" in (2)
  and TO_TIMESTAMP_TZ(t1."created_at")>='2025-09-15'

union all 

select
    t1."id"::NUMBER(38,0) membership_event_key,
    t1."user_id"::NUMBER(38,0) CUSTOMER_ID,
    26 as STORE_ID,
    iff(t1."shopify_order_id"='',null,t1."shopify_order_id"::NUMBER(38,0)) as ORDER_ID,
    null as session_id,
    case when t1."event" in('create_subscription','reactivation_subscription')  then 3
         when t1."event" = 'cancel_subscription' then 5
    else null end as membership_event_type_key,
    case when t1."event" in('create_subscription','reactivation_subscription')  then 'Activation'
         when t1."event" = 'cancel_subscription' then 'Cancellation'
    else null end as membership_event_type,
     case when t1."event" in('create_subscription','reactivation_subscription')  then 'Monthly'
         when t1."event" = 'cancel_subscription' then 'Soft'
    else null end as membership_type_detail,
    case membership_event_type when 'Activation'   then 'VIP'
                           when 'Cancellation' then 'Cancelled'
                           else null end  as membership_state,
    TO_TIMESTAMP_TZ(t1."created_at") as event_start_local_datetime,
    lead(TO_TIMESTAMP_TZ(t1."created_at"),1,'9999-12-31'::TIMESTAMP) over (partition by t1."user_id" order by t1."created_at") as event_end_local_datetime,
    TO_TIMESTAMP_TZ(t2."last_join_vip_time") as recent_activation_local_datetime,
    null as is_scrubs_customer,
    iff(event_end_local_datetime = '9999-12-31' ,1,0) as is_current,
    current_date as meta_create_datetime,
    current_date as meta_update_datetime
from LAKE_MMOS."mmos_membership_marketing_us"."user_subscription_actions_shard_all" t1
left join LAKE_MMOS."mmos_membership_marketing_us"."user_shard_all" t2
    on t1."user_id" = t2."id"
where t1."is_done" = 1
  and t1."event" in ('create_subscription','reactivation_subscription','cancel_subscription')
  and t1."source" in (2)
  and TO_TIMESTAMP_TZ(t1."created_at")>='2025-09-23'

union all

select
    t1."id"::NUMBER(38,0) membership_event_key,
    t1."user_id"::NUMBER(38,0) CUSTOMER_ID,
    46 as STORE_ID,
    iff(t1."shopify_order_id"='',null,t1."shopify_order_id"::NUMBER(38,0)) as ORDER_ID,
    null as session_id,
    case when t1."event" in('create_subscription','reactivation_subscription')  then 3
         when t1."event" = 'cancel_subscription' then 5
    else null end as membership_event_type_key,
    case when t1."event" in('create_subscription','reactivation_subscription')  then 'Activation'
         when t1."event" = 'cancel_subscription' then 'Cancellation'
    else null end as membership_event_type,
     case when t1."event" in('create_subscription','reactivation_subscription')  then 'Monthly'
         when t1."event" = 'cancel_subscription' then 'Soft'
    else null end as membership_type_detail,
    case membership_event_type when 'Activation'   then 'VIP'
                           when 'Cancellation' then 'Cancelled'
                           else null end  as membership_state,
    TO_TIMESTAMP_TZ(t1."created_at") as event_start_local_datetime,
    lead(TO_TIMESTAMP_TZ(t1."created_at"),1,'9999-12-31'::TIMESTAMP) over (partition by t1."user_id" order by t1."created_at") as event_end_local_datetime,
    TO_TIMESTAMP_TZ(t2."last_join_vip_time") as recent_activation_local_datetime,
    null as is_scrubs_customer,
    iff(event_end_local_datetime = '9999-12-31' ,1,0) as is_current,
    current_date as meta_create_datetime,
    current_date as meta_update_datetime
from LAKE_MMOS."mmos_membership_marketing_fabkids"."user_subscription_actions_shard_all" t1
left join LAKE_MMOS."mmos_membership_marketing_fabkids".user_shard_all t2
    on t1."user_id" = t2."id"
where t1."is_done" = 1
  and t1."event" in ('create_subscription','reactivation_subscription','cancel_subscription')
  and t1."source" in (2)
  and TO_TIMESTAMP_TZ(t1."created_at")>='2025-10-15'

union all 

select
    t1."id"::NUMBER(38,0) membership_event_key,
    t1."user_id"::NUMBER(38,0) CUSTOMER_ID,
    t2.store_id as STORE_ID,
    iff(t1."shopify_order_id"='',null,t1."shopify_order_id"::NUMBER(38,0)) as ORDER_ID,
    null as session_id,
    case when t1."event" in('create_subscription','reactivation_subscription')  then 3
         when t1."event" = 'cancel_subscription' then 5
    else null end as membership_event_type_key,
    case when t1."event" in('create_subscription','reactivation_subscription')  then 'Activation'
         when t1."event" = 'cancel_subscription' then 'Cancellation'
    else null end as membership_event_type,
     case when t1."event" in('create_subscription','reactivation_subscription')  then 'Monthly'
         when t1."event" = 'cancel_subscription' then 'Soft'
    else null end as membership_type_detail,
    case membership_event_type when 'Activation'   then 'VIP'
                           when 'Cancellation' then 'Cancelled'
                           else null end  as membership_state,
    TO_TIMESTAMP_TZ(t1."created_at") as event_start_local_datetime,
    lead(TO_TIMESTAMP_TZ(t1."created_at"),1,'9999-12-31'::TIMESTAMP) over (partition by t1."user_id" order by t1."created_at") as event_end_local_datetime,
    TO_TIMESTAMP_TZ(t2."last_join_vip_time") as recent_activation_local_datetime,
    null as is_scrubs_customer,
    iff(event_end_local_datetime = '9999-12-31' ,1,0) as is_current,
    current_date as meta_create_datetime,
    current_date as meta_update_datetime
from LAKE_MMOS."mmos_membership_marketing_eu".USER_SUBSCRIPTION_ACTIONS_SHARD_ALL t1
left join LAKE_MMOS."mmos_membership_marketing_eu".USER_SHARD_ALL t2
    on t1."user_id" = t2."id"
where t1."is_done" = 1
  and t1."event" in ('create_subscription','reactivation_subscription','cancel_subscription')
  and t1."source" in (2)
  and TO_TIMESTAMP_TZ(t1."created_at")>='2025-10-23'
)
select 
    membership_event_key,
    customer_id,
    store_id,
    order_id,
    session_id,
    membership_event_type_key,
    membership_event_type,
    membership_type_detail,
    membership_state,
    event_start_local_datetime,
    -- event_end_local_datetime,
    iff(event_end_local_datetime = '9999-12-31',(lead(TO_TIMESTAMP_TZ(event_start_local_datetime),1,'9999-12-31'::TIMESTAMP) over (partition by customer_id,store_id order by event_start_local_datetime)),event_end_local_datetime) event_end_local_datetime_1,
    recent_activation_local_datetime,
    is_scrubs_customer,
    iff(event_end_local_datetime_1 = '9999-12-31' ,1,0) as is_current,
    meta_create_datetime,
    meta_update_datetime
from t1 


union all 

select 
     null membership_event_key
    ,t1."id" CUSTOMER_ID
    ,55 as STORE_ID
    ,null ORDER_ID
    ,null as session_id
    ,2 as membership_event_type_key
    ,'Registration' as membership_event_type
    ,'Regular' as  membership_type_detail
    ,'Lead' as membership_state
    ,TO_TIMESTAMP_TZ(t1."created_at") as event_start_local_datetime
    ,null as event_end_local_datetime
    ,TO_TIMESTAMP_TZ(t1."last_join_vip_time") as recent_activation_local_datetime
    ,null as is_scrubs_customer
    ,1 is_current
    ,current_date as meta_create_datetime
    ,current_date as meta_update_datetime
from LAKE_MMOS."mmos_membership_marketing_shoedazzle"."user_shard_all" t1
where TO_TIMESTAMP_TZ(t1."created_at")>='2025-09-15' and t1."is_delete" = 0

union all 

select 
     null membership_event_key
    ,t1."id" CUSTOMER_ID
    ,46 as STORE_ID
    ,null ORDER_ID
    ,null as session_id
    ,2 as membership_event_type_key
    ,'Registration' as membership_event_type
    ,'Regular' as  membership_type_detail
    ,'Lead' as membership_state
    ,TO_TIMESTAMP_TZ(t1."created_at") as event_start_local_datetime
    ,null as event_end_local_datetime
    ,TO_TIMESTAMP_TZ(t1."last_join_vip_time") as recent_activation_local_datetime
    ,null as is_scrubs_customer
    ,1 is_current
    ,current_date as meta_create_datetime
    ,current_date as meta_update_datetime
from LAKE_MMOS."mmos_membership_marketing_fabkids".user_shard_all t1
where TO_TIMESTAMP_TZ(t1."created_at")>='2025-10-15' and t1."is_delete" = 0

union all

select
     null membership_event_key
    ,t1."id" CUSTOMER_ID
    ,26 as STORE_ID
    ,null ORDER_ID
    ,null as session_id
    ,2 as membership_event_type_key
    ,'Registration' as membership_event_type
    ,'Regular' as  membership_type_detail
    ,'Lead' as membership_state
    ,TO_TIMESTAMP_TZ(t1."created_at") as event_start_local_datetime
    ,null as event_end_local_datetime
    ,TO_TIMESTAMP_TZ(t1."last_join_vip_time") as recent_activation_local_datetime
    ,null as is_scrubs_customer
    ,1 is_current
    ,current_date as meta_create_datetime
    ,current_date as meta_update_datetime
from LAKE_MMOS."mmos_membership_marketing_us"."user_shard_all" t1
where TO_TIMESTAMP_TZ(t1."created_at")>='2025-10-15' and t1."is_delete" = 0

union all

select
     null membership_event_key
    ,t1."id" CUSTOMER_ID
    ,t1.store_id as STORE_ID
    ,null ORDER_ID
    ,null as session_id
    ,2 as membership_event_type_key
    ,'Registration' as membership_event_type
    ,'Regular' as  membership_type_detail
    ,'Lead' as membership_state
    ,TO_TIMESTAMP_TZ(t1."created_at") as event_start_local_datetime
    ,null as event_end_local_datetime
    ,TO_TIMESTAMP_TZ(t1."last_join_vip_time") as recent_activation_local_datetime
    ,null as is_scrubs_customer
    ,1 is_current
    ,current_date as meta_create_datetime
    ,current_date as meta_update_datetime
from LAKE_MMOS."mmos_membership_marketing_eu".USER_SHARD_ALL t1
where TO_TIMESTAMP_TZ(t1."created_at")>='2025-10-15' and t1."is_delete" = 0

;


