truncate table EDW_PROD.NEW_STG.FACT_REGISTRATION;
-- fact_registration
insert into EDW_PROD.NEW_STG.FACT_REGISTRATION
select
     null REGISTRATION_KEY,
     null MEMBERSHIP_EVENT_KEY,
     26 as store_id,
     t1."id"::NUMBER(38,0) CUSTOMER_ID,
     null META_ORIGINAL_CUSTOMER_ID,
     TO_CHAR(TO_TIMESTAMP_TZ(t1."created_at"), 'YYYY-MM-DD HH24:MI:SS') AS registration_local_datetime,
     null as session_id,
     null as is_reactivated_lead,
     null as uri,
     null as hdyh_values_shown,
     null as cdetail_lead_type,
     null as cdetail_origin,
     null as cdetail_initial_medium,
     null as how_did_you_hear,
     null as dm_gateway_id,
     null as dm_site_id,
     null as utm_medium,
     null as utm_source,
     null as utm_campaign,
     null as utm_content,
     null as utm_term,
     null as ccode,
     null as pcode,
     null as scode,
     null as acode,
     null as master_product_id,
     case when t1."register_device_platform" in ('ios','android') then 'Mobile'
          when t1."register_device_platform" in ('web','h5') then 'Desktop'
          else 'Unknown' end as device,
     null as browser,
     case when t1."register_device_platform" ='ios' then 'IOS'
          when t1."register_device_platform" ='android' then 'Android'
          else 'Unknown' end as operating_system,
     null as META_ROW_HASH,
     null as META_CREATE_DATETIME,
     null as META_UPDATE_DATETIME,
     null as IS_DELETED,
     null as IS_TEST_CUSTOMER,
     null as registration_channel_type,
     null as registration_channel,
     null as registration_subchannel,
     false is_secondary_registration,
     false is_secondary_registration_after_vip,
     null as has_membership_password,
     null as has_membership_password_first_value,
     false as is_retail_registration,
     null as retail_location,
     null as uri_utm_medium,
     null as uri_utm_source,
     null as uri_utm_campaign,
     null as uri_utm_content,
     null as uri_utm_term,
     null as uri_device,
     null as uri_browser,
     null as rpt_utm_medium,
     null as rpt_utm_source,
     null as rpt_utm_campaign,
     null as rpt_utm_content,
     null as rpt_utm_term,
     null as rpt_device,
     null as rpt_browser,
     false as is_fake_retail_registration,
     null as IS_REACTIVATED_LEAD_RAW,
     null as how_did_you_hear_parent
FROM LAKE_MMOS."mmos_membership_marketing_us"."user_shard_all" t1
-- left join edw_prod.stg.fact_membership_event t2 on t1."member_number" = t2.customer_id and t2.membership_event_type = 'registration'
where TO_TIMESTAMP_TZ(t1."created_at") >= '2025-09-23'
and "is_delete" = 0

union all 

select
     null REGISTRATION_KEY,
     null MEMBERSHIP_EVENT_KEY,
     55 as store_id,
     t1."id"::NUMBER(38,0) CUSTOMER_ID,
     null META_ORIGINAL_CUSTOMER_ID,
     TO_CHAR(TO_TIMESTAMP_TZ(t1."created_at"), 'YYYY-MM-DD HH24:MI:SS') AS registration_local_datetime,
     null as session_id,
     null as is_reactivated_lead,
     null as uri,
     null as hdyh_values_shown,
     null as cdetail_lead_type,
     null as cdetail_origin,
     null as cdetail_initial_medium,
     null as how_did_you_hear,
     null as dm_gateway_id,
     null as dm_site_id,
     null as utm_medium,
     null as utm_source,
     null as utm_campaign,
     null as utm_content,
     null as utm_term,
     null as ccode,
     null as pcode,
     null as scode,
     null as acode,
     null as master_product_id,
     case when t1."register_device_platform" in ('ios','android') then 'Mobile'
          when t1."register_device_platform" in ('web','h5') then 'Desktop'
          else 'Unknown' end as device,
     null as browser,
     case when t1."register_device_platform" ='ios' then 'IOS'
          when t1."register_device_platform" ='android' then 'Android'
          else 'Unknown' end as operating_system,
     null as META_ROW_HASH,
     null as META_CREATE_DATETIME,
     null as META_UPDATE_DATETIME,
     null as IS_DELETED,
     null as IS_TEST_CUSTOMER,
     null as registration_channel_type,
     null as registration_channel,
     null as registration_subchannel,
     false is_secondary_registration,
     false is_secondary_registration_after_vip,
     null as has_membership_password,
     null as has_membership_password_first_value,
     false as is_retail_registration,
     null as retail_location,
     null as uri_utm_medium,
     null as uri_utm_source,
     null as uri_utm_campaign,
     null as uri_utm_content,
     null as uri_utm_term,
     null as uri_device,
     null as uri_browser,
     null as rpt_utm_medium,
     null as rpt_utm_source,
     null as rpt_utm_campaign,
     null as rpt_utm_content,
     null as rpt_utm_term,
     null as rpt_device,
     null as rpt_browser,
     false as is_fake_retail_registration,
     null as IS_REACTIVATED_LEAD_RAW,
     null as how_did_you_hear_parent
FROM LAKE_MMOS."mmos_membership_marketing_shoedazzle"."user_shard_all" t1
-- left join edw_prod.stg.fact_membership_event t2 on t1."member_number" = t2.customer_id and t2.membership_event_type = 'registration'
where TO_TIMESTAMP_TZ(t1."created_at") >= '2025-09-15'
and "is_delete" = 0
union all

select
     null REGISTRATION_KEY,
     null MEMBERSHIP_EVENT_KEY,
     46 as store_id,
     t1."id"::NUMBER(38,0) CUSTOMER_ID,
     null META_ORIGINAL_CUSTOMER_ID,
     TO_CHAR(TO_TIMESTAMP_TZ(t1."created_at"), 'YYYY-MM-DD HH24:MI:SS') AS registration_local_datetime,
     null as session_id,
     null as is_reactivated_lead,
     null as uri,
     null as hdyh_values_shown,
     null as cdetail_lead_type,
     null as cdetail_origin,
     null as cdetail_initial_medium,
     null as how_did_you_hear,
     null as dm_gateway_id,
     null as dm_site_id,
     null as utm_medium,
     null as utm_source,
     null as utm_campaign,
     null as utm_content,
     null as utm_term,
     null as ccode,
     null as pcode,
     null as scode,
     null as acode,
     null as master_product_id,
     case when t1."register_device_platform" in ('ios','android') then 'Mobile'
          when t1."register_device_platform" in ('web','h5') then 'Desktop'
          else 'Unknown' end as device,
     null as browser,
     case when t1."register_device_platform" ='ios' then 'IOS'
          when t1."register_device_platform" ='android' then 'Android'
          else 'Unknown' end as operating_system,
     null as META_ROW_HASH,
     null as META_CREATE_DATETIME,
     null as META_UPDATE_DATETIME,
     null as IS_DELETED,
     null as IS_TEST_CUSTOMER,
     null as registration_channel_type,
     null as registration_channel,
     null as registration_subchannel,
     false is_secondary_registration,
     false is_secondary_registration_after_vip,
     null as has_membership_password,
     null as has_membership_password_first_value,
     false as is_retail_registration,
     null as retail_location,
     null as uri_utm_medium,
     null as uri_utm_source,
     null as uri_utm_campaign,
     null as uri_utm_content,
     null as uri_utm_term,
     null as uri_device,
     null as uri_browser,
     null as rpt_utm_medium,
     null as rpt_utm_source,
     null as rpt_utm_campaign,
     null as rpt_utm_content,
     null as rpt_utm_term,
     null as rpt_device,
     null as rpt_browser,
     false as is_fake_retail_registration,
     null as IS_REACTIVATED_LEAD_RAW,
     null as how_did_you_hear_parent
FROM LAKE_MMOS."mmos_membership_marketing_fabkids".user_shard_all t1
-- left join edw_prod.stg.fact_membership_event t2 on t1."member_number" = t2.customer_id and t2.membership_event_type = 'registration'
where TO_TIMESTAMP_TZ(t1."created_at") >= '2025-10-15'
and "is_delete" = 0
;


UPDATE EDW_PROD.NEW_STG.FACT_REGISTRATION AS tgt                   -- 要更新的目标表
set 
    tgt.utm_medium = src.utm_medium,
    tgt.utm_source = src.utm_source,
    tgt.utm_campaign = src.utm_campaign,
    tgt.utm_content = src.utm_content,
    tgt.utm_term = src.utm_term
FROM ( 
     with t1 as (
         select 
           iff(us."id" is null,LEAD(us."id") IGNORE NULLS OVER (partition by t1.anonymous_id,t1.store_id,date(CONVERT_TIMEZONE('UTC','America/Los_Angeles',t1.timestamp)) ORDER BY CONVERT_TIMEZONE('UTC','America/Los_Angeles',t1.timestamp))
             ,us."id") customer_id
         ,t1.SESSION_ID
         ,t1.store_id store_id
         ,t1.CONTEXT_CAMPAIGN_MEDIUM as utm_medium
         ,t1.CONTEXT_CAMPAIGN_SOURCE as utm_source
         ,t1.CONTEXT_CAMPAIGN_NAME as utm_campaign
         ,t1.CONTEXT_CAMPAIGN_CONTENT as utm_content
         ,t1.CONTEXT_CAMPAIGN_TERM as utm_term
         ,CONVERT_TIMEZONE('UTC','America/Los_Angeles',t1.timestamp)::TIMESTAMP_TZ(3) registration_local_datetime
         ,date(CONVERT_TIMEZONE('UTC','America/Los_Angeles',t1.timestamp)) registration_local_date
         ,2 label
         ,anonymous_id
         from SEGMENT_EVENTS.JAVASCRIPT_JUSTFAB_NA_PROD.PAGES t1
         left join LAKE_MMOS."mmos_membership_marketing_shoedazzle"."user_shard_all" us on t1.shopify_customer_id = us."user_gid"

         union all 

                  select 
           iff(us."id" is null,LEAD(us."id") IGNORE NULLS OVER (partition by t1.anonymous_id,t1.store_id,date(CONVERT_TIMEZONE('UTC','America/Los_Angeles',t1.timestamp)) ORDER BY CONVERT_TIMEZONE('UTC','America/Los_Angeles',t1.timestamp))
             ,us."id") customer_id
         ,t1.SESSION_ID
         ,t1.store_id store_id
         ,t1.CONTEXT_CAMPAIGN_MEDIUM as utm_medium
         ,t1.CONTEXT_CAMPAIGN_SOURCE as utm_source
         ,t1.CONTEXT_CAMPAIGN_NAME as utm_campaign
         ,t1.CONTEXT_CAMPAIGN_CONTENT as utm_content
         ,t1.CONTEXT_CAMPAIGN_TERM as utm_term
         ,CONVERT_TIMEZONE('UTC','America/Los_Angeles',t1.timestamp)::TIMESTAMP_TZ(3) registration_local_datetime
         ,date(CONVERT_TIMEZONE('UTC','America/Los_Angeles',t1.timestamp)) registration_local_date
         ,2 label
         ,anonymous_id
         from SEGMENT_EVENTS.JAVASCRIPT_JUSTFAB_NA_PROD2.PAGES t1
         left join LAKE_MMOS."mmos_membership_marketing_us"."user_shard_all" us on t1.shopify_customer_id = us."user_gid"

         union all 
     
                  select 
          customer_id
         ,cast(SESSION_ID as varchar) SESSION_ID
         ,store_id
         ,utm_medium
         ,utm_source
         ,utm_campaign
         ,utm_content
         ,utm_term
         ,registration_local_datetime
         ,date(registration_local_datetime) registration_local_date
         ,1 label
         ,null anonymous_id
         -- ,cast(null as VARCHAR(16777216)) anonymous_id
         from EDW_PROD.NEW_STG.FACT_REGISTRATION
         where store_id in (55,26)
     )
     ,t2 as (
     select 
          customer_id
         ,LAG(session_id) IGNORE NULLS OVER (partition by customer_id,store_id,registration_local_date ORDER BY registration_local_datetime) AS session_id
         ,store_id
         ,LAG(utm_medium) IGNORE NULLS OVER (partition by customer_id,store_id,registration_local_date ORDER BY registration_local_datetime) AS utm_medium
         ,LAG(utm_source) IGNORE NULLS OVER (partition by customer_id,store_id,registration_local_date ORDER BY registration_local_datetime) AS utm_source
         ,LAG(utm_campaign) IGNORE NULLS OVER (partition by customer_id,store_id,registration_local_date ORDER BY registration_local_datetime) AS utm_campaign
         ,LAG(utm_content) IGNORE NULLS OVER (partition by customer_id,store_id,registration_local_date ORDER BY registration_local_datetime) AS utm_content
         ,LAG(utm_term) IGNORE NULLS OVER (partition by customer_id,store_id,registration_local_date ORDER BY registration_local_datetime) AS utm_term
         ,registration_local_datetime
         ,registration_local_date
         ,label
     from t1
     )
     select 
      customer_id
     ,registration_local_datetime
     ,store_id
     ,utm_medium
     ,utm_source
     ,utm_campaign
     ,utm_content
     ,utm_term
     ,label
     from t2 
     -- where customer_id =1958536024606900224
     where 
     label =1 
     and (
         utm_medium  is not null or 
         utm_source  is not null or 
         utm_campaign  is not null or 
         utm_content  is not null or 
         utm_term  is not null
     )
)AS src
WHERE 
     tgt.customer_id = src.customer_id
     and tgt.registration_local_datetime = src.registration_local_datetime
     and tgt.store_id = src.store_id
;

