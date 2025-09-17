SET current_end_date_hq = (SELECT (MIN(t.max_hq_datetime))
                           FROM (
                                    SELECT MAX(convert_timezone('America/Los_Angeles',SESSION_LOCAL_DATETIME))::datetime as max_hq_datetime FROM reporting_base_prod.SHARED.SESSION
                                    UNION SELECT MAX(convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME))::datetime as max_hq_datetime from reporting_base_prod.SHARED.SESSION_AB_TEST_START
                                ) t);

CREATE OR REPLACE TEMPORARY TABLE _oof_tests as
select *, case when adjusted_end_datetime = '1900-01-01' then '9999-12-01' else adjusted_end_datetime end as adjusted_end_datetime_adj
from lake_view.sharepoint.ab_test_metadata_import_oof
where test_type ilike '%Detail'
and status <> 'Closed / Paused';

delete from _oof_tests
    where
        test_key = 'pdp' and test_label = 'fl bmig pdp migration, bug issues'
            or test_key = 'pdp' and test_label = 'fl bmig pdp migration, 3/28'
            or test_key = 'pdp' and test_label = 'fl bmig pdp migration, 6/3';

CREATE OR REPLACE TEMP TABLE _non_framework_session_detail AS
SELECT distinct
    test_type
    ,ticket as TEST_FRAMEWORK_TICKET
    ,test_key
    ,test_label AS test_name
    ,test_label
    ,brand
    ,region
    ,test_platforms
    ,oo.membership_state
    ,gender
    ,test_start_location
    ,test_split
    ,adjusted_activated_datetime AS test_activated_datetime_pst
    ,status
    ,sd.SESSION_ID
    ,value AS ab_test_segment
    ,case when sd.value between control_start and control_end or sd.value = control_start then 'Control'
        when (sd.value between variant_a_start and variant_a_end and variant_b_start = 'N/A') or value = variant_a_start then 'Variant'
        when (sd.value between variant_a_start and variant_a_end and variant_b_start <> 'N/A') or value = variant_a_start then 'Variant A'
        when sd.value between variant_b_start and variant_b_end or value = variant_b_start then 'Variant B'
        when sd.value between variant_c_start and variant_c_end or value = variant_c_start then 'Variant C'
        when sd.value between variant_d_start and variant_d_end or value = variant_d_start then 'Variant D'
        else null end as test_group
    ,sd.DATETIME_ADDED as test_start_datetime_added_pst
    ,CASE WHEN f.MEMBERSHIP_STATE IS NULL THEN 'Prospect' else f.MEMBERSHIP_STATE end AS test_start_membership_state
    ,CASE WHEN f.MEMBERSHIP_STATE = 'VIP' then datediff('month',CONVERT_TIMEZONE('America/Los_Angeles', EVENT_START_LOCAL_DATETIME)::datetime,sd.DATETIME_ADDED) + 1 else null end AS test_start_vip_month_tenure
    ,CASE WHEN f.MEMBERSHIP_STATE = 'Lead' then datediff('day',CONVERT_TIMEZONE('America/Los_Angeles', EVENT_START_LOCAL_DATETIME)::datetime,sd.DATETIME_ADDED) + 1 else null end AS test_start_lead_daily_tenure
    ,case when t.STORE_REGION = 'NA' and oo.region in ('NA','Global') then TRUE
           when t.STORE_REGION = 'EU' and oo.region in ('EU','Global') then TRUE
               else FALSE end as is_valid_session
    ,IS_BOT
    ,case when yty_exclusion = true and t.store_brand = 'Yitty' then FALSE else TRUE end as is_valid_brand_session
    ,s.platform,
     NVL(is_in_segment, TRUE) as is_in_segment
FROM _oof_tests as oo
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.SESSION_DETAIL as sd on sd.name = oo.test_key
join reporting_base_prod.SHARED.SESSION as s on s.SESSION_ID = sd.SESSION_ID
join edw_prod.DATA_MODEL.DIM_STORE as t on t.STORE_ID = s.STORE_ID
    and t.store_type <> 'Retail'
LEFT JOIN edw_prod.reference.store_timezone AS st ON st.store_id = t.store_id
LEFT JOIN edw_prod.DATA_MODEL.FACT_MEMBERSHIP_EVENT f ON f.customer_id = s.customer_id
    AND CONVERT_TIMEZONE('America/Los_Angeles', f.EVENT_START_LOCAL_DATETIME)::datetime <= sd.DATETIME_ADDED
    AND CONVERT_TIMEZONE('America/Los_Angeles', f.EVENT_END_LOCAL_DATETIME)::datetime > sd.DATETIME_ADDED
WHERE
    test_type = 'Session Detail'
    and sd.DATETIME_ADDED between oo.adjusted_activated_datetime and oo.adjusted_end_datetime_adj
    and CONVERT_TIMEZONE('America/Los_Angeles', s.SESSION_LOCAL_DATETIME)::datetime between oo.adjusted_activated_datetime and coalesce(oo.adjusted_end_datetime_adj,'9999-12-01')
    and IS_TEST_CUSTOMER_ACCOUNT = FALSE
    AND NVL(is_in_segment, TRUE) != FALSE;

delete from _non_framework_session_detail
where
    membership_state in ('Prospects','Prospect') and test_start_membership_state <> 'Prospect'
        or membership_state in ('Leads','Lead') and test_start_membership_state <> 'Lead'
        or membership_state in ('VIPS','VIP') and test_start_membership_state <> 'VIP'
        or membership_state in ('Prospects and Leads','Prospect and Lead') and test_start_membership_state not in ('Prospect','Lead')
        or membership_state in ('Leads and VIPS','Lead and VIP') and test_start_membership_state not in ('Lead','VIP')
        or (test_start_membership_state = 'Prospect' and IS_BOT = TRUE)
        OR NVL(is_in_segment, TRUE) = FALSE;

delete from _non_framework_session_detail
where
    (test_platforms = 'Desktop & Mobile' and platform = 'Mobile App')
    or (test_platforms = 'Desktop' and platform not in ('Desktop','Tablet'))
    or (test_platforms = 'Mobile' and platform not in ('Mobile','Unknown'))
    or (test_platforms = 'MobileApp' and platform <> 'Mobile App')
    or (test_platforms = 'Mobile & App' and platform not in ('Mobile','Mobile App'));

CREATE OR REPLACE TEMP TABLE _non_framework_membership_customer_detail_unadjusted AS
SELECT distinct
    test_type
    ,ticket as TEST_FRAMEWORK_TICKET
    ,test_key
    ,test_label AS test_name
    ,test_label
    ,yty_exclusion
    ,brand
    ,region
    ,test_platforms
    ,oo.membership_state
    ,gender
    ,test_start_location
    ,test_split
    ,adjusted_activated_datetime AS test_activated_datetime_pst
    ,status
    ,md.membership_id
    ,m.CUSTOMER_ID
    ,value AS ab_test_segment
    ,case when (try_cast(md.value AS INT) between control_start and control_end) or value = control_start then 'Control'
        when (try_cast(md.value AS INT) between variant_a_start and variant_a_end and variant_b_start = 'N/A') or value = variant_a_start then 'Variant'
        when (try_cast(md.value AS INT) between variant_a_start and variant_a_end and variant_b_start <> 'N/A') or value = variant_a_start then 'Variant A'
        when (try_cast(md.value AS INT) between variant_b_start and variant_b_end) or value = variant_b_start then 'Variant B'
        when (try_cast(md.value AS INT) between variant_c_start and variant_c_end) or value = variant_c_start then 'Variant C'
        when (try_cast(md.value AS INT) between variant_d_start and variant_d_end) or value = variant_d_start then 'Variant D'
        else null end as test_group
    ,min(md.datetime_added) as min_customer_test_start_datetime_added_pst
FROM _oof_tests as oo
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.membership_detail as md on md.name = oo.test_key
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP as m on m.MEMBERSHIP_ID = md.MEMBERSHIP_ID
WHERE
    test_type = 'Membership Detail'
    and md.DATETIME_ADDED between oo.adjusted_activated_datetime and coalesce(oo.adjusted_end_datetime_adj,'9999-12-01')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19

union all

SELECT distinct
    test_type
    ,ticket as TEST_FRAMEWORK_TICKET
    ,test_key
    ,test_label AS test_name
    ,test_label
    ,yty_exclusion
    ,brand
    ,region
    ,test_platforms
    ,oo.membership_state
    ,gender
    ,test_start_location
    ,test_split
    ,adjusted_activated_datetime AS test_activated_datetime_pst
    ,status
    ,m.membership_id
    ,cd.CUSTOMER_ID
    ,value AS ab_test_segment
    ,case when (try_cast(cd.value AS INT) between control_start and control_end) or value = control_start then 'Control'
        when (try_cast(cd.value AS INT) between variant_a_start and variant_a_end and variant_b_start = 'N/A') or value = variant_a_start then 'Variant'
        when (try_cast(cd.value AS INT) between variant_a_start and variant_a_end and variant_b_start <> 'N/A') or value = variant_a_start then 'Variant A'
        when (try_cast(cd.value AS INT) between variant_b_start and variant_b_end) or value = variant_b_start then 'Variant B'
        when (try_cast(cd.value AS INT) between variant_c_start and variant_c_end) or value = variant_c_start then 'Variant C'
        when (try_cast(cd.value AS INT) between variant_d_start and variant_d_end) or value = variant_d_start then 'Variant D'
        else null end as test_group
    ,min(cd.datetime_added) as min_customer_test_start_datetime_added_pst
FROM _oof_tests as oo
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.CUSTOMER_DETAIL as cd on cd.name = oo.test_key
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP as m on m.CUSTOMER_ID = cd.CUSTOMER_ID
WHERE
    test_type = 'Customer Detail'
    and cd.DATETIME_ADDED between oo.adjusted_activated_datetime and coalesce(oo.adjusted_end_datetime_adj,'9999-12-01')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
;

CREATE OR REPLACE TEMPORARY TABLE _non_framework_membership_customer_detail as
select
    c.*
     ,CASE WHEN f.MEMBERSHIP_STATE IS NULL THEN 'Prospect' else f.MEMBERSHIP_STATE end AS test_start_membership_state
     ,CASE WHEN f.MEMBERSHIP_STATE = 'VIP' then datediff('month',CONVERT_TIMEZONE('America/Los_Angeles', EVENT_START_LOCAL_DATETIME)::datetime,min_customer_test_start_datetime_added_pst) + 1 else null end AS test_start_vip_month_tenure
     ,CASE WHEN f.MEMBERSHIP_STATE = 'Lead' then datediff('day',CONVERT_TIMEZONE('America/Los_Angeles', EVENT_START_LOCAL_DATETIME)::datetime,min_customer_test_start_datetime_added_pst) + 1 else null end AS test_start_lead_daily_tenure
from _non_framework_membership_customer_detail_unadjusted as c
 LEFT JOIN edw_prod.DATA_MODEL.FACT_MEMBERSHIP_EVENT f ON f.customer_id = c.customer_id
    AND CONVERT_TIMEZONE('America/Los_Angeles', f.EVENT_START_LOCAL_DATETIME)::datetime <= c.min_customer_test_start_datetime_added_pst
    AND CONVERT_TIMEZONE('America/Los_Angeles', f.EVENT_END_LOCAL_DATETIME)::datetime > c.min_customer_test_start_datetime_added_pst
 join edw_prod.DATA_MODEL.DIM_CUSTOMER as dc on dc.CUSTOMER_ID = c.customer_id
where dc.IS_TEST_CUSTOMER = FALSE;

delete from _non_framework_membership_customer_detail
where
    membership_state = 'Prospect' and test_start_membership_state <> 'Prospects'
        or membership_state = 'Lead' and test_start_membership_state <> 'Leads'
        or membership_state = 'VIP' and test_start_membership_state NOT IN ('VIP','VIPS')
        or membership_state = 'Prospect and Lead' and test_start_membership_state not in ('Prospects','Leads')
        or membership_state = 'Lead and VIP' and test_start_membership_state not in ('Leads','VIP','VIPS');

CREATE OR REPLACE TEMPORARY TABLE _max_datetimes as
select
    test_key
    ,max(test_start_datetime_added_pst) as max_datetime
from _non_framework_session_detail
group by 1

union

select
    test_key
     ,max(min_customer_test_start_datetime_added_pst) as max_datetime
from _non_framework_membership_customer_detail
group by 1;

CREATE OR REPLACE TEMPORARY TABLE _all_sessions_non_framework_membership_customer_detail as
select distinct
    s.test_key
    ,test_label
    ,SESSION_ID
    ,case when st.STORE_REGION = 'NA' and s.region in ('NA','Global') then TRUE
            when st.STORE_REGION = 'EU' and s.region in ('EU','Global') then TRUE
                else FALSE end as is_valid_session
    ,case when yty_exclusion = TRUE and st.store_brand = 'Yitty' then FALSE else TRUE end as is_valid_brand_session
    ,m.platform
    ,test_platforms
FROM _non_framework_membership_customer_detail AS s
JOIN reporting_base_prod.SHARED.SESSION AS m ON s.customer_id = m.customer_id
join _max_datetimes as max on s.test_key = max.test_key
join edw_prod.DATA_MODEL.DIM_STORE as st on st.STORE_ID = m.STORE_ID
    and st.store_type <> 'Retail'
where
    CONVERT_TIMEZONE('America/Los_Angeles',SESSION_LOCAL_DATETIME)::datetime between test_activated_datetime_pst and max_datetime

union

select distinct
    s.test_key
    ,test_label
    ,fo.SESSION_ID
    ,case when st.STORE_REGION = 'NA' and s.region in ('NA','Global') then TRUE
        when st.STORE_REGION = 'EU' and s.region in ('EU','Global') then TRUE
            else FALSE end as is_valid_session
    ,case when yty_exclusion = TRUE and st.store_brand = 'Yitty' then FALSE else TRUE end as is_valid_brand_session
    ,n.platform
    ,test_platforms
FROM _non_framework_membership_customer_detail s
join edw_prod.DATA_MODEL.FACT_ORDER as fo on fo.CUSTOMER_ID = s.CUSTOMER_ID
join edw_prod.DATA_MODEL.DIM_ORDER_STATUS os on os.ORDER_STATUS_KEY = fo.ORDER_STATUS_KEY
join edw_prod.DATA_MODEL.DIM_ORDER_SALES_CHANNEL osc on osc.ORDER_SALES_CHANNEL_KEY = fo.ORDER_SALES_CHANNEL_KEY
join edw_prod.DATA_MODEL.DIM_ORDER_MEMBERSHIP_CLASSIFICATION mem on mem.ORDER_MEMBERSHIP_CLASSIFICATION_KEY = fo.ORDER_MEMBERSHIP_CLASSIFICATION_KEY
join edw_prod.DATA_MODEL.DIM_ORDER_PROCESSING_STATUS as ops on ops.ORDER_PROCESSING_STATUS_KEY = fo.ORDER_PROCESSING_STATUS_KEY
join _max_datetimes as max on s.test_key = max.test_key
join edw_prod.DATA_MODEL.DIM_STORE as st on st.STORE_ID = fo.STORE_ID
    and st.store_type <> 'Retail'
JOIN reporting_base_prod.SHARED.SESSION AS n ON fo.SESSION_ID = n.session_id
where
    ORDER_SALES_CHANNEL_L1 = 'Online Order'
  and ORDER_CLASSIFICATION_L1 = 'Product Order'
  AND fo.order_local_datetime between min_customer_test_start_datetime_added_pst and max_datetime;

delete from _all_sessions_non_framework_membership_customer_detail
where
    (test_platforms = 'Desktop & Mobile' and platform = 'Mobile App')
    or (test_platforms = 'Desktop' and platform not in ('Desktop','Tablet'))
    or (test_platforms = 'Mobile' and platform not in ('Mobile','Unknown'))
    or (test_platforms = 'MobileApp' and platform <> 'Mobile App')
    or (test_platforms = 'Mobile & App' and platform not in ('Mobile','Mobile App'));

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.session_ab_test_non_framework as
select
    9999 as TEST_FRAMEWORK_ID
     ,s.test_label
     ,s.test_label as TEST_FRAMEWORK_DESCRIPTION
     ,s.TEST_FRAMEWORK_TICKET
     ,s.TEST_KEY::varchar CAMPAIGN_CODE
     ,s.TEST_TYPE
     ,s.test_activated_datetime_pst as cms_min_test_start_datetime_hq
     ,null as CMS_START_DATE
     ,null as CMS_END_DATE

     ,m.STORE_ID
     ,r.store_brand
     ,r.STORE_REGION
     ,r.STORE_COUNTRY
     ,STORE_NAME
     ,SPECIALTY_COUNTRY_CODE
     ,s.TEST_KEY::varchar as test_key
     ,cast(s.AB_TEST_SEGMENT as varchar) as AB_TEST_SEGMENT
     ,s.test_group

     ,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz as AB_TEST_START_LOCAL_DATETIME
     ,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz::date as AB_TEST_START_LOCAL_DATE
     ,date_trunc('month',CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz::date) as AB_TEST_START_LOCAL_MONTH_DATE
     ,rank() over (partition by m.customer_id,s.TEST_KEY,test_start_datetime_added_pst ORDER BY s.session_id asc) as rnk_monthly

     ,m.SESSION_LOCAL_DATETIME
     ,date_trunc('month',m.SESSION_LOCAL_DATETIME)::date as SESSION_LOCAL_MONTH_DATE
     ,m.VISITOR_ID
     ,m.SESSION_ID
     ,m.CUSTOMER_ID
     ,m.MEMBERSHIP_ID

     -- membership
     ,m.MEMBERSHIP_STATE as SESSION_START_MEMBERSHIP_STATE
     ,s.TEST_START_MEMBERSHIP_STATE

     ,s.TEST_START_LEAD_DAILY_TENURE

     ,case when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('hh',REGISTRATION_LOCAL_DATETIME,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz) + 1) between 1 and 3 THEN concat('H',datediff('hh',REGISTRATION_LOCAL_DATETIME,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz) + 1)
           when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('hh',REGISTRATION_LOCAL_DATETIME,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz) + 1) BETWEEN 4 and 24 THEN 'D1'
           when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('day',REGISTRATION_LOCAL_DATETIME,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz) + 1) between 2 and 30 then concat('D',TEST_START_LEAD_DAILY_TENURE)
           when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('day',REGISTRATION_LOCAL_DATETIME,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),test_start_datetime_added_pst)::timestamp_tz) + 1) >= 31 then 'M2+'
           ELSE null end as TEST_START_LEAD_TENURE_GROUP
     ,TEST_START_VIP_MONTH_TENURE
     ,case when s.TEST_START_VIP_MONTH_TENURE between 13 and 24 then 'M13-24'
           when s.TEST_START_VIP_MONTH_TENURE >= 25 then 'M25+'
           else concat('M',s.TEST_START_VIP_MONTH_TENURE) end as TEST_START_VIP_MONTH_TENURE_GROUP

     ,price as MEMBERSHIP_PRICE
     ,t.LABEL as MEMBERSHIP_TYPE
     ,case when s.TEST_START_MEMBERSHIP_STATE = 'VIP' then CUMULATIVE_PRODUCT_GROSS_PROFIT_DECILE else null end as CUMULATIVE_PRODUCT_GROSS_PROFIT_DECILE
     ,case when s.TEST_START_MEMBERSHIP_STATE = 'VIP' then CUMULATIVE_CASH_GROSS_PROFIT_DECILE else null end as CUMULATIVE_CASH_GROSS_PROFIT_DECILE
     --tech
     ,case when lower(m.platform) in ('desktop','tablet') then 'Desktop'
           when lower(m.platform) in ('mobile','unknown') then 'Mobile'
           when m.platform = 'Mobile App' then 'App' else m.platform end as PLATFORM
     ,m.PLATFORM AS platform_raw
     ,case when os in ('Mac OS','Mac OSX','iOS') then 'Apple'
           when os = 'Unknown' or os is null then 'Unknown'
           else 'Android' end as OS
     ,OS AS OS_RAW
     ,BROWSER
     --quiz / reg funnel
     ,IS_QUIZ_START_ACTION
     ,IS_QUIZ_COMPLETE_ACTION
     ,IS_SKIP_QUIZ_ACTION
     ,IS_LEAD_REGISTRATION_ACTION
     ,IS_QUIZ_REGISTRATION_ACTION
     ,IS_SPEEDY_REGISTRATION_ACTION
     ,IS_SKIP_QUIZ_REGISTRATION_ACTION
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
           when TEST_START_MEMBERSHIP_STATE = 'Prospect' and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
           when TEST_START_MEMBERSHIP_STATE = 'Prospect' and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end as lead_reg_type

     ,REGISTRATION_LOCAL_DATETIME
     -- flags for FL
     ,case when lower(GATEWAY_NAME) ilike 'yt_%%' or lower(GATEWAY_NAME) ilike '%%ytyus%%' then TRUE else FALSE end IS_YITTY_GATEWAY
     ,case when lower(GATEWAY_NAME) ilike '%%flm%%' then TRUE else FALSE end IS_MALE_GATEWAY
     ,case when r.STORE_BRAND <> 'Fabletics' then 'N/A' else IS_MALE_SESSION_ACTION end IS_MALE_SESSION_ACTION
     ,case when r.STORE_BRAND <> 'Fabletics' then 'N/A' else IS_MALE_SESSION end IS_MALE_SESSION
     ,case when r.STORE_BRAND <> 'Fabletics' then 'N/A' else IS_MALE_CUSTOMER end IS_MALE_CUSTOMER
     ,case when r.STORE_BRAND <> 'Fabletics' then 'N/A'
           when r.STORE_BRAND = 'Fabletics' and IS_MALE_SESSION = TRUE or IS_MALE_CUSTOMER = TRUE then TRUE else FALSE end AS IS_MALE_SESSION_CUSTOMER

     ,coalesce(IS_PDP_VISIT,false)::BOOLEAN as IS_PDP_VISIT
     ,IS_ATB_ACTION
     ,IS_SKIP_MONTH_ACTION
     ,IS_LOGIN_ACTION
    ,coalesce(IS_CART_CHECKOUT_VISIT,false)::BOOLEAN as IS_CART_CHECKOUT_VISIT
     --acquisition
     ,h.CHANNEL
     ,h.SUBCHANNEL
     ,g.GATEWAY_TYPE
     ,g.GATEWAY_SUB_TYPE
     ,m.DM_GATEWAY_ID
     ,g.GATEWAY_CODE
     ,g.GATEWAY_NAME
     ,m.DM_SITE_ID

     ,DEFAULT_STATE_PROVINCE as customer_state
     ,HOW_DID_YOU_HEAR as HDYH

     ,null as IS_CMS_SPECIFIC_GENDER
     ,case when s.gender = 'Women Only' and test_start_membership_state = 'Prospect' and IS_MALE_SESSION_CUSTOMER = FALSE then TRUE
           when s.gender = 'Women Only' and test_start_membership_state <> 'Prospect' and IS_MALE_CUSTOMER = FALSE then TRUE
           when s.gender = 'Men Only' and test_start_membership_state = 'Prospect' and IS_MALE_SESSION_CUSTOMER = TRUE then TRUE
           when s.gender = 'Men Only' and test_start_membership_state <> 'Prospect' and IS_MALE_CUSTOMER = TRUE then TRUE
           when s.gender = 'Men and Women' and test_start_membership_state = 'Prospect' then TRUE
           when s.gender = 'Men and Women' and test_start_membership_state <> 'Prospect' then TRUE
           else false end as is_valid_gender_sessions
     ,m.IS_BOT
     ,NVL(m.is_in_segment, TRUE) as is_in_segment
     ,null as IS_CMS_SPECIFIC_LEAD_DAY
     ,null as flag_is_cms_filtered_lead
     ,m.is_migrated_session
    ,$current_end_date_hq as hq_max_datetime
from _non_framework_session_detail as s
join _oof_tests as oo on oo.test_key = s.test_key and oo.test_label = s.test_label
JOIN reporting_base_prod.SHARED.SESSION AS m ON s.SESSION_ID = m.SESSION_ID
join edw_prod.DATA_MODEL.DIM_STORE as r on r.STORE_ID = m.STORE_ID
         LEFT JOIN reporting_base_prod.SHARED.DIM_GATEWAY g ON g.dm_gateway_id = m.dm_gateway_id
    and EFFECTIVE_START_DATETIME <= m.SESSION_LOCAL_DATETIME
    and EFFECTIVE_END_DATETIME > m.SESSION_LOCAL_DATETIME
         left join edw_prod.DATA_MODEL.dim_customer as dc on dc.CUSTOMER_ID = m.CUSTOMER_ID
         left join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP as p on p.CUSTOMER_ID = dc.CUSTOMER_ID
         left join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP_TYPE as t on t.MEMBERSHIP_TYPE_ID = p.MEMBERSHIP_TYPE_ID
         left join reporting_base_prod.SHARED.MEDIA_SOURCE_CHANNEL_MAPPING as h on h.MEDIA_SOURCE_HASH = m.MEDIA_SOURCE_HASH
         left join (select distinct ltd.customer_id,SOURCE_ACTIVATION_LOCAL_DATETIME,SOURCE_NEXT_ACTIVATION_LOCAL_DATETIME,ltd.VIP_COHORT_MONTH_DATE,CUMULATIVE_PRODUCT_GROSS_PROFIT_DECILE,CUMULATIVE_CASH_GROSS_PROFIT_DECILE
                    from edw_prod.analytics_base.customer_lifetime_value_LTD as ltd
                    join edw_prod.DATA_MODEL.FACT_ACTIVATION as f on f.ACTIVATION_KEY = ltd.ACTIVATION_KEY and f.customer_id = ltd.customer_id) as ltd
                        on ltd.customer_id = m.customer_id and SESSION_LOCAL_DATETIME between SOURCE_ACTIVATION_LOCAL_DATETIME and SOURCE_NEXT_ACTIVATION_LOCAL_DATETIME
where
      is_valid_session = TRUE and IS_TEST_CUSTOMER_ACCOUNT = FALSE and is_valid_brand_session = TRUE and SESSION_LOCAL_DATETIME between test_activated_datetime_pst and adjusted_end_datetime_adj
        and NVL(m.is_in_segment, TRUE) != FALSE

union all

select
    9999 as TEST_FRAMEWORK_ID
     ,o.test_label
     ,o.test_label as TEST_FRAMEWORK_DESCRIPTION
     ,o.TEST_FRAMEWORK_TICKET
     ,s.TEST_KEY::varchar CAMPAIGN_CODE
     ,o.TEST_TYPE
     ,o.test_activated_datetime_pst as cms_min_test_start_datetime_hq
     ,null as CMS_START_DATE
     ,null as CMS_END_DATE

     ,m.STORE_ID
     ,r.store_brand
     ,r.STORE_REGION
     ,r.STORE_COUNTRY
     ,STORE_NAME
     ,SPECIALTY_COUNTRY_CODE
     ,o.TEST_KEY::varchar as test_key
     ,cast(o.AB_TEST_SEGMENT as varchar) as AB_TEST_SEGMENT
     ,o.test_group
     ,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),min_customer_test_start_datetime_added_pst)::timestamp_tz as AB_TEST_START_LOCAL_DATETIME
     ,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),min_customer_test_start_datetime_added_pst)::timestamp_tz::date as AB_TEST_START_LOCAL_DATE
     ,date_trunc('month',CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),min_customer_test_start_datetime_added_pst)::timestamp_tz::date) as AB_TEST_START_LOCAL_MONTH_DATE
     ,rank() over (partition by m.customer_id,o.TEST_KEY,min_customer_test_start_datetime_added_pst ORDER BY s.session_id asc) as rnk_monthly
     ,m.SESSION_LOCAL_DATETIME
     ,date_trunc('month',m.SESSION_LOCAL_DATETIME)::date as SESSION_LOCAL_MONTH_DATE
     ,m.VISITOR_ID
     ,m.SESSION_ID
     ,m.CUSTOMER_ID
     ,m.MEMBERSHIP_ID

     -- membership
     ,m.MEMBERSHIP_STATE as SESSION_START_MEMBERSHIP_STATE
     ,o.TEST_START_MEMBERSHIP_STATE

     ,o.TEST_START_LEAD_DAILY_TENURE

     ,case when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('hh',REGISTRATION_LOCAL_DATETIME,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),min_customer_test_start_datetime_added_pst)::timestamp_tz) + 1) between 1 and 3 THEN concat('H',datediff('hh',REGISTRATION_LOCAL_DATETIME,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),min_customer_test_start_datetime_added_pst)::timestamp_tz) + 1)
           when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('hh',REGISTRATION_LOCAL_DATETIME,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),min_customer_test_start_datetime_added_pst)::timestamp_tz) + 1) BETWEEN 4 and 24 THEN 'D1'
           when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('day',REGISTRATION_LOCAL_DATETIME,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),min_customer_test_start_datetime_added_pst)::timestamp_tz) + 1) between 2 and 30 then concat('D',TEST_START_LEAD_DAILY_TENURE)
           when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('day',REGISTRATION_LOCAL_DATETIME,CONVERT_TIMEZONE(COALESCE(store_time_zone,'America/Los_Angeles'),min_customer_test_start_datetime_added_pst)::timestamp_tz) + 1) >= 31 then 'M2+'
           ELSE null end as TEST_START_LEAD_TENURE_GROUP
     ,TEST_START_VIP_MONTH_TENURE
     ,case when o.TEST_START_VIP_MONTH_TENURE between 13 and 24 then 'M13-24'
           when o.TEST_START_VIP_MONTH_TENURE >= 25 then 'M25+'
           else concat('M',o.TEST_START_VIP_MONTH_TENURE) end as TEST_START_VIP_MONTH_TENURE_GROUP

     ,price as MEMBERSHIP_PRICE
     ,t.LABEL as MEMBERSHIP_TYPE
     ,case when o.TEST_START_MEMBERSHIP_STATE = 'VIP' then CUMULATIVE_PRODUCT_GROSS_PROFIT_DECILE else null end as CUMULATIVE_PRODUCT_GROSS_PROFIT_DECILE
     ,case when o.TEST_START_MEMBERSHIP_STATE = 'VIP' then CUMULATIVE_CASH_GROSS_PROFIT_DECILE else null end as CUMULATIVE_CASH_GROSS_PROFIT_DECILE
     --tech
     ,case when lower(m.platform) in ('desktop','tablet') then 'Desktop'
           when lower(m.platform) in ('mobile','unknown') then 'Mobile'
           when m.platform = 'Mobile App' then 'App' else m.platform end as PLATFORM
     ,m.PLATFORM AS platform_raw
     ,case when os in ('Mac OS','Mac OSX','iOS') then 'Apple'
           when os = 'Unknown' or os is null then 'Unknown'
           else 'Android' end as OS
     ,OS AS OS_RAW
     ,BROWSER
     --quiz / reg funnel
     ,IS_QUIZ_START_ACTION
     ,IS_QUIZ_COMPLETE_ACTION
     ,IS_SKIP_QUIZ_ACTION
     ,IS_LEAD_REGISTRATION_ACTION
     ,IS_QUIZ_REGISTRATION_ACTION
     ,IS_SPEEDY_REGISTRATION_ACTION
     ,IS_SKIP_QUIZ_REGISTRATION_ACTION
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
           when TEST_START_MEMBERSHIP_STATE = 'Prospect' and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
           when TEST_START_MEMBERSHIP_STATE = 'Prospect' and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end as lead_reg_type

     ,REGISTRATION_LOCAL_DATETIME
     -- flags for FL male customers
     ,case when lower(GATEWAY_NAME) ilike 'yt_%%' or lower(GATEWAY_NAME) ilike '%%ytyus%%' then TRUE else FALSE end IS_YITTY_GATEWAY
     ,case when lower(GATEWAY_NAME) ilike '%%flm%%' then TRUE else FALSE end IS_MALE_GATEWAY
     ,case when r.STORE_BRAND <> 'Fabletics' then 'N/A' else IS_MALE_SESSION_ACTION end IS_MALE_SESSION_ACTION
     ,case when r.STORE_BRAND <> 'Fabletics' then 'N/A' else IS_MALE_SESSION end IS_MALE_SESSION
     ,case when r.STORE_BRAND <> 'Fabletics' then 'N/A' else IS_MALE_CUSTOMER end IS_MALE_CUSTOMER
     ,case when r.STORE_BRAND <> 'Fabletics' then 'N/A'
           when r.STORE_BRAND = 'Fabletics' and IS_MALE_SESSION = TRUE or IS_MALE_CUSTOMER = TRUE then TRUE else FALSE end AS IS_MALE_SESSION_CUSTOMER

     ,coalesce(IS_PDP_VISIT,false)::BOOLEAN as IS_PDP_VISIT
     ,IS_ATB_ACTION
     ,coalesce(IS_CART_CHECKOUT_VISIT,false)::BOOLEAN as IS_CART_CHECKOUT_VISIT
     ,IS_SKIP_MONTH_ACTION
     ,IS_LOGIN_ACTION
     --acquisition
     ,h.CHANNEL
     ,h.SUBCHANNEL
     ,g.GATEWAY_TYPE
     ,g.GATEWAY_SUB_TYPE
     ,m.DM_GATEWAY_ID
     ,g.GATEWAY_CODE
     ,g.GATEWAY_NAME
     ,m.DM_SITE_ID

     ,DEFAULT_STATE_PROVINCE as customer_state
     ,HOW_DID_YOU_HEAR as HDYH

     ,null as IS_CMS_SPECIFIC_GENDER
     ,case when o.gender = 'Women Only' and test_start_membership_state = 'Prospect' and IS_MALE_SESSION_CUSTOMER = FALSE then TRUE
           when o.gender = 'Women Only' and test_start_membership_state <> 'Prospect' and IS_MALE_CUSTOMER = FALSE then TRUE
           when o.gender = 'Men Only' and test_start_membership_state = 'Prospect' and IS_MALE_SESSION_CUSTOMER = TRUE then TRUE
           when o.gender = 'Men Only' and test_start_membership_state <> 'Prospect' and IS_MALE_CUSTOMER = TRUE then TRUE
           when o.gender = 'Men and Women' and test_start_membership_state = 'Prospect' then TRUE
           when o.gender = 'Men and Women' and test_start_membership_state <> 'Prospect' then TRUE
           else false end as is_valid_gender_sessions
     ,IS_BOT
     ,NVL(m.is_in_segment, TRUE) as is_in_segment
     ,null as IS_CMS_SPECIFIC_LEAD_DAY
     ,null as flag_is_cms_filtered_lead
     ,m.is_migrated_session
    ,$current_end_date_hq as hq_max_datetime
from _all_sessions_non_framework_membership_customer_detail as s --9565581
JOIN reporting_base_prod.SHARED.SESSION AS m ON s.SESSION_ID = m.SESSION_ID --9565581
join _non_framework_membership_customer_detail as o on o.customer_id = m.CUSTOMER_ID --9565580
    and s.test_key = o.test_key
    and s.test_label = o.test_label
         join edw_prod.DATA_MODEL.DIM_STORE as r on r.STORE_ID = m.STORE_ID--9565469
    --and r.STORE_BRAND = o.brand
         LEFT JOIN reporting_base_prod.SHARED.DIM_GATEWAY g ON g.dm_gateway_id = m.dm_gateway_id
    and EFFECTIVE_START_DATETIME <= m.SESSION_LOCAL_DATETIME
    and EFFECTIVE_END_DATETIME > m.SESSION_LOCAL_DATETIME
         left join edw_prod.DATA_MODEL.dim_customer as dc on dc.CUSTOMER_ID = m.CUSTOMER_ID
         left join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP as p on p.CUSTOMER_ID = dc.CUSTOMER_ID
         left join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP_TYPE as t on t.MEMBERSHIP_TYPE_ID = p.MEMBERSHIP_TYPE_ID
         left join reporting_base_prod.SHARED.MEDIA_SOURCE_CHANNEL_MAPPING as h on h.MEDIA_SOURCE_HASH = m.MEDIA_SOURCE_HASH
         left join (select distinct ltd.customer_id,SOURCE_ACTIVATION_LOCAL_DATETIME,SOURCE_NEXT_ACTIVATION_LOCAL_DATETIME,ltd.VIP_COHORT_MONTH_DATE,CUMULATIVE_PRODUCT_GROSS_PROFIT_DECILE,CUMULATIVE_CASH_GROSS_PROFIT_DECILE
                    from edw_prod.analytics_base.customer_lifetime_value_LTD as ltd
                             join edw_prod.DATA_MODEL.FACT_ACTIVATION as f on f.ACTIVATION_KEY = ltd.ACTIVATION_KEY and f.customer_id = ltd.customer_id) as ltd
                   on ltd.customer_id = m.customer_id and SESSION_LOCAL_DATETIME between SOURCE_ACTIVATION_LOCAL_DATETIME and SOURCE_NEXT_ACTIVATION_LOCAL_DATETIME
where is_valid_session = TRUE and IS_TEST_CUSTOMER_ACCOUNT = FALSE and is_valid_brand_session = TRUE
AND NVL(m.is_in_segment, TRUE) != FALSE;

ALTER TABLE reporting_base_prod.shared.session_ab_test_non_framework SET DATA_RETENTION_TIME_IN_DAYS = 0;

delete from reporting_base_prod.shared.session_ab_test_non_framework
where
    is_valid_gender_sessions = FALSE
    or (IS_BOT = TRUE and TEST_START_MEMBERSHIP_STATE = 'Prospect')
    or (store_brand = 'JustFab' and test_label ilike 'fl bmig pdp migration%')
    or test_group is null
    OR NVL(is_in_segment, TRUE) = FALSE;

-- select store_brand,test_key,test_label,test_group,AB_TEST_SEGMENT,platform,count(*),count(distinct SESSION_ID)
-- from reporting_base_prod.shared.session_ab_test_non_framework group by 1,2,3,4,5,6

