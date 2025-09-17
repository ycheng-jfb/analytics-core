
SET current_end_date_hq = (SELECT (MIN(t.max_hq_datetime))
                           FROM (
                                    SELECT MAX(convert_timezone('America/Los_Angeles',SESSION_LOCAL_DATETIME))::datetime as max_hq_datetime FROM REPORTING_BASE_PROD.SHARED.SESSION
                                ) t);

CREATE OR REPLACE TEMPORARY TABLE _sorting_hat_tests as
select *
from lake_view.sharepoint.ab_test_metadata_import_oof
where test_type ilike 'Sorting Hat%'
and ADJUSTED_ACTIVATED_DATETIME >= '2021-01-01' and status <> 'Closed / Paused';

create or replace temporary table _brand_company_id as
select distinct store_brand,company_id
from lake_consolidated.reference.dim_store;

delete from _sorting_hat_tests
where
    (test_key = '49' and test_label = 'flus vpt,1/31 (49)')
        or (test_key = '55' and  test_label = 'flus bundle vpt (55)');

CREATE OR REPLACE TEMP TABLE _sorting_hat_test_starts_unadjusted AS
SELECT distinct
    test_type
    ,ticket as TEST_FRAMEWORK_TICKET
    ,tm.test_metadata_id as test_key
    ,tm.name AS test_name
    ,oo.test_label
    ,tm.label as test_framework_description
    ,brand
    ,region
    ,yty_exclusion
    ,test_platforms
    ,membership_state
    ,gender
    ,test_start_location
    ,test_split
    ,case when adjusted_activated_datetime = '1900-01-01' then tm.datetime_activated
        else adjusted_activated_datetime end as test_activated_datetime_pst
    ,case when adjusted_end_datetime = '1900-01-01' then tw.datetime_added else '9999-12-01' end as test_winner_local_datetime
    ,tw.variant_number AS variant_winner
    ,tm.statuscode --6138 (active), 6139 (archived)
    ,tm.label as status
    ,sh.customer_id||company_id as customer_id

    ,IFF(sh.variant_version = '',0,sh.variant_version) AS ab_test_segment
    ,case when ab_test_segment between control_start and control_end then 'Control'
          when ab_test_segment between variant_a_start and variant_a_end and variant_b_start = 'N/A' then 'Variant'
          when ab_test_segment between variant_a_start and variant_a_end and variant_b_start <> 'N/A' then 'Variant A'
          when ab_test_segment between variant_b_start and variant_b_end then 'Variant B'
          when ab_test_segment between variant_c_start and variant_c_end then 'Variant C'
          when ab_test_segment between variant_d_start and variant_d_end then 'Variant D'
          else null end as test_group
    ,min(CONVERT_TIMEZONE('America/Los_Angeles', sh.datetime_created))::datetime as min_customer_test_start_datetime_added_pst
    ,min(CONVERT_TIMEZONE('America/Los_Angeles', sh.meta_update_datetime))::datetime as min_customer_meta_update_datetime_added_pst
FROM _sorting_hat_tests as oo
join lake_consolidated_view.ultra_merchant.test_metadata AS tm on oo.test_key = edw_prod.stg.udf_unconcat_brand(tm.TEST_METADATA_ID) --activated in cms --PST
JOIN lake_consolidated.ultra_merchant.statuscode AS st ON st.statuscode = tm.statuscode
LEFT JOIN lake_consolidated.ultra_merchant.test_winner AS tw ON tw.test_metadata_id = tm.test_metadata_id
JOIN lake_view.experiments.sorting_hat AS sh ON sh.test_metadata_id = edw_prod.stg.udf_unconcat_brand(tm.test_metadata_id) --inserted when session occurs; must come to the site to make an api call
join _brand_company_id bc on brand=bc.store_brand
WHERE
    CONVERT_TIMEZONE('America/Los_Angeles',sh.datetime_created)::datetime >= coalesce(tm.datetime_activated,'9999-12-01')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22;

delete from _sorting_hat_test_starts_unadjusted
    where min_customer_test_start_datetime_added_pst < test_activated_datetime_pst;

CREATE OR REPLACE TEMPORARY TABLE _sorting_hat_test_starts as
select
    c.*
     ,CASE WHEN f.MEMBERSHIP_STATE IS NULL THEN 'Prospect' else f.MEMBERSHIP_STATE end AS test_start_membership_state
     ,CASE WHEN f.MEMBERSHIP_STATE = 'VIP' then datediff('month',CONVERT_TIMEZONE('America/Los_Angeles', EVENT_START_LOCAL_DATETIME)::datetime,min_customer_test_start_datetime_added_pst) + 1 else null end AS test_start_vip_month_tenure
     ,CASE WHEN f.MEMBERSHIP_STATE = 'Lead' then datediff('day',CONVERT_TIMEZONE('America/Los_Angeles', EVENT_START_LOCAL_DATETIME)::datetime,min_customer_test_start_datetime_added_pst) + 1 else null end AS test_start_lead_daily_tenure
from _sorting_hat_test_starts_unadjusted as c
LEFT JOIN edw_prod.DATA_MODEL.FACT_MEMBERSHIP_EVENT f ON f.customer_id = c.customer_id
    AND CONVERT_TIMEZONE('America/Los_Angeles', f.EVENT_START_LOCAL_DATETIME)::datetime <= c.min_customer_test_start_datetime_added_pst
    AND CONVERT_TIMEZONE('America/Los_Angeles', f.EVENT_END_LOCAL_DATETIME)::datetime > c.min_customer_test_start_datetime_added_pst
join edw_prod.DATA_MODEL.DIM_CUSTOMER as dc on dc.CUSTOMER_ID = c.customer_id
where dc.IS_TEST_CUSTOMER = FALSE; --and dc.CUSTOMER_ID = 580150174

delete from _sorting_hat_test_starts
where
    membership_state = 'ALL' and test_start_membership_state not in ('Prospect','Lead','VIP','Cancelled','Guest')
        or membership_state in ('Prospects','Prospect') and test_start_membership_state <> 'Prospect'
        or membership_state in ('Leads','Lead') and test_start_membership_state <> 'Lead'
        or membership_state in ('VIPS','VIP') and test_start_membership_state <> 'VIP'
        or membership_state in ('Prospects and Leads','Prospect and Lead') and test_start_membership_state not in ('Prospect','Lead')
        or membership_state in ('Leads and VIPS','Lead and VIP') and test_start_membership_state not in ('Lead','VIP');

CREATE OR REPLACE TEMPORARY TABLE _all_sessions as
select distinct
    s.test_key
    ,SESSION_ID
    ,st.STORE_BRAND
    ,case when s.yty_exclusion = TRUE and st.store_brand = 'Yitty' then FALSE else TRUE end as is_valid_brand_session
    ,case when st.STORE_REGION = 'NA' and t.region in ('NA','Global') then TRUE
        when st.STORE_REGION = 'EU' and t.region in ('EU','Global') then TRUE
        else FALSE end as is_valid_session
    ,m.platform
    ,s.test_platforms,
     NVL(m.is_in_segment, TRUE) as is_in_segment
FROM _sorting_hat_test_starts AS s
JOIN REPORTING_BASE_PROD.SHARED.SESSION AS m ON s.customer_id = m.customer_id
join edw_prod.DATA_MODEL.DIM_STORE as st on st.STORE_ID = m.STORE_ID
    and st.store_type <> 'Retail'
join _sorting_hat_tests as t on t.test_key = edw_prod.stg.udf_unconcat_brand(s.test_key)
where
    CONVERT_TIMEZONE('America/Los_Angeles',SESSION_LOCAL_DATETIME)::datetime between test_activated_datetime_pst AND COALESCE(s.test_winner_local_datetime,'9999-01-01')
    and st.store_brand = s.brand
    and st.store_brand = t.brand
    and NVL(is_in_segment, TRUE) != FALSE

union

select distinct
    s.test_key
    ,fo.SESSION_ID
    ,st.STORE_BRAND
    ,case when s.yty_exclusion = TRUE and st.store_brand = 'Yitty' then FALSE else TRUE end as is_valid_brand_session
    ,case when st.STORE_REGION = 'NA' and t.region in ('NA','Global') then TRUE
         when st.STORE_REGION = 'EU' and t.region in ('EU','Global') then TRUE
         else FALSE end as is_valid_session
    ,n.platform
    ,s.test_platforms,
     NVL(n.is_in_segment, TRUE) as is_in_segment
FROM _sorting_hat_test_starts s
join edw_prod.DATA_MODEL.FACT_ORDER as fo on fo.CUSTOMER_ID = s.CUSTOMER_ID
join edw_prod.DATA_MODEL.DIM_ORDER_STATUS os on os.ORDER_STATUS_KEY = fo.ORDER_STATUS_KEY
join edw_prod.DATA_MODEL.DIM_ORDER_SALES_CHANNEL osc on osc.ORDER_SALES_CHANNEL_KEY = fo.ORDER_SALES_CHANNEL_KEY
join edw_prod.DATA_MODEL.DIM_ORDER_MEMBERSHIP_CLASSIFICATION mem on mem.ORDER_MEMBERSHIP_CLASSIFICATION_KEY = fo.ORDER_MEMBERSHIP_CLASSIFICATION_KEY
join edw_prod.DATA_MODEL.DIM_ORDER_PROCESSING_STATUS as ops on ops.ORDER_PROCESSING_STATUS_KEY = fo.ORDER_PROCESSING_STATUS_KEY
join edw_prod.DATA_MODEL.DIM_STORE as st on st.STORE_ID = fo.STORE_ID
    and st.store_type <> 'Retail'
join _sorting_hat_tests as t on t.test_key = edw_prod.stg.udf_unconcat_brand(s.test_key)
join reporting_base_prod.SHARED.session as n on n.session_id = fo.session_id
where
    ORDER_SALES_CHANNEL_L1 = 'Online Order'
    and ORDER_CLASSIFICATION_L1 = 'Product Order'
    AND fo.order_local_datetime BETWEEN min_customer_test_start_datetime_added_pst AND COALESCE(s.test_winner_local_datetime,'9999-01-01')
    and st.store_brand = s.brand
    and st.store_brand = t.brand
    AND NVL(n.is_in_segment, TRUE) != FALSE;

delete from _all_sessions
where
    (test_platforms = 'Desktop & Mobile' and platform = 'Mobile App')
    or (test_platforms = 'Desktop' and platform not in ('Desktop','Tablet'))
    or (test_platforms = 'Mobile' and platform not in ('Mobile','Unknown'))
    or (test_platforms = 'MobileApp' and platform <> 'Mobile App')
    or (test_platforms = 'Mobile & App' and platform not in ('Mobile','Mobile App'))
    OR NVL(is_in_segment, TRUE) = FALSE;

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.session_ab_test_sorting_hat as
select
    9999 as TEST_FRAMEWORK_ID
    ,o.test_label
    ,TEST_FRAMEWORK_DESCRIPTION
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

    ,CONVERT_TIMEZONE(COALESCE(r.store_time_zone,'America/Los_Angeles'),o.min_customer_test_start_datetime_added_pst)::timestamp_tz as AB_TEST_START_LOCAL_DATETIME
    ,CONVERT_TIMEZONE(COALESCE(r.store_time_zone,'America/Los_Angeles'),o.min_customer_test_start_datetime_added_pst)::timestamp_tz::date as AB_TEST_START_LOCAL_DATE
    ,date_trunc('month',CONVERT_TIMEZONE(COALESCE(r.store_time_zone,'America/Los_Angeles'),o.min_customer_test_start_datetime_added_pst)::timestamp_tz::date) as AB_TEST_START_LOCAL_MONTH_DATE
    ,rank() over (partition by m.customer_id,o.TEST_KEY,CONVERT_TIMEZONE(COALESCE(r.store_time_zone,'America/Los_Angeles'),o.min_customer_test_start_datetime_added_pst)::timestamp_tz ORDER BY s.session_id asc) as rnk_monthly

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

    ,case when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('hh',REGISTRATION_LOCAL_DATETIME,AB_TEST_START_LOCAL_DATETIME) + 1) between 1 and 3 THEN concat('H',datediff('hh',REGISTRATION_LOCAL_DATETIME,AB_TEST_START_LOCAL_DATETIME) + 1)
       when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('hh',REGISTRATION_LOCAL_DATETIME,AB_TEST_START_LOCAL_DATETIME) + 1) BETWEEN 4 and 24 THEN 'D1'
       when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('day',REGISTRATION_LOCAL_DATETIME,AB_TEST_START_LOCAL_DATETIME) + 1) between 2 and 30 then concat('D',TEST_START_LEAD_DAILY_TENURE)
       when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('day',REGISTRATION_LOCAL_DATETIME,AB_TEST_START_LOCAL_DATETIME) + 1) >= 31 then 'M2+'
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
    ,coalesce(IS_CART_CHECKOUT_VISIT,false)::BOOLEAN as IS_CART_CHECKOUT_VISIT
    ,IS_SKIP_MONTH_ACTION
    ,IS_LOGIN_ACTION
--     ,is_cart_checkout_visit as IS_CART_VISIT
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
    ,case when o.gender = 'Women Only' and test_start_membership_state = 'Prospect' and (IS_MALE_SESSION = FALSE or IS_MALE_CUSTOMER = FALSE) then TRUE
       when o.gender = 'Women Only' and test_start_membership_state <> 'Prospect' and IS_MALE_CUSTOMER = FALSE then TRUE
       when o.gender = 'Men Only' and test_start_membership_state = 'Prospect' and (IS_MALE_SESSION = TRUE or IS_MALE_CUSTOMER = TRUE) then TRUE
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
from _all_sessions as s --9565581
JOIN REPORTING_BASE_PROD.SHARED.SESSION AS m ON s.SESSION_ID = m.SESSION_ID
join _sorting_hat_test_starts as o on o.customer_id = m.CUSTOMER_ID
                            and s.test_key = o.test_key
join edw_prod.DATA_MODEL.DIM_STORE as r on r.STORE_ID = m.STORE_ID
    and r.STORE_BRAND = o.brand
LEFT JOIN REPORTING_BASE_PROD.SHARED.DIM_GATEWAY g ON g.dm_gateway_id = m.dm_gateway_id
    and EFFECTIVE_START_DATETIME <= m.SESSION_LOCAL_DATETIME
    and EFFECTIVE_END_DATETIME > m.SESSION_LOCAL_DATETIME
left join edw_prod.DATA_MODEL.dim_customer as dc on dc.CUSTOMER_ID = m.CUSTOMER_ID
left join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP as p on p.CUSTOMER_ID = dc.CUSTOMER_ID
left join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP_TYPE as t on t.MEMBERSHIP_TYPE_ID = p.MEMBERSHIP_TYPE_ID
left join REPORTING_BASE_PROD.SHARED.MEDIA_SOURCE_CHANNEL_MAPPING as h on h.MEDIA_SOURCE_HASH = m.MEDIA_SOURCE_HASH
left join (select distinct ltd.customer_id,SOURCE_ACTIVATION_LOCAL_DATETIME,SOURCE_NEXT_ACTIVATION_LOCAL_DATETIME,ltd.VIP_COHORT_MONTH_DATE,CUMULATIVE_PRODUCT_GROSS_PROFIT_DECILE,CUMULATIVE_CASH_GROSS_PROFIT_DECILE
            from edw_prod.analytics_base.customer_lifetime_value_LTD as ltd
            join edw_prod.DATA_MODEL.FACT_ACTIVATION as f on f.ACTIVATION_KEY = ltd.ACTIVATION_KEY and f.customer_id = ltd.customer_id) as ltd
                on ltd.customer_id = m.customer_id and SESSION_LOCAL_DATETIME between SOURCE_ACTIVATION_LOCAL_DATETIME and SOURCE_NEXT_ACTIVATION_LOCAL_DATETIME
where is_valid_session = TRUE and IS_TEST_CUSTOMER_ACCOUNT = FALSE and is_valid_brand_session = TRUE
AND NVL(m.is_in_segment, TRUE) != FALSE;

ALTER TABLE reporting_base_prod.shared.session_ab_test_sorting_hat SET DATA_RETENTION_TIME_IN_DAYS = 0;

delete from reporting_base_prod.shared.session_ab_test_sorting_hat
    where
        is_valid_gender_sessions = FALSE
        or (IS_BOT = TRUE and TEST_START_MEMBERSHIP_STATE = 'Prospect')
        OR NVL(is_in_segment, TRUE) = FALSE;
--         or PLATFORM = 'App'

-- select
--     store_brand
--     ,test_key
--     ,test_label
--     ,AB_TEST_SEGMENT
--     ,platform
--     ,count(*)
--     ,count(distinct SESSION_ID)
-- from reporting_base_prod.shared.session_ab_test_sorting_hat
-- group by 1,2,3,4,5
