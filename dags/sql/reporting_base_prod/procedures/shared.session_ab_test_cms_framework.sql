
/* Change lookback_period for backfills! */
SET lookback_period = (SELECT DATEADD(DAY, -3, CURRENT_DATE));

SET current_end_date_hq = (
    SELECT (MIN(t.max_hq_datetime))
    FROM (
        SELECT MAX(convert_timezone('America/Los_Angeles',SESSION_LOCAL_DATETIME))::datetime as max_hq_datetime
        FROM REPORTING_BASE_PROD.SHARED.SESSION
        WHERE SESSION_LOCAL_DATETIME >= $lookback_period
        ) t
    );

set last_load_datetime = (
    select max(a.session_local_datetime)::date-7 as last_load_datetime --comment
    from reporting_base_prod.shared.session_ab_test_cms_framework a
    join reporting_base_prod.shared.session b on a.session_id = b.session_id
    WHERE a.SESSION_LOCAL_DATETIME >= $lookback_period
    );

--------------- CONSTRUCTOR CUSTOM REPORTING ---------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _constructor_session_base_unadj as
select
    SESSION_ID
    ,META_ORIGINAL_SESSION_ID
    ,CUSTOMER_ID
    ,ab_test_key
    ,AB_TEST_START_LOCAL_DATETIME
    ,AB_TEST_SEGMENT as original_ab_test_segment
    ,null as new_ab_test_segment
    ,TEST_START_MEMBERSHIP_STATE
    ,TEST_START_LEAD_DAILY_TENURE
    ,TEST_START_VIP_MONTH_TENURE
    ,iff(p.PROPERTIES_SESSION_ID is not null or pp.PROPERTIES_SESSION_ID is not null,TRUE,FALSE) as is_search_event_sessions
    ,rank() over (partition by customer_id,AB_TEST_KEY,is_search_event_sessions ORDER BY session_id asc) as rnk_sessions_from_customer
from REPORTING_BASE_PROD.SHARED.SESSION_AB_TEST_START as m
left join (
    select distinct PROPERTIES_SESSION_ID
    from lake.SEGMENT_FL.JAVASCRIPT_FABLETICS_PRODUCT_SEARCHED
    WHERE PROPERTIES_SESSION_ID IS NOT NULL
    ) as p
    on p.PROPERTIES_SESSION_ID = m.META_ORIGINAL_SESSION_ID
left join (
    select distinct PROPERTIES_SESSION_ID
    from lake.SEGMENT_FL.REACT_NATIVE_FABLETICS_PRODUCT_SEARCHED
    where PROPERTIES_SESSION_ID IS NOT NULL
    ) as pp
    on IFNULL(try_to_number(pp.PROPERTIES_SESSION_ID)::VARCHAR, '0')::VARCHAR = m.META_ORIGINAL_SESSION_ID
    and pp.PROPERTIES_SESSION_ID <> '0'
where
    ab_test_key ilike 'FLUSSSVCSTR_v%'
    and split_part(AB_TEST_KEY,'_v',2) >= 2;

CREATE OR REPLACE TEMPORARY TABLE _customers_with_multiple_test_segments as
select distinct CUSTOMER_ID
from (
    select
        CUSTOMER_ID,
        count(distinct original_ab_test_segment) as ab_test_segment_count
    from _constructor_session_base_unadj
    group by 1)
where ab_test_segment_count > 1;

CREATE OR REPLACE TEMPORARY TABLE _customers_with_single_test_segments as
select distinct u.customer_id
from (
    select distinct customer_id
    from _constructor_session_base_unadj
    ) as u
left join _customers_with_multiple_test_segments as m
    on u.customer_id = m.customer_id
where m.CUSTOMER_ID is null;

CREATE OR REPLACE TEMPORARY TABLE _new_segments as
select
        m.CUSTOMER_ID,original_ab_test_segment,
        3 as new_ab_test_segment,
        ab_test_key
from _customers_with_multiple_test_segments as m
join _constructor_session_base_unadj as s
    on s.CUSTOMER_ID = m.CUSTOMER_ID
where rnk_sessions_from_customer = 1
    and original_ab_test_segment = 1
    and is_search_event_sessions = true

union

select
    m.CUSTOMER_ID,
    original_ab_test_segment,
    4 as new_ab_test_segment,ab_test_key
from _customers_with_multiple_test_segments as m
join _constructor_session_base_unadj as s
    on s.CUSTOMER_ID = m.CUSTOMER_ID
where rnk_sessions_from_customer = 1
    and original_ab_test_segment = 2
    and is_search_event_sessions = true;

delete from _constructor_session_base_unadj where is_search_event_sessions = false;

update _constructor_session_base_unadj
set new_ab_test_segment = 3
where CUSTOMER_ID in (select customer_id from _new_segments where original_ab_test_segment = 1);

update _constructor_session_base_unadj
set new_ab_test_segment = 4
where CUSTOMER_ID in (select customer_id from _new_segments where original_ab_test_segment = 2);

CREATE OR REPLACE TEMPORARY TABLE _constructor_user_single_groups as
select
    TRUE as is_user_level
    ,SESSION_ID
    ,META_ORIGINAL_SESSION_ID
    ,s.CUSTOMER_ID
    ,AB_TEST_START_LOCAL_DATETIME
    ,original_ab_test_segment
    ,new_ab_test_segment
    ,TEST_START_MEMBERSHIP_STATE
    ,TEST_START_LEAD_DAILY_TENURE
    ,TEST_START_VIP_MONTH_TENURE
    ,ab_test_key
    ,concat(AB_TEST_KEY,' (User - Single Group)') as new_ab_test_key --user level
    ,CAST(coalesce(new_ab_test_segment,original_ab_test_segment) AS VARCHAR) AS AB_TEST_SEGMENT
    ,case when AB_TEST_SEGMENT = 1 then 'Control'
            when AB_TEST_SEGMENT = 2 then 'Variant' end as test_group
from _constructor_session_base_unadj as s
join _customers_with_single_test_segments as m on m.customer_id = s.CUSTOMER_ID
where
    split_part(AB_TEST_KEY,'_v',2) >= 3;

CREATE OR REPLACE TEMPORARY TABLE _constructor_user_combo_groups as
select
    TRUE as is_user_level
    ,SESSION_ID
    ,META_ORIGINAL_SESSION_ID
    ,s.CUSTOMER_ID
    ,AB_TEST_START_LOCAL_DATETIME
    ,original_ab_test_segment
    ,new_ab_test_segment
    ,TEST_START_MEMBERSHIP_STATE
    ,TEST_START_LEAD_DAILY_TENURE
    ,TEST_START_VIP_MONTH_TENURE
    ,ab_test_key
    ,concat(AB_TEST_KEY,' (User - Combo Groups)') as new_ab_test_key --user level
    ,CAST(coalesce(new_ab_test_segment,original_ab_test_segment) AS VARCHAR) AS AB_TEST_SEGMENT
    ,case when AB_TEST_SEGMENT = 3 then 'Control'
            when AB_TEST_SEGMENT = 4 then 'Variant' end as test_group
from _constructor_session_base_unadj as s
join _customers_with_multiple_test_segments as m on m.customer_id = s.CUSTOMER_ID
where
    split_part(AB_TEST_KEY,'_v',2) >= 3;

CREATE OR REPLACE TEMPORARY TABLE _constructor_session as
select
    FALSE as is_user_level
    ,SESSION_ID
    ,META_ORIGINAL_SESSION_ID
    ,CUSTOMER_ID
    ,AB_TEST_START_LOCAL_DATETIME
    ,original_ab_test_segment
    ,null new_ab_test_segment
    ,TEST_START_MEMBERSHIP_STATE
    ,TEST_START_LEAD_DAILY_TENURE
    ,TEST_START_VIP_MONTH_TENURE
    ,ab_test_key
    ,AB_TEST_KEY as new_ab_test_key --user level
    ,original_ab_test_segment AS AB_TEST_SEGMENT
    ,case when AB_TEST_SEGMENT = 1 then 'Control'
            when AB_TEST_SEGMENT = 2 then 'Variant' end as test_group
from _constructor_session_base_unadj;

CREATE OR REPLACE TEMPORARY TABLE _constructor_session_base as
select * from _constructor_session
union all
select * from _constructor_user_single_groups
union all
select * from _constructor_user_combo_groups;
--counts will be off bcuz user level reporting filters out prospects with no customer id / lead reg

// constructor user level validation
-- select
--     is_user_level
--     ,new_ab_test_key
--     ,test_group
--     ,coalesce(new_ab_test_segment,original_ab_test_segment) as test_segment_adjust
--     ,count(*) as sessions
--     ,count(distinct CUSTOMER_ID) as customers
-- from _constructor_session_base
-- where ab_test_key ilike 'FLUSSSVCSTR_v3%' and TEST_START_MEMBERSHIP_STATE <> 'Prospect'
-- group by 1,2,3,4

-- select distinct a.CUSTOMER_ID
-- from (select distinct customer_id from _constructor_session where ab_test_key = 'FLUSSSVCSTR_v3') as a
-- left join (select distinct customer_id from _constructor_user_single_groups) as b on a.customer_id = b.CUSTOMER_ID
-- left join (select distinct customer_id from _constructor_user_combo_groups) as c on a.customer_id = c.CUSTOMER_ID
-- where b.CUSTOMER_ID is null and c.CUSTOMER_ID is null

------------------------------------------------------------------------------------------

create or replace temp table _session_ab_test_cms_framework as --comment
-- create or replace transient table reporting_base_prod.shared.session_ab_test_cms_framework  as --comment
select
    k.TEST_FRAMEWORK_ID
    ,k.test_label
    ,k.TEST_FRAMEWORK_DESCRIPTION
    ,k.TEST_FRAMEWORK_TICKET
    ,k.CAMPAIGN_CODE
    ,k.TEST_TYPE
    ,k.CMS_MIN_TEST_START_DATETIME_HQ
    ,k.CMS_START_DATE
    ,k.CMS_END_DATE

    ,m.STORE_ID
    ,r.store_brand
    ,r.STORE_REGION
    ,r.STORE_COUNTRY
    ,STORE_NAME
    ,SPECIALTY_COUNTRY_CODE
    ,k.TEST_KEY
    ,CAST(s.AB_TEST_SEGMENT AS VARCHAR) AS AB_TEST_SEGMENT
    ,null as test_group
    ,s.AB_TEST_START_LOCAL_DATETIME
    ,s.AB_TEST_START_LOCAL_DATETIME::date as AB_TEST_START_LOCAL_DATE
    ,date_trunc('month',s.AB_TEST_START_LOCAL_DATETIME)::date as AB_TEST_START_LOCAL_MONTH_DATE
    ,rank() over (partition by s.customer_id,k.TEST_KEY,ab_test_start_local_month_date ORDER BY s.session_id asc) as rnk_monthly
    ,m.SESSION_LOCAL_DATETIME
    ,date_trunc('month',s.AB_TEST_START_LOCAL_DATETIME)::date as SESSION_LOCAL_MONTH_DATE
    ,m.VISITOR_ID
    ,m.SESSION_ID
    ,m.CUSTOMER_ID
    ,m.MEMBERSHIP_ID

    -- membership
    ,m.MEMBERSHIP_STATE as SESSION_START_MEMBERSHIP_STATE
    ,s.TEST_START_MEMBERSHIP_STATE

    ,s.TEST_START_LEAD_DAILY_TENURE

    ,case when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('hh',REGISTRATION_LOCAL_DATETIME,AB_TEST_START_LOCAL_DATETIME) + 1) between 1 and 3 THEN concat('H',datediff('hh',REGISTRATION_LOCAL_DATETIME,AB_TEST_START_LOCAL_DATETIME) + 1)
          when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('hh',REGISTRATION_LOCAL_DATETIME,AB_TEST_START_LOCAL_DATETIME) + 1) BETWEEN 4 and 24 THEN 'D1'
          when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('day',REGISTRATION_LOCAL_DATETIME,AB_TEST_START_LOCAL_DATETIME) + 1) between 2 and 30 then concat('D',TEST_START_LEAD_DAILY_TENURE)
          when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('day',REGISTRATION_LOCAL_DATETIME,AB_TEST_START_LOCAL_DATETIME) + 1) >= 31 then 'M2+'
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
    ,case when lower(platform) in ('desktop','tablet') then 'Desktop'
        when lower(platform) in ('mobile','unknown') then 'Mobile'
        when platform = 'Mobile App' then 'App' else platform end as PLATFORM
    ,PLATFORM AS platform_raw
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
--     ,NULL as IS_SHOPPING_GRID_VISIT
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

    ,k.IS_CMS_SPECIFIC_GENDER
    ,case when lower(k.IS_CMS_SPECIFIC_GENDER) = 'm' and k.test_type = 'session' and is_male_session_customer = TRUE then TRUE
    when lower(k.IS_CMS_SPECIFIC_GENDER) = 'm' and k.test_type = 'membership' and IS_MALE_CUSTOMER = TRUE then TRUE
    when lower(k.IS_CMS_SPECIFIC_GENDER) = 'f' and k.test_type = 'session' and is_male_session_customer = FALSE then TRUE
    when lower(k.IS_CMS_SPECIFIC_GENDER) = 'f' and k.test_type = 'membership' and IS_MALE_CUSTOMER = FALSE then TRUE
    when lower(k.IS_CMS_SPECIFIC_GENDER) is null then TRUE else FALSE end as is_valid_gender_sessions

    ,IS_BOT
    ,k.IS_CMS_SPECIFIC_LEAD_DAY
    ,contains(k.IS_CMS_SPECIFIC_LEAD_DAY,s.TEST_START_LEAD_DAILY_TENURE) as flag_is_cms_filtered_lead
    ,IS_MIGRATED_SESSION,
    NVL(is_in_segment, TRUE) as is_in_segment
    ,$current_end_date_hq as hq_max_datetime
from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA_FILTERED as k
join (
    SELECT A.*, B.store_id
    FROM REPORTING_BASE_PROD.SHARED.SESSION_AB_TEST_START AS A
    LEFT JOIN REPORTING_BASE_PROD.SHARED.SESSION AS B
    ON A.session_id = B.session_id
    ) as s on s.AB_TEST_KEY = k.test_key
JOIN REPORTING_BASE_PROD.SHARED.SESSION AS m ON s.SESSION_ID = m.SESSION_ID
    and s.store_id = m.store_id
    and convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME)::datetime between CMS_MIN_TEST_START_DATETIME_HQ and $current_end_date_hq
    and m.SESSION_LOCAL_DATETIME>$last_load_datetime --comment
JOIN EDW_PROD.DATA_MODEL.DIM_STORE AS r ON m.store_id = r.store_id
    and k.STORE_BRAND = r.STORE_BRAND
    and r.store_type <> 'Retail'
join REPORTING_PROD.shared.AB_TEST_CMS_METADATA_SCAFFOLD as f on f.TEST_KEY = s.AB_TEST_KEY
    and f.TEST_LABEL = k.TEST_LABEL
    and f.STORE_COUNTRY = r.STORE_COUNTRY
    and f.CMS_MEMBERSHIP_STATE = s.TEST_START_MEMBERSHIP_STATE
    and f.STORE_BRAND = k.STORE_BRAND
    and f.statuscode = k.statuscode
LEFT JOIN REPORTING_BASE_PROD.SHARED.DIM_GATEWAY g ON g.dm_gateway_id = m.dm_gateway_id
    and EFFECTIVE_START_DATETIME <= m.SESSION_LOCAL_DATETIME
    and EFFECTIVE_END_DATETIME > m.SESSION_LOCAL_DATETIME
left join edw_prod.DATA_MODEL.DIM_CUSTOMER as dc on dc.CUSTOMER_ID = m.CUSTOMER_ID
left join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP as p on p.CUSTOMER_ID = dc.CUSTOMER_ID
left join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP_TYPE as t on t.MEMBERSHIP_TYPE_ID = p.MEMBERSHIP_TYPE_ID
left join REPORTING_BASE_PROD.SHARED.MEDIA_SOURCE_CHANNEL_MAPPING as h on h.MEDIA_SOURCE_HASH = m.MEDIA_SOURCE_HASH
left join (select distinct ltd.customer_id,SOURCE_ACTIVATION_LOCAL_DATETIME,SOURCE_NEXT_ACTIVATION_LOCAL_DATETIME,ltd.VIP_COHORT_MONTH_DATE,CUMULATIVE_PRODUCT_GROSS_PROFIT_DECILE,CUMULATIVE_CASH_GROSS_PROFIT_DECILE
            from edw_prod.analytics_base.customer_lifetime_value_LTD as ltd
            join edw_prod.DATA_MODEL.FACT_ACTIVATION as f on f.ACTIVATION_KEY = ltd.ACTIVATION_KEY and f.customer_id = ltd.customer_id) as ltd on
                ltd.customer_id = m.customer_id
                and s.AB_TEST_START_LOCAL_DATETIME between SOURCE_ACTIVATION_LOCAL_DATETIME and SOURCE_NEXT_ACTIVATION_LOCAL_DATETIME
where
    IS_TEST_CUSTOMER_ACCOUNT = FALSE
    AND NVL(is_in_segment, TRUE) != FALSE
    and k.CAMPAIGN_CODE <> 'FLUSSSVCSTR'

union all

------------ CONSTRUCTOR CUSTOM REPORTING
select
    k.TEST_FRAMEWORK_ID
    ,k.test_label
    ,k.TEST_FRAMEWORK_DESCRIPTION
    ,k.TEST_FRAMEWORK_TICKET
    ,k.CAMPAIGN_CODE
    ,iff(is_user_level = TRUE,'membership','session') as TEST_TYPE
    ,k.CMS_MIN_TEST_START_DATETIME_HQ
    ,k.CMS_START_DATE
    ,k.CMS_END_DATE

    ,m.STORE_ID
    ,r.store_brand
    ,r.STORE_REGION
    ,r.STORE_COUNTRY
    ,STORE_NAME
    ,SPECIALTY_COUNTRY_CODE

 --constructor user level custom reporting
    ,new_ab_test_key as TEST_KEY --user level
    ,CAST(coalesce(new_ab_test_segment,original_ab_test_segment) AS VARCHAR) AS AB_TEST_SEGMENT
    ,s.test_group

    ,s.AB_TEST_START_LOCAL_DATETIME as AB_TEST_START_LOCAL_DATETIME
    ,s.AB_TEST_START_LOCAL_DATETIME::date as AB_TEST_START_LOCAL_DATE
    ,date_trunc('month',s.AB_TEST_START_LOCAL_DATETIME)::date as AB_TEST_START_LOCAL_MONTH_DATE
    ,null as rnk_monthly
    ,m.SESSION_LOCAL_DATETIME
    ,date_trunc('month',s.AB_TEST_START_LOCAL_DATETIME)::date as SESSION_LOCAL_MONTH_DATE
    ,m.VISITOR_ID
    ,m.SESSION_ID
    ,m.CUSTOMER_ID
    ,m.MEMBERSHIP_ID

    -- membership
    ,m.MEMBERSHIP_STATE as SESSION_START_MEMBERSHIP_STATE
    ,s.TEST_START_MEMBERSHIP_STATE

    ,s.TEST_START_LEAD_DAILY_TENURE

    ,case when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('hh',REGISTRATION_LOCAL_DATETIME,AB_TEST_START_LOCAL_DATETIME) + 1) between 1 and 3 THEN concat('H',datediff('hh',REGISTRATION_LOCAL_DATETIME,AB_TEST_START_LOCAL_DATETIME) + 1)
          when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('hh',REGISTRATION_LOCAL_DATETIME,AB_TEST_START_LOCAL_DATETIME) + 1) BETWEEN 4 and 24 THEN 'D1'
          when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('day',REGISTRATION_LOCAL_DATETIME,AB_TEST_START_LOCAL_DATETIME) + 1) between 2 and 30 then concat('D',TEST_START_LEAD_DAILY_TENURE)
          when TEST_START_MEMBERSHIP_STATE = 'Lead' and (datediff('day',REGISTRATION_LOCAL_DATETIME,AB_TEST_START_LOCAL_DATETIME) + 1) >= 31 then 'M2+'
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
    ,case when lower(platform) in ('desktop','tablet') then 'Desktop'
        when lower(platform) in ('mobile','unknown') then 'Mobile'
        when platform = 'Mobile App' then 'App' else platform end as PLATFORM
    ,PLATFORM AS platform_raw
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
--     ,NULL as IS_SHOPPING_GRID_VISIT
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

    ,k.IS_CMS_SPECIFIC_GENDER
    ,case when lower(k.IS_CMS_SPECIFIC_GENDER) = 'm' and k.test_type = 'session' and is_male_session_customer = TRUE then TRUE
    when lower(k.IS_CMS_SPECIFIC_GENDER) = 'm' and k.test_type = 'membership' and IS_MALE_CUSTOMER = TRUE then TRUE
    when lower(k.IS_CMS_SPECIFIC_GENDER) = 'f' and k.test_type = 'session' and is_male_session_customer = FALSE then TRUE
    when lower(k.IS_CMS_SPECIFIC_GENDER) = 'f' and k.test_type = 'membership' and IS_MALE_CUSTOMER = FALSE then TRUE
    when lower(k.IS_CMS_SPECIFIC_GENDER) is null then TRUE else FALSE end as is_valid_gender_sessions

    ,IS_BOT
    ,k.IS_CMS_SPECIFIC_LEAD_DAY
    ,contains(k.IS_CMS_SPECIFIC_LEAD_DAY,s.TEST_START_LEAD_DAILY_TENURE) as flag_is_cms_filtered_lead
    ,IS_MIGRATED_SESSION,
    NVL(is_in_segment, TRUE) as is_in_segment
    ,$current_end_date_hq as hq_max_datetime
from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA_FILTERED as k
join _constructor_session_base as s on s.AB_TEST_KEY = k.test_key
JOIN REPORTING_BASE_PROD.SHARED.SESSION AS m ON s.SESSION_ID = m.SESSION_ID
    and convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME)::datetime between CMS_MIN_TEST_START_DATETIME_HQ and $current_end_date_hq
    and m.SESSION_LOCAL_DATETIME>$last_load_datetime --comment
JOIN EDW_PROD.DATA_MODEL.DIM_STORE AS r ON m.store_id = r.store_id
    and k.STORE_BRAND = r.STORE_BRAND
    and r.store_type <> 'Retail'
join REPORTING_PROD.shared.AB_TEST_CMS_METADATA_SCAFFOLD as f on f.TEST_KEY = s.AB_TEST_KEY
    and f.TEST_LABEL = k.TEST_LABEL
    and f.STORE_COUNTRY = r.STORE_COUNTRY
    and f.CMS_MEMBERSHIP_STATE = s.TEST_START_MEMBERSHIP_STATE
    and f.STORE_BRAND = k.STORE_BRAND
    and f.statuscode = k.statuscode
LEFT JOIN REPORTING_BASE_PROD.SHARED.DIM_GATEWAY g ON g.dm_gateway_id = m.dm_gateway_id
    and EFFECTIVE_START_DATETIME <= m.SESSION_LOCAL_DATETIME
    and EFFECTIVE_END_DATETIME > m.SESSION_LOCAL_DATETIME
left join edw_prod.DATA_MODEL.DIM_CUSTOMER as dc on dc.CUSTOMER_ID = m.CUSTOMER_ID
left join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP as p on p.CUSTOMER_ID = dc.CUSTOMER_ID
left join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP_TYPE as t on t.MEMBERSHIP_TYPE_ID = p.MEMBERSHIP_TYPE_ID
left join REPORTING_BASE_PROD.SHARED.MEDIA_SOURCE_CHANNEL_MAPPING as h on h.MEDIA_SOURCE_HASH = m.MEDIA_SOURCE_HASH
left join (select distinct ltd.customer_id,SOURCE_ACTIVATION_LOCAL_DATETIME,SOURCE_NEXT_ACTIVATION_LOCAL_DATETIME,ltd.VIP_COHORT_MONTH_DATE,CUMULATIVE_PRODUCT_GROSS_PROFIT_DECILE,CUMULATIVE_CASH_GROSS_PROFIT_DECILE
            from edw_prod.analytics_base.customer_lifetime_value_LTD as ltd
            join edw_prod.DATA_MODEL.FACT_ACTIVATION as f on f.ACTIVATION_KEY = ltd.ACTIVATION_KEY and f.customer_id = ltd.customer_id) as ltd on
                ltd.customer_id = m.customer_id
                and s.AB_TEST_START_LOCAL_DATETIME between SOURCE_ACTIVATION_LOCAL_DATETIME and SOURCE_NEXT_ACTIVATION_LOCAL_DATETIME
where
    IS_TEST_CUSTOMER_ACCOUNT = FALSE
    AND NVL(is_in_segment, TRUE) != FALSE;

BEGIN; --comment

delete from reporting_base_prod.shared.session_ab_test_cms_framework s  --comment
using _session_ab_test_cms_framework AS m
where s.SESSION_ID = m.SESSION_ID;

insert into reporting_base_prod.shared.session_ab_test_cms_framework  --comment
(
TEST_FRAMEWORK_ID,
TEST_LABEL,
TEST_FRAMEWORK_DESCRIPTION,
TEST_FRAMEWORK_TICKET,
CAMPAIGN_CODE,
TEST_TYPE,
CMS_MIN_TEST_START_DATETIME_HQ,
CMS_START_DATE,
CMS_END_DATE,
STORE_ID,
STORE_BRAND,
STORE_REGION,
STORE_COUNTRY,
STORE_NAME,
SPECIALTY_COUNTRY_CODE,
TEST_KEY,
AB_TEST_SEGMENT,
TEST_GROUP,
AB_TEST_START_LOCAL_DATETIME,
AB_TEST_START_LOCAL_DATE,
AB_TEST_START_LOCAL_MONTH_DATE,
RNK_MONTHLY,
SESSION_LOCAL_DATETIME,
SESSION_LOCAL_MONTH_DATE,
VISITOR_ID,
SESSION_ID,
CUSTOMER_ID,
MEMBERSHIP_ID,
SESSION_START_MEMBERSHIP_STATE,
TEST_START_MEMBERSHIP_STATE,
TEST_START_LEAD_DAILY_TENURE,
TEST_START_LEAD_TENURE_GROUP,
TEST_START_VIP_MONTH_TENURE,
TEST_START_VIP_MONTH_TENURE_GROUP,
MEMBERSHIP_PRICE,
MEMBERSHIP_TYPE,
CUMULATIVE_PRODUCT_GROSS_PROFIT_DECILE,
CUMULATIVE_CASH_GROSS_PROFIT_DECILE,
PLATFORM,
PLATFORM_RAW,
OS,
OS_RAW,
BROWSER,
IS_QUIZ_START_ACTION,
IS_QUIZ_COMPLETE_ACTION,
IS_SKIP_QUIZ_ACTION,
IS_LEAD_REGISTRATION_ACTION,
IS_QUIZ_REGISTRATION_ACTION,
IS_SPEEDY_REGISTRATION_ACTION,
IS_SKIP_QUIZ_REGISTRATION_ACTION,
LEAD_REG_TYPE,
REGISTRATION_LOCAL_DATETIME,
IS_YITTY_GATEWAY,
IS_MALE_GATEWAY,
IS_MALE_SESSION_ACTION,
IS_MALE_SESSION,
IS_MALE_CUSTOMER,
IS_MALE_SESSION_CUSTOMER,
-- IS_SHOPPING_GRID_VISIT,
IS_PDP_VISIT,
IS_ATB_ACTION,
IS_CART_CHECKOUT_VISIT,
IS_SKIP_MONTH_ACTION,
IS_LOGIN_ACTION,
CHANNEL,
SUBCHANNEL,
GATEWAY_TYPE,
GATEWAY_SUB_TYPE,
DM_GATEWAY_ID,
GATEWAY_CODE,
GATEWAY_NAME,
DM_SITE_ID,
CUSTOMER_STATE,
HDYH,
IS_CMS_SPECIFIC_GENDER,
IS_VALID_GENDER_SESSIONS,
IS_BOT,
IS_CMS_SPECIFIC_LEAD_DAY,
FLAG_IS_CMS_FILTERED_LEAD,
IS_MIGRATED_SESSION,
hq_max_datetime,
is_in_segment
)

-- create or replace transient table reporting_base.shared.session_ab_test_cms_framework as
select
TEST_FRAMEWORK_ID,
TEST_LABEL,
TEST_FRAMEWORK_DESCRIPTION,
TEST_FRAMEWORK_TICKET,
CAMPAIGN_CODE,
TEST_TYPE,
CMS_MIN_TEST_START_DATETIME_HQ,
CMS_START_DATE,
CMS_END_DATE,
STORE_ID,
STORE_BRAND,
STORE_REGION,
STORE_COUNTRY,
STORE_NAME,
SPECIALTY_COUNTRY_CODE,
TEST_KEY,
AB_TEST_SEGMENT,
TEST_GROUP,
AB_TEST_START_LOCAL_DATETIME,
AB_TEST_START_LOCAL_DATE,
AB_TEST_START_LOCAL_MONTH_DATE,
RNK_MONTHLY,
SESSION_LOCAL_DATETIME,
SESSION_LOCAL_MONTH_DATE,
VISITOR_ID,
SESSION_ID,
CUSTOMER_ID,
MEMBERSHIP_ID,
SESSION_START_MEMBERSHIP_STATE,
TEST_START_MEMBERSHIP_STATE,
TEST_START_LEAD_DAILY_TENURE,
TEST_START_LEAD_TENURE_GROUP,
TEST_START_VIP_MONTH_TENURE,
TEST_START_VIP_MONTH_TENURE_GROUP,
MEMBERSHIP_PRICE,
MEMBERSHIP_TYPE,
CUMULATIVE_PRODUCT_GROSS_PROFIT_DECILE,
CUMULATIVE_CASH_GROSS_PROFIT_DECILE,
PLATFORM,
PLATFORM_RAW,
OS,
OS_RAW,
BROWSER,
IS_QUIZ_START_ACTION,
IS_QUIZ_COMPLETE_ACTION,
IS_SKIP_QUIZ_ACTION,
IS_LEAD_REGISTRATION_ACTION,
IS_QUIZ_REGISTRATION_ACTION,
IS_SPEEDY_REGISTRATION_ACTION,
IS_SKIP_QUIZ_REGISTRATION_ACTION,
LEAD_REG_TYPE,
REGISTRATION_LOCAL_DATETIME,
IS_YITTY_GATEWAY,
IS_MALE_GATEWAY,
IS_MALE_SESSION_ACTION,
IS_MALE_SESSION,
IS_MALE_CUSTOMER,
IS_MALE_SESSION_CUSTOMER,
-- IS_SHOPPING_GRID_VISIT,
IS_PDP_VISIT,
IS_ATB_ACTION,
IS_CART_CHECKOUT_VISIT,
IS_SKIP_MONTH_ACTION,
IS_LOGIN_ACTION,
CHANNEL,
SUBCHANNEL,
GATEWAY_TYPE,
GATEWAY_SUB_TYPE,
DM_GATEWAY_ID,
GATEWAY_CODE,
GATEWAY_NAME,
DM_SITE_ID,
CUSTOMER_STATE,
HDYH,
IS_CMS_SPECIFIC_GENDER,
IS_VALID_GENDER_SESSIONS,
IS_BOT,
IS_CMS_SPECIFIC_LEAD_DAY,
FLAG_IS_CMS_FILTERED_LEAD,
IS_MIGRATED_SESSION,
hq_max_datetime,
is_in_segment
from _session_ab_test_cms_framework;

delete from reporting_base_prod.shared.session_ab_test_cms_framework  --comment
where
    (is_valid_gender_sessions = FALSE and store_brand = 'Fabletics')
    or IS_BOT = TRUE
    or flag_is_cms_filtered_lead = FALSE
    OR NVL(is_in_segment, TRUE) = FALSE;

COMMIT;  --comment

-- select --comment
--     store_brand
--     ,test_key
--     ,test_label
--     ,test_group
--     ,AB_TEST_SEGMENT
--     ,count(*)
--     ,count(distinct SESSION_ID)
-- from reporting_base_prod.shared.session_ab_test_cms_framework
-- group by 1,2,3,4,5
