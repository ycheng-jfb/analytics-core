SET current_end_date_hq = (
    SELECT MIN(t.max_hq_datetime)
    FROM (
        SELECT MAX(convert_timezone('America/Los_Angeles', SESSION_LOCAL_DATETIME))::datetime as max_hq_datetime
        FROM REPORTING_BASE_PROD.SHARED.SESSION
    ) t
);

-- set last_load_datetime = ( --comment
--     SELECT max(a.max_record_pst)::date - 14 as last_load_datetime
--     FROM reporting_base_prod.shared.session_ab_test_ds_image_sort a
--     JOIN reporting_base_prod.shared.session b ON a.session_id = b.session_id
-- );

CREATE OR REPLACE TEMPORARY TABLE _last_3_months as
select distinct
    TEST_KEY
    ,test_label
    ,test_framework_id
    ,STORE_BRAND
    ,min(AB_TEST_START_LOCAL_DATETIME) as MIN_AB_TEST_START_LOCAL_DATETIME
    ,max(AB_TEST_START_LOCAL_DATETIME) as MAX_AB_TEST_START_LOCAL_DATETIME
from REPORTING_BASE_PROD.SHARED.SESSION_ORDER_AB_TEST_DETAIL
where
    (TEST_KEY ilike 'FLUSSSVCSTR_v3%' and TEST_KEY not ilike '%PSRC%')
        or (TEST_TYPE = 'Builder' and TEST_ACTIVATED_DATETIME >= '2024-10-10')
group by 1,2,3,4;

delete from _last_3_months
    where max_AB_TEST_START_LOCAL_DATETIME <= CURRENT_DATE - INTERVAL '3 MONTH';

CREATE OR REPLACE TEMPORARY TABLE _abt_sessions as
SELECT DISTINCT
    s.SESSION_ID
    ,edw_prod.stg.udf_unconcat_brand(s.SESSION_ID) as meta_original_session_id
    ,s.CUSTOMER_ID
    ,edw_prod.stg.udf_unconcat_brand(s.CUSTOMER_ID) as meta_original_customer_id
    ,AB_TEST_START_LOCAL_DATETIME
    ,convert_timezone(STORE_TIME_ZONE,'UTC',AB_TEST_START_LOCAL_DATETIME::datetime)::datetime as AB_TEST_START_UTC_DATETIME
    ,convert_timezone(STORE_TIME_ZONE,'America/Los_Angeles', AB_TEST_START_LOCAL_DATETIME::datetime)::datetime as AB_TEST_START_PST_DATETIME
    ,s.SESSION_LOCAL_DATETIME
    ,s.TEST_KEY
    ,s.TEST_LABEL
    ,s.test_framework_id
    ,TEST_ACTIVATED_DATETIME AS TEST_EFFECTIVE_START_DATETIME_PST
    ,convert_timezone('America/Los_Angeles','UTC',TEST_ACTIVATED_DATETIME::datetime)::datetime as TEST_EFFECTIVE_START_DATETIME_UTC
    ,convert_timezone('America/Los_Angeles',STORE_TIME_ZONE,TEST_ACTIVATED_DATETIME::datetime)::datetime as TEST_EFFECTIVE_START_DATETIME_LOCAL
    ,TEST_FRAMEWORK_TICKET
    ,TEST_TYPE
    ,s.CAMPAIGN_CODE
    ,s.TEST_FRAMEWORK_DESCRIPTION
    ,s.platform
    ,t.STORE_ID
    ,AB_TEST_SEGMENT
    ,TEST_GROUP_DESCRIPTION
    ,test_group
    ,s.STORE_BRAND
    ,s.STORE_REGION
    ,s.STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,s.is_male_customer
    ,IS_TEST_CUSTOMER_ACCOUNT
    ,IS_BOT
    ,IS_IN_SEGMENT
FROM REPORTING_BASE_PROD.SHARED.SESSION_ORDER_AB_TEST_DETAIL as s
join REPORTING_BASE_PROD.SHARED.SESSION as n on n.SESSION_ID = s.SESSION_ID
    and convert_timezone('America/Los_Angeles', s.AB_TEST_START_LOCAL_DATETIME::datetime)::datetime
        between s.TEST_ACTIVATED_DATETIME and $current_end_date_hq
            --and s.SESSION_LOCAL_DATETIME > $last_load_datetime --comment
JOIN _last_3_months AS l3 on l3.test_key = s.test_key
        and l3.STORE_BRAND = s.STORE_BRAND
join EDW_PROD.DATA_MODEL.DIM_STORE as t on t.STORE_ID = n.STORE_ID
left join (select test_framework_id,code,min(EFFECTIVE_START_DATETIME) as EFFECTIVE_START_DATETIME,max(EFFECTIVE_END_DATETIME) EFFECTIVE_END_DATETIME
          from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS_HISTORY.TEST_FRAMEWORK where STATUSCODE = 113 group by 1,2) as fr ON fr.TEST_FRAMEWORK_ID = s.TEST_FRAMEWORK_ID
          and s.TEST_KEY = fr.CODE
where
    n.SESSION_LOCAL_DATETIME >= TEST_EFFECTIVE_START_DATETIME_LOCAL
    and TEST_START_MEMBERSHIP_STATE in ('VIP','Cancelled','Lead','Prospect');

-- select test_key,count(*) as count,count(distinct SESSION_ID) as count_distinct,count - count_distinct as diff
-- from _abt_sessions group by all

CREATE OR REPLACE TEMPORARY TABLE _min_max as
select
    test_key
    ,TEST_EFFECTIVE_START_DATETIME_PST
    ,TEST_EFFECTIVE_START_DATETIME_UTC
    ,min(meta_original_session_id) as min_session_id
    ,max(AB_TEST_START_PST_DATETIME::datetime) as max_datetime_pst
    ,max(AB_TEST_START_UTC_DATETIME::datetime) as max_datetime_utc
from _abt_sessions
group by all;

CREATE OR REPLACE TEMPORARY TABLE _grid_views_psource as
select
    EVENT
    ,'Web' as event_type
    ,a.test_key
    ,a.test_group
    ,a.meta_original_session_id
    ,a.meta_original_customer_id
    ,AB_TEST_START_UTC_DATETIME
    ,PROPERTIES_LIST_ID as psource
    ,count(*) as count
from _abt_sessions as a
join LAKE.SEGMENT_FL.JAVASCRIPT_FABLETICS_PRODUCT_LIST_VIEWED as p on a.meta_original_session_id = try_to_number(p.PROPERTIES_SESSION_ID)
    and properties_automated_test = FALSE
join _min_max as mm on mm.test_key = a.TEST_KEY
where
    try_to_number(p.PROPERTIES_SESSION_ID) >= mm.min_session_id
    and TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
group by all

union all

select
    EVENT
    ,'App' as event_type
    ,a.test_key
    ,a.test_group
    ,a.meta_original_session_id
    ,a.meta_original_customer_id
    ,AB_TEST_START_UTC_DATETIME
    ,PROPERTIES_LIST_ID as psource
    ,count(*) as count
from _abt_sessions as a
join LAKE.SEGMENT_FL.REACT_NATIVE_FABLETICS_PRODUCT_LIST_VIEWED as p on a.meta_original_session_id = try_to_number(p.PROPERTIES_SESSION_ID)
    and properties_automated_test = FALSE
join _min_max as mm on mm.test_key = a.TEST_KEY
where
    try_to_number(p.PROPERTIES_SESSION_ID) >= mm.min_session_id
    and TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
group by all;

CREATE OR REPLACE TEMPORARY TABLE _grid_views as
select
    EVENT
    ,event_type
    ,test_key
    ,test_group
    ,meta_original_session_id
    ,meta_original_customer_id
    ,AB_TEST_START_UTC_DATETIME
    ,count(*) as count
from _grid_views_psource
group by all;

// VALIDATION
-- select test_key,test_group,psource,count(*),count(distinct meta_original_session_id)
-- from _grid_views group by 1,2,3

-----------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _search_events as
select
    EVENT
    ,'Web' as event_type
    ,a.test_key
    ,a.test_group
    ,a.meta_original_session_id
    ,a.meta_original_customer_id
    ,a.AB_TEST_START_UTC_DATETIME
    ,count(*) as count
from _abt_sessions as a
join LAKE.SEGMENT_FL.JAVASCRIPT_FABLETICS_PRODUCT_SEARCHED as p on a.meta_original_session_id = try_to_number(p.PROPERTIES_SESSION_ID)
    and properties_automated_test = FALSE
join _min_max as mm on mm.test_key = a.TEST_KEY
where
    try_to_number(p.PROPERTIES_SESSION_ID) >= mm.min_session_id
    and TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
group by all

union all

select
    EVENT
    ,'App' as event_type
    ,a.TEST_KEY
    ,a.test_group
    ,a.meta_original_session_id
    ,a.meta_original_customer_id
    ,a.AB_TEST_START_UTC_DATETIME
    ,count(*) as count
from _abt_sessions as a
join LAKE.SEGMENT_FL.REACT_NATIVE_FABLETICS_PRODUCT_SEARCHED as p on a.meta_original_session_id = try_to_number(p.PROPERTIES_SESSION_ID)
    and properties_automated_test = FALSE
join _min_max as mm on mm.test_key = a.TEST_KEY
where
    try_to_number(p.PROPERTIES_SESSION_ID) >= mm.min_session_id
    and TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
group by all;

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE _pdp_views_psource as
select
    EVENT
    ,'Web' as event_type
    ,a.test_key
    ,a.test_group
    ,a.meta_original_session_id
    ,a.meta_original_customer_id
    ,a.AB_TEST_START_UTC_DATETIME
    ,PROPERTIES_LIST as psource
    ,count(*) as count
from _abt_sessions as a
join LAKE.SEGMENT_FL.JAVASCRIPT_FABLETICS_PRODUCT_VIEWED as p on a.meta_original_session_id = try_to_number(p.PROPERTIES_SESSION_ID)
    and properties_automated_test = FALSE
join _min_max as mm on mm.test_key = a.TEST_KEY
where --a.SESSION_ID = 1547354056520 and
    try_to_number(p.PROPERTIES_SESSION_ID) >= mm.min_session_id
    and TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME

group by all

union all

select
    EVENT
    ,'App' as event_type
    ,a.TEST_KEY
    ,a.test_group
    ,a.meta_original_session_id
    ,a.meta_original_customer_id
    ,a.AB_TEST_START_UTC_DATETIME
    ,PROPERTIES_LIST_ID as psource
    ,count(*) as count
from _abt_sessions as a
join LAKE.SEGMENT_FL.REACT_NATIVE_FABLETICS_PRODUCT_VIEWED as p on a.meta_original_session_id = try_to_number(p.PROPERTIES_SESSION_ID)
    and properties_automated_test = FALSE
join _min_max as mm on mm.test_key = a.TEST_KEY
where
    try_to_number(p.PROPERTIES_SESSION_ID) >= mm.min_session_id
    and TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
group by all;

CREATE OR REPLACE TEMPORARY TABLE _pdp_views as
select
    EVENT
    ,event_type
    ,test_key
    ,test_group
    ,meta_original_session_id
    ,meta_original_customer_id
    ,AB_TEST_START_UTC_DATETIME
    ,count(*) as count
from _pdp_views_psource
group by all;

-- no atb react table for app. per jon, use java version since ATB is supposed to be server side event
CREATE OR REPLACE TEMPORARY TABLE _atb_psource as
select
    EVENT
    ,'Web' as event_type
    ,a.TEST_KEY
    ,TEST_GROUP
    ,a.meta_original_session_id
    ,a.meta_original_customer_id
    ,a.AB_TEST_START_UTC_DATETIME
    ,PROPERTIES_LIST_ID as psource
    ,count(*) as count
from _abt_sessions as a
join LAKE.SEGMENT_FL.java_FABLETICS_product_added as p on a.meta_original_session_id = try_to_number(p.PROPERTIES_SESSION_ID)
    and properties_automated_test = FALSE
join _min_max as mm on mm.test_key = a.TEST_KEY
where
    try_to_number(p.PROPERTIES_SESSION_ID) >= mm.min_session_id
    and TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
group by all

union all

select
    EVENT
    ,'App' as event_type
    ,a.TEST_KEY
    ,TEST_GROUP
    ,a.meta_original_session_id
    ,a.meta_original_customer_id
    ,a.AB_TEST_START_UTC_DATETIME
    ,PROPERTIES_LIST_ID as psource
    ,count(*) as count
from _abt_sessions as a
join LAKE.SEGMENT_FL.JAVA_FABLETICS_ECOM_MOBILE_APP_PRODUCT_ADDED as p on a.meta_original_session_id = try_to_number(p.PROPERTIES_SESSION_ID)
    and properties_automated_test = FALSE
join _min_max as mm on mm.test_key = a.TEST_KEY
where
    try_to_number(p.PROPERTIES_SESSION_ID) >= mm.min_session_id
    and TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
group by all;

CREATE OR REPLACE TEMPORARY TABLE _atb as
select
    EVENT
    ,event_type
    ,test_key
    ,test_group
    ,meta_original_session_id
    ,meta_original_customer_id
    ,AB_TEST_START_UTC_DATETIME
    ,count(*) as count
from _atb_psource
group by all;

CREATE OR REPLACE TEMPORARY TABLE _orders_psource as
select
    EVENT
    ,'Web' as event_type
    ,a.TEST_KEY
    ,TEST_GROUP
    ,a.meta_original_session_id
    ,a.meta_original_customer_id
    ,a.AB_TEST_START_UTC_DATETIME
    ,PROPERTIES_PRODUCTS_LIST_ID as psource
    ,count(distinct case when properties_is_activating = true then o.PROPERTIES_ORDER_ID else 0 end) as orders_activating
    ,count(distinct case when properties_is_activating = false then o.PROPERTIES_ORDER_ID else 0 end) as orders_nonactivating
    ,count(case when properties_is_activating = true then o.PROPERTIES_ORDER_ID else 0 end) as units_activating
    ,count(case when properties_is_activating = false then o.PROPERTIES_ORDER_ID else 0 end) as units_nonactivating
    ,sum(case when properties_is_activating = true then o.PROPERTIES_PRODUCTS_PRICE else 0 end) as product_gross_revenue_activating
    ,sum(case when properties_is_activating = false then o.PROPERTIES_PRODUCTS_PRICE else 0 end) as product_gross_revenue_nonactivating
from _abt_sessions as a
join LAKE.SEGMENT_FL.JAVA_FABLETICS_ORDER_COMPLETED as o on a.meta_original_session_id = try_to_number(o.PROPERTIES_SESSION_ID)
    and properties_automated_test = FALSE
join _min_max as mm on mm.test_key = a.TEST_KEY
where
    try_to_number(o.PROPERTIES_SESSION_ID) >= mm.min_session_id
    and TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
group by all

union all

select
    EVENT
    ,'App' as event_type
    ,a.TEST_KEY
    ,TEST_GROUP
    ,a.meta_original_session_id
    ,a.meta_original_customer_id
    ,a.AB_TEST_START_UTC_DATETIME
    ,PROPERTIES_PRODUCTS_LIST_ID as psource
    ,count(distinct case when properties_is_activating = true then o.PROPERTIES_ORDER_ID else 0 end) as orders_activating
    ,count(distinct case when properties_is_activating = false then o.PROPERTIES_ORDER_ID else 0 end) as orders_nonactivating
    ,count(case when properties_is_activating = true then o.PROPERTIES_ORDER_ID else 0 end) as units_activating
    ,count(case when properties_is_activating = false then o.PROPERTIES_ORDER_ID else 0 end) as units_nonactivating
    ,sum(case when properties_is_activating = true then o.PROPERTIES_PRODUCTS_PRICE else 0 end) as product_gross_revenue_activating
    ,sum(case when properties_is_activating = false then o.PROPERTIES_PRODUCTS_PRICE else 0 end) as product_gross_revenue_nonactivating
from _abt_sessions as a
join LAKE.SEGMENT_FL.JAVA_FABLETICS_ECOM_MOBILE_APP_ORDER_COMPLETED as o on a.meta_original_session_id = try_to_number(o.PROPERTIES_SESSION_ID)
    and properties_automated_test = FALSE
join _min_max as mm on mm.test_key = a.TEST_KEY
where
    try_to_number(o.PROPERTIES_SESSION_ID) >= mm.min_session_id
    and TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
group by all;

CREATE OR REPLACE TEMPORARY TABLE _orders as
select
    EVENT
    ,'Web' as event_type
    ,a.TEST_KEY
    ,TEST_GROUP
    ,a.meta_original_session_id
    ,a.meta_original_customer_id
    ,a.AB_TEST_START_UTC_DATETIME
    ,count(distinct case when properties_is_activating = true then o.PROPERTIES_ORDER_ID else 0 end) as orders_activating
    ,count(distinct case when properties_is_activating = false then o.PROPERTIES_ORDER_ID else 0 end) as orders_nonactivating
    ,count(case when properties_is_activating = true then o.PROPERTIES_ORDER_ID else 0 end) as units_activating
    ,count(case when properties_is_activating = false then o.PROPERTIES_ORDER_ID else 0 end) as units_nonactivating
    ,sum(case when properties_is_activating = true then o.PROPERTIES_PRODUCTS_PRICE else 0 end) as product_gross_revenue_activating
    ,sum(case when properties_is_activating = false then o.PROPERTIES_PRODUCTS_PRICE else 0 end) as product_gross_revenue_nonactivating
from _abt_sessions as a
join LAKE.SEGMENT_FL.JAVA_FABLETICS_ORDER_COMPLETED as o on a.meta_original_session_id = try_to_number(o.PROPERTIES_SESSION_ID)
    and properties_automated_test = FALSE
join _min_max as mm on mm.test_key = a.TEST_KEY
where
    try_to_number(o.PROPERTIES_SESSION_ID) >= mm.min_session_id
    and TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
group by all

union all

select
    EVENT
    ,'App' as event_type
    ,a.TEST_KEY
    ,TEST_GROUP
    ,a.meta_original_session_id
    ,a.meta_original_customer_id
    ,a.AB_TEST_START_UTC_DATETIME
    ,count(distinct case when properties_is_activating = true then o.PROPERTIES_ORDER_ID else 0 end) as orders_activating
    ,count(distinct case when properties_is_activating = false then o.PROPERTIES_ORDER_ID else 0 end) as orders_nonactivating
    ,count(case when properties_is_activating = true then o.PROPERTIES_ORDER_ID else 0 end) as units_activating
    ,count(case when properties_is_activating = false then o.PROPERTIES_ORDER_ID else 0 end) as units_nonactivating
    ,sum(case when properties_is_activating = true then o.PROPERTIES_PRODUCTS_PRICE else 0 end) as product_gross_revenue_activating
    ,sum(case when properties_is_activating = false then o.PROPERTIES_PRODUCTS_PRICE else 0 end) as product_gross_revenue_nonactivating
from _abt_sessions as a
join LAKE.SEGMENT_FL.JAVA_FABLETICS_ECOM_MOBILE_APP_ORDER_COMPLETED as o on a.meta_original_session_id = try_to_number(o.PROPERTIES_SESSION_ID)
    and properties_automated_test = FALSE
join _min_max as mm on mm.test_key = a.TEST_KEY
where
    try_to_number(o.PROPERTIES_SESSION_ID) >= mm.min_session_id
    and TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
group by all;

CREATE OR REPLACE TEMPORARY TABLE _session_ab_test_funnel as
SELECT
    'GRID' as funnel_report_version
    ,'INDIRECT' AS psource_attribution_version
    ,a.SESSION_ID
    ,a.CUSTOMER_ID
    ,a.AB_TEST_START_LOCAL_DATETIME::date as ab_test_start_local_date
    ,a.AB_TEST_START_LOCAL_DATETIME
    ,a.AB_TEST_START_PST_DATETIME
    ,a.TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,a.TEST_TYPE
    ,a.TEST_EFFECTIVE_START_DATETIME_PST as test_activated_datetime
--     ,a.TEST_EFFECTIVE_END_DATETIME_PST
    ,a.TEST_GROUP
    ,TEST_GROUP_DESCRIPTION
    ,STORE_BRAND
    ,STORE_REGION
    ,STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,a.PLATFORM
    ,g.psource
    ,count(distinct a.SESSION_ID) as sessions
    ,count(distinct a.CUSTOMER_ID) as distinct_customers
    ,0 as search_events
    ,count(distinct g.meta_original_session_id) as grid_views
    ,count(distinct pv.meta_original_session_id) as pdp_views
    ,count(distinct atb.meta_original_session_id) as atb
    ,sum(nvl(orders_activating,0)) as orders_activating
    ,sum(nvl(orders_nonactivating,0)) as orders_nonactivating
    ,sum(nvl(product_gross_revenue_activating,0)) as product_gross_revenue_activating
    ,sum(nvl(product_gross_revenue_nonactivating,0)) as product_gross_revenue_nonactivating
    ,sum(nvl(units_activating,0)) as units_activating
    ,sum(nvl(units_nonactivating,0)) as units_nonactivating
    ,max_datetime_pst as max_record_pst
from _abt_sessions as a
join _grid_views_psource as g on a.meta_original_session_id = g.meta_original_session_id
    and a.TEST_KEY = g.test_key
    and a.test_group = g.test_group
left join _pdp_views as pv on g.meta_original_session_id = pv.meta_original_session_id
    and g.TEST_KEY = pv.test_key
    and g.test_group = pv.test_group
left join _atb as atb on pv.meta_original_session_id = atb.meta_original_session_id
    and pv.TEST_KEY = atb.test_key
    and pv.test_group = atb.test_group
left join _orders as ord on atb.meta_original_session_id = ord.meta_original_session_id
    and atb.TEST_KEY = ord.test_key
    and atb.test_group = ord.test_group
JOIN _min_max as mm on mm.test_key = a.test_key
group by all

union all

SELECT
    'PDP' as funnel_report_version
    ,'INDIRECT' AS psource_attribution_version
    ,a.SESSION_ID
    ,a.CUSTOMER_ID
    ,a.AB_TEST_START_LOCAL_DATETIME::date as ab_test_start_local_date
    ,a.AB_TEST_START_LOCAL_DATETIME
    ,a.AB_TEST_START_PST_DATETIME
    ,a.TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,a.TEST_TYPE
    ,a.TEST_EFFECTIVE_START_DATETIME_PST as test_activated_datetime
--     ,a.TEST_EFFECTIVE_END_DATETIME_PST
    ,a.TEST_GROUP
    ,TEST_GROUP_DESCRIPTION
    ,STORE_BRAND
    ,STORE_REGION
    ,STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,a.PLATFORM
    ,pv.psource
    ,count(distinct a.SESSION_ID) as sessions
    ,count(distinct a.CUSTOMER_ID) as distinct_customers
    ,0 as search_events
    ,0 as grid_views
    ,count(distinct pv.meta_original_session_id) as pdp_views
    ,count(distinct atb.meta_original_session_id) as atb
    ,sum(nvl(orders_activating,0)) as orders_activating
    ,sum(nvl(orders_nonactivating,0)) as orders_nonactivating
    ,sum(nvl(product_gross_revenue_activating,0)) as product_gross_revenue_activating
    ,sum(nvl(product_gross_revenue_nonactivating,0)) as product_gross_revenue_nonactivating
    ,sum(nvl(units_activating,0)) as units_activating
    ,sum(nvl(units_nonactivating,0)) as units_nonactivating
    ,max_datetime_pst as max_record_pst
from _abt_sessions as a
join _pdp_views_psource as pv on a.meta_original_session_id = pv.meta_original_session_id
    and a.TEST_KEY = pv.test_key
    and a.test_group = pv.test_group
left join _atb as atb on pv.meta_original_session_id = atb.meta_original_session_id
    and pv.TEST_KEY = atb.test_key
    and pv.test_group = atb.test_group
left join _orders as ord on atb.meta_original_session_id = ord.meta_original_session_id
    and atb.TEST_KEY = ord.test_key
    and atb.test_group = ord.test_group
JOIN _min_max as mm on mm.test_key = a.test_key
group by all

union all

SELECT
    'ATB' as funnel_report_version
    ,'INDIRECT' AS psource_attribution_version
    ,a.SESSION_ID
    ,a.CUSTOMER_ID
    ,a.AB_TEST_START_LOCAL_DATETIME::date as ab_test_start_local_date
    ,a.AB_TEST_START_LOCAL_DATETIME
    ,a.AB_TEST_START_PST_DATETIME
    ,a.TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,a.TEST_TYPE
    ,a.TEST_EFFECTIVE_START_DATETIME_PST as test_activated_datetime
--     ,a.TEST_EFFECTIVE_END_DATETIME_PST
    ,a.TEST_GROUP
    ,TEST_GROUP_DESCRIPTION
    ,STORE_BRAND
    ,STORE_REGION
    ,STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,a.PLATFORM
    ,atb.psource
    ,count(distinct a.SESSION_ID) as sessions
    ,count(distinct a.CUSTOMER_ID) as distinct_customers
    ,0 as search_events
    ,0 as grid_views
    ,0 as pdp_views
    ,count(distinct atb.meta_original_session_id) as atb
    ,sum(nvl(orders_activating,0)) as orders_activating
    ,sum(nvl(orders_nonactivating,0)) as orders_nonactivating
    ,sum(nvl(product_gross_revenue_activating,0)) as product_gross_revenue_activating
    ,sum(nvl(product_gross_revenue_nonactivating,0)) as product_gross_revenue_nonactivating
    ,sum(nvl(units_activating,0)) as units_activating
    ,sum(nvl(units_nonactivating,0)) as units_nonactivating
    ,max_datetime_pst as max_record_pst
from _abt_sessions as a
join _atb_psource as atb on a.meta_original_session_id = atb.meta_original_session_id
    and a.TEST_KEY = atb.test_key
    and a.test_group = atb.test_group
left join _orders as ord on atb.meta_original_session_id = ord.meta_original_session_id
    and atb.TEST_KEY = ord.test_key
    and atb.test_group = ord.test_group
JOIN _min_max as mm on mm.test_key = a.test_key
group by all

union all

SELECT
    'SEARCH' as funnel_report_version
    ,'INDIRECT' AS psource_attribution_version
    ,a.SESSION_ID
    ,a.CUSTOMER_ID
    ,a.AB_TEST_START_LOCAL_DATETIME::date as ab_test_start_local_date
    ,a.AB_TEST_START_LOCAL_DATETIME
    ,a.AB_TEST_START_PST_DATETIME
    ,a.TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,a.TEST_TYPE
    ,a.TEST_EFFECTIVE_START_DATETIME_PST as test_activated_datetime
--     ,a.TEST_EFFECTIVE_END_DATETIME_PST
    ,a.TEST_GROUP
    ,TEST_GROUP_DESCRIPTION
    ,STORE_BRAND
    ,STORE_REGION
    ,STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,a.PLATFORM
    ,null psource
    ,count(distinct a.SESSION_ID) as sessions
    ,count(distinct a.CUSTOMER_ID) as distinct_customers
    ,count(distinct g.meta_original_session_id) as search_events
    ,0 as grid_views
    ,count(distinct pv.meta_original_session_id) as pdp_views
    ,count(distinct atb.meta_original_session_id) as atb
    ,sum(nvl(orders_activating,0)) as orders_activating
    ,sum(nvl(orders_nonactivating,0)) as orders_nonactivating
    ,sum(nvl(product_gross_revenue_activating,0)) as product_gross_revenue_activating
    ,sum(nvl(product_gross_revenue_nonactivating,0)) as product_gross_revenue_nonactivating
    ,sum(nvl(units_activating,0)) as units_activating
    ,sum(nvl(units_nonactivating,0)) as units_nonactivating
    ,max_datetime_pst as max_record_pst
from _abt_sessions as a
join _search_events as g on a.meta_original_session_id = g.meta_original_session_id
    and a.TEST_KEY = g.test_key
    and a.test_group = g.test_group
left join _pdp_views_psource as pv on g.meta_original_session_id = pv.meta_original_session_id
    and g.TEST_KEY = pv.test_key
    and g.test_group = pv.test_group
    and pv.psource ilike '%search%'
left join _atb as atb on pv.meta_original_session_id = atb.meta_original_session_id
    and pv.TEST_KEY = atb.test_key
    and pv.test_group = atb.test_group
left join _orders as ord on atb.meta_original_session_id = ord.meta_original_session_id
    and atb.TEST_KEY = ord.test_key
    and atb.test_group = ord.test_group
JOIN _min_max as mm on mm.test_key = a.test_key
group by all

union all

SELECT
    'GRID' as funnel_report_version
    ,'DIRECT' AS psource_attribution_version
    ,a.SESSION_ID
    ,a.CUSTOMER_ID
    ,a.AB_TEST_START_LOCAL_DATETIME::date as ab_test_start_local_date
    ,a.AB_TEST_START_LOCAL_DATETIME
    ,a.AB_TEST_START_PST_DATETIME
    ,a.TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,a.TEST_TYPE
    ,a.TEST_EFFECTIVE_START_DATETIME_PST as test_activated_datetime
--     ,a.TEST_EFFECTIVE_END_DATETIME_PST
    ,a.TEST_GROUP
    ,TEST_GROUP_DESCRIPTION
    ,STORE_BRAND
    ,STORE_REGION
    ,STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,a.PLATFORM
    ,g.psource
    ,count(distinct a.SESSION_ID) as sessions
    ,count(distinct a.CUSTOMER_ID) as distinct_customers
    ,0 as search_events
    ,count(distinct g.meta_original_session_id) as grid_views
    ,count(distinct pv.meta_original_session_id) as pdp_views
    ,count(distinct atb.meta_original_session_id) as atb
    ,sum(nvl(orders_activating,0)) as orders_activating
    ,sum(nvl(orders_nonactivating,0)) as orders_nonactivating
    ,sum(nvl(product_gross_revenue_activating,0)) as product_gross_revenue_activating
    ,sum(nvl(product_gross_revenue_nonactivating,0)) as product_gross_revenue_nonactivating
    ,sum(nvl(units_activating,0)) as units_activating
    ,sum(nvl(units_nonactivating,0)) as units_nonactivating
    ,max_datetime_pst as max_record_pst
from _abt_sessions as a
join _grid_views_psource as g on a.meta_original_session_id = g.meta_original_session_id
    and a.TEST_KEY = g.test_key
    and a.test_group = g.test_group
left join _pdp_views_psource as pv on g.meta_original_session_id = pv.meta_original_session_id
    and g.TEST_KEY = pv.test_key
    and g.test_group = pv.test_group
    and g.psource = pv.psource
left join _atb_psource as atb on pv.meta_original_session_id = atb.meta_original_session_id
    and pv.TEST_KEY = atb.test_key
    and pv.test_group = atb.test_group
    and pv.psource = atb.psource
left join _orders_psource as ord on atb.meta_original_session_id = ord.meta_original_session_id
    and atb.TEST_KEY = ord.test_key
    and atb.test_group = ord.test_group
    and atb.psource = ord.psource
JOIN _min_max as mm on mm.test_key = a.test_key
group by all

union all

SELECT
    'PDP' as funnel_report_version
    ,'DIRECT' AS psource_attribution_version
    ,a.SESSION_ID
    ,a.CUSTOMER_ID
    ,a.AB_TEST_START_LOCAL_DATETIME::date as ab_test_start_local_date
    ,a.AB_TEST_START_LOCAL_DATETIME
    ,a.AB_TEST_START_PST_DATETIME
    ,a.TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,a.TEST_TYPE
    ,a.TEST_EFFECTIVE_START_DATETIME_PST as test_activated_datetime
--     ,a.TEST_EFFECTIVE_END_DATETIME_PST
    ,a.TEST_GROUP
    ,TEST_GROUP_DESCRIPTION
    ,STORE_BRAND
    ,STORE_REGION
    ,STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,a.PLATFORM
    ,pv.psource
    ,count(distinct a.SESSION_ID) as sessions
    ,count(distinct a.CUSTOMER_ID) as distinct_customers
    ,0 as search_events
    ,0 as grid_views
    ,count(distinct pv.meta_original_session_id) as pdp_views
    ,count(distinct atb.meta_original_session_id) as atb
    ,sum(nvl(orders_activating,0)) as orders_activating
    ,sum(nvl(orders_nonactivating,0)) as orders_nonactivating
    ,sum(nvl(product_gross_revenue_activating,0)) as product_gross_revenue_activating
    ,sum(nvl(product_gross_revenue_nonactivating,0)) as product_gross_revenue_nonactivating
    ,sum(nvl(units_activating,0)) as units_activating
    ,sum(nvl(units_nonactivating,0)) as units_nonactivating
    ,max_datetime_pst as max_record_pst
from _abt_sessions as a
join _pdp_views_psource as pv on a.meta_original_session_id = pv.meta_original_session_id
    and a.TEST_KEY = pv.test_key
    and a.test_group = pv.test_group
left join _atb_psource as atb on pv.meta_original_session_id = atb.meta_original_session_id
    and pv.TEST_KEY = atb.test_key
    and pv.test_group = atb.test_group
    and pv.psource = atb.psource
left join _orders_psource as ord on atb.meta_original_session_id = ord.meta_original_session_id
    and atb.TEST_KEY = ord.test_key
    and atb.test_group = ord.test_group
    and atb.psource = ord.psource
JOIN _min_max as mm on mm.test_key = a.test_key
group by all

union all

SELECT
    'ATB' as funnel_report_version
    ,'DIRECT' AS psource_attribution_version
    ,a.SESSION_ID
    ,a.CUSTOMER_ID
    ,a.AB_TEST_START_LOCAL_DATETIME::date as ab_test_start_local_date
    ,a.AB_TEST_START_LOCAL_DATETIME
    ,a.AB_TEST_START_PST_DATETIME
    ,a.TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,a.TEST_TYPE
    ,a.TEST_EFFECTIVE_START_DATETIME_PST as test_activated_datetime
--     ,a.TEST_EFFECTIVE_END_DATETIME_PST
    ,a.TEST_GROUP
    ,TEST_GROUP_DESCRIPTION
    ,STORE_BRAND
    ,STORE_REGION
    ,STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,a.PLATFORM
    ,atb.psource
    ,count(distinct a.SESSION_ID) as sessions
    ,count(distinct a.CUSTOMER_ID) as distinct_customers
    ,0 as search_events
    ,0 as grid_views
    ,0 as pdp_views
    ,count(distinct atb.meta_original_session_id) as atb
    ,sum(nvl(orders_activating,0)) as orders_activating
    ,sum(nvl(orders_nonactivating,0)) as orders_nonactivating
    ,sum(nvl(product_gross_revenue_activating,0)) as product_gross_revenue_activating
    ,sum(nvl(product_gross_revenue_nonactivating,0)) as product_gross_revenue_nonactivating
    ,sum(nvl(units_activating,0)) as units_activating
    ,sum(nvl(units_nonactivating,0)) as units_nonactivating
    ,max_datetime_pst as max_record_pst
from _abt_sessions as a
join _atb_psource as atb on a.meta_original_session_id = atb.meta_original_session_id
    and a.TEST_KEY = atb.test_key
    and a.test_group = atb.test_group
left join _orders_psource as ord on atb.meta_original_session_id = ord.meta_original_session_id
    and atb.TEST_KEY = ord.test_key
    and atb.test_group = ord.test_group
    and atb.psource = ord.psource
JOIN _min_max as mm on mm.test_key = a.test_key
group by all

union all

SELECT
    'SEARCH' as funnel_report_version
    ,'DIRECT' AS psource_attribution_version
    ,a.SESSION_ID
    ,a.CUSTOMER_ID
    ,a.AB_TEST_START_LOCAL_DATETIME::date as ab_test_start_local_date
    ,a.AB_TEST_START_LOCAL_DATETIME
    ,a.AB_TEST_START_PST_DATETIME
    ,a.TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,a.TEST_TYPE
    ,a.TEST_EFFECTIVE_START_DATETIME_PST as test_activated_datetime
--     ,a.TEST_EFFECTIVE_END_DATETIME_PST
    ,a.TEST_GROUP
    ,TEST_GROUP_DESCRIPTION
    ,STORE_BRAND
    ,STORE_REGION
    ,STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,a.PLATFORM
    ,null psource
    ,count(distinct a.SESSION_ID) as sessions
    ,count(distinct a.CUSTOMER_ID) as distinct_customers
    ,count(distinct g.meta_original_session_id) as search_events
    ,0 as grid_views
    ,count(distinct pv.meta_original_session_id) as pdp_views
    ,count(distinct atb.meta_original_session_id) as atb
    ,sum(nvl(orders_activating,0)) as orders_activating
    ,sum(nvl(orders_nonactivating,0)) as orders_nonactivating
    ,sum(nvl(product_gross_revenue_activating,0)) as product_gross_revenue_activating
    ,sum(nvl(product_gross_revenue_nonactivating,0)) as product_gross_revenue_nonactivating
    ,sum(nvl(units_activating,0)) as units_activating
    ,sum(nvl(units_nonactivating,0)) as units_nonactivating
    ,max_datetime_pst as max_record_pst
from _abt_sessions as a
join _search_events as g on a.meta_original_session_id = g.meta_original_session_id
    and a.TEST_KEY = g.test_key
    and a.test_group = g.test_group
left join _pdp_views_psource as pv on g.meta_original_session_id = pv.meta_original_session_id
    and g.TEST_KEY = pv.test_key
    and g.test_group = pv.test_group
    and pv.psource ilike '%search%'
left join _atb_psource as atb on pv.meta_original_session_id = atb.meta_original_session_id
    and pv.TEST_KEY = atb.test_key
    and pv.test_group = atb.test_group
    and atb.psource ilike '%search%'
left join _orders_psource as ord on atb.meta_original_session_id = ord.meta_original_session_id
    and atb.TEST_KEY = ord.test_key
    and atb.test_group = ord.test_group
    and ord.psource ilike '%search%'
JOIN _min_max as mm on mm.test_key = a.test_key
group by all;

DELETE
FROM reporting_base_prod.shared.session_ab_test_funnel
WHERE max_record_pst >= CURRENT_DATE - INTERVAL '14 DAY';

INSERT INTO reporting_base_prod.shared.session_ab_test_funnel
SELECT *,
       current_timestamp as META_CREATE_DATETIME
FROM _session_ab_test_funnel
where max_record_pst >= CURRENT_DATE - INTERVAL '14 DAY';
