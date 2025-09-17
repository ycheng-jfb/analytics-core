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

CREATE OR REPLACE TEMPORARY TABLE _builder_test_labels as
select distinct builder_id,TEST_NAME as builder_test_label_adj
from lake.builder.builder_api_metadata api
WHERE ADJUSTED_ACTIVATED_DATETIME_PST >= '2024-10-10'
order by builder_id;


CREATE OR REPLACE TEMPORARY TABLE _ab_test_funnel AS
SELECT
    funnel_report_version
    ,psource_attribution_version
    ,'individual country and individual platform' as locale_version
    ,ab_test_start_local_date
    ,TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,coalesce(u.builder_test_label_adj,d.test_label) as test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,TEST_TYPE
    ,test_activated_datetime
--     ,TEST_EFFECTIVE_END_DATETIME_PST
    ,TEST_GROUP
    ,TEST_GROUP_DESCRIPTION
    ,STORE_BRAND
    ,STORE_REGION
    ,STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,PLATFORM
    ,psource
    ,sum(sessions) as sessions
    ,sum(distinct_customers) as distinct_customers
    ,sum(search_events) as search_events
    ,sum(grid_views) as grid_views
    ,sum(pdp_views) as pdp_views
    ,sum(atb) as atb
    ,sum(orders_activating) as orders_activating
    ,sum(orders_nonactivating) as orders_nonactivating
    ,sum(product_gross_revenue_activating) as product_gross_revenue_activating
    ,sum(product_gross_revenue_nonactivating) as product_gross_revenue_nonactivating
    ,sum(units_activating) as units_activating
    ,sum(units_nonactivating) as units_nonactivating
    ,max_record_pst
from REPORTING_BASE_PROD.shared.session_ab_test_funnel as d
left join _builder_test_labels as u on u.builder_id = d.test_key
group by all

UNION ALL

SELECT
    funnel_report_version
    ,psource_attribution_version
    ,'individual country and total platform' as locale_version
    ,ab_test_start_local_date
    ,TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,coalesce(u.builder_test_label_adj,d.test_label) as test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,TEST_TYPE
    ,test_activated_datetime
--     ,TEST_EFFECTIVE_END_DATETIME_PST
    ,TEST_GROUP
    ,TEST_GROUP_DESCRIPTION
    ,STORE_BRAND
    ,STORE_REGION
    ,STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,'Total' AS PLATFORM
    ,psource
    ,sum(sessions) as sessions
    ,sum(distinct_customers) as distinct_customers
    ,sum(search_events) as search_events
    ,sum(grid_views) as grid_views
    ,sum(pdp_views) as pdp_views
    ,sum(atb) as atb
    ,sum(orders_activating) as orders_activating
    ,sum(orders_nonactivating) as orders_nonactivating
    ,sum(product_gross_revenue_activating) as product_gross_revenue_activating
    ,sum(product_gross_revenue_nonactivating) as product_gross_revenue_nonactivating
    ,sum(units_activating) as units_activating
    ,sum(units_nonactivating) as units_nonactivating
    ,max_record_pst
from REPORTING_BASE_PROD.shared.session_ab_test_funnel as d
left join _builder_test_labels as u on u.builder_id = d.test_key
group by all

union all

SELECT
    funnel_report_version
    ,psource_attribution_version
    ,'individual region and individual platform' as locale_version
    ,ab_test_start_local_date
    ,d.TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,coalesce(u.builder_test_label_adj,d.test_label) as test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,TEST_TYPE
    ,test_activated_datetime
--     ,TEST_EFFECTIVE_END_DATETIME_PST
    ,TEST_GROUP
    ,TEST_GROUP_DESCRIPTION
    ,STORE_BRAND
    ,concat(d.STORE_REGION,' TTL') as STORE_REGION
    ,concat(d.STORE_REGION,' TTL') as STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,PLATFORM
    ,psource
    ,sum(sessions) as sessions
    ,sum(distinct_customers) as distinct_customers
    ,sum(search_events) as search_events
    ,sum(grid_views) as grid_views
    ,sum(pdp_views) as pdp_views
    ,sum(atb) as atb
    ,sum(orders_activating) as orders_activating
    ,sum(orders_nonactivating) as orders_nonactivating
    ,sum(product_gross_revenue_activating) as product_gross_revenue_activating
    ,sum(product_gross_revenue_nonactivating) as product_gross_revenue_nonactivating
    ,sum(units_activating) as units_activating
    ,sum(units_nonactivating) as units_nonactivating
    ,max_record_pst
from REPORTING_BASE_PROD.shared.session_ab_test_funnel as d
left join _builder_test_labels as u on u.builder_id = d.test_key
left join
    (select
        test_key
        ,count(distinct case when STORE_REGION = 'NA' then STORE_COUNTRY end) as na_store_count
        ,count(distinct case when STORE_REGION = 'EU' then STORE_COUNTRY end) as eu_store_count
        from REPORTING_BASE_PROD.SHARED.SESSION_ORDER_AB_TEST_DETAIL
    group by 1) as t1 on t1.TEST_KEY = d.test_key
where
    d.STORE_BRAND not in ('FabKids','ShoeDazzle','Yitty')
    and ((t1.na_store_count > 0 or t1.eu_store_count > 1) and (t1.na_store_count <> 1 or t1.eu_store_count <> 0))
group by all

union all

SELECT
    funnel_report_version
    ,psource_attribution_version
    ,'individual region and total platform' as locale_version
    ,ab_test_start_local_date
    ,d.TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,coalesce(u.builder_test_label_adj,d.test_label) as test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,TEST_TYPE
    ,test_activated_datetime
--     ,TEST_EFFECTIVE_END_DATETIME_PST
    ,TEST_GROUP
    ,TEST_GROUP_DESCRIPTION
    ,STORE_BRAND
    ,concat(d.STORE_REGION,' TTL') as STORE_REGION
    ,concat(d.STORE_REGION,' TTL') as STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,'Total' PLATFORM
    ,psource
    ,sum(sessions) as sessions
    ,sum(distinct_customers) as distinct_customers
    ,sum(search_events) as search_events
    ,sum(grid_views) as grid_views
    ,sum(pdp_views) as pdp_views
    ,sum(atb) as atb
    ,sum(orders_activating) as orders_activating
    ,sum(orders_nonactivating) as orders_nonactivating
    ,sum(product_gross_revenue_activating) as product_gross_revenue_activating
    ,sum(product_gross_revenue_nonactivating) as product_gross_revenue_nonactivating
    ,sum(units_activating) as units_activating
    ,sum(units_nonactivating) as units_nonactivating
    ,max_record_pst
from REPORTING_BASE_PROD.shared.session_ab_test_funnel as d
left join _builder_test_labels as u on u.builder_id = d.test_key
left join
    (select
        test_key
        ,count(distinct case when STORE_REGION = 'NA' then STORE_COUNTRY end) as na_store_count
        ,count(distinct case when STORE_REGION = 'EU' then STORE_COUNTRY end) as eu_store_count
        from REPORTING_BASE_PROD.SHARED.SESSION_ORDER_AB_TEST_DETAIL
    group by 1) as t1 on t1.TEST_KEY = d.test_key
where
    d.STORE_BRAND not in ('FabKids','ShoeDazzle','Yitty')
    and ((t1.na_store_count > 0 or t1.eu_store_count > 1) and (t1.na_store_count <> 1 or t1.eu_store_count <> 0))
group by all

UNION ALL

SELECT
    funnel_report_version
    ,psource_attribution_version
    ,'global and individual platform' as locale_version
    ,ab_test_start_local_date
    ,d.TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,coalesce(u.builder_test_label_adj,d.test_label) as test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,TEST_TYPE
    ,test_activated_datetime
--     ,TEST_EFFECTIVE_END_DATETIME_PST
    ,TEST_GROUP
    ,TEST_GROUP_DESCRIPTION
    ,STORE_BRAND
    ,'NA+EU TTL' as STORE_REGION
    ,'NA+EU TTL' as STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,PLATFORM
    ,psource
    ,sum(sessions) as sessions
    ,sum(distinct_customers) as distinct_customers
    ,sum(search_events) as search_events
    ,sum(grid_views) as grid_views
    ,sum(pdp_views) as pdp_views
    ,sum(atb) as atb
    ,sum(orders_activating) as orders_activating
    ,sum(orders_nonactivating) as orders_nonactivating
    ,sum(product_gross_revenue_activating) as product_gross_revenue_activating
    ,sum(product_gross_revenue_nonactivating) as product_gross_revenue_nonactivating
    ,sum(units_activating) as units_activating
    ,sum(units_nonactivating) as units_nonactivating
    ,max_record_pst
from REPORTING_BASE_PROD.shared.session_ab_test_funnel as d
left join _builder_test_labels as u on u.builder_id = d.test_key
left join (select test_key,count(distinct STORE_REGION) as region_count from REPORTING_BASE_PROD.SHARED.SESSION_ORDER_AB_TEST_DETAIL group by 1) as t1 on t1.TEST_KEY = d.test_key
where
    d.store_brand not in ('FabKids','ShoeDazzle','Yitty')
    and t1.region_count > 1
group by all

union all

SELECT
    funnel_report_version
    ,psource_attribution_version
    ,'global and total platform' as locale_version
    ,ab_test_start_local_date
    ,d.TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,coalesce(u.builder_test_label_adj,d.test_label) as test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,TEST_TYPE
    ,test_activated_datetime
--     ,TEST_EFFECTIVE_END_DATETIME_PST
    ,TEST_GROUP
    ,TEST_GROUP_DESCRIPTION
    ,STORE_BRAND
    ,'NA+EU TTL' as STORE_REGION
    ,'NA+EU TTL' as STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,'Total' PLATFORM
    ,psource
    ,sum(sessions) as sessions
    ,sum(distinct_customers) as distinct_customers
    ,sum(search_events) as search_events
    ,sum(grid_views) as grid_views
    ,sum(pdp_views) as pdp_views
    ,sum(atb) as atb
    ,sum(orders_activating) as orders_activating
    ,sum(orders_nonactivating) as orders_nonactivating
    ,sum(product_gross_revenue_activating) as product_gross_revenue_activating
    ,sum(product_gross_revenue_nonactivating) as product_gross_revenue_nonactivating
    ,sum(units_activating) as units_activating
    ,sum(units_nonactivating) as units_nonactivating
    ,max_record_pst
from REPORTING_BASE_PROD.shared.session_ab_test_funnel as d
left join _builder_test_labels as u on u.builder_id = d.test_key
left join (select test_key,count(distinct STORE_REGION) as region_count from REPORTING_BASE_PROD.SHARED.SESSION_ORDER_AB_TEST_DETAIL group by 1) as t1 on t1.TEST_KEY = d.test_key
where
    d.store_brand not in ('FabKids','ShoeDazzle','Yitty')
    and t1.region_count > 1
group by all;

delete from _ab_test_funnel
where
    TEST_KEY not in (select test_key from _last_3_months)
        and test_label not in (select test_label from _last_3_months);

DELETE
FROM reporting_prod.shared.ab_test_funnel
WHERE ab_test_start_local_date >= CURRENT_DATE - INTERVAL '14 DAY';

INSERT INTO reporting_prod.shared.ab_test_funnel
SELECT *,
       current_timestamp as META_CREATE_DATETIME
FROM _ab_test_funnel
where ab_test_start_local_date >= CURRENT_DATE - INTERVAL '14 DAY';
