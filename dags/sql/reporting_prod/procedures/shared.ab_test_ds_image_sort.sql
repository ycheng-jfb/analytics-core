CREATE OR REPLACE TRANSIENT TABLE reporting_prod.shared.ab_test_ds_image_sort as
select
    'INDIV COUNTRY + INDIV PLATFORM' AS version
    ,'WITH COLOR' as PRODUCT_REPORT_VERSION
    ,'GRID - DIRECT' AS funnel_report_version
    ,TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,a.MASTER_TEST_NUMBER
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,TEST_TYPE
    ,test_activated_datetime
    ,TEST_EFFECTIVE_END_DATETIME_PST
    ,test_group
    ,TEST_GROUP_DESCRIPTION
    ,ab_test_start_local_date
    ,STORE_BRAND
    ,STORE_REGION
    ,STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,PLATFORM
    ,a.MASTER_PRODUCT_ID product_id
    ,rnk_mpid_status_changed
    ,a.PRODUCT_EFFECTIVE_START_DATE_PST
    ,a.PRODUCT_EFFECTIVE_END_DATE_PST
    ,product_name
    ,base_sku
    ,a.product_sku
    ,color
    ,sub_brand
    ,gender
    ,segment
    ,IMAGE_URL
    ,COALESCE(CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/', u.PRODUCT_SKU, '/', u.PRODUCT_SKU, '-', CAST(COALESCE(u.SORT_NOW_CURRENT,u.SORT_CURRENT) AS NUMBER(38,0)), '_271x407.jpg'),IMAGE_URL) AS IMAGE_SORT_URL_CONTROL
    ,CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/', u.PRODUCT_SKU, '/', u.PRODUCT_SKU, '-', CAST(COALESCE(u.SORT_NOW_TEST,u.SORT_TEST) AS NUMBER(38,0)), '_271x407.jpg') AS IMAGE_SORT_URL_VARIANT
    ,count(distinct SESSION_ID) as sessions
    ,count(distinct CUSTOMER_ID) as distinct_customers
    ,COUNT(distinct case when test_type in ('membership','Sorting Hat, Persistent','Membership Detail','Customer Detail') then customer_id
                          when test_type in ('session','Session Detail','Sorting Hat, Non Persistent','Builder') then session_id end) as test_starts_actual
    ,sum(grid_views) as grid_views
    ,sum(pdp_views) as pdp_views
    ,sum(atb) as atb
    ,sum(units_activating) as units_activating
    ,sum(units_nonactivating) as units_nonactivating
    ,max_record_pst
from reporting_base_prod.shared.session_ab_test_ds_image_sort as a
left join REPORTING_BASE_PROD.SHARED.IMAGE_TESTS_WITH_CURRENT_URLS as u on u.MASTER_TEST_NUMBER = a.MASTER_TEST_NUMBER
    and u.MASTER_PRODUCT_ID = a.MASTER_PRODUCT_ID
group by all

union all

select
    'INDIV COUNTRY + TOTAL PLATFORM' AS version
    ,'WITH COLOR' as PRODUCT_REPORT_VERSION
    ,'GRID - DIRECT' AS funnel_report_version
    ,TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,a.MASTER_TEST_NUMBER
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,TEST_TYPE
    ,test_activated_datetime
    ,TEST_EFFECTIVE_END_DATETIME_PST
    ,test_group
    ,TEST_GROUP_DESCRIPTION
    ,ab_test_start_local_date
    ,STORE_BRAND
    ,STORE_REGION
    ,STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,'Total' as PLATFORM
    ,a.MASTER_PRODUCT_ID product_id
    ,rnk_mpid_status_changed
    ,a.PRODUCT_EFFECTIVE_START_DATE_PST
    ,a.PRODUCT_EFFECTIVE_END_DATE_PST
    ,product_name
    ,base_sku
    ,a.product_sku
    ,color
    ,sub_brand
    ,gender
    ,segment
    ,IMAGE_URL
    ,COALESCE(CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/', u.PRODUCT_SKU, '/', u.PRODUCT_SKU, '-', CAST(COALESCE(u.SORT_NOW_CURRENT,u.SORT_CURRENT) AS NUMBER(38,0)), '_271x407.jpg'),IMAGE_URL) AS IMAGE_SORT_URL_CONTROL
    ,CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/', u.PRODUCT_SKU, '/', u.PRODUCT_SKU, '-', CAST(COALESCE(u.SORT_NOW_TEST,u.SORT_TEST) AS NUMBER(38,0)), '_271x407.jpg') AS IMAGE_SORT_URL_VARIANT
    ,count(distinct SESSION_ID) as sessions
    ,count(distinct CUSTOMER_ID) as distinct_customers
    ,COUNT(distinct case when test_type in ('membership','Sorting Hat, Persistent','Membership Detail','Customer Detail') then customer_id
                          when test_type in ('session','Session Detail','Sorting Hat, Non Persistent','Builder') then session_id end) as test_starts_actual
    ,sum(grid_views) as grid_views
    ,sum(pdp_views) as pdp_views
    ,sum(atb) as atb
    ,sum(units_activating) as units_activating
    ,sum(units_nonactivating) as units_nonactivating
    ,max_record_pst
from reporting_base_prod.shared.session_ab_test_ds_image_sort as a
left join REPORTING_BASE_PROD.SHARED.IMAGE_TESTS_WITH_CURRENT_URLS as u on u.MASTER_TEST_NUMBER = a.MASTER_TEST_NUMBER
    and u.MASTER_PRODUCT_ID = a.MASTER_PRODUCT_ID
group by all

UNION ALL

select
    'INDIV REGION + INDIV PLATFORM' AS version
    ,'WITH COLOR' as PRODUCT_REPORT_VERSION
    ,'GRID - DIRECT' AS funnel_report_version
    ,TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,a.MASTER_TEST_NUMBER
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,TEST_TYPE
    ,test_activated_datetime
    ,TEST_EFFECTIVE_END_DATETIME_PST
    ,test_group
    ,TEST_GROUP_DESCRIPTION
    ,ab_test_start_local_date
    ,STORE_BRAND
    ,STORE_REGION
    ,STORE_REGION as STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,PLATFORM
    ,a.MASTER_PRODUCT_ID product_id
    ,rnk_mpid_status_changed
    ,a.PRODUCT_EFFECTIVE_START_DATE_PST
    ,a.PRODUCT_EFFECTIVE_END_DATE_PST
    ,product_name
    ,base_sku
    ,a.product_sku
    ,color
    ,sub_brand
    ,gender
    ,segment
    ,IMAGE_URL
    ,COALESCE(CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/', u.PRODUCT_SKU, '/', u.PRODUCT_SKU, '-', CAST(COALESCE(u.SORT_NOW_CURRENT,u.SORT_CURRENT) AS NUMBER(38,0)), '_271x407.jpg'),IMAGE_URL) AS IMAGE_SORT_URL_CONTROL
    ,CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/', u.PRODUCT_SKU, '/', u.PRODUCT_SKU, '-', CAST(COALESCE(u.SORT_NOW_TEST,u.SORT_TEST) AS NUMBER(38,0)), '_271x407.jpg') AS IMAGE_SORT_URL_VARIANT
    ,count(distinct SESSION_ID) as sessions
    ,count(distinct CUSTOMER_ID) as distinct_customers
    ,COUNT(distinct case when test_type in ('membership','Sorting Hat, Persistent','Membership Detail','Customer Detail') then customer_id
                          when test_type in ('session','Session Detail','Sorting Hat, Non Persistent','Builder') then session_id end) as test_starts_actual
    ,sum(grid_views) as grid_views
    ,sum(pdp_views) as pdp_views
    ,sum(atb) as atb
    ,sum(units_activating) as units_activating
    ,sum(units_nonactivating) as units_nonactivating
    ,max_record_pst
from reporting_base_prod.shared.session_ab_test_ds_image_sort as a
left join REPORTING_BASE_PROD.SHARED.IMAGE_TESTS_WITH_CURRENT_URLS as u on u.MASTER_TEST_NUMBER = a.MASTER_TEST_NUMBER
    and u.MASTER_PRODUCT_ID = a.MASTER_PRODUCT_ID
group by all

union all

select
    'INDIV REGION + TOTAL PLATFORM' AS version
    ,'WITH COLOR' as PRODUCT_REPORT_VERSION
    ,'GRID - DIRECT' AS funnel_report_version
    ,TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,a.MASTER_TEST_NUMBER
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,TEST_TYPE
    ,test_activated_datetime
    ,TEST_EFFECTIVE_END_DATETIME_PST
    ,test_group
    ,TEST_GROUP_DESCRIPTION
    ,ab_test_start_local_date
    ,STORE_BRAND
    ,STORE_REGION
    ,STORE_REGION as STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,'Total' as PLATFORM
    ,a.MASTER_PRODUCT_ID product_id
    ,rnk_mpid_status_changed
    ,a.PRODUCT_EFFECTIVE_START_DATE_PST
    ,a.PRODUCT_EFFECTIVE_END_DATE_PST
    ,product_name
    ,base_sku
    ,a.product_sku
    ,color
    ,sub_brand
    ,gender
    ,segment
    ,IMAGE_URL
    ,COALESCE(CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/', u.PRODUCT_SKU, '/', u.PRODUCT_SKU, '-', CAST(COALESCE(u.SORT_NOW_CURRENT,u.SORT_CURRENT) AS NUMBER(38,0)), '_271x407.jpg'),IMAGE_URL) AS IMAGE_SORT_URL_CONTROL
    ,CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/', u.PRODUCT_SKU, '/', u.PRODUCT_SKU, '-', CAST(COALESCE(u.SORT_NOW_TEST,u.SORT_TEST) AS NUMBER(38,0)), '_271x407.jpg') AS IMAGE_SORT_URL_VARIANT
    ,count(distinct SESSION_ID) as sessions
    ,count(distinct CUSTOMER_ID) as distinct_customers
    ,COUNT(distinct case when test_type in ('membership','Sorting Hat, Persistent','Membership Detail','Customer Detail') then customer_id
                          when test_type in ('session','Session Detail','Sorting Hat, Non Persistent','Builder') then session_id end) as test_starts_actual
    ,sum(grid_views) as grid_views
    ,sum(pdp_views) as pdp_views
    ,sum(atb) as atb
    ,sum(units_activating) as units_activating
    ,sum(units_nonactivating) as units_nonactivating
    ,max_record_pst
from reporting_base_prod.shared.session_ab_test_ds_image_sort as a
left join REPORTING_BASE_PROD.SHARED.IMAGE_TESTS_WITH_CURRENT_URLS as u on u.MASTER_TEST_NUMBER = a.MASTER_TEST_NUMBER
    and u.MASTER_PRODUCT_ID = a.MASTER_PRODUCT_ID
group by all


UNION ALL

select
    'GLOBAL + INDIV PLATFORM' AS version
    ,'WITH COLOR' as PRODUCT_REPORT_VERSION
    ,'GRID - DIRECT' AS funnel_report_version
    ,TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,a.MASTER_TEST_NUMBER
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,TEST_TYPE
    ,test_activated_datetime
    ,TEST_EFFECTIVE_END_DATETIME_PST
    ,test_group
    ,TEST_GROUP_DESCRIPTION
    ,ab_test_start_local_date
    ,STORE_BRAND
    ,'GLOBAL' STORE_REGION
    ,'GLOBAL' STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,PLATFORM
    ,a.MASTER_PRODUCT_ID product_id
    ,rnk_mpid_status_changed
    ,a.PRODUCT_EFFECTIVE_START_DATE_PST
    ,a.PRODUCT_EFFECTIVE_END_DATE_PST
    ,product_name
    ,base_sku
    ,a.product_sku
    ,color
    ,sub_brand
    ,gender
    ,segment
    ,IMAGE_URL
    ,COALESCE(CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/', u.PRODUCT_SKU, '/', u.PRODUCT_SKU, '-', CAST(COALESCE(u.SORT_NOW_CURRENT,u.SORT_CURRENT) AS NUMBER(38,0)), '_271x407.jpg'),IMAGE_URL) AS IMAGE_SORT_URL_CONTROL
    ,CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/', u.PRODUCT_SKU, '/', u.PRODUCT_SKU, '-', CAST(COALESCE(u.SORT_NOW_TEST,u.SORT_TEST) AS NUMBER(38,0)), '_271x407.jpg') AS IMAGE_SORT_URL_VARIANT
    ,count(distinct SESSION_ID) as sessions
    ,count(distinct CUSTOMER_ID) as distinct_customers
    ,COUNT(distinct case when test_type in ('membership','Sorting Hat, Persistent','Membership Detail','Customer Detail') then customer_id
                          when test_type in ('session','Session Detail','Sorting Hat, Non Persistent','Builder') then session_id end) as test_starts_actual
    ,sum(grid_views) as grid_views
    ,sum(pdp_views) as pdp_views
    ,sum(atb) as atb
    ,sum(units_activating) as units_activating
    ,sum(units_nonactivating) as units_nonactivating
    ,max_record_pst
from reporting_base_prod.shared.session_ab_test_ds_image_sort as a
left join REPORTING_BASE_PROD.SHARED.IMAGE_TESTS_WITH_CURRENT_URLS as u on u.MASTER_TEST_NUMBER = a.MASTER_TEST_NUMBER
    and u.MASTER_PRODUCT_ID = a.MASTER_PRODUCT_ID
group by all

union all

select
    'GLOBAL + TOTAL PLATFORM' AS version
    ,'WITH COLOR' as PRODUCT_REPORT_VERSION
    ,'GRID - DIRECT' AS funnel_report_version
    ,TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,a.MASTER_TEST_NUMBER
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,TEST_TYPE
    ,test_activated_datetime
    ,TEST_EFFECTIVE_END_DATETIME_PST
    ,test_group
    ,TEST_GROUP_DESCRIPTION
    ,ab_test_start_local_date
    ,STORE_BRAND
    ,'GLOBAL' STORE_REGION
    ,'GLOBAL' STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,'Total' as PLATFORM
    ,a.MASTER_PRODUCT_ID product_id
    ,rnk_mpid_status_changed
    ,a.PRODUCT_EFFECTIVE_START_DATE_PST
    ,a.PRODUCT_EFFECTIVE_END_DATE_PST
    ,product_name
    ,base_sku
    ,a.product_sku
    ,color
    ,sub_brand
    ,gender
    ,segment
    ,IMAGE_URL
    ,COALESCE(CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/', u.PRODUCT_SKU, '/', u.PRODUCT_SKU, '-', CAST(COALESCE(u.SORT_NOW_CURRENT,u.SORT_CURRENT) AS NUMBER(38,0)), '_271x407.jpg'),IMAGE_URL) AS IMAGE_SORT_URL_CONTROL
    ,CONCAT('https://fabletics-us-cdn.justfab.com/media/images/products/', u.PRODUCT_SKU, '/', u.PRODUCT_SKU, '-', CAST(COALESCE(u.SORT_NOW_TEST,u.SORT_TEST) AS NUMBER(38,0)), '_271x407.jpg') AS IMAGE_SORT_URL_VARIANT
    ,count(distinct SESSION_ID) as sessions
    ,count(distinct CUSTOMER_ID) as distinct_customers
    ,COUNT(distinct case when test_type in ('membership','Sorting Hat, Persistent','Membership Detail','Customer Detail') then customer_id
                          when test_type in ('session','Session Detail','Sorting Hat, Non Persistent','Builder') then session_id end) as test_starts_actual
    ,sum(grid_views) as grid_views
    ,sum(pdp_views) as pdp_views
    ,sum(atb) as atb
    ,sum(units_activating) as units_activating
    ,sum(units_nonactivating) as units_nonactivating
    ,max_record_pst
from reporting_base_prod.shared.session_ab_test_ds_image_sort as a
left join REPORTING_BASE_PROD.SHARED.IMAGE_TESTS_WITH_CURRENT_URLS as u on u.MASTER_TEST_NUMBER = a.MASTER_TEST_NUMBER
    and u.MASTER_PRODUCT_ID = a.MASTER_PRODUCT_ID
group by all

UNION ALL

---- SEGMENT EVENTS TABLE

select
    'INDIV COUNTRY + INDIV PLATFORM' AS version
    ,PRODUCT_REPORT_VERSION
    ,funnel_report_version
    ,TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,a.MASTER_TEST_NUMBER
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,TEST_TYPE
    ,test_activated_datetime
    ,TEST_EFFECTIVE_END_DATETIME_PST
    ,test_group
    ,TEST_GROUP_DESCRIPTION
    ,ab_test_start_local_date
    ,STORE_BRAND
    ,STORE_REGION
    ,STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,PLATFORM
    ,a.MASTER_PRODUCT_ID product_id
    ,NULL rnk_mpid_status_changed
    ,a.PRODUCT_EFFECTIVE_START_DATE_PST
    ,a.PRODUCT_EFFECTIVE_END_DATE_PST
    ,product_name
    ,base_sku
    ,a.product_sku
    ,color
    ,sub_brand
    ,gender
    ,segment
    ,IMAGE_URL
    ,NULL AS IMAGE_SORT_URL_CONTROL
    ,NULL AS IMAGE_SORT_URL_VARIANT
    ,count(distinct SESSION_ID) as sessions
    ,count(distinct CUSTOMER_ID) as distinct_customers
    ,COUNT(distinct case when test_type in ('membership','Sorting Hat, Persistent','Membership Detail','Customer Detail') then customer_id
                          when test_type in ('session','Session Detail','Sorting Hat, Non Persistent','Builder') then session_id end) as test_starts_actual
    ,sum(grid_views) as grid_views
    ,sum(pdp_views) as pdp_views
    ,sum(atb) as atb
    ,sum(units_activating) as units_activating
    ,sum(units_nonactivating) as units_nonactivating
    ,max_record_pst
from reporting_base_prod.shared.session_ab_test_funnel_events as a
group by all

union all

select
    'INDIV COUNTRY + TOTAL PLATFORM' AS version
    ,PRODUCT_REPORT_VERSION
    ,funnel_report_version
    ,TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,a.MASTER_TEST_NUMBER
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,TEST_TYPE
    ,test_activated_datetime
    ,TEST_EFFECTIVE_END_DATETIME_PST
    ,test_group
    ,TEST_GROUP_DESCRIPTION
    ,ab_test_start_local_date
    ,STORE_BRAND
    ,STORE_REGION
    ,STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,'Total' as PLATFORM
    ,a.MASTER_PRODUCT_ID product_id
    ,NULL rnk_mpid_status_changed
    ,a.PRODUCT_EFFECTIVE_START_DATE_PST
    ,a.PRODUCT_EFFECTIVE_END_DATE_PST
    ,product_name
    ,base_sku
    ,a.product_sku
    ,color
    ,sub_brand
    ,gender
    ,segment
    ,IMAGE_URL
    ,NULL AS IMAGE_SORT_URL_CONTROL
    ,NULL AS IMAGE_SORT_URL_VARIANT
    ,count(distinct SESSION_ID) as sessions
    ,count(distinct CUSTOMER_ID) as distinct_customers
    ,COUNT(distinct case when test_type in ('membership','Sorting Hat, Persistent','Membership Detail','Customer Detail') then customer_id
                          when test_type in ('session','Session Detail','Sorting Hat, Non Persistent','Builder') then session_id end) as test_starts_actual
    ,sum(grid_views) as grid_views
    ,sum(pdp_views) as pdp_views
    ,sum(atb) as atb
    ,sum(units_activating) as units_activating
    ,sum(units_nonactivating) as units_nonactivating
    ,max_record_pst
from reporting_base_prod.shared.session_ab_test_funnel_events as a
group by all

UNION ALL

select
    'INDIV REGION + INDIV PLATFORM' AS version
    ,PRODUCT_REPORT_VERSION
    ,funnel_report_version
    ,TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,a.MASTER_TEST_NUMBER
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,TEST_TYPE
    ,test_activated_datetime
    ,TEST_EFFECTIVE_END_DATETIME_PST
    ,test_group
    ,TEST_GROUP_DESCRIPTION
    ,ab_test_start_local_date
    ,STORE_BRAND
    ,STORE_REGION
    ,STORE_REGION as STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,PLATFORM
    ,a.MASTER_PRODUCT_ID product_id
    ,NULL rnk_mpid_status_changed
    ,a.PRODUCT_EFFECTIVE_START_DATE_PST
    ,a.PRODUCT_EFFECTIVE_END_DATE_PST
    ,product_name
    ,base_sku
    ,a.product_sku
    ,color
    ,sub_brand
    ,gender
    ,segment
    ,IMAGE_URL
    ,NULL AS IMAGE_SORT_URL_CONTROL
    ,NULL AS IMAGE_SORT_URL_VARIANT
    ,count(distinct SESSION_ID) as sessions
    ,count(distinct CUSTOMER_ID) as distinct_customers
    ,COUNT(distinct case when test_type in ('membership','Sorting Hat, Persistent','Membership Detail','Customer Detail') then customer_id
                          when test_type in ('session','Session Detail','Sorting Hat, Non Persistent','Builder') then session_id end) as test_starts_actual
    ,sum(grid_views) as grid_views
    ,sum(pdp_views) as pdp_views
    ,sum(atb) as atb
    ,sum(units_activating) as units_activating
    ,sum(units_nonactivating) as units_nonactivating
    ,max_record_pst
from reporting_base_prod.shared.session_ab_test_funnel_events as a
group by all

union all

select
    'INDIV REGION + TOTAL PLATFORM' AS version
    ,PRODUCT_REPORT_VERSION
    ,funnel_report_version
    ,TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,a.MASTER_TEST_NUMBER
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,TEST_TYPE
    ,test_activated_datetime
    ,TEST_EFFECTIVE_END_DATETIME_PST
    ,test_group
    ,TEST_GROUP_DESCRIPTION
    ,ab_test_start_local_date
    ,STORE_BRAND
    ,STORE_REGION
    ,STORE_REGION as STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,'Total' as PLATFORM
    ,a.MASTER_PRODUCT_ID product_id
    ,NULL rnk_mpid_status_changed
    ,a.PRODUCT_EFFECTIVE_START_DATE_PST
    ,a.PRODUCT_EFFECTIVE_END_DATE_PST
    ,product_name
    ,base_sku
    ,a.product_sku
    ,color
    ,sub_brand
    ,gender
    ,segment
    ,IMAGE_URL
    ,NULL AS IMAGE_SORT_URL_CONTROL
    ,NULL AS IMAGE_SORT_URL_VARIANT
    ,count(distinct SESSION_ID) as sessions
    ,count(distinct CUSTOMER_ID) as distinct_customers
    ,COUNT(distinct case when test_type in ('membership','Sorting Hat, Persistent','Membership Detail','Customer Detail') then customer_id
                          when test_type in ('session','Session Detail','Sorting Hat, Non Persistent','Builder') then session_id end) as test_starts_actual
    ,sum(grid_views) as grid_views
    ,sum(pdp_views) as pdp_views
    ,sum(atb) as atb
    ,sum(units_activating) as units_activating
    ,sum(units_nonactivating) as units_nonactivating
    ,max_record_pst
from reporting_base_prod.shared.session_ab_test_funnel_events as a
group by all


UNION ALL

select
    'GLOBAL + INDIV PLATFORM' AS version
    ,PRODUCT_REPORT_VERSION
    ,funnel_report_version
    ,TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,a.MASTER_TEST_NUMBER
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,TEST_TYPE
    ,test_activated_datetime
    ,TEST_EFFECTIVE_END_DATETIME_PST
    ,test_group
    ,TEST_GROUP_DESCRIPTION
    ,ab_test_start_local_date
    ,STORE_BRAND
    ,'GLOBAL' STORE_REGION
    ,'GLOBAL' STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,PLATFORM
    ,a.MASTER_PRODUCT_ID product_id
    ,NULL rnk_mpid_status_changed
    ,a.PRODUCT_EFFECTIVE_START_DATE_PST
    ,a.PRODUCT_EFFECTIVE_END_DATE_PST
    ,product_name
    ,base_sku
    ,a.product_sku
    ,color
    ,sub_brand
    ,gender
    ,segment
    ,IMAGE_URL
    ,NULL AS IMAGE_SORT_URL_CONTROL
    ,NULL AS IMAGE_SORT_URL_VARIANT
    ,count(distinct SESSION_ID) as sessions
    ,count(distinct CUSTOMER_ID) as distinct_customers
    ,COUNT(distinct case when test_type in ('membership','Sorting Hat, Persistent','Membership Detail','Customer Detail') then customer_id
                          when test_type in ('session','Session Detail','Sorting Hat, Non Persistent','Builder') then session_id end) as test_starts_actual
    ,sum(grid_views) as grid_views
    ,sum(pdp_views) as pdp_views
    ,sum(atb) as atb
    ,sum(units_activating) as units_activating
    ,sum(units_nonactivating) as units_nonactivating
    ,max_record_pst
from reporting_base_prod.shared.session_ab_test_funnel_events as a
group by all

union all

select
    'GLOBAL + TOTAL PLATFORM' AS version
    ,PRODUCT_REPORT_VERSION
    ,funnel_report_version
    ,TEST_KEY
    ,TEST_FRAMEWORK_ID
    ,test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,a.MASTER_TEST_NUMBER
    ,TEST_FRAMEWORK_TICKET
    ,CAMPAIGN_CODE
    ,TEST_TYPE
    ,test_activated_datetime
    ,TEST_EFFECTIVE_END_DATETIME_PST
    ,test_group
    ,TEST_GROUP_DESCRIPTION
    ,ab_test_start_local_date
    ,STORE_BRAND
    ,'GLOBAL' STORE_REGION
    ,'GLOBAL' STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,IS_MALE_CUSTOMER
    ,'Total' as PLATFORM
    ,a.MASTER_PRODUCT_ID product_id
    ,NULL rnk_mpid_status_changed
    ,a.PRODUCT_EFFECTIVE_START_DATE_PST
    ,a.PRODUCT_EFFECTIVE_END_DATE_PST
    ,product_name
    ,base_sku
    ,a.product_sku
    ,color
    ,sub_brand
    ,gender
    ,segment
    ,IMAGE_URL
    ,NULL AS IMAGE_SORT_URL_CONTROL
    ,NULL AS IMAGE_SORT_URL_VARIANT
    ,count(distinct SESSION_ID) as sessions
    ,count(distinct CUSTOMER_ID) as distinct_customers
    ,COUNT(distinct case when test_type in ('membership','Sorting Hat, Persistent','Membership Detail','Customer Detail') then customer_id
                          when test_type in ('session','Session Detail','Sorting Hat, Non Persistent','Builder') then session_id end) as test_starts_actual
    ,sum(grid_views) as grid_views
    ,sum(pdp_views) as pdp_views
    ,sum(atb) as atb
    ,sum(units_activating) as units_activating
    ,sum(units_nonactivating) as units_nonactivating
    ,max_record_pst
from reporting_base_prod.shared.session_ab_test_funnel_events as a
group by all;
