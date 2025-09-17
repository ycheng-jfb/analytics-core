
--test start month date = event action month (can be test start, pause, or skip)

-- SET CAMPAIGN_CODE = 'membershippricetest';
-- SET TEST_FRAMEWORK_ID_1 = 128220;
-- SET TEST_FRAMEWORK_ID_2 = 128320;

CREATE OR REPLACE TEMPORARY TABLE _test_keys as
select distinct
    f.TEST_FRAMEWORK_ID
    ,f.label
    ,f.TEST_FRAMEWORK_DESCRIPTION
    ,case when f.TEST_FRAMEWORK_ID = 128220 then 'US' else 'UK' end as test_country --need to join by test_framework_id, test_key, and country to be sure
    ,f.CAMPAIGN_CODE
    ,f.CODE AS test_key
    ,case when t.code is not null then TRUE else FALSE end as is_current_version
    ,f.STATUSCODE
    ,s.label as status
    ,f.STORE_GROUP_ID
    ,EFFECTIVE_START_DATETIME::datetime as EFFECTIVE_START_DATETIME_PST
    ,EFFECTIVE_END_DATETIME::datetime as EFFECTIVE_END_DATETIME_PST --9999-12-31 00:00:00.000 -08:00 for current live tests
    ,max(f.DATETIME_MODIFIED)  as min_test_start_datetime_hq
from LAKE_CONSOLIDATED_VIEW.ULTRA_CMS_HISTORY.TEST_FRAMEWORK as f
join LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.STATUSCODE as s on f.STATUSCODE = s.STATUSCODE
left join LAKE_CONSOLIDATED_VIEW.ULTRA_CMS.TEST_FRAMEWORK as t on t.CODE = f.code
where
    f.CAMPAIGN_CODE = 'membershippricetest' and f.TEST_FRAMEWORK_ID in (128220,128320)
    and ((f.STATUSCODE = 113 and f.TEST_FRAMEWORK_GROUP_ID is null) --active but missing a test split
            or (f.STATUSCODE = 113 and f.test_framework_group_id NOT IN (19,20,24,25,150,151))) --excluding splits 0/100 or 100/0
group by 1,2,3,4,5,6,7,8,9,10,11,12
order by 11 asc;

CREATE OR REPLACE TEMPORARY TABLE _session_detail_sessions as
select distinct name,s.SESSION_ID,r.STORE_GROUP_ID
from LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.session_detail as s
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.session as n on s.SESSION_ID = n.SESSION_ID
join EDW_PROD.DATA_MODEL.DIM_STORE as r on r.STORE_ID = n.STORE_ID
join _test_keys as t on t.test_key = s.name and s.DATETIME_ADDED >= '2023-11-15' and n.DATETIME_ADDED >= '2023-11-15';

CREATE OR REPLACE TEMPORARY TABLE _customer_test_starts_new_leads as
select distinct
    'New Leads' as lead_segment
    ,'In Test' test_key_assignment
    ,TEST_FRAMEWORK_ID
    ,label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,test_country --need to join by test_framework_id, test_key, and country to be sure
    ,CAMPAIGN_CODE
    ,test_key
    ,EFFECTIVE_START_DATETIME_PST
    ,EFFECTIVE_END_DATETIME_PST
    ,is_current_version
    ,cd.CUSTOMER_ID
    ,n.SESSION_ID as registration_session_id
    ,n.STORE_ID
    ,STORE_BRAND_ABBR
    ,r.STORE_COUNTRY
    ,cd.VALUE as cohort --customer detail record for persistence
    ,case when cd.value = 1 then 'control'
            when cd.value = 2  then 'v1'
            when cd.value = 3 then 'v2' end as test_group
    ,case when cd.value = 1 and r.STORE_COUNTRY = 'US' then '59.95'
            when cd.value = 2 and r.STORE_COUNTRY = 'US' then '64.95'
            when cd.value = 3 and r.STORE_COUNTRY = 'US' then '69.95'
            when cd.value = 1 and r.STORE_COUNTRY = 'UK' then '54.99'
            when cd.value = 2 and r.STORE_COUNTRY = 'UK' then '59.99'
                end as test_group_price
    ,PRICE
    ,platform
    ,IS_MALE_CUSTOMER
    ,cd2.value as registration_signup_source
    ,fr.REGISTRATION_CHANNEL
    ,fr.REGISTRATION_SUBCHANNEL
    ,n.MEMBERSHIP_STATE
    ,min(cd.DATETIME_ADDED) as abt_start_datetime_pst
    ,min(convert_timezone('America/Los_Angeles',STORE_TIME_ZONE,cd.DATETIME_ADDED)::datetime) as abt_start_datetime_local
from _session_detail_sessions as s
left join _test_keys as t on t.test_key = s.name
        and t.STORE_GROUP_ID = s.STORE_GROUP_ID
join REPORTING_BASE_PROD.shared.SESSION as n on n.SESSION_ID = s.SESSION_ID
    and IS_LEAD_REGISTRATION_ACTION = TRUE
join EDW_PROD.DATA_MODEL.DIM_STORE as r on r.STORE_ID = n.STORE_ID
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.CUSTOMER_DETAIL as cd on cd.CUSTOMER_ID = n.CUSTOMER_ID
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP as p on p.MEMBERSHIP_ID = n.MEMBERSHIP_ID
join EDW_PROD.DATA_MODEL.DIM_CUSTOMER as dc on dc.CUSTOMER_ID = cd.CUSTOMER_ID
left join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.CUSTOMER_DETAIL as cd2 on cd2.CUSTOMER_ID = p.CUSTOMER_ID
    and cd2.name = 'signup_source'
join edw_prod.DATA_MODEL.FACT_REGISTRATION as fr on fr.CUSTOMER_ID = cd.CUSTOMER_ID
    and s.SESSION_ID = fr.SESSION_ID
where
    cd.name = 'membershippricetest' --and s.SESSION_ID = 1366495972520
--     and cd.DATETIME_ADDED >= convert_timezone(STORE_TIME_ZONE,'America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME::datetime)::datetime
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26;

delete from _customer_test_starts_new_leads where registration_signup_source in ('ecom_retail_signup','retail');

CREATE OR REPLACE TEMPORARY TABLE _customers_not_in_test_new_leads as
select
    'New Leads' as lead_segment
    ,'Not In Test' test_key_assignment
    ,STORE_BRAND_ABBR
    ,STORE_COUNTRY
    ,fr.CUSTOMER_ID
    ,fr.SESSION_ID as registration_session_id
    ,REGISTRATION_LOCAL_DATETIME
    ,PLATFORM
    ,IS_MALE_CUSTOMER
    ,REGISTRATION_CHANNEL
    ,REGISTRATION_SUBCHANNEL
    ,convert_timezone(STORE_TIME_ZONE,'America/Los_Angeles',fr.REGISTRATION_LOCAL_DATETIME::datetime)::datetime as REGISTRATION_PST_DATETIME
    ,PRICE
    ,case when STORE_COUNTRY = 'US' and price = '59.95' then 'control'
            when STORE_COUNTRY = 'US' and price = '64.95' then 'v1'
            when STORE_COUNTRY = 'US' and price = '69.95' then 'v2'
            when STORE_COUNTRY = 'UK' and price = '54.99' then 'control'
            when STORE_COUNTRY = 'UK' and price = '59.99' then 'v1'
                end as test_group_price
    ,value as registration_signup_source
from EDW_PROD.DATA_MODEL.FACT_REGISTRATION as fr
join edw_prod.DATA_MODEL.DIM_STORE as s on s.STORE_ID = fr.STORE_ID
join REPORTING_BASE_PROD.SHARED.SESSION as n on n.SESSION_ID = fr.SESSION_ID
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP as p on p.CUSTOMER_ID = fr.CUSTOMER_ID
join (select distinct min(EFFECTIVE_START_DATETIME_PST)::datetime EFFECTIVE_START_DATETIME_PST,max(EFFECTIVE_END_DATETIME_PST::datetime) EFFECTIVE_END_DATETIME_PST from _test_keys)
left join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.CUSTOMER_DETAIL as cd on cd.CUSTOMER_ID = fr.CUSTOMER_ID
    and name = 'signup_source'
where
    convert_timezone(STORE_TIME_ZONE,'America/Los_Angeles',fr.REGISTRATION_LOCAL_DATETIME::datetime)::Datetime >= EFFECTIVE_START_DATETIME_PST
    and convert_timezone(STORE_TIME_ZONE,'America/Los_Angeles',fr.REGISTRATION_LOCAL_DATETIME::datetime)::Datetime <= EFFECTIVE_END_DATETIME_PST
    and s.STORE_BRAND_ABBR = 'FL' and STORE_COUNTRY in ('US','UK')
    and fr.CUSTOMER_ID not in (select customer_id from _customer_test_starts_new_leads);

delete from _customers_not_in_test_new_leads where registration_signup_source in ('ecom_retail_signup','retail');

CREATE OR REPLACE TEMPORARY TABLE _customers_existing_leads as
select
    'Existing Leads' as lead_segment
    ,'In Test' test_key_assignment
    ,SESSION_LOCAL_DATETIME
    ,s.CUSTOMER_ID
    ,SESSION_ID
    ,STORE_BRAND_ABBR
    ,STORE_COUNTRY
    ,case when STORE_COUNTRY = 'US' and price = '59.95' then 1
            when STORE_COUNTRY = 'US' and price = '64.95' then 2
            when STORE_COUNTRY = 'US' and price = '69.95' then 3
            when STORE_COUNTRY = 'UK' and price = '54.99' then 1
            when STORE_COUNTRY = 'UK' and price = '59.99' then 2
                end as cohort
     ,case when STORE_COUNTRY = 'US' and price = '59.95' then 'control'
            when STORE_COUNTRY = 'US' and price = '64.95' then 'v1'
            when STORE_COUNTRY = 'US' and price = '69.95' then 'v2'
            when STORE_COUNTRY = 'UK' and price = '54.99' then 'control'
            when STORE_COUNTRY = 'UK' and price = '59.99' then 'v1'
                end as test_group
    ,PRICE
    ,m.DATETIME_MODIFIED as price_datetime_modified
    ,PLATFORM
    ,case when IS_MALE_CUSTOMER = TRUE then 'MEN' else 'WOMEN' end as gender
from REPORTING_BASE_PROD.SHARED.SESSION as s
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP as m on m.MEMBERSHIP_ID = s.MEMBERSHIP_ID
join EDW_PROD.DATA_MODEL.DIM_STORE as r on r.STORE_ID = s.STORE_ID
where
    SESSION_LOCAL_DATETIME >= '2023-11-15 14:44:46.980000000'
    and MEMBERSHIP_STATE = 'Lead'
    and r.STORE_BRAND_ABBR in ('FL','YTY')
    and r.STORE_COUNTRY in ('US','UK')
    and s.CUSTOMER_ID not in (select distinct customer_id from _customer_test_starts_new_leads);

delete from _customers_existing_leads where cohort is null; --5 customers that have weird prices
delete from _customers_not_in_test_new_leads where CUSTOMER_ID in (select customer_id from _customers_existing_leads); --deleting new leads that are not in test but show up as a lead
delete from _customers_not_in_test_new_leads where registration_session_id in (select SESSION_ID from _customers_existing_leads); --deleting new leads that are not in test but show up as a lead

CREATE OR REPLACE TEMPORARY TABLE _new_lead_orders as
select
    lead_segment
    ,test_key_assignment --in test
    ,STORE_COUNTRY
    ,c.CUSTOMER_ID
    --,ORDER_ID
    ,cohort
    ,date_trunc('month',ORDER_LOCAL_DATETIME)::date as test_start_COHORT_MONTH_DATE
    ,count(case when MEMBERSHIP_ORDER_TYPE_L3 = 'Activating VIP' and c.registration_session_id = o.SESSION_ID then ORDER_ID end) as vip_activations_same_session
    ,count(case when MEMBERSHIP_ORDER_TYPE_L3 = 'Activating VIP' then ORDER_ID end) as vip_activations
    ,count(*) as orders
    ,sum(case when MEMBERSHIP_ORDER_TYPE_L3 = 'Activating VIP' then TTL_UNITS end) as units
    ,sum(case when MEMBERSHIP_ORDER_TYPE_L3 = 'Activating VIP' then TTL_PRODUCT_GROSS_REVENUE end) as PRODUCT_GROSS_REVENUE
    ,sum(case when MEMBERSHIP_ORDER_TYPE_L3 = 'Activating VIP' then TTL_PRODUCT_MARGIN_PRE_RETURN end) as PRODUCT_MARGIN_PRE_RETURN
-- select count(*),count(distinct ORDER_ID)
from _customer_test_starts_new_leads as c
join REPORTING_BASE_PROD.SHARED.ORDER_LINE_PSOURCE_PAYMENT as o on o.CUSTOMER_ID = c.CUSTOMER_ID
where --c.CUSTOMER_ID = 95041256520 and
    ORDER_SALES_CHANNEL_L1 = 'Online Order'
    and ORDER_CLASSIFICATION_L1 = 'Product Order'
    and TTL_ORDER_RNK = 1
    and ORDER_LOCAL_DATETIME >= o.REGISTRATION_LOCAL_DATETIME
    and order_status IN ('Success','Pending')
group by 1,2,3,4,5,6

union all

select
    lead_segment
    ,test_key_assignment --not in test
    ,STORE_COUNTRY
    ,c.CUSTOMER_ID
    --,ORDER_ID
    ,null cohort
    ,date_trunc('month',ORDER_LOCAL_DATETIME)::date as test_start_COHORT_MONTH_DATE
    ,count(case when MEMBERSHIP_ORDER_TYPE_L3 = 'Activating VIP' and c.registration_session_id = o.SESSION_ID then ORDER_ID end) as vip_activations_same_session
    ,count(case when MEMBERSHIP_ORDER_TYPE_L3 = 'Activating VIP' then ORDER_ID end) as vip_activations
    ,count(*) as orders
    ,sum(case when MEMBERSHIP_ORDER_TYPE_L3 = 'Activating VIP' then TTL_UNITS end) as units --change to activating
    ,sum(case when MEMBERSHIP_ORDER_TYPE_L3 = 'Activating VIP' then TTL_PRODUCT_GROSS_REVENUE end) as PRODUCT_GROSS_REVENUE --change to activating
    ,sum(case when MEMBERSHIP_ORDER_TYPE_L3 = 'Activating VIP' then TTL_PRODUCT_MARGIN_PRE_RETURN end) as PRODUCT_MARGIN_PRE_RETURN --change to activating
-- select count(*),count(distinct ORDER_ID)
from _customers_not_in_test_new_leads as c
join REPORTING_BASE_PROD.SHARED.ORDER_LINE_PSOURCE_PAYMENT as o on o.CUSTOMER_ID = c.CUSTOMER_ID
where
    ORDER_SALES_CHANNEL_L1 = 'Online Order'
    and ORDER_CLASSIFICATION_L1 = 'Product Order'
    and TTL_ORDER_RNK = 1
    and ORDER_LOCAL_DATETIME >= o.REGISTRATION_LOCAL_DATETIME
    and order_status IN ('Success','Pending')
group by 1,2,3,4,5,6;

-- select test_start_COHORT_MONTH_DATE,count(*),count(distinct CUSTOMER_ID)
-- from _new_lead_orders
-- group by 1

CREATE OR REPLACE TEMPORARY TABLE _existing_lead_orders as
select
    lead_segment
    ,'In Test' as test_key_assignemnt
    ,STORE_COUNTRY
    ,c.CUSTOMER_ID
    ,o.SESSION_ID
    --,ORDER_ID
    ,cohort
    ,date_trunc('month',ORDER_LOCAL_DATETIME)::date as test_start_COHORT_MONTH_DATE
    ,0 as vip_activations_same_session
    ,count(case when MEMBERSHIP_ORDER_TYPE_L3 = 'Activating VIP' then ORDER_ID end) as vip_activations
    ,count(*) as orders
    ,sum(case when MEMBERSHIP_ORDER_TYPE_L3 = 'Activating VIP' then TTL_UNITS end) as units --change to activating
    ,sum(case when MEMBERSHIP_ORDER_TYPE_L3 = 'Activating VIP' then TTL_PRODUCT_GROSS_REVENUE end) as PRODUCT_GROSS_REVENUE --change to activating
    ,sum(case when MEMBERSHIP_ORDER_TYPE_L3 = 'Activating VIP' then TTL_PRODUCT_MARGIN_PRE_RETURN end) as PRODUCT_MARGIN_PRE_RETURN --change to activating
-- select count(*),count(distinct ORDER_ID)
from _customers_existing_leads as c
join REPORTING_BASE_PROD.SHARED.ORDER_LINE_PSOURCE_PAYMENT as o on o.SESSION_ID = c.SESSION_ID
where
    ORDER_SALES_CHANNEL_L1 = 'Online Order'
    and ORDER_CLASSIFICATION_L1 = 'Product Order'
    and TTL_ORDER_RNK = 1
    and ORDER_LOCAL_DATETIME >= '2023-11-15'
    and order_status IN ('Success','Pending')
    and c.CUSTOMER_ID not in (select CUSTOMER_ID from _new_lead_orders)
group by 1,2,3,4,5,6,7;

CREATE OR REPLACE TRANSIENT TABLE reporting_prod.dbo.session_ab_test_fl_membership_price_new_leads_2023 as
select
    c.lead_segment --'New Leads'
    ,c.test_key_assignment --in test
    ,TEST_FRAMEWORK_ID
    ,label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,test_country --need to join by test_framework_id, test_key, and country to be sure
    ,CAMPAIGN_CODE
    ,test_key
    ,EFFECTIVE_START_DATETIME_PST
    ,EFFECTIVE_END_DATETIME_PST
    ,is_current_version
    ,abt_start_datetime_local::datetime as datetime
    ,STORE_BRAND_ABBR
    ,c.STORE_COUNTRY
    ,c.cohort --customer detail record for persistence
    ,test_group
    ,test_group_price
    ,PRICE
    ,case when platform in ('Unknown','Mobile App','Mobile') then 'MOBILE'
            when platform IN ('Desktop','Tablet') then 'DESKTOP' else platform end as platform
    ,case when IS_MALE_CUSTOMER = TRUE then 'MEN' else 'WOMEN' end as gender
    ,registration_session_id as SESSION_ID
    ,c.CUSTOMER_ID
    ,REGISTRATION_CHANNEL
    ,REGISTRATION_SUBCHANNEL
    ,test_start_COHORT_MONTH_DATE
    ,sum(vip_activations_same_session) as vip_activations_same_session
    ,sum(vip_activations) as vip_activations
    ,sum(orders) as orders
    ,sum(units) as units
    ,sum(PRODUCT_GROSS_REVENUE) as PRODUCT_GROSS_REVENUE
    ,sum(PRODUCT_MARGIN_PRE_RETURN) as PRODUCT_MARGIN_PRE_RETURN
from _customer_test_starts_new_leads as c
left join _new_lead_orders as o on o.CUSTOMER_ID = c.CUSTOMER_ID
    and c.cohort = o.cohort
    and c.STORE_COUNTRY = o.STORE_COUNTRY
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25

union all

select
    c.lead_segment --New Leads
    ,c.test_key_assignment --not in test
    ,null TEST_FRAMEWORK_ID
    ,null label
    ,null TEST_FRAMEWORK_DESCRIPTION
    ,null test_country --need to join by test_framework_id, test_key, and country to be sure
    ,null CAMPAIGN_CODE
    ,null test_key
    ,null EFFECTIVE_START_DATETIME_PST
    ,null EFFECTIVE_END_DATETIME_PST
    ,null is_current_version
    ,REGISTRATION_LOCAL_DATETIME::datetime as datetime
    ,STORE_BRAND_ABBR
    ,c.STORE_COUNTRY
    ,cohort --customer detail record for persistence
    ,'Not In Test' test_group
    ,null test_group_price
    ,PRICE
    ,case when platform in ('Unknown','Mobile App','Mobile') then 'MOBILE'
            when platform IN ('Desktop','Tablet') then 'DESKTOP' else platform end as platform
    ,case when STORE_BRAND_ABBR = 'FL' and IS_MALE_CUSTOMER = TRUE then 'MEN' else 'WOMEN' end as gender
    ,registration_session_id as session_id
    ,c.CUSTOMER_ID
    ,REGISTRATION_CHANNEL
    ,REGISTRATION_SUBCHANNEL
    ,test_start_COHORT_MONTH_DATE
    ,sum(vip_activations_same_session) as vip_activations_same_session
    ,sum(vip_activations) as vip_activations
    ,sum(orders) as orders
    ,sum(units) as units
    ,sum(PRODUCT_GROSS_REVENUE) as PRODUCT_GROSS_REVENUE
    ,sum(PRODUCT_MARGIN_PRE_RETURN) as PRODUCT_MARGIN_PRE_RETURN
from _customers_not_in_test_new_leads as c
left join _new_lead_orders as o on o.CUSTOMER_ID = c.CUSTOMER_ID
    and o.STORE_COUNTRY = c.STORE_COUNTRY
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25

union all

select
    c.lead_segment --'Existing Leads'
    ,test_key_assignment
    ,9999 TEST_FRAMEWORK_ID
    ,'FL Existing Lead Mem Price' label
    ,'FL Existing Lead Mem Price' TEST_FRAMEWORK_DESCRIPTION
    ,case when c.store_country = 'US' then 'US'
            when c.store_country = 'UK' then 'UK' end as test_country --need to join by test_framework_id, test_key, and country to be sure
    ,null CAMPAIGN_CODE
    ,null test_key
    ,null EFFECTIVE_START_DATETIME_PST
    ,null EFFECTIVE_END_DATETIME_PST
    ,null is_current_version
    ,SESSION_LOCAL_DATETIME::datetime as datetime
    ,STORE_BRAND_ABBR
    ,c.STORE_COUNTRY
    ,c.cohort --customer detail record for persistence
    ,test_group
    ,null test_group_price
    ,PRICE
    ,case when platform in ('Unknown','Mobile App','Mobile') then 'MOBILE'
            when platform IN ('Desktop','Tablet') then 'DESKTOP' else platform end as platform
    ,gender
    ,c.SESSION_ID
    ,c.CUSTOMER_ID
    ,NULL REGISTRATION_CHANNEL
    ,NULL REGISTRATION_SUBCHANNEL
    ,o.test_start_COHORT_MONTH_DATE
    ,sum(vip_activations_same_session) as vip_activations_same_session
    ,sum(vip_activations) as vip_activations
    ,sum(orders) as orders
    ,sum(units) as units
    ,sum(PRODUCT_GROSS_REVENUE) as PRODUCT_GROSS_REVENUE
    ,sum(PRODUCT_MARGIN_PRE_RETURN) as PRODUCT_MARGIN_PRE_RETURN
from _customers_existing_leads as c
left join _existing_lead_orders as o on o.SESSION_ID = c.SESSION_ID
    and o.STORE_COUNTRY = c.STORE_COUNTRY
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25;

-- NEED THESE VERSIONS
-- individual country, individual platform, individual gender
-- individual country, total platform, individual gender
-- individual country, individual platform, total gender
-- individual country, total platform, total gender

CREATE OR REPLACE TRANSIENT TABLE reporting_prod.dbo.ab_test_fl_membership_price_new_leads_2023 as
select
    'individual country, individual platform, individual gender' AS version
    ,lead_segment
    ,TEST_FRAMEWORK_ID
     ,label as test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,test_country --need to join by test_framework_id, test_key, and country to be sure
    ,CAMPAIGN_CODE
    ,test_key
    ,EFFECTIVE_START_DATETIME_PST
    ,EFFECTIVE_END_DATETIME_PST
    ,is_current_version
    ,test_key_assignment
    ,STORE_BRAND_ABBR
    ,STORE_COUNTRY
    ,cohort
    ,test_group
    ,test_group_price
    ,PRICE
    ,platform
    ,CASE WHEN STORE_BRAND_ABBR = 'YTY' THEN 'WOMEN' ELSE gender END AS gender
    ,DATE_TRUNC('month',datetime)::date as AB_TEST_START_LOCAL_MONTH_DATE
    ,datetime::date as AB_TEST_START_LOCAL_DATE
    ,count(*) as customers
    ,count(distinct CUSTOMER_ID) as customers_distinct
    ,sum(vip_activations_same_session) as vip_activations_same_session
    ,sum(vip_activations) as vip_activations
    ,sum(orders) as orders
    ,sum(units) as units
    ,sum(PRODUCT_GROSS_REVENUE) as PRODUCT_GROSS_REVENUE
    ,sum(PRODUCT_MARGIN_PRE_RETURN) as PRODUCT_MARGIN_PRE_RETURN
    ,max_record_datetime
    ,case when datetime < current_date then TRUE else FALSE end as is_complete_day
from reporting_prod.dbo.session_ab_test_fl_membership_price_new_leads_2023
FULL JOIN (select max(datetime) as max_record_datetime from reporting_prod.dbo.session_ab_test_fl_membership_price_new_leads_2023 where STORE_COUNTRY = 'US')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,max_record_datetime,case when datetime < current_date then TRUE else FALSE end

union all

select
    'individual country, total platform, individual gender' AS version
    ,lead_segment
    ,TEST_FRAMEWORK_ID
     ,label as test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,test_country --need to join by test_framework_id, test_key, and country to be sure
    ,CAMPAIGN_CODE
    ,test_key
    ,EFFECTIVE_START_DATETIME_PST
    ,EFFECTIVE_END_DATETIME_PST
    ,is_current_version
    ,test_key_assignment
    ,STORE_BRAND_ABBR
    ,STORE_COUNTRY
    ,cohort
    ,test_group
    ,test_group_price
    ,PRICE
    ,'TOTAL' platform
    ,CASE WHEN STORE_BRAND_ABBR = 'YTY' THEN 'WOMEN' ELSE gender END AS gender
    ,DATE_TRUNC('month',datetime)::date as AB_TEST_START_LOCAL_MONTH_DATE
    ,datetime::date as AB_TEST_START_LOCAL_DATE
    ,count(*) as customers
    ,count(distinct CUSTOMER_ID) as customers_distinct
    ,sum(vip_activations_same_session) as vip_activations_same_session
    ,sum(vip_activations) as vip_activations
    ,sum(orders) as orders
    ,sum(units) as units
    ,sum(PRODUCT_GROSS_REVENUE) as PRODUCT_GROSS_REVENUE
    ,sum(PRODUCT_MARGIN_PRE_RETURN) as PRODUCT_MARGIN_PRE_RETURN
    ,max_record_datetime
    ,case when datetime < current_date then TRUE else FALSE end as is_complete_day
from reporting_prod.dbo.session_ab_test_fl_membership_price_new_leads_2023
FULL JOIN (select max(datetime) as max_record_datetime from reporting_prod.dbo.session_ab_test_fl_membership_price_new_leads_2023 where STORE_COUNTRY = 'US')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,max_record_datetime,case when datetime < current_date then TRUE else FALSE end

union all

select
    'individual country, individual platform, total gender' AS version
    ,lead_segment
    ,TEST_FRAMEWORK_ID
     ,label as test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,test_country --need to join by test_framework_id, test_key, and country to be sure
    ,CAMPAIGN_CODE
    ,test_key
    ,EFFECTIVE_START_DATETIME_PST
    ,EFFECTIVE_END_DATETIME_PST
    ,is_current_version
    ,test_key_assignment
    ,STORE_BRAND_ABBR
    ,STORE_COUNTRY
    ,cohort
    ,test_group
    ,test_group_price
    ,PRICE
    ,platform
    ,'TOTAL' gender
    ,DATE_TRUNC('month',datetime)::date as AB_TEST_START_LOCAL_MONTH_DATE
    ,datetime::date as AB_TEST_START_LOCAL_DATE
    ,count(*) as customers
    ,count(distinct CUSTOMER_ID) as customers_distinct
    ,sum(vip_activations_same_session) as vip_activations_same_session
    ,sum(vip_activations) as vip_activations
    ,sum(orders) as orders
    ,sum(units) as units
    ,sum(PRODUCT_GROSS_REVENUE) as PRODUCT_GROSS_REVENUE
    ,sum(PRODUCT_MARGIN_PRE_RETURN) as PRODUCT_MARGIN_PRE_RETURN
    ,max_record_datetime
    ,case when datetime < current_date then TRUE else FALSE end as is_complete_day
from reporting_prod.dbo.session_ab_test_fl_membership_price_new_leads_2023
FULL JOIN (select max(datetime) as max_record_datetime from reporting_prod.dbo.session_ab_test_fl_membership_price_new_leads_2023 where STORE_COUNTRY = 'US')
WHERE STORE_BRAND_ABBR <> 'YTY'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,max_record_datetime,case when datetime < current_date then TRUE else FALSE end

union all

select
    'individual country, total platform, total gender' AS version
    ,lead_segment
    ,TEST_FRAMEWORK_ID
     ,label as test_label
    ,TEST_FRAMEWORK_DESCRIPTION
    ,test_country --need to join by test_framework_id, test_key, and country to be sure
    ,CAMPAIGN_CODE
    ,test_key
    ,EFFECTIVE_START_DATETIME_PST
    ,EFFECTIVE_END_DATETIME_PST
    ,is_current_version
    ,test_key_assignment
    ,STORE_BRAND_ABBR
    ,STORE_COUNTRY
    ,cohort
    ,test_group
    ,test_group_price
    ,PRICE
    ,'TOTAL' platform
    ,'TOTAL' gender
    ,DATE_TRUNC('month',datetime)::date as AB_TEST_START_LOCAL_MONTH_DATE
    ,datetime::date as AB_TEST_START_LOCAL_DATE
    ,count(*) as customers
    ,count(distinct CUSTOMER_ID) as customers_distinct
    ,sum(vip_activations_same_session) as vip_activations_same_session
    ,sum(vip_activations) as vip_activations
    ,sum(orders) as orders
    ,sum(units) as units
    ,sum(PRODUCT_GROSS_REVENUE) as PRODUCT_GROSS_REVENUE
    ,sum(PRODUCT_MARGIN_PRE_RETURN) as PRODUCT_MARGIN_PRE_RETURN
    ,max_record_datetime
    ,case when datetime < current_date then TRUE else FALSE end as is_complete_day
from reporting_prod.dbo.session_ab_test_fl_membership_price_new_leads_2023
FULL JOIN (select max(datetime) as max_record_datetime from reporting_prod.dbo.session_ab_test_fl_membership_price_new_leads_2023 where STORE_COUNTRY = 'US')
WHERE STORE_BRAND_ABBR <> 'YTY'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,max_record_datetime,case when datetime < current_date then TRUE else FALSE end;

------------------------------------------------------------------------------------------------------------------------
-- bounceback staging - test still active as of 10/4

CREATE OR REPLACE TEMPORARY TABLE _bounceback_pdp_view_sessions as
select
    test_key
    ,CUSTOMER_ID
    ,AB_TEST_SEGMENT
    ,iff(AB_TEST_SEGMENT = 1,'Control','Variant') as test_group
    ,min(minimum_AB_TEST_START_PST_DATETIME) as minimum_AB_TEST_START_PST_DATETIME
    ,min(minimum_AB_TEST_START_LOCAL_DATETIME) as minimum_AB_TEST_START_LOCAL_DATETIME
    ,min(minimum_session_id) as minimum_session_id
    ,min(minimum_session_local_datetime) as minimum_session_local_datetime
from
    (
        select
            test_key
            ,CUSTOMER_ID
            ,AB_TEST_SEGMENT
            ,min(convert_timezone(STORE_TIME_ZONE,'America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME::Datetime)::Datetime) as minimum_AB_TEST_START_PST_DATETIME
            ,min(AB_TEST_START_LOCAL_DATETIME) as minimum_AB_TEST_START_LOCAL_DATETIME
            ,min(SESSION_ID) as minimum_session_id
            ,min(SESSION_LOCAL_DATETIME) as minimum_session_local_datetime
        from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as s
        join EDW_PROD.DATA_MODEL.DIM_STORE as t on t.STORE_ID = s.STORE_ID
        join lake.SEGMENT_FL.JAVASCRIPT_FABLETICS_PRODUCT_VIEWED as v on edw_prod.stg.udf_unconcat_brand(SESSION_ID) = v.PROPERTIES_SESSION_ID
        where
            CAMPAIGN_CODE = 'bounceBackECHO'
            and timestamp >= '2024-08-01'
            and PROPERTIES_IS_BUNDLE = false
        group by all

        union

        select
            test_key
            ,CUSTOMER_ID
            ,AB_TEST_SEGMENT
            ,min(convert_timezone(STORE_TIME_ZONE,'America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME::Datetime)::Datetime) as minimum_AB_TEST_START_PST_DATETIME
            ,min(AB_TEST_START_LOCAL_DATETIME) as minimum_AB_TEST_START_LOCAL_DATETIME
            ,min(try_to_number(SESSION_ID)) as minimum_session_id
            ,min(SESSION_LOCAL_DATETIME) as minimum_session_local_datetime
        from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as s
        join EDW_PROD.DATA_MODEL.DIM_STORE as t on t.STORE_ID = s.STORE_ID
        join lake.SEGMENT_FL.REACT_NATIVE_FABLETICS_PRODUCT_VIEWED as v on edw_prod.stg.udf_unconcat_brand(SESSION_ID::varchar) = IFNULL(try_to_number(PROPERTIES_SESSION_ID)::VARCHAR, '0')::VARCHAR
        where
            CAMPAIGN_CODE = 'bounceBackECHO'
            and timestamp >= '2024-08-01'
            and PROPERTIES_SESSION_ID <> '0'
            and PROPERTIES_IS_BUNDLE = false
        group by all
    )
group by all;

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.fl_abt_bounceback_customers_pdp_view_outsanding_tokens as
SELECT DISTINCT
    m.CUSTOMER_ID
    ,minimum_session_id
    ,test_key
    ,test_group
    ,mt.MEMBERSHIP_TOKEN_ID
    ,minimum_AB_TEST_START_PST_DATETIME
    ,minimum_AB_TEST_START_LOCAL_DATETIME
    ,EFFECTIVE_START_DATETIME
    ,EFFECTIVE_END_DATETIME
    ,sc.label
from LAKE_CONSOLIDATED.ULTRA_MERCHANT_HISTORY.MEMBERSHIP_TOKEN mt
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP m on m.MEMBERSHIP_ID = mt.MEMBERSHIP_ID
join _bounceback_pdp_view_sessions as bb on bb.CUSTOMER_ID = m.CUSTOMER_ID
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP_TOKEN_REASON mtr on mtr.MEMBERSHIP_TOKEN_REASON_ID = mt.MEMBERSHIP_TOKEN_REASON_ID
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.STATUSCODE sc on sc.STATUSCODE = mt.STATUSCODE
where
    sc.LABEL = 'Active'
    and mtr.label in ('Token Billing', 'Converted Membership Credit')
    and EFFECTIVE_START_DATETIME <= minimum_AB_TEST_START_PST_DATETIME
    and EFFECTIVE_END_DATETIME >= minimum_AB_TEST_START_PST_DATETIME;

----------------------------------------------------
--code for reporting_prod.shared.ab_test_vip_ltv

CREATE OR REPLACE TEMPORARY TABLE _customer_base as
select -- dec cohort
    'FL 2023 Mem Price - Existing VIP (DEC)' as test_key
    ,l.customer_id_concat as customer_id
    ,l.VIP_COHORT_MONTH_DATE
    ,p.price as membership_price
    , case
          when excl.customer_id is not null then 'sept excl' --exclude customers who are in 69.95 sept. price bucket, as they would be labeled as 'v2' for nov/dec
          when l.test_segment = 'exclude' then 'exclude'
          when p.price = '59.95' then 'control'
          when p.price = '64.95' then 'v1'
          when p.price = '69.95' and excl.CUSTOMER_ID is null then 'v2'
          else null end as test_group_1
    ,case
          when excl.customer_id is not null then 'sept excl'
          when p.price = '59.95' or l.test_segment = 'exclude' then '$59.95'
          when p.price = '64.95' then '$64.95'
          when p.price = '69.95' then '$69.95'
    end test_group_2
    ,null test_group_3
    ,'2023-12-01' test_start_month_date
from reporting_prod.shared.price_testing_existing_vips_migration_list as l --previously in work db / dragan schema
join lake_consolidated_view.ultra_merchant.membership as p on l.customer_id_concat = p.customer_id
left join reporting_prod.fabletics.abt_ltv_price_testing_sept_2024_migration_list excl on excl.customer_id = l.customer_id

union all

select -- nov cohort
    'FL 2023 Mem Price - Existing VIP (NOV)' as test_key
    ,l.customer_id_concat as customer_id
    ,l.VIP_COHORT_MONTH_DATE
    ,p.price as membership_price
    , case
          when excl.customer_id is not null then 'sept excl'
          when l.test_segment = 'exclude' then 'exclude'
          when p.price = '59.95' then 'control'
          when p.price = '64.95' then 'v1'
          when p.price = '69.95' and excl.CUSTOMER_ID is null then 'v2'
          else null end as test_group_1
    , case
          when excl.customer_id is not null then 'sept excl'
          when p.price = '59.95' or l.test_segment = 'exclude' then '$59.95'
          when p.price = '64.95' then '$64.95'
          when p.price = '69.95' then '$69.95'
    end                    test_group_2
    ,null test_group_3
    ,'2023-11-01' test_start_month_date
from reporting_prod.shared.price_testing_existing_vips_migration_list as l --previously in work db / dragan schema
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP as p on l.customer_id_concat = p.CUSTOMER_ID
left join reporting_prod.fabletics.abt_ltv_price_testing_sept_2024_migration_list excl on excl.customer_id = l.customer_id

union all

select -- sept cohort
    'FL 2024 Mem Price - Existing VIP (SEP)' as test_key
    ,l.customer_id||'20' as customer_id
    ,l.vip_cohort_month_date
    ,p.price as membership_price
    , case
          when l.test_segment = 'CONTROL' then 'control'
          when l.test_segment = 'TEST' then 'v1'
          when l.test_segment = 'EXCLUDE' then 'exclude'
          else null end as test_group_1
    ,null test_group_2
    ,null test_group_3
    ,'2024-08-01' test_start_month_date
from reporting_prod.fabletics.abt_ltv_sept_2024_price_test_segments as l
join lake_consolidated_view.ultra_merchant.membership as p on l.customer_id||'20' = p.customer_id

union all

select distinct
    'FL 2023 Mem Price - New VIP' as test_key
    ,l.customer_id as customer_id
    ,test_start_cohort_month_date as VIP_COHORT_MONTH_DATE
    ,l.price as membership_price
    ,case when excl.customer_id is not null then 'sept excl' --exclude customers who are in 69.95 sept. price bucket
            when l.price = '59.95' and STORE_COUNTRY = 'US' then 'control'
            when l.price = '64.95' and STORE_COUNTRY = 'US' then 'v1'
            when l.price = '69.95' and STORE_COUNTRY = 'US' then 'v2'
            when l.price = '54.99' and STORE_COUNTRY = 'UK' then 'control'
            when l.price = '59.95' and STORE_COUNTRY = 'UK' then 'v1'
            else test_group end as test_group_1
    ,concat('$',to_number(l.price,10,2)) as test_group_2
    ,null test_group_3
    ,test_start_cohort_month_date as test_start_month_date
from reporting_prod.dbo.session_ab_test_fl_membership_price_new_leads_2023 as l
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP as p on p.CUSTOMER_ID = l.CUSTOMER_ID
left join reporting_prod.fabletics.abt_ltv_price_testing_sept_2024_migration_list excl on excl.customer_id||'20' = l.customer_id
where test_start_COHORT_MONTH_DATE is not null and VIP_ACTIVATIONS > 0
and STORE_COUNTRY in ('US','UK')

union all

select 'FL 2024 Jun Skipper Test'                         as test_key,
       cst.CUSTOMER_ID || '20'                            as customer_id,          --concatenated customer id
       date_trunc(month, ACTIVATION_LOCAL_DATETIME::date) as vip_cohort_month_date,
       BILLING_PRICE                                      as membership_price,     --we are not testing price, should I leave null?
       TEST_OR_CONTROL                                    as test_group_1,
       null                                               as test_group_2,
       null                                               as test_group_3,
       '2024-06-01'                                       as test_start_month_date --hardcode to june
from reporting_prod.shared.crm_skip_test_june2024 cst --previously in work db / dragan schema
         join EDW_PROD.DATA_MODEL_FL.FACT_ACTIVATION fa on fa.CUSTOMER_ID = cst.CUSTOMER_ID and IS_CURRENT

union all

select 'FL 2024-' || right(left(FAILED_BILLING_TEST_MONTH, 7), 2) ||
       ' Failed Billing Test'                             as test_key,
       fbcs.CUSTOMER_ID || '20'                           as customer_id,      --concatenated customer id
       date_trunc(month, ACTIVATION_LOCAL_DATETIME::date) as vip_cohort_month_date,
       null                                               as membership_price, --we are not testing price, leave null
       PURCHASE_SEGMENT                                   as test_group_1,
       null                                               as test_group_2,
       null                                               as test_group_3,
       FAILED_BILLING_TEST_MONTH                          as test_start_month_date
from reporting_prod.shared.FAILED_BILLER_CUSTOMER_SEGMENTS fbcs --previously in work db / dragan schema
         join EDW_PROD.DATA_MODEL_FL.FACT_ACTIVATION fa on fa.CUSTOMER_ID = fbcs.CUSTOMER_ID and IS_CURRENT

union all

select
    CONCAT('FL 2024, ',AB_TEST_KEY) AS test_key
    ,p.CUSTOMER_ID
    ,date_trunc('month',ACTIVATION_LOCAL_DATETIME)::date as vip_cohort_month_date
    ,null as membership_price
    ,test_group as test_group_1
    ,null as test_group_2
    ,null as test_group_3
    ,date_trunc('month',AB_TEST_START_LOCAL_DATETIME)::date as test_start_month_date
from reporting_base_prod.shared.fl_abt_passwordless_new_vips as p --static table
join EDW_PROD.DATA_MODEL.FACT_ACTIVATION as fa on fa.CUSTOMER_ID = p.CUSTOMER_ID
    and fa.SESSION_ID = p.SESSION_ID
    and fa.ACTIVATION_LOCAL_DATETIME >= AB_TEST_START_LOCAL_DATETIME

union all

select distinct
    CONCAT('FL 2024, ',test_key) AS test_key
    ,t.CUSTOMER_ID
    ,fa.VIP_COHORT_MONTH_DATE as vip_cohort_month_date
    ,null as membership_price
    ,test_group as test_group_1
    ,null as test_group_2
    ,null as test_group_3
    ,date_trunc('month',minimum_ab_test_start_local_datetime)::date as test_start_month_date
from reporting_base_prod.shared.fl_abt_bounceback_customers_pdp_view_outsanding_tokens as t
join EDW_PROD.DATA_MODEL.FACT_ACTIVATION as fa on fa.CUSTOMER_ID = t.CUSTOMER_ID

union all

select distinct
    concat('FLUK 2025, Jan Mem Price Change, ',MEMBERSHIP_STATE_BEFORE_PRICE_CHANGE) AS test_key
    ,CUSTOMER_ID||20 as CUSTOMER_ID
    ,vip_cohort_month_date
    ,original_price as membership_price
    ,test_group as test_group_1
    ,null as test_group_2
    ,null as test_group_3
    ,test_start_month_date
from reporting_prod.SHARED.da35518_fluk_jan_2025_mem_price_change_abt_ltv

union all

select
    test_key
    ,CUSTOMER_ID
    ,vip_cohort_month_date
    ,membership_price
    ,test_group_1
    ,test_group_2
    ,test_group_3
    ,test_start_month_date
from reporting_base_prod.shared.flus_oc_first_activating_order_customers

;

-------------------------------------------------------------------------------------------------------
--compiling all data at the customer ID and month level

CREATE OR REPLACE TEMPORARY TABLE _customer_ab_test_vip_ltv as
select
    base.customer_id
    ,test_key
    ,STORE_BRAND_ABBR
    ,STORE_REGION
    ,STORE_COUNTRY
    ,MEMBERSHIP_ID
    ,CASE WHEN t.STORE_BRAND_ABBR = 'FL' and l.GENDER = 'M' and dc.IS_SCRUBS_CUSTOMER = TRUE THEN 'Scrubs Men'
             WHEN t.STORE_BRAND_ABBR = 'FL' and dc.IS_SCRUBS_CUSTOMER = TRUE THEN 'Scrubs Women'
                WHEN t.STORE_BRAND_ABBR = 'FL' and l.GENDER = 'M' THEN 'Fabletics Men'
                     WHEN t.STORE_BRAND_ABBR = 'FL' THEN 'Fabletics Women'
                         else null end as customer_segment
    ,test_start_month_date
    ,test_group_1
    ,test_group_2
    ,test_group_3
    ,concat('$',to_decimal(base.membership_price,10,2)) as price
    ,l.VIP_COHORT_MONTH_DATE
    ,case when datediff('month',l.VIP_COHORT_MONTH_DATE,test_start_month_date) + 1 between 1 and 3 then 'M1-M3'
                when datediff('month',l.VIP_COHORT_MONTH_DATE,test_start_month_date) + 1 between 4 and 12 then 'M4-M12'
                when datediff('month',l.VIP_COHORT_MONTH_DATE,test_start_month_date) + 1 between 13 and 24 then 'M13-M24'
                when datediff('month',l.VIP_COHORT_MONTH_DATE,test_start_month_date) + 1 >= 25 then 'M25+'
        end as vip_tenure_group --test_start_month_date
     ,case when datediff('month',l.VIP_COHORT_MONTH_DATE,MONTH_DATE) + 1 between 1 and 3 then 'M1-M3'
                when datediff('month',l.VIP_COHORT_MONTH_DATE,MONTH_DATE) + 1 between 4 and 12 then 'M4-M12'
                when datediff('month',l.VIP_COHORT_MONTH_DATE,MONTH_DATE) + 1 between 13 and 24 then 'M13-M24'
                when datediff('month',l.VIP_COHORT_MONTH_DATE,MONTH_DATE) + 1 >= 25 then 'M25+'
        end as vip_tenure_group_month_date
    ,CONCAT('M',datediff('month',l.VIP_COHORT_MONTH_DATE,test_start_month_date) + 1) as vip_tenure
    ,month_date
    ,datediff('month',test_start_month_date,MONTH_DATE) + 1 as test_start_tenure
    ,CASE WHEN test_key = 'FL 2023 Mem Price - New VIP' AND l.VIP_COHORT_MONTH_DATE = MONTH_DATE THEN true
        ELSE IS_BOP_VIP END is_bop_vip
    ,IS_LOGIN
    ,IS_SKIP
    ,IS_SNOOZE
    ,IS_MERCH_PURCHASER
    ,IS_CANCEL
    ,IS_PASSIVE_CANCEL
    ,case when IS_CANCEL = TRUE and IS_PASSIVE_CANCEL = FALSE then TRUE else FALSE end as IS_CANCEL_EXCL_PASSIVE
    ,IS_SUCCESSFUL_BILLING
    ,IS_PENDING_BILLING
    ,case when IS_SUCCESSFUL_BILLING = TRUE OR IS_PENDING_BILLING = TRUE then TRUE else FALSE end as IS_BILLING_SUCC_PENDING
    ,is_failed_billing
     ,PRODUCT_ORDER_COUNT
     ,PRODUCT_GROSS_REVENUE
     ,PRODUCT_GROSS_PROFIT
     ,PRODUCT_MARGIN_PRE_RETURN
     ,PRODUCT_ORDER_CASH_GROSS_REVENUE_AMOUNT
     ,PRODUCT_ORDER_CASH_MARGIN_PRE_RETURN
     ,CASH_GROSS_PROFIT
     ,PRODUCT_ORDER_CASH_REFUND_AMOUNT_AND_CHARGEBACK_AMOUNT
     ,BILLED_CASH_CREDIT_REDEEMED_AMOUNT
     ,PRODUCT_ORDER_UNIT_COUNT
    ,PRODUCT_GROSS_REVENUE_EXCL_SHIPPING
    ,PRODUCT_ORDER_LANDED_PRODUCT_COST_AMOUNT
    ,refresh_datetime
from edw_prod.ANALYTICS_BASE.CUSTOMER_LIFETIME_VALUE_MONTHLY as l
join _customer_base as base on base.CUSTOMER_ID = l.CUSTOMER_ID
        and l.VIP_COHORT_MONTH_DATE = base.VIP_COHORT_MONTH_DATE
join EDW_PROD.DATA_MODEL.DIM_CUSTOMER as dc on dc.CUSTOMER_ID = l.CUSTOMER_ID
join edw_prod.DATA_MODEL.DIM_STORE as t on t.STORE_ID = l.STORE_ID
join LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.MEMBERSHIP as m on m.CUSTOMER_ID = base.CUSTOMER_ID
full join (select max(META_UPDATE_DATETIME) as refresh_datetime from edw_prod.ANALYTICS_BASE.CUSTOMER_LIFETIME_VALUE_MONTHLY where STORE_ID = 52)
where MONTH_DATE >= test_start_month_date
order by month_date;

CREATE OR REPLACE TRANSIENT TABLE reporting_prod.shared.ab_test_vip_ltv as
select
    month_date
    ,test_key
    ,test_group_1
    ,test_group_2 --custom segment 1
    ,test_group_3 --custom segment
    ,STORE_BRAND_ABBR
    ,STORE_REGION
    ,store_country
    ,CUSTOMER_SEGMENT
    ,test_start_month_date
    ,concat('M',test_start_tenure) as test_start_tenure
    ,PRICE
    ,a.VIP_COHORT_MONTH_DATE
    ,a.VIP_TENURE
    ,a.VIP_TENURE_GROUP
    ,vip_tenure_group_month_date
    ,count(distinct customer_id) as customers
    ,count(case when IS_BOP_VIP then customer_id end) as bop_vips
    ,count(case when IS_LOGIN and IS_BOP_VIP then customer_id end) as login_actions
    ,count(case when IS_MERCH_PURCHASER and IS_BOP_VIP then customer_id end) as merch_purchase_actions
    ,count(case when IS_SKIP or IS_SNOOZE then customer_id end) as skipped_pause_actions
    ,count(case when IS_CANCEL then customer_id end) as cancelled_actions
    ,count(case when IS_PASSIVE_CANCEL then customer_id end) as cancelled_passive_actions
    ,count(case when IS_CANCEL_EXCL_PASSIVE then customer_id end) as cancelled_excl_passive_actions
    ,count(case when IS_SUCCESSFUL_BILLING or IS_PENDING_BILLING then customer_id end) as successful_pending_billing_actions
    ,count(case when is_failed_billing then customer_id end) as failed_billing_actions
    ,sum(PRODUCT_ORDER_COUNT) as order_count
    ,sum(PRODUCT_ORDER_UNIT_COUNT) as unit_count
    ,sum(PRODUCT_GROSS_REVENUE) as product_gross_revenue
    ,sum(PRODUCT_GROSS_REVENUE_EXCL_SHIPPING) as PRODUCT_GROSS_REVENUE_EXCL_SHIPPING
    ,sum(PRODUCT_GROSS_PROFIT) as product_gross_profit
    ,sum(PRODUCT_MARGIN_PRE_RETURN) as product_margin_pre_return
    ,sum(PRODUCT_ORDER_CASH_GROSS_REVENUE_AMOUNT) as product_cash_gross_revenue
    ,sum(PRODUCT_ORDER_CASH_MARGIN_PRE_RETURN) as product_cash_gross_margin
    ,sum(CASH_GROSS_PROFIT) as cash_gross_profit
    ,sum(PRODUCT_ORDER_CASH_REFUND_AMOUNT_AND_CHARGEBACK_AMOUNT) as product_refund_chargebacks
    ,sum(BILLED_CASH_CREDIT_REDEEMED_AMOUNT) as product_credit_redemptions
    ,sum(PRODUCT_ORDER_LANDED_PRODUCT_COST_AMOUNT) as PRODUCT_ORDER_LANDED_PRODUCT_COST_AMOUNT
    ,refresh_datetime
from _customer_ab_test_vip_ltv as a
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,refresh_datetime;
