
set last_refreshed_datetime_hq = (select max(CONVERT_TIMEZONE('America/Los_Angeles',session_local_datetime)::datetime) from reporting_base_prod.shared.session_order_ab_test_detail);

set process_from_date = (select MAX(convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATE))::date-7  --comment
                         from reporting_prod.SHARED.PSOURCE_PROMO_REVENUE_AB_TEST_FINAL);

CREATE OR REPLACE TEMPORARY TABLE _last_3_months as
select distinct
    TEST_KEY
    ,test_label
    ,test_framework_id
    ,STORE_BRAND
    ,min(AB_TEST_START_LOCAL_DATETIME) as MIN_AB_TEST_START_LOCAL_DATETIME
    ,max(AB_TEST_START_LOCAL_DATETIME) as MAX_AB_TEST_START_LOCAL_DATETIME
from REPORTING_BASE_PROD.SHARED.SESSION_ORDER_AB_TEST_DETAIL
where campaign_code <> 'FLimagesort'
group by 1,2,3,4;
--
-- union
--
-- select distinct
--     TEST_KEY
--     ,test_label
--     ,test_framework_id
--     ,STORE_BRAND
--     ,CMS_MIN_TEST_START_DATETIME_HQ
--     ,current_date as max_AB_TEST_START_LOCAL_DATETIME
-- from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA_FILTERED
-- where CAMPAIGN_CODE in ('MYVIPSNOOZEOPTION','SAVEOFFERSEGMENTATIONLITE')
-- order by 1;

delete from _last_3_months
    where max_AB_TEST_START_LOCAL_DATETIME <= CURRENT_DATE - INTERVAL '3 MONTH';

CREATE OR REPLACE TEMPORARY TABLE _same_test_group_descriptions_across_platforms as
select distinct
    test_framework_id
    ,test_label
    ,test_key
    ,store_brand
    ,test_group
    ,count(distinct test_group_description) as distinct_test_group_descriptions
from reporting_base_prod.shared.session_order_ab_test_detail
group by 1,2,3,4,5;

delete from _same_test_group_descriptions_across_platforms
where distinct_test_group_descriptions > 1;

CREATE OR REPLACE TEMPORARY TABLE _total_descriptions as
select distinct
   p.test_framework_id
  ,p.test_label
  ,p.test_key
  ,p.store_brand
  ,p.test_group
  ,d.test_group_description
from _same_test_group_descriptions_across_platforms as p
join reporting_base_prod.shared.session_order_ab_test_detail as d on d.test_key = p.test_key
    and d.test_framework_id = p.test_framework_id
    and d.test_label = p.test_label
    and d.store_brand = p.store_brand
    AND d.test_group = p.test_group;

CREATE OR REPLACE TEMPORARY TABLE _sorting_hat_metadata_ids as
select brand,test_key,test_platforms,
    case when brand in ('Fabletics','Yitty') then concat(test_key,20)
        when brand in ('JustFab','ShoeDazzle','FabKids') then concat(test_key,10)
        when brand = 'Savage X' then concat(test_key,30) end meta_original_metadata_test_id
from lake_view.sharepoint.ab_test_metadata_import_oof
where
    test_type ilike 'Sorting Hat%';

CREATE OR REPLACE TEMPORARY TABLE _builder_test_labels as
select distinct builder_id,TEST_NAME as builder_test_label_adj
from lake.builder.builder_api_metadata api
WHERE ADJUSTED_ACTIVATED_DATETIME_PST >= '2024-09-13'
order by builder_id;

CREATE OR REPLACE TEMP TABLE _PSOURCE_PROMO_REVENUE_AB_TEST_FINAL AS --comment
-- CREATE OR REPLACE TRANSIENT TABLE reporting_prod.SHARED.PSOURCE_PROMO_REVENUE_AB_TEST_FINAL as --comment
SELECT
    'individual country and individual platform' as version
     ,s.TEST_KEY
     ,s.TEST_FRAMEWORK_ID
     ,coalesce(u.builder_test_label_adj,s.test_label) as test_label
     ,TEST_FRAMEWORK_DESCRIPTION
     ,TEST_FRAMEWORK_TICKET
     ,s.CAMPAIGN_CODE
     ,TEST_TYPE
     ,test_activated_datetime
     ,case when TEST_GROUP = 'Control' THEN 'Control' else 'Variant' end as TEST_GROUP_TYPE
     ,TEST_GROUP
     ,GROUP_NAME
     ,TRAFFIC_SPLIT_TYPE
     ,test_group_description
     ,AB_TEST_START_LOCAL_DATETIME::date as ab_test_start_local_date
     ,s.STORE_BRAND
     ,STORE_REGION
     ,STORE_COUNTRY
     ,TEST_START_MEMBERSHIP_STATE
     ,TEST_START_LEAD_TENURE_GROUP
     ,TEST_START_VIP_MONTH_TENURE_GROUP
     ,case when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
           when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
           when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end as lead_registration_type
     ,MEMBERSHIP_PRICE
     ,MEMBERSHIP_TYPE
     ,NULL as IS_MOBILE_APP_USER
     ,NULL as IS_FREE_TRIAL
     ,NULL as IS_CROSS_PROMO
     ,NULL as IS_MALE_GATEWAY
     ,IS_MALE_SESSION
     ,IS_MALE_CUSTOMER
     ,IS_MALE_SESSION_CUSTOMER
     ,s.PLATFORM
     ,s.OS
     ,NULL as BROWSER
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then CHANNEL else 'Null - Non Prospect' end as CHANNEL
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then SUBCHANNEL else 'Null - Non Prospect' end as SUBCHANNEL
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then DM_GATEWAY_ID else null end as DM_GATEWAY_ID
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then GATEWAY_NAME else 'Null - Non Prospect' end as GATEWAY_NAME
     ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN AS IS_SKIP_MONTH_ACTION
     ,FLAG_1_NAME
     ,FLAG_1_DESCRIPTION
     ,FLAG_2_NAME
     ,FLAG_2_DESCRIPTION
     ,FLAG_3_NAME
     ,FLAG_3_DESCRIPTION
     ,CUSTOM_FLAG_NAME
     ,CUSTOM_FLAG_DESCRIPTION
     ,CUSTOM_FLAG_2_NAME
     ,CUSTOM_FLAG_2_DESCRIPTION
     ,product_type
     ,product_lifestyle_type
     ,product_sub_brand
     ,product_gender
     ,product_segment
     ,product_department
     ,PRODUCT_CATEGORY
     ,product_name
     ,byo_bundle_name
     ,bundle_name
     ,CASE WHEN brand <> 'Savage X' THEN coalesce(psource_bundle,psource_pieced,'None')
           WHEN brand = 'Savage X' AND product_type = 'Normal' then coalesce(psource_bundle,psource_pieced,'None') -- SXF normal items
           WHEN brand = 'Savage X' AND (lower(PRODUCT_NAME) not ilike '%box' AND not contains(lower(bundle_name),'boxer'))
               AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'browse_sets_')  --sxf bundles that are null
           WHEN brand = 'Savage X' AND byo_bundle_name is not null AND psource_pieced ilike 'byo%' THEN
               coalesce(psource_pieced,psource_bundle,'byo_') --byo
           WHEN brand = 'Savage X' AND byo_bundle_name is not null AND (psource_pieced not ilike 'byo%'
               OR psource_pieced is null) then coalesce(psource_bundle,'byo_') --byo
           WHEN brand = 'Savage X' AND (lower(bundle_name) ilike '%box' AND not contains(lower(bundle_name),'boxer'))
               AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'xtrashop_')
           ELSE 'None' END AS psource
     ,promo_code
     ,promo_name
     ,case when SALES_TYPE = 'Normal - Cash' then 'Pieced Good - Cash'
            when SALES_TYPE = 'Normal - Token' then 'Pieced Good - Token' else SALES_TYPE end as SALES_TYPE
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then UNITS end) as units_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then UNITS end) as units_nonactivating
     ,sum(UNITS) as units_total
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then outfit_component_units end) as outfit_component_units_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then outfit_component_units end) as outfit_component_units_nonactivating
     ,sum(pp.outfit_component_units) as outfit_component_units_total
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then product_gross_revenue_excl_shipping end) as product_gross_revenue_excl_shipping_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then product_gross_revenue_excl_shipping end) as product_gross_revenue_excl_shipping_nonactivating
     ,sum(pp.product_gross_revenue_excl_shipping) as product_gross_revenue_excl_shipping_total
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then product_cost end) as product_cost_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then product_cost end) as product_cost_nonactivating
     ,sum(pp.product_cost) as product_cost_total
     ,$last_refreshed_datetime_hq as last_refreshed_datetime_hq
--'individual country and individual platform'
FROM reporting_base_prod.SHARED.session_order_ab_test_detail s
join reporting_base_prod.shared.order_line_psource_payment as pp on pp.SESSION_ID = s.SESSION_ID
join _last_3_months as ll on ll.test_framework_id = s.test_framework_id
    and ll.TEST_KEY = s.TEST_KEY
    and ll.test_label = s.test_label
    and ll.store_brand = s.store_brand
left join _builder_test_labels as u on u.builder_id = s.test_key
where
    test_group is not null
    AND ORDER_SALES_CHANNEL_L1 = 'Online Order'
    AND ORDER_CLASSIFICATION_L1 = 'Product Order'
    and ORDER_LOCAL_DATETIME >= AB_TEST_START_LOCAL_DATETIME
    and convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME)::date>=$process_from_date --comment
group by
    s.TEST_KEY
    ,s.TEST_FRAMEWORK_ID
    ,coalesce(u.builder_test_label_adj,s.test_label)
    ,TEST_FRAMEWORK_DESCRIPTION
    ,TEST_FRAMEWORK_TICKET
    ,s.CAMPAIGN_CODE
    ,TEST_TYPE
    ,test_activated_datetime
    ,case when TEST_GROUP = 'Control' THEN 'Control' else 'Variant' end
    ,TEST_GROUP
    ,GROUP_NAME
    ,TRAFFIC_SPLIT_TYPE
    ,test_group_description
    ,AB_TEST_START_LOCAL_DATETIME::date
    ,s.STORE_BRAND
    ,STORE_REGION
    ,STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,TEST_START_LEAD_TENURE_GROUP
    ,TEST_START_VIP_MONTH_TENURE_GROUP
    ,case when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
    when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
    when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end
    ,MEMBERSHIP_PRICE
    ,MEMBERSHIP_TYPE
    --      ,IS_MOBILE_APP_USER
    --      ,IS_FREE_TRIAL
    --      ,IS_CROSS_PROMO
    --      ,IS_MALE_GATEWAY
    ,IS_MALE_SESSION
    ,IS_MALE_CUSTOMER
    ,IS_MALE_SESSION_CUSTOMER
    ,s.PLATFORM
    ,s.OS
    --      ,s.BROWSER
    ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then CHANNEL else 'Null - Non Prospect' end
    ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then SUBCHANNEL else 'Null - Non Prospect' end
    ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then DM_GATEWAY_ID else null end
    ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then GATEWAY_NAME else 'Null - Non Prospect' end
    ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN
    ,FLAG_1_NAME
    ,FLAG_1_DESCRIPTION
    ,FLAG_2_NAME
    ,FLAG_2_DESCRIPTION
    ,FLAG_3_NAME
    ,FLAG_3_DESCRIPTION
    ,CUSTOM_FLAG_NAME
    ,CUSTOM_FLAG_DESCRIPTION
    ,CUSTOM_FLAG_2_NAME
    ,CUSTOM_FLAG_2_DESCRIPTION
    ,product_type
    ,product_lifestyle_type
     ,product_sub_brand
        ,product_gender
     ,product_segment
    ,product_department
    ,PRODUCT_CATEGORY
    ,product_name
    ,byo_bundle_name
    ,bundle_name
    ,CASE WHEN brand <> 'Savage X' THEN coalesce(psource_bundle,psource_pieced,'None')
    WHEN brand = 'Savage X' AND product_type = 'Normal' then coalesce(psource_bundle,psource_pieced,'None') -- SXF normal items
    WHEN brand = 'Savage X' AND (lower(PRODUCT_NAME) not ilike '%box' AND not contains(lower(bundle_name),'boxer'))
     AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'browse_sets_')  --sxf bundles that are null
    WHEN brand = 'Savage X' AND byo_bundle_name is not null AND psource_pieced ilike 'byo%' THEN
     coalesce(psource_pieced,psource_bundle,'byo_') --byo
    WHEN brand = 'Savage X' AND byo_bundle_name is not null AND (psource_pieced not ilike 'byo%'
     OR psource_pieced is null) then coalesce(psource_bundle,'byo_') --byo
    WHEN brand = 'Savage X' AND (lower(bundle_name) ilike '%box' AND not contains(lower(bundle_name),'boxer'))
     AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'xtrashop_')
    ELSE 'None' END
    ,promo_code
    ,promo_name
    ,case when SALES_TYPE = 'Normal - Cash' then 'Pieced Good - Cash'
            when SALES_TYPE = 'Normal - Token' then 'Pieced Good - Token' else SALES_TYPE end

union all

SELECT
    'individual country and total platform' as version
     ,s.TEST_KEY
     ,s.TEST_FRAMEWORK_ID
     ,coalesce(u.builder_test_label_adj,s.test_label) as test_label
     ,TEST_FRAMEWORK_DESCRIPTION
     ,TEST_FRAMEWORK_TICKET
     ,s.CAMPAIGN_CODE
     ,TEST_TYPE
     ,test_activated_datetime
     ,case when s.TEST_GROUP = 'Control' THEN 'Control' else 'Variant' end as TEST_GROUP_TYPE
     ,s.TEST_GROUP
     ,GROUP_NAME
     ,TRAFFIC_SPLIT_TYPE
     ,sc.TEST_GROUP_DESCRIPTION
     ,AB_TEST_START_LOCAL_DATETIME::date as ab_test_start_local_date
     ,s.STORE_BRAND
     ,STORE_REGION
     ,STORE_COUNTRY
     ,TEST_START_MEMBERSHIP_STATE
     ,TEST_START_LEAD_TENURE_GROUP
     ,TEST_START_VIP_MONTH_TENURE_GROUP
     ,case when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
           when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
           when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end as lead_registration_type
     ,MEMBERSHIP_PRICE
     ,MEMBERSHIP_TYPE
     ,IS_MOBILE_APP_USER
     ,IS_FREE_TRIAL
     ,IS_CROSS_PROMO
     ,IS_MALE_GATEWAY
     ,IS_MALE_SESSION
     ,IS_MALE_CUSTOMER
     ,IS_MALE_SESSION_CUSTOMER
     ,'Total' as PLATFORM
     ,'Total' as OS
     ,NULL as BROWSER
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then CHANNEL else 'Null - Non Prospect' end as CHANNEL
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then SUBCHANNEL else 'Null - Non Prospect' end as SUBCHANNEL
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then DM_GATEWAY_ID else null end as DM_GATEWAY_ID
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then GATEWAY_NAME else 'Null - Non Prospect' end as GATEWAY_NAME
     --session actions
     ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN AS IS_SKIP_MONTH_ACTION
     --session flags
     ,FLAG_1_NAME
     ,FLAG_1_DESCRIPTION
     ,FLAG_2_NAME
     ,FLAG_2_DESCRIPTION
     ,FLAG_3_NAME
     ,FLAG_3_DESCRIPTION
     ,CUSTOM_FLAG_NAME
     ,CUSTOM_FLAG_DESCRIPTION
     ,CUSTOM_FLAG_2_NAME
     ,CUSTOM_FLAG_2_DESCRIPTION
     ,product_type
     ,product_lifestyle_type
     ,product_sub_brand
     ,product_gender
     ,product_segment
     ,product_department
    ,PRODUCT_CATEGORY
     ,product_name
     ,byo_bundle_name
     ,bundle_name
     ,CASE WHEN brand <> 'Savage X' THEN coalesce(psource_bundle,psource_pieced,'None')
           WHEN brand = 'Savage X' AND product_type = 'Normal' then coalesce(psource_bundle,psource_pieced,'None') -- SXF normal items
           WHEN brand = 'Savage X' AND (lower(PRODUCT_NAME) not ilike '%box' AND not contains(lower(bundle_name),'boxer'))
               AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'browse_sets_')  --sxf bundles that are null
           WHEN brand = 'Savage X' AND byo_bundle_name is not null AND psource_pieced ilike 'byo%' THEN
               coalesce(psource_pieced,psource_bundle,'byo_') --byo
           WHEN brand = 'Savage X' AND byo_bundle_name is not null AND (psource_pieced not ilike 'byo%'
               OR psource_pieced is null) then coalesce(psource_bundle,'byo_') --byo
           WHEN brand = 'Savage X' AND (lower(bundle_name) ilike '%box' AND not contains(lower(bundle_name),'boxer'))
               AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'xtrashop_')
           ELSE 'None' END AS psource
     ,promo_code
     ,promo_name
     ,case when SALES_TYPE = 'Normal - Cash' then 'Pieced Good - Cash'
            when SALES_TYPE = 'Normal - Token' then 'Pieced Good - Token' else SALES_TYPE end as SALES_TYPE
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then UNITS end) as units_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then UNITS end) as units_nonactivating
     ,sum(UNITS) as units_total
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then outfit_component_units end) as outfit_component_units_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then outfit_component_units end) as outfit_component_units_nonactivating
     ,sum(pp.outfit_component_units) as outfit_component_units_total
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then product_gross_revenue_excl_shipping end) as product_gross_revenue_excl_shipping_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then product_gross_revenue_excl_shipping end) as product_gross_revenue_excl_shipping_nonactivating
     ,sum(pp.product_gross_revenue_excl_shipping) as product_gross_revenue_excl_shipping_total
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then product_cost end) as product_cost_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then product_cost end) as product_cost_nonactivating
     ,sum(pp.product_cost) as product_cost_total
     ,$last_refreshed_datetime_hq as last_refreshed_datetime_hq
--'individual country and total platform'
FROM reporting_base_prod.SHARED.session_order_ab_test_detail s
join reporting_base_prod.shared.order_line_psource_payment as pp on pp.SESSION_ID = s.SESSION_ID
join _last_3_months as ll on ll.test_framework_id = s.test_framework_id
    and ll.TEST_KEY = s.TEST_KEY
    and ll.test_label = s.test_label
    and ll.store_brand = s.store_brand
left join _total_descriptions as sc on s.test_key = sc.test_key
    and s.test_framework_id = sc.test_framework_id
    and s.test_label = sc.test_label
    and s.store_brand = sc.store_brand
    AND s.test_group = sc.test_group
left join _builder_test_labels as u on u.builder_id = s.test_key
where
    s.test_group is not null
    AND ORDER_SALES_CHANNEL_L1 = 'Online Order'
    AND ORDER_CLASSIFICATION_L1 = 'Product Order'
    and ORDER_LOCAL_DATETIME >= AB_TEST_START_LOCAL_DATETIME
    and (s.TEST_KEY ilike '%(User%'
        or (test_type in ('membership','session') and s.test_key in (select distinct test_key from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA where DEVICE in ('Desktop & Mobile','Desktop & App','Mobile & App','Desktop, Mobile & App')))
        or (test_type in ('Builder','Session Detail','Customer Detail','Membership Detail') and s.test_key in (select distinct test_key from lake_view.sharepoint.ab_test_metadata_import_oof where test_platforms in ('Desktop & Mobile','Mobile & App','ALL','Desktop and Mobile')))
        or (test_type in ('Builder','Session Detail','Customer Detail','Membership Detail') and s.test_key in (select distinct concat(test_key,' (PSRC)') as test_key from lake_view.sharepoint.ab_test_metadata_import_oof where test_platforms in ('Desktop & Mobile','Mobile & App','ALL','Desktop and Mobile')))
        or (test_type ilike 'Sorting Hat%' and s.test_key in (select distinct meta_original_metadata_test_id from _sorting_hat_metadata_ids where test_platforms in ('Desktop & Mobile','Mobile & App','ALL','Desktop and Mobile'))))
    and convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME)::date>=$process_from_date  --comment
group by
    s.TEST_KEY
    ,s.TEST_FRAMEWORK_ID
    ,coalesce(u.builder_test_label_adj,s.TEST_LABEL)
    ,TEST_FRAMEWORK_DESCRIPTION
    ,TEST_FRAMEWORK_TICKET
    ,s.CAMPAIGN_CODE
    ,TEST_TYPE
    ,test_activated_datetime
    ,case when s.TEST_GROUP = 'Control' THEN 'Control' else 'Variant' end
    ,s.TEST_GROUP
    ,GROUP_NAME
    ,TRAFFIC_SPLIT_TYPE
    ,sc.TEST_GROUP_DESCRIPTION
    ,AB_TEST_START_LOCAL_DATETIME::date
    ,s.STORE_BRAND
    ,STORE_REGION
    ,STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,TEST_START_LEAD_TENURE_GROUP
    ,TEST_START_VIP_MONTH_TENURE_GROUP
    ,case when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
     when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
     when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end
    ,MEMBERSHIP_PRICE
    ,MEMBERSHIP_TYPE
    ,IS_MOBILE_APP_USER
    ,IS_FREE_TRIAL
    ,IS_CROSS_PROMO
    ,IS_MALE_GATEWAY
    ,IS_MALE_SESSION
    ,IS_MALE_CUSTOMER
    ,IS_MALE_SESSION_CUSTOMER
    ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then CHANNEL else 'Null - Non Prospect' end
    ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then SUBCHANNEL else 'Null - Non Prospect' end
    ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then DM_GATEWAY_ID else null end
    ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then GATEWAY_NAME else 'Null - Non Prospect' end
    ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN
    ,FLAG_1_NAME
    ,FLAG_1_DESCRIPTION
    ,FLAG_2_NAME
    ,FLAG_2_DESCRIPTION
    ,FLAG_3_NAME
    ,FLAG_3_DESCRIPTION
    ,CUSTOM_FLAG_NAME
    ,CUSTOM_FLAG_DESCRIPTION
    ,CUSTOM_FLAG_2_NAME
    ,CUSTOM_FLAG_2_DESCRIPTION
    ,product_type
    ,product_lifestyle_type
    ,product_sub_brand
        ,product_gender
     ,product_segment
    ,product_department
    ,PRODUCT_CATEGORY
    ,product_name
    ,byo_bundle_name
    ,bundle_name
    ,CASE WHEN brand <> 'Savage X' THEN coalesce(psource_bundle,psource_pieced,'None')
     WHEN brand = 'Savage X' AND product_type = 'Normal' then coalesce(psource_bundle,psource_pieced,'None') -- SXF normal items
     WHEN brand = 'Savage X' AND (lower(PRODUCT_NAME) not ilike '%box' AND not contains(lower(bundle_name),'boxer'))
         AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'browse_sets_')  --sxf bundles that are null
     WHEN brand = 'Savage X' AND byo_bundle_name is not null AND psource_pieced ilike 'byo%' THEN
         coalesce(psource_pieced,psource_bundle,'byo_') --byo
     WHEN brand = 'Savage X' AND byo_bundle_name is not null AND (psource_pieced not ilike 'byo%'
         OR psource_pieced is null) then coalesce(psource_bundle,'byo_') --byo
     WHEN brand = 'Savage X' AND (lower(bundle_name) ilike '%box' AND not contains(lower(bundle_name),'boxer'))
         AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'xtrashop_')
     ELSE 'None' END
    ,promo_code
    ,promo_name
    ,case when SALES_TYPE = 'Normal - Cash' then 'Pieced Good - Cash'
            when SALES_TYPE = 'Normal - Token' then 'Pieced Good - Token' else SALES_TYPE end

union all

SELECT
    'individual region and individual platform' as version
     ,s.TEST_KEY
     ,s.TEST_FRAMEWORK_ID
     ,coalesce(u.builder_test_label_adj,s.test_label) as test_label
     ,TEST_FRAMEWORK_DESCRIPTION
     ,TEST_FRAMEWORK_TICKET
     ,s.CAMPAIGN_CODE
     ,TEST_TYPE
     ,test_activated_datetime
     ,case when TEST_GROUP = 'Control' THEN 'Control' else 'Variant' end as TEST_GROUP_TYPE
     ,TEST_GROUP
     ,GROUP_NAME
     ,TRAFFIC_SPLIT_TYPE
     ,test_group_description
     ,AB_TEST_START_LOCAL_DATETIME::date as ab_test_start_local_date
     ,s.STORE_BRAND
     ,concat(s.STORE_REGION,' TTL') as STORE_REGION
     ,concat(s.STORE_REGION,' TTL') as STORE_COUNTRY
     ,TEST_START_MEMBERSHIP_STATE
     ,TEST_START_LEAD_TENURE_GROUP
     ,TEST_START_VIP_MONTH_TENURE_GROUP
     ,case when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
           when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
           when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end as lead_registration_type
     ,MEMBERSHIP_PRICE
     ,MEMBERSHIP_TYPE
     ,NULL as IS_MOBILE_APP_USER
     ,NULL as IS_FREE_TRIAL
     ,NULL as IS_CROSS_PROMO
     ,NULL as IS_MALE_GATEWAY
     ,IS_MALE_SESSION
     ,IS_MALE_CUSTOMER
     ,IS_MALE_SESSION_CUSTOMER
     ,s.PLATFORM
     ,s.OS
     ,NULL as BROWSER
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then CHANNEL else 'Null - Non Prospect' end as CHANNEL
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then SUBCHANNEL else 'Null - Non Prospect' end as SUBCHANNEL
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then DM_GATEWAY_ID else null end as DM_GATEWAY_ID
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then GATEWAY_NAME else 'Null - Non Prospect' end as GATEWAY_NAME
     ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN AS IS_SKIP_MONTH_ACTION
     ,FLAG_1_NAME
     ,FLAG_1_DESCRIPTION
     ,FLAG_2_NAME
     ,FLAG_2_DESCRIPTION
     ,FLAG_3_NAME
     ,FLAG_3_DESCRIPTION
     ,CUSTOM_FLAG_NAME
     ,CUSTOM_FLAG_DESCRIPTION
     ,CUSTOM_FLAG_2_NAME
     ,CUSTOM_FLAG_2_DESCRIPTION
     ,product_type
     ,product_lifestyle_type
     ,product_sub_brand
     ,product_gender
     ,product_segment
     ,product_department
     ,PRODUCT_CATEGORY
     ,product_name
     ,byo_bundle_name
     ,bundle_name
     ,CASE WHEN brand <> 'Savage X' THEN coalesce(psource_bundle,psource_pieced,'None')
           WHEN brand = 'Savage X' AND product_type = 'Normal' then coalesce(psource_bundle,psource_pieced,'None') -- SXF normal items
           WHEN brand = 'Savage X' AND (lower(PRODUCT_NAME) not ilike '%box' AND not contains(lower(bundle_name),'boxer'))
               AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'browse_sets_')  --sxf bundles that are null
           WHEN brand = 'Savage X' AND byo_bundle_name is not null AND psource_pieced ilike 'byo%' THEN
               coalesce(psource_pieced,psource_bundle,'byo_') --byo
           WHEN brand = 'Savage X' AND byo_bundle_name is not null AND (psource_pieced not ilike 'byo%'
               OR psource_pieced is null) then coalesce(psource_bundle,'byo_') --byo
           WHEN brand = 'Savage X' AND (lower(bundle_name) ilike '%box' AND not contains(lower(bundle_name),'boxer'))
               AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'xtrashop_')
           ELSE 'None' END AS psource
     ,promo_code
     ,promo_name
     ,case when SALES_TYPE = 'Normal - Cash' then 'Pieced Good - Cash'
            when SALES_TYPE = 'Normal - Token' then 'Pieced Good - Token' else SALES_TYPE end as SALES_TYPE
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then UNITS end) as units_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then UNITS end) as units_nonactivating
     ,sum(UNITS) as units_total
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then outfit_component_units end) as outfit_component_units_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then outfit_component_units end) as outfit_component_units_nonactivating
     ,sum(pp.outfit_component_units) as outfit_component_units_total
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then product_gross_revenue_excl_shipping end) as product_gross_revenue_excl_shipping_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then product_gross_revenue_excl_shipping end) as product_gross_revenue_excl_shipping_nonactivating
     ,sum(pp.product_gross_revenue_excl_shipping) as product_gross_revenue_excl_shipping_total
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then product_cost end) as product_cost_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then product_cost end) as product_cost_nonactivating
     ,sum(pp.product_cost) as product_cost_total
     ,$last_refreshed_datetime_hq
--'individual region and individual platform'
FROM reporting_base_prod.SHARED.session_order_ab_test_detail s
join reporting_base_prod.shared.order_line_psource_payment as pp on pp.SESSION_ID = s.SESSION_ID
join _last_3_months as ll on ll.test_framework_id = s.test_framework_id
    and ll.TEST_KEY = s.TEST_KEY
    and ll.test_label = s.test_label
    and ll.store_brand = s.store_brand
left join (select test_key
             ,count(distinct case when STORE_REGION = 'NA' then STORE_COUNTRY end) as na_store_count
             ,count(distinct case when STORE_REGION = 'EU' then STORE_COUNTRY end) as eu_store_count
        from reporting_base_prod.SHARED.SESSION_ORDER_AB_TEST_DETAIL group by 1) as t1 on t1.TEST_KEY = s.test_key
left join _builder_test_labels as u on u.builder_id = s.test_key
where
    test_group is not null
    AND ORDER_SALES_CHANNEL_L1 = 'Online Order'
    AND ORDER_CLASSIFICATION_L1 = 'Product Order'
    and ORDER_LOCAL_DATETIME >= AB_TEST_START_LOCAL_DATETIME
    and s.STORE_BRAND not in ('FabKids','ShoeDazzle','Yitty')
    and ((t1.na_store_count > 0 or t1.eu_store_count > 1) and (t1.na_store_count <> 1 or t1.eu_store_count <> 0))
    and convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME)::date>=$process_from_date  --comment
group by
    s.TEST_KEY
    ,s.TEST_FRAMEWORK_ID
    ,coalesce(u.builder_test_label_adj,s.test_label)
    ,TEST_FRAMEWORK_DESCRIPTION
    ,TEST_FRAMEWORK_TICKET
    ,s.CAMPAIGN_CODE
    ,TEST_TYPE
    ,test_activated_datetime
    ,case when TEST_GROUP = 'Control' THEN 'Control' else 'Variant' end
    ,TEST_GROUP
    ,GROUP_NAME
    ,TRAFFIC_SPLIT_TYPE
    ,test_group_description
    ,AB_TEST_START_LOCAL_DATETIME::date
    ,s.STORE_BRAND
    ,concat(s.STORE_REGION,' TTL')
    ,concat(s.STORE_REGION,' TTL')
    ,TEST_START_MEMBERSHIP_STATE
    ,TEST_START_LEAD_TENURE_GROUP
    ,TEST_START_VIP_MONTH_TENURE_GROUP
    ,case when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
     when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
     when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end
    ,MEMBERSHIP_PRICE
    ,MEMBERSHIP_TYPE
    ,IS_MOBILE_APP_USER
    --      ,IS_FREE_TRIAL
    --      ,IS_CROSS_PROMO
    --      ,IS_MALE_GATEWAY
    ,IS_MALE_SESSION
    ,IS_MALE_CUSTOMER
    ,IS_MALE_SESSION_CUSTOMER
    ,s.PLATFORM
    ,s.OS
    --      ,s.BROWSER
    ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then CHANNEL else 'Null - Non Prospect' end
    ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then SUBCHANNEL else 'Null - Non Prospect' end
    ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then DM_GATEWAY_ID else null end
    ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then GATEWAY_NAME else 'Null - Non Prospect' end
    ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN
    ,FLAG_1_NAME
    ,FLAG_1_DESCRIPTION
    ,FLAG_2_NAME
    ,FLAG_2_DESCRIPTION
    ,FLAG_3_NAME
    ,FLAG_3_DESCRIPTION
    ,CUSTOM_FLAG_NAME
    ,CUSTOM_FLAG_DESCRIPTION
    ,CUSTOM_FLAG_2_NAME
    ,CUSTOM_FLAG_2_DESCRIPTION
    ,product_type
    ,product_lifestyle_type
    ,product_sub_brand
    ,product_gender
    ,product_segment
    ,product_department
    ,PRODUCT_CATEGORY
    ,product_name
    ,byo_bundle_name
    ,bundle_name
    ,CASE WHEN brand <> 'Savage X' THEN coalesce(psource_bundle,psource_pieced,'None')
     WHEN brand = 'Savage X' AND product_type = 'Normal' then coalesce(psource_bundle,psource_pieced,'None') -- SXF normal items
     WHEN brand = 'Savage X' AND (lower(PRODUCT_NAME) not ilike '%box' AND not contains(lower(bundle_name),'boxer'))
         AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'browse_sets_')  --sxf bundles that are null
     WHEN brand = 'Savage X' AND byo_bundle_name is not null AND psource_pieced ilike 'byo%' THEN
         coalesce(psource_pieced,psource_bundle,'byo_') --byo
     WHEN brand = 'Savage X' AND byo_bundle_name is not null AND (psource_pieced not ilike 'byo%'
         OR psource_pieced is null) then coalesce(psource_bundle,'byo_') --byo
     WHEN brand = 'Savage X' AND (lower(bundle_name) ilike '%box' AND not contains(lower(bundle_name),'boxer'))
         AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'xtrashop_')
     ELSE 'None' END
    ,promo_code
    ,promo_name
    ,case when SALES_TYPE = 'Normal - Cash' then 'Pieced Good - Cash'
            when SALES_TYPE = 'Normal - Token' then 'Pieced Good - Token' else SALES_TYPE end

union all

SELECT
    'individual region and total platform' as version
     ,s.TEST_KEY
     ,s.TEST_FRAMEWORK_ID
     ,coalesce(u.builder_test_label_adj,s.test_label) as test_label
     ,TEST_FRAMEWORK_DESCRIPTION
     ,TEST_FRAMEWORK_TICKET
     ,s.CAMPAIGN_CODE
     ,TEST_TYPE
     ,test_activated_datetime
     ,case when s.TEST_GROUP = 'Control' THEN 'Control' else 'Variant' end as TEST_GROUP_TYPE
     ,s.TEST_GROUP
     ,GROUP_NAME
     ,TRAFFIC_SPLIT_TYPE
     ,sc.TEST_GROUP_DESCRIPTION
     ,AB_TEST_START_LOCAL_DATETIME::date as ab_test_start_local_date
     ,s.STORE_BRAND
     ,concat(s.STORE_REGION,' TTL') as STORE_REGION
     ,concat(s.STORE_REGION,' TTL') as STORE_COUNTRY
     ,TEST_START_MEMBERSHIP_STATE
     ,TEST_START_LEAD_TENURE_GROUP
     ,TEST_START_VIP_MONTH_TENURE_GROUP
     ,case when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
           when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
           when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end as lead_registration_type
     ,MEMBERSHIP_PRICE
     ,MEMBERSHIP_TYPE
     ,NULL as IS_MOBILE_APP_USER
     ,NULL as IS_FREE_TRIAL
     ,NULL as IS_CROSS_PROMO
     ,NULL as IS_MALE_GATEWAY
     ,IS_MALE_SESSION
     ,IS_MALE_CUSTOMER
     ,IS_MALE_SESSION_CUSTOMER
     ,'Total' AS PLATFORM
     ,'Total' as OS
     ,NULL as BROWSER
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then CHANNEL else 'Null - Non Prospect' end as CHANNEL
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then SUBCHANNEL else 'Null - Non Prospect' end as SUBCHANNEL
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then DM_GATEWAY_ID else null end as DM_GATEWAY_ID
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then GATEWAY_NAME else 'Null - Non Prospect' end as GATEWAY_NAME
     ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN
     ,FLAG_1_NAME
     ,FLAG_1_DESCRIPTION
     ,FLAG_2_NAME
     ,FLAG_2_DESCRIPTION
     ,FLAG_3_NAME
     ,FLAG_3_DESCRIPTION
     ,CUSTOM_FLAG_NAME
     ,CUSTOM_FLAG_DESCRIPTION
     ,CUSTOM_FLAG_2_NAME
     ,CUSTOM_FLAG_2_DESCRIPTION
     ,product_type
     ,product_lifestyle_type
     ,product_sub_brand
     ,product_gender
     ,product_segment
     ,product_department
    ,PRODUCT_CATEGORY
     ,product_name
     ,byo_bundle_name
     ,bundle_name
     ,CASE WHEN brand <> 'Savage X' THEN coalesce(psource_bundle,psource_pieced,'None')
           WHEN brand = 'Savage X' AND product_type = 'Normal' then coalesce(psource_bundle,psource_pieced,'None') -- SXF normal items
           WHEN brand = 'Savage X' AND (lower(PRODUCT_NAME) not ilike '%box' AND not contains(lower(bundle_name),'boxer'))
               AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'browse_sets_')  --sxf bundles that are null
           WHEN brand = 'Savage X' AND byo_bundle_name is not null AND psource_pieced ilike 'byo%' THEN
               coalesce(psource_pieced,psource_bundle,'byo_') --byo
           WHEN brand = 'Savage X' AND byo_bundle_name is not null AND (psource_pieced not ilike 'byo%'
               OR psource_pieced is null) then coalesce(psource_bundle,'byo_') --byo
           WHEN brand = 'Savage X' AND (lower(bundle_name) ilike '%box' AND not contains(lower(bundle_name),'boxer'))
               AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'xtrashop_')
           ELSE 'None' END AS psource
     ,promo_code
     ,promo_name
     ,case when SALES_TYPE = 'Normal - Cash' then 'Pieced Good - Cash'
            when SALES_TYPE = 'Normal - Token' then 'Pieced Good - Token' else SALES_TYPE end as SALES_TYPE
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then UNITS end) as units_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then UNITS end) as units_nonactivating
     ,sum(UNITS) as units_total

     ,sum(case when membership_order_type_L1 = 'Activating VIP' then outfit_component_units end) as outfit_component_units_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then outfit_component_units end) as outfit_component_units_nonactivating
     ,sum(pp.outfit_component_units) as outfit_component_units_total

     ,sum(case when membership_order_type_L1 = 'Activating VIP' then product_gross_revenue_excl_shipping end) as product_gross_revenue_excl_shipping_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then product_gross_revenue_excl_shipping end) as product_gross_revenue_excl_shipping_nonactivating
     ,sum(pp.product_gross_revenue_excl_shipping) as product_gross_revenue_excl_shipping_total

     ,sum(case when membership_order_type_L1 = 'Activating VIP' then product_cost end) as product_cost_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then product_cost end) as product_cost_nonactivating
     ,sum(pp.product_cost) as product_cost_total
     ,$last_refreshed_datetime_hq
-- 'individual region and total platform'
FROM reporting_base_prod.SHARED.session_order_ab_test_detail s
join reporting_base_prod.shared.order_line_psource_payment as pp on pp.SESSION_ID = s.SESSION_ID
join _last_3_months as ll on ll.test_framework_id = s.test_framework_id
    and ll.TEST_KEY = s.TEST_KEY
    and ll.test_label = s.test_label
    and ll.store_brand = s.store_brand
left join _total_descriptions as sc on s.test_key = sc.test_key
    and s.test_framework_id = sc.test_framework_id
    and s.test_label = sc.test_label
    and s.store_brand = sc.store_brand
    AND s.test_group = sc.test_group
         join (select test_key
                    ,count(distinct case when STORE_REGION = 'NA' then STORE_COUNTRY end) as na_store_count
                    ,count(distinct case when STORE_REGION = 'EU' then STORE_COUNTRY end) as eu_store_count
               from reporting_base_prod.SHARED.SESSION_ORDER_AB_TEST_DETAIL group by 1) as t1 on t1.TEST_KEY = s.test_key
left join _builder_test_labels as u on u.builder_id = s.test_key
where
     s.test_group is not null
    AND ORDER_SALES_CHANNEL_L1 = 'Online Order'
    AND ORDER_CLASSIFICATION_L1 = 'Product Order'
    and ORDER_LOCAL_DATETIME >= AB_TEST_START_LOCAL_DATETIME
    and s.STORE_BRAND not in ('FabKids','ShoeDazzle','Yitty')
    and (s.TEST_KEY ilike '%(User%'
        or (test_type in ('membership','session') and s.test_key in (select distinct test_key from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA where DEVICE in ('Desktop & Mobile','Desktop & App','Mobile & App','Desktop, Mobile & App')))
        or (test_type in ('Builder','Session Detail','Customer Detail','Membership Detail') and s.test_key in (select distinct test_key from lake_view.sharepoint.ab_test_metadata_import_oof where test_platforms in ('Desktop & Mobile','Mobile & App','ALL','Desktop and Mobile')))
        or (test_type in ('Builder','Session Detail','Customer Detail','Membership Detail') and s.test_key in (select distinct concat(test_key,' (PSRC)') as test_key from lake_view.sharepoint.ab_test_metadata_import_oof where test_platforms in ('Desktop & Mobile','Mobile & App','ALL','Desktop and Mobile')))
        or (test_type ilike 'Sorting Hat%' and s.test_key in (select distinct meta_original_metadata_test_id from _sorting_hat_metadata_ids where test_platforms in ('Desktop & Mobile','Mobile & App','ALL','Desktop and Mobile'))))
    and ((t1.na_store_count > 0 or t1.eu_store_count > 1) and (t1.na_store_count <> 1 or t1.eu_store_count <> 0))
    and convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME)::date>=$process_from_date  --comment
group by
    s.TEST_KEY
       ,s.TEST_FRAMEWORK_ID
       ,coalesce(u.builder_test_label_adj,s.test_label)
       ,TEST_FRAMEWORK_DESCRIPTION
       ,TEST_FRAMEWORK_TICKET
       ,s.CAMPAIGN_CODE
       ,TEST_TYPE
       ,test_activated_datetime
       ,case when s.TEST_GROUP = 'Control' THEN 'Control' else 'Variant' end
       ,s.TEST_GROUP
       ,GROUP_NAME
       ,TRAFFIC_SPLIT_TYPE
       ,sc.TEST_GROUP_DESCRIPTION
       ,AB_TEST_START_LOCAL_DATETIME::date
       ,s.STORE_BRAND
       ,concat(s.STORE_REGION,' TTL')
       ,concat(s.STORE_REGION,' TTL')
       ,TEST_START_MEMBERSHIP_STATE
       ,TEST_START_LEAD_TENURE_GROUP
       ,TEST_START_VIP_MONTH_TENURE_GROUP
       ,case when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
             when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
             when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end
       ,MEMBERSHIP_PRICE
       ,MEMBERSHIP_TYPE
--      ,IS_MOBILE_APP_USER
--      ,IS_FREE_TRIAL
--      ,IS_CROSS_PROMO
--      ,IS_MALE_GATEWAY
       ,IS_MALE_SESSION
       ,IS_MALE_CUSTOMER
       ,IS_MALE_SESSION_CUSTOMER
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then CHANNEL else 'Null - Non Prospect' end
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then SUBCHANNEL else 'Null - Non Prospect' end
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then DM_GATEWAY_ID else null end
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then GATEWAY_NAME else 'Null - Non Prospect' end
       ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN
       ,FLAG_1_NAME
       ,FLAG_1_DESCRIPTION
       ,FLAG_2_NAME
       ,FLAG_2_DESCRIPTION
       ,FLAG_3_NAME
       ,FLAG_3_DESCRIPTION
       ,CUSTOM_FLAG_NAME
       ,CUSTOM_FLAG_DESCRIPTION
       ,CUSTOM_FLAG_2_NAME
       ,CUSTOM_FLAG_2_DESCRIPTION
       ,product_type
       ,product_lifestyle_type
       ,product_sub_brand
           ,product_gender
     ,product_segment
       ,product_department
    ,PRODUCT_CATEGORY
       ,product_name
       ,byo_bundle_name
       ,bundle_name
       ,CASE WHEN brand <> 'Savage X' THEN coalesce(psource_bundle,psource_pieced,'None')
             WHEN brand = 'Savage X' AND product_type = 'Normal' then coalesce(psource_bundle,psource_pieced,'None') -- SXF normal items
             WHEN brand = 'Savage X' AND (lower(PRODUCT_NAME) not ilike '%box' AND not contains(lower(bundle_name),'boxer'))
                 AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'browse_sets_')  --sxf bundles that are null
             WHEN brand = 'Savage X' AND byo_bundle_name is not null AND psource_pieced ilike 'byo%' THEN
                 coalesce(psource_pieced,psource_bundle,'byo_') --byo
             WHEN brand = 'Savage X' AND byo_bundle_name is not null AND (psource_pieced not ilike 'byo%'
                 OR psource_pieced is null) then coalesce(psource_bundle,'byo_') --byo
             WHEN brand = 'Savage X' AND (lower(bundle_name) ilike '%box' AND not contains(lower(bundle_name),'boxer'))
                 AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'xtrashop_')
             ELSE 'None' END
       ,promo_code
       ,promo_name
    ,case when SALES_TYPE = 'Normal - Cash' then 'Pieced Good - Cash'
            when SALES_TYPE = 'Normal - Token' then 'Pieced Good - Token' else SALES_TYPE end

union all

SELECT
    'total region and individual platform' as version
     ,s.TEST_KEY
     ,s.TEST_FRAMEWORK_ID
     ,coalesce(u.builder_test_label_adj,s.test_label) as test_label
     ,TEST_FRAMEWORK_DESCRIPTION
     ,TEST_FRAMEWORK_TICKET
     ,s.CAMPAIGN_CODE
     ,TEST_TYPE
     ,test_activated_datetime
     ,case when TEST_GROUP = 'Control' THEN 'Control' else 'Variant' end as TEST_GROUP_TYPE
     ,TEST_GROUP
     ,GROUP_NAME
     ,TRAFFIC_SPLIT_TYPE
     ,test_group_description
     ,AB_TEST_START_LOCAL_DATETIME::date as ab_test_start_local_date
     ,s.STORE_BRAND
     ,'NA+EU TTL' as STORE_REGION
     ,'NA+EU TTL' as STORE_COUNTRY
     ,TEST_START_MEMBERSHIP_STATE
     ,TEST_START_LEAD_TENURE_GROUP
     ,TEST_START_VIP_MONTH_TENURE_GROUP
     ,case when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
           when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
           when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end as lead_registration_type
     ,MEMBERSHIP_PRICE
     ,MEMBERSHIP_TYPE
     ,NULL as IS_MOBILE_APP_USER
     ,NULL as IS_FREE_TRIAL
     ,NULL as IS_CROSS_PROMO
     ,NULL as IS_MALE_GATEWAY
     ,IS_MALE_SESSION
     ,IS_MALE_CUSTOMER
     ,IS_MALE_SESSION_CUSTOMER
     ,s.PLATFORM
     ,s.OS
     ,NULL as BROWSER
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then CHANNEL else 'Null - Non Prospect' end as CHANNEL
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then SUBCHANNEL else 'Null - Non Prospect' end as SUBCHANNEL
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then DM_GATEWAY_ID else null end as DM_GATEWAY_ID
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then GATEWAY_NAME else 'Null - Non Prospect' end as GATEWAY_NAME
     ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN AS IS_SKIP_MONTH_ACTION
     ,FLAG_1_NAME
     ,FLAG_1_DESCRIPTION
     ,FLAG_2_NAME
     ,FLAG_2_DESCRIPTION
     ,FLAG_3_NAME
     ,FLAG_3_DESCRIPTION
     ,CUSTOM_FLAG_NAME
     ,CUSTOM_FLAG_DESCRIPTION
     ,CUSTOM_FLAG_2_NAME
     ,CUSTOM_FLAG_2_DESCRIPTION
     ,product_type
      ,product_lifestyle_type
     ,product_sub_brand
     ,product_gender
     ,product_segment
     ,product_department
    ,PRODUCT_CATEGORY
     ,product_name
     ,byo_bundle_name
     ,bundle_name
     ,CASE WHEN brand <> 'Savage X' THEN coalesce(psource_bundle,psource_pieced,'None')
           WHEN brand = 'Savage X' AND product_type = 'Normal' then coalesce(psource_bundle,psource_pieced,'None') -- SXF normal items
           WHEN brand = 'Savage X' AND (lower(PRODUCT_NAME) not ilike '%box' AND not contains(lower(bundle_name),'boxer'))
               AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'browse_sets_')  --sxf bundles that are null
           WHEN brand = 'Savage X' AND byo_bundle_name is not null AND psource_pieced ilike 'byo%' THEN
               coalesce(psource_pieced,psource_bundle,'byo_') --byo
           WHEN brand = 'Savage X' AND byo_bundle_name is not null AND (psource_pieced not ilike 'byo%'
               OR psource_pieced is null) then coalesce(psource_bundle,'byo_') --byo
           WHEN brand = 'Savage X' AND (lower(bundle_name) ilike '%box' AND not contains(lower(bundle_name),'boxer'))
               AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'xtrashop_')
           ELSE 'None' END AS psource
     ,promo_code
     ,promo_name
     ,case when SALES_TYPE = 'Normal - Cash' then 'Pieced Good - Cash'
            when SALES_TYPE = 'Normal - Token' then 'Pieced Good - Token' else SALES_TYPE end as SALES_TYPE
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then UNITS end) as units_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then UNITS end) as units_nonactivating
     ,sum(UNITS) as units_total
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then outfit_component_units end) as outfit_component_units_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then outfit_component_units end) as outfit_component_units_nonactivating
     ,sum(pp.outfit_component_units) as outfit_component_units_total
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then product_gross_revenue_excl_shipping end) as product_gross_revenue_excl_shipping_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then product_gross_revenue_excl_shipping end) as product_gross_revenue_excl_shipping_nonactivating
     ,sum(pp.product_gross_revenue_excl_shipping) as product_gross_revenue_excl_shipping_total
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then product_cost end) as product_cost_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then product_cost end) as product_cost_nonactivating
     ,sum(pp.product_cost) as product_cost_total
     ,$last_refreshed_datetime_hq
--'total region and individual platform'
FROM reporting_base_prod.SHARED.session_order_ab_test_detail s
join reporting_base_prod.shared.order_line_psource_payment as pp on pp.SESSION_ID = s.SESSION_ID
join _last_3_months as ll on ll.test_framework_id = s.test_framework_id
    and ll.TEST_KEY = s.TEST_KEY
    and ll.test_label = s.test_label
    and ll.store_brand = s.store_brand
left join (select test_key,count(distinct STORE_REGION) as region_count from reporting_base_prod.SHARED.SESSION_ORDER_AB_TEST_DETAIL group by 1) as t1 on t1.TEST_KEY = s.test_key
left join _builder_test_labels as u on u.builder_id = s.test_key
where
    test_group is not null
    AND ORDER_SALES_CHANNEL_L1 = 'Online Order'
    AND ORDER_CLASSIFICATION_L1 = 'Product Order'
    and ORDER_LOCAL_DATETIME >= AB_TEST_START_LOCAL_DATETIME
    and s.STORE_BRAND not in ('FabKids','ShoeDazzle','Yitty')
    and t1.region_count > 1
    and convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME)::date>=$process_from_date  --comment
group by
    s.TEST_KEY
       ,s.TEST_FRAMEWORK_ID
       ,coalesce(u.builder_test_label_adj,s.test_label)
       ,TEST_FRAMEWORK_DESCRIPTION
       ,TEST_FRAMEWORK_TICKET
       ,s.CAMPAIGN_CODE
       ,TEST_TYPE
       ,test_activated_datetime
       ,case when TEST_GROUP = 'Control' THEN 'Control' else 'Variant' end
       ,TEST_GROUP
       ,GROUP_NAME
       ,TRAFFIC_SPLIT_TYPE
       ,test_group_description
       ,AB_TEST_START_LOCAL_DATETIME::date
       ,s.STORE_BRAND
       ,TEST_START_MEMBERSHIP_STATE
       ,TEST_START_LEAD_TENURE_GROUP
       ,TEST_START_VIP_MONTH_TENURE_GROUP
       ,case when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
             when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
             when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end
       ,MEMBERSHIP_PRICE
       ,MEMBERSHIP_TYPE
--      ,IS_MOBILE_APP_USER
--      ,IS_FREE_TRIAL
--      ,IS_CROSS_PROMO
--      ,IS_MALE_GATEWAY
       ,IS_MALE_SESSION
       ,IS_MALE_CUSTOMER
       ,IS_MALE_SESSION_CUSTOMER
       ,s.PLATFORM
       ,s.OS
--      ,s.BROWSER
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then CHANNEL else 'Null - Non Prospect' end
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then SUBCHANNEL else 'Null - Non Prospect' end
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then DM_GATEWAY_ID else null end
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then GATEWAY_NAME else 'Null - Non Prospect' end
       ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN
       ,FLAG_1_NAME
       ,FLAG_1_DESCRIPTION
       ,FLAG_2_NAME
       ,FLAG_2_DESCRIPTION
       ,FLAG_3_NAME
       ,FLAG_3_DESCRIPTION
       ,CUSTOM_FLAG_NAME
       ,CUSTOM_FLAG_DESCRIPTION
       ,CUSTOM_FLAG_2_NAME
       ,CUSTOM_FLAG_2_DESCRIPTION
       ,product_type
       ,product_lifestyle_type
     ,product_sub_brand
           ,product_gender
     ,product_segment
       ,product_department
    ,PRODUCT_CATEGORY
       ,product_name
       ,byo_bundle_name
       ,bundle_name
       ,CASE WHEN brand <> 'Savage X' THEN coalesce(psource_bundle,psource_pieced,'None')
             WHEN brand = 'Savage X' AND product_type = 'Normal' then coalesce(psource_bundle,psource_pieced,'None') -- SXF normal items
             WHEN brand = 'Savage X' AND (lower(PRODUCT_NAME) not ilike '%box' AND not contains(lower(bundle_name),'boxer'))
                 AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'browse_sets_')  --sxf bundles that are null
             WHEN brand = 'Savage X' AND byo_bundle_name is not null AND psource_pieced ilike 'byo%' THEN
                 coalesce(psource_pieced,psource_bundle,'byo_') --byo
             WHEN brand = 'Savage X' AND byo_bundle_name is not null AND (psource_pieced not ilike 'byo%'
                 OR psource_pieced is null) then coalesce(psource_bundle,'byo_') --byo
             WHEN brand = 'Savage X' AND (lower(bundle_name) ilike '%box' AND not contains(lower(bundle_name),'boxer'))
                 AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'xtrashop_')
             ELSE 'None' END
        ,promo_code
        ,promo_name
        ,case when SALES_TYPE = 'Normal - Cash' then 'Pieced Good - Cash'
            when SALES_TYPE = 'Normal - Token' then 'Pieced Good - Token' else SALES_TYPE end

union all

SELECT
    'total region and total platform' as version
     ,s.TEST_KEY
     ,s.TEST_FRAMEWORK_ID
     ,coalesce(u.builder_test_label_adj,s.test_label) as test_label
     ,TEST_FRAMEWORK_DESCRIPTION
     ,TEST_FRAMEWORK_TICKET
     ,s.CAMPAIGN_CODE
     ,TEST_TYPE
     ,test_activated_datetime
     ,case when s.TEST_GROUP = 'Control' THEN 'Control' else 'Variant' end as TEST_GROUP_TYPE
     ,s.TEST_GROUP
     ,GROUP_NAME
     ,TRAFFIC_SPLIT_TYPE
     ,sc.TEST_GROUP_DESCRIPTION
     ,AB_TEST_START_LOCAL_DATETIME::date as ab_test_start_local_date
     ,s.STORE_BRAND
     ,'NA+EU TTL' as STORE_REGION
     ,'NA+EU TTL' as STORE_COUNTRY
     ,TEST_START_MEMBERSHIP_STATE
     ,TEST_START_LEAD_TENURE_GROUP
     ,TEST_START_VIP_MONTH_TENURE_GROUP
     ,case when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
           when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
           when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end as lead_registration_type
     ,MEMBERSHIP_PRICE
     ,MEMBERSHIP_TYPE
     ,NULL as IS_MOBILE_APP_USER
     ,NULL as IS_FREE_TRIAL
     ,NULL as IS_CROSS_PROMO
     ,NULL as IS_MALE_GATEWAY
     ,IS_MALE_SESSION
     ,IS_MALE_CUSTOMER
     ,IS_MALE_SESSION_CUSTOMER
     ,'Total' as PLATFORM
     ,'Total' as OS
     ,NULL as BROWSER
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then CHANNEL else 'Null - Non Prospect' end as CHANNEL
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then SUBCHANNEL else 'Null - Non Prospect' end as SUBCHANNEL
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then DM_GATEWAY_ID else null end as DM_GATEWAY_ID
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then GATEWAY_NAME else 'Null - Non Prospect' end as GATEWAY_NAME
     ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN AS IS_SKIP_MONTH_ACTION
     ,FLAG_1_NAME
     ,FLAG_1_DESCRIPTION
     ,FLAG_2_NAME
     ,FLAG_2_DESCRIPTION
     ,FLAG_3_NAME
     ,FLAG_3_DESCRIPTION
     ,CUSTOM_FLAG_NAME
     ,CUSTOM_FLAG_DESCRIPTION
     ,CUSTOM_FLAG_2_NAME
     ,CUSTOM_FLAG_2_DESCRIPTION
     ,product_type
      ,product_lifestyle_type
     ,product_sub_brand
     ,product_gender
     ,product_segment
     ,product_department
     ,PRODUCT_CATEGORY
     ,product_name
     ,byo_bundle_name
     ,bundle_name
     ,CASE WHEN brand <> 'Savage X' THEN coalesce(psource_bundle,psource_pieced,'None')
           WHEN brand = 'Savage X' AND product_type = 'Normal' then coalesce(psource_bundle,psource_pieced,'None') -- SXF normal items
           WHEN brand = 'Savage X' AND (lower(PRODUCT_NAME) not ilike '%box' AND not contains(lower(bundle_name),'boxer'))
               AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'browse_sets_')  --sxf bundles that are null
           WHEN brand = 'Savage X' AND byo_bundle_name is not null AND psource_pieced ilike 'byo%' THEN
               coalesce(psource_pieced,psource_bundle,'byo_') --byo
           WHEN brand = 'Savage X' AND byo_bundle_name is not null AND (psource_pieced not ilike 'byo%'
               OR psource_pieced is null) then coalesce(psource_bundle,'byo_') --byo
           WHEN brand = 'Savage X' AND (lower(bundle_name) ilike '%box' AND not contains(lower(bundle_name),'boxer'))
               AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'xtrashop_')
           ELSE 'None' END AS psource
     ,promo_code
     ,promo_name
     ,case when SALES_TYPE = 'Normal - Cash' then 'Pieced Good - Cash'
            when SALES_TYPE = 'Normal - Token' then 'Pieced Good - Token' else SALES_TYPE end as SALES_TYPE
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then UNITS end) as units_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then UNITS end) as units_nonactivating
     ,sum(UNITS) as units_total
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then outfit_component_units end) as outfit_component_units_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then outfit_component_units end) as outfit_component_units_nonactivating
     ,sum(pp.outfit_component_units) as outfit_component_units_total
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then product_gross_revenue_excl_shipping end) as product_gross_revenue_excl_shipping_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then product_gross_revenue_excl_shipping end) as product_gross_revenue_excl_shipping_nonactivating
     ,sum(pp.product_gross_revenue_excl_shipping) as product_gross_revenue_excl_shipping_total
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then product_cost end) as product_cost_activating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then product_cost end) as product_cost_nonactivating
     ,sum(pp.product_cost) as product_cost_total
     ,$last_refreshed_datetime_hq
--'total region and total platform'
FROM reporting_base_prod.SHARED.session_order_ab_test_detail s
join reporting_base_prod.shared.order_line_psource_payment as pp on pp.SESSION_ID = s.SESSION_ID
join _last_3_months as ll on ll.test_framework_id = s.test_framework_id
    and ll.TEST_KEY = s.TEST_KEY
    and ll.test_label = s.test_label
    and ll.store_brand = s.store_brand
left join (select test_key,count(distinct STORE_REGION) as region_count from reporting_base_prod.SHARED.SESSION_ORDER_AB_TEST_DETAIL group by 1) as t1 on t1.TEST_KEY = s.test_key
left join _total_descriptions as sc on s.test_key = sc.test_key
    and s.test_framework_id = sc.test_framework_id
    and s.test_label = sc.test_label
    and s.store_brand = sc.store_brand
    AND s.test_group = sc.test_group
left join _builder_test_labels as u on u.builder_id = s.test_key
where
    s.test_group is not null
    AND ORDER_SALES_CHANNEL_L1 = 'Online Order'
    AND ORDER_CLASSIFICATION_L1 = 'Product Order'
    and ORDER_LOCAL_DATETIME >= AB_TEST_START_LOCAL_DATETIME
    and s.STORE_BRAND not in ('FabKids','ShoeDazzle','Yitty')
    and (s.TEST_KEY ilike '%(User%'
        or (test_type in ('membership','session') and s.test_key in (select distinct test_key from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA where DEVICE in ('Desktop & Mobile','Desktop & App','Mobile & App','Desktop, Mobile & App')))
        or (test_type in ('Builder','Session Detail','Customer Detail','Membership Detail') and s.test_key in (select distinct test_key from lake_view.sharepoint.ab_test_metadata_import_oof where test_platforms in ('Desktop & Mobile','Mobile & App','ALL','Desktop and Mobile')))
        or (test_type in ('Builder','Session Detail','Customer Detail','Membership Detail') and s.test_key in (select distinct concat(test_key,' (PSRC)') as test_key from lake_view.sharepoint.ab_test_metadata_import_oof where test_platforms in ('Desktop & Mobile','Mobile & App','ALL','Desktop and Mobile')))
        or (test_type ilike 'Sorting Hat%' and s.test_key in (select distinct meta_original_metadata_test_id from _sorting_hat_metadata_ids where test_platforms in ('Desktop & Mobile','Mobile & App','ALL','Desktop and Mobile'))))
    and t1.region_count > 1
    and convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME)::date>=$process_from_date --comment
group by
    s.TEST_KEY
       ,s.TEST_FRAMEWORK_ID
       ,coalesce(u.builder_test_label_adj,s.TEST_LABEL)
       ,TEST_FRAMEWORK_DESCRIPTION
       ,TEST_FRAMEWORK_TICKET
       ,s.CAMPAIGN_CODE
       ,TEST_TYPE
       ,test_activated_datetime
       ,case when s.TEST_GROUP = 'Control' THEN 'Control' else 'Variant' end
       ,s.TEST_GROUP
       ,GROUP_NAME
       ,TRAFFIC_SPLIT_TYPE
       ,sc.TEST_GROUP_DESCRIPTION
       --datetimes
       ,AB_TEST_START_LOCAL_DATETIME::date
       ,s.STORE_BRAND
       ,TEST_START_MEMBERSHIP_STATE
       ,TEST_START_LEAD_TENURE_GROUP
       ,TEST_START_VIP_MONTH_TENURE_GROUP
       ,case when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
             when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
             when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end
       ,MEMBERSHIP_PRICE
       ,MEMBERSHIP_TYPE
--      ,IS_MOBILE_APP_USER
--      ,IS_FREE_TRIAL
--      ,IS_CROSS_PROMO
--      ,IS_MALE_GATEWAY
       ,IS_MALE_SESSION
       ,IS_MALE_CUSTOMER
       ,IS_MALE_SESSION_CUSTOMER
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then CHANNEL else 'Null - Non Prospect' end
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then SUBCHANNEL else 'Null - Non Prospect' end
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then DM_GATEWAY_ID else null end
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then GATEWAY_NAME else 'Null - Non Prospect' end
       ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN
       ,FLAG_1_NAME
       ,FLAG_1_DESCRIPTION
       ,FLAG_2_NAME
       ,FLAG_2_DESCRIPTION
       ,FLAG_3_NAME
       ,FLAG_3_DESCRIPTION
       ,CUSTOM_FLAG_NAME
       ,CUSTOM_FLAG_DESCRIPTION
       ,CUSTOM_FLAG_2_NAME
       ,CUSTOM_FLAG_2_DESCRIPTION
       ,product_type
        ,product_lifestyle_type
        ,product_sub_brand
        ,product_gender
        ,product_segment
       ,product_department
        ,PRODUCT_CATEGORY
       ,product_name
       ,byo_bundle_name
       ,bundle_name
       ,CASE WHEN brand <> 'Savage X' THEN coalesce(psource_bundle,psource_pieced,'None')
             WHEN brand = 'Savage X' AND product_type = 'Normal' then coalesce(psource_bundle,psource_pieced,'None') -- SXF normal items
             WHEN brand = 'Savage X' AND (lower(PRODUCT_NAME) not ilike '%box' AND not contains(lower(bundle_name),'boxer'))
                 AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'browse_sets_')  --sxf bundles that are null
             WHEN brand = 'Savage X' AND byo_bundle_name is not null AND psource_pieced ilike 'byo%' THEN
                 coalesce(psource_pieced,psource_bundle,'byo_') --byo
             WHEN brand = 'Savage X' AND byo_bundle_name is not null AND (psource_pieced not ilike 'byo%'
                 OR psource_pieced is null) then coalesce(psource_bundle,'byo_') --byo
             WHEN brand = 'Savage X' AND (lower(bundle_name) ilike '%box' AND not contains(lower(bundle_name),'boxer'))
                 AND byo_bundle_product_id is null THEN coalesce(psource_bundle,'xtrashop_')
             ELSE 'None' END
       ,promo_code
       ,promo_name
       ,case when SALES_TYPE = 'Normal - Cash' then 'Pieced Good - Cash'
            when SALES_TYPE = 'Normal - Token' then 'Pieced Good - Token' else SALES_TYPE end;

BEGIN;  --comment

DELETE FROM reporting_prod.SHARED.PSOURCE_PROMO_REVENUE_AB_TEST_FINAL --comment
WHERE AB_TEST_START_LOCAL_DATE>=$process_from_date;

DELETE FROM REPORTING_PROD.shared.PSOURCE_PROMO_REVENUE_AB_TEST_FINAL
where
    TEST_KEY not in (select test_key from _last_3_months)
        and test_label not in (select test_label from _last_3_months);

INSERT INTO reporting_prod.SHARED.PSOURCE_PROMO_REVENUE_AB_TEST_FINAL
(VERSION,
 TEST_KEY,
 TEST_FRAMEWORK_ID,
 TEST_LABEL,
 TEST_FRAMEWORK_DESCRIPTION,
 TEST_FRAMEWORK_TICKET,
 CAMPAIGN_CODE,
 TEST_TYPE,
 TEST_ACTIVATED_DATETIME,
 TEST_GROUP_TYPE,
 TEST_GROUP,
 GROUP_NAME,
 TRAFFIC_SPLIT_TYPE,
 TEST_GROUP_DESCRIPTION,
 AB_TEST_START_LOCAL_DATE,
 STORE_BRAND,
 STORE_REGION,
 STORE_COUNTRY,
 TEST_START_MEMBERSHIP_STATE,
 TEST_START_LEAD_TENURE_GROUP,
 TEST_START_VIP_MONTH_TENURE_GROUP,
 LEAD_REGISTRATION_TYPE,
 MEMBERSHIP_PRICE,
 MEMBERSHIP_TYPE,
 IS_MOBILE_APP_USER,
 IS_FREE_TRIAL,
 IS_CROSS_PROMO,
 IS_MALE_GATEWAY,
 IS_MALE_SESSION,
 IS_MALE_CUSTOMER,
 IS_MALE_SESSION_CUSTOMER,
 PLATFORM,
 OS,
 BROWSER,
 CHANNEL,
 SUBCHANNEL,
 DM_GATEWAY_ID,
 GATEWAY_NAME,
 IS_SKIP_MONTH_ACTION,
 FLAG_1_NAME,
 FLAG_1_DESCRIPTION,
 FLAG_2_NAME,
 FLAG_2_DESCRIPTION,
 FLAG_3_NAME,
 FLAG_3_DESCRIPTION,
 CUSTOM_FLAG_NAME,
 CUSTOM_FLAG_DESCRIPTION,
CUSTOM_FLAG_2_NAME,
 CUSTOM_FLAG_2_DESCRIPTION,
 PRODUCT_TYPE,
product_lifestyle_type,
product_sub_brand,
 product_gender,
    product_segment,
 PRODUCT_DEPARTMENT,
 PRODUCT_CATEGORY,
 PRODUCT_NAME,
 BYO_BUNDLE_NAME,
 BUNDLE_NAME,
 PSOURCE,
 PROMO_CODE,
 PROMO_NAME,
 SALES_TYPE,
 UNITS_ACTIVATING,
 UNITS_NONACTIVATING,
 UNITS_TOTAL,
 OUTFIT_COMPONENT_UNITS_ACTIVATING,
 OUTFIT_COMPONENT_UNITS_NONACTIVATING,
 OUTFIT_COMPONENT_UNITS_TOTAL,
 PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_ACTIVATING,
 PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_NONACTIVATING,
 PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_TOTAL,
 PRODUCT_COST_ACTIVATING,
 PRODUCT_COST_NONACTIVATING,
 PRODUCT_COST_TOTAL,
 LAST_REFRESHED_DATETIME_HQ
)
select VERSION,
       TEST_KEY,
       TEST_FRAMEWORK_ID,
       TEST_LABEL,
       TEST_FRAMEWORK_DESCRIPTION,
       TEST_FRAMEWORK_TICKET,
       CAMPAIGN_CODE,
       TEST_TYPE,
       TEST_ACTIVATED_DATETIME,
       TEST_GROUP_TYPE,
       TEST_GROUP,
       GROUP_NAME,
       TRAFFIC_SPLIT_TYPE,
       TEST_GROUP_DESCRIPTION,
       AB_TEST_START_LOCAL_DATE,
       STORE_BRAND,
       STORE_REGION,
       STORE_COUNTRY,
       TEST_START_MEMBERSHIP_STATE,
       TEST_START_LEAD_TENURE_GROUP,
       TEST_START_VIP_MONTH_TENURE_GROUP,
       LEAD_REGISTRATION_TYPE,
       MEMBERSHIP_PRICE,
       MEMBERSHIP_TYPE,
       IS_MOBILE_APP_USER,
       IS_FREE_TRIAL,
       IS_CROSS_PROMO,
       IS_MALE_GATEWAY,
       IS_MALE_SESSION,
       IS_MALE_CUSTOMER,
       IS_MALE_SESSION_CUSTOMER,
       PLATFORM,
       OS,
       BROWSER,
       CHANNEL,
       SUBCHANNEL,
       DM_GATEWAY_ID,
       GATEWAY_NAME,
       IS_SKIP_MONTH_ACTION,
       FLAG_1_NAME,
       FLAG_1_DESCRIPTION,
       FLAG_2_NAME,
       FLAG_2_DESCRIPTION,
       FLAG_3_NAME,
       FLAG_3_DESCRIPTION,
       CUSTOM_FLAG_NAME,
       CUSTOM_FLAG_DESCRIPTION,
       CUSTOM_FLAG_2_NAME,
       CUSTOM_FLAG_2_DESCRIPTION,
       PRODUCT_TYPE,
       product_lifestyle_type,
       product_sub_brand,
        product_gender,
        product_segment,
       PRODUCT_DEPARTMENT,
       PRODUCT_CATEGORY,
       PRODUCT_NAME,
       BYO_BUNDLE_NAME,
       BUNDLE_NAME,
       PSOURCE,
       PROMO_CODE,
       PROMO_NAME,
       SALES_TYPE,
       UNITS_ACTIVATING,
       UNITS_NONACTIVATING,
       UNITS_TOTAL,
       OUTFIT_COMPONENT_UNITS_ACTIVATING,
       OUTFIT_COMPONENT_UNITS_NONACTIVATING,
       OUTFIT_COMPONENT_UNITS_TOTAL,
       PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_ACTIVATING,
       PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_NONACTIVATING,
       PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_TOTAL,
       PRODUCT_COST_ACTIVATING,
       PRODUCT_COST_NONACTIVATING,
       PRODUCT_COST_TOTAL,
       LAST_REFRESHED_DATETIME_HQ
from _PSOURCE_PROMO_REVENUE_AB_TEST_FINAL;

COMMIT;




-- select version,TEST_KEY,STORE_REGION,STORE_COUNTRY,platform,sum(units_total)
-- from reporting_prod.SHARED.PSOURCE_PROMO_REVENUE_AB_TEST_FINAL
-- group by 1,2,3,4,5 order by 2,1,platform

-- select *
-- from reporting_prod.SHARED.PSOURCE_PROMO_REVENUE_AB_TEST_FINAL limit 25
