
set last_refreshed_datetime_hq = (select max(CONVERT_TIMEZONE('America/Los_Angeles',session_local_datetime)::datetime) from reporting_base_prod.shared.session_order_ab_test_detail);

set process_from_date = (select MAX(convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATE))::date-7  --comment
                         from reporting_prod.SHARED.PAYMENT_SHIPPING_TYPE_AB_TEST_FINAL);

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

CREATE OR REPLACE TEMPORARY TABLE _agg_promo_codes as
SELECT DISTINCT
    s.SESSION_ID
    ,ORDER_ID
    ,s.TEST_FRAMEWORK_ID
    ,s.TEST_KEY
    ,s.test_label
    ,s.STORE_BRAND
    ,s.AB_TEST_SEGMENT
    ,listagg(distinct promo_code,'|') WITHIN GROUP (ORDER BY promo_code) as promo_codes
    ,listagg(distinct promo_name,'|') WITHIN GROUP (ORDER BY promo_name) AS promo_names
FROM reporting_base_prod.SHARED.session_order_ab_test_detail s
join reporting_base_prod.shared.order_line_psource_payment as pp on pp.SESSION_ID = s.SESSION_ID
join _last_3_months as ll on ll.test_framework_id = s.test_framework_id
    and ll.TEST_KEY = s.TEST_KEY
    and ll.test_label = s.test_label
    and ll.store_brand = s.store_brand
where
    s.test_group is not null
    AND ORDER_SALES_CHANNEL_L1 = 'Online Order'
    AND ORDER_CLASSIFICATION_L1 = 'Product Order'
    and ORDER_LOCAL_DATETIME >= AB_TEST_START_LOCAL_DATETIME
group by 1,2,3,4,5,6,7;

CREATE OR REPLACE TEMPORARY TABLE _agg_product_segment as
SELECT DISTINCT
    s.SESSION_ID
    ,ORDER_ID
    ,s.TEST_FRAMEWORK_ID
    ,s.TEST_KEY
    ,s.test_label
    ,s.STORE_BRAND
    ,s.AB_TEST_SEGMENT
    ,listagg(distinct product_segment,'|') WITHIN GROUP (ORDER BY product_segment) as product_segment
FROM reporting_base_prod.SHARED.session_order_ab_test_detail s
join reporting_base_prod.shared.order_line_psource_payment as pp on pp.SESSION_ID = s.SESSION_ID
join _last_3_months as ll on ll.test_framework_id = s.test_framework_id
    and ll.TEST_KEY = s.TEST_KEY
    and ll.test_label = s.test_label
    and ll.store_brand = s.store_brand
where
    s.test_group is not null
    AND ORDER_SALES_CHANNEL_L1 = 'Online Order'
    AND ORDER_CLASSIFICATION_L1 = 'Product Order'
    and ORDER_LOCAL_DATETIME >= AB_TEST_START_LOCAL_DATETIME
group by 1,2,3,4,5,6,7;

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

-- CREATE OR REPLACE TRANSIENT TABLE reporting_prod.SHARED.PAYMENT_SHIPPING_TYPE_AB_TEST_FINAL AS --comment
CREATE OR REPLACE TEMP TABLE _PAYMENT_SHIPPING_TYPE_AB_TEST_FINAL AS  --comment
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
    ,TEST_GROUP_DESCRIPTION
    ,AB_TEST_START_LOCAL_DATETIME::date as ab_test_start_local_date
    ,s.STORE_BRAND
    ,STORE_REGION
    ,STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,TEST_START_LEAD_TENURE_GROUP
    ,TEST_START_VIP_MONTH_TENURE_GROUP
    ,IS_QUIZ_START_ACTION
    ,IS_QUIZ_COMPLETE_ACTION
    ,IS_LEAD_REGISTRATION_ACTION
    ,IS_QUIZ_REGISTRATION_ACTION
    ,IS_SPEEDY_REGISTRATION_ACTION
    ,IS_SKIP_QUIZ_REGISTRATION_ACTION
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
    ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN IS_SKIP_MONTH_ACTION
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
    ,IS_PREPAID_CREDITCARD
    ,payment_type_adjusted AS payment_type
    ,digital_wallet_payment_type_adjusted AS digital_wallet_payment_type
    ,payment_method_type_adjusted AS payment_method_type
    ,credit_card_type_adjusted AS credit_card_type
    ,CUSTOMER_SELECTED_SHIPPING_TYPE
    ,CUSTOMER_SELECTED_SHIPPING_SERVICE
    ,pc.promo_codes
    ,promo_names
    ,sg.product_segment
    ,count(distinct case when membership_order_type_L1 = 'Activating VIP' then pp.ORDER_ID end) AS orders_activating
    ,count(distinct case when membership_order_type_L1 <> 'Activating VIP' then pp.ORDER_ID end) AS orders_nonactivating
    ,count(distinct pp.ORDER_ID) AS orders_total
    ,count(distinct case when membership_order_type_L1 = 'Activating VIP' and ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_activating
    ,count(distinct case when membership_order_type_L1 <> 'Activating VIP' and ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_nonactivating
    ,count(distinct case when ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_units end) as units_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_units end) as units_nonactivating
    ,sum(ttl_units) as units_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_gross_revenue end) as ttl_product_gross_revenue_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_gross_revenue end) as ttl_product_gross_revenue_nonactivating
    ,sum(ttl_product_gross_revenue) as product_revenue_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_gross_revenue_excl_shipping end) as product_revenue_excl_shipping_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_gross_revenue_excl_shipping end) as product_revenue_excl_shipping_nonactivating
    ,sum(ttl_product_gross_revenue_excl_shipping) as product_revenue_excl_shipping_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_margin_pre_return end) as product_margin_pre_return_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_margin_pre_return end) as product_margin_pre_return_nonactivating
    ,sum(ttl_product_margin_pre_return) AS product_margin_pre_return_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_margin_pre_return_excl_shipping end) as product_margin_pre_return_excl_shipping_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_margin_pre_return_excl_shipping end) as product_margin_pre_return_excl_shipping_nonactivating
    ,sum(ttl_product_margin_pre_return_excl_shipping) AS product_margin_pre_return_excl_shipping_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_cash_gross_revenue end) as cash_gross_revenue_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_cash_gross_revenue end) as cash_gross_revenue_nonactivating
    ,sum(ttl_cash_gross_revenue) AS cash_gross_revenue_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then TTL_SHIPPING_REVENUE end) as shipping_revenue_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then TTL_SHIPPING_REVENUE end) as shipping_revenue_nonactivating
    ,sum(TTL_SHIPPING_REVENUE) AS shipping_revenue_total
    ,$last_refreshed_datetime_hq as last_refreshed_datetime_hq
--'individual country and individual platform'
-- select s.test_key,s.test_label,s.ab_test_segment,count(*),count(distinct pp.order_id), count(*)-count(distinct pp.order_id) as diff
FROM reporting_base_prod.SHARED.session_order_ab_test_detail s
join reporting_base_prod.shared.order_line_psource_payment as pp on pp.SESSION_ID = s.SESSION_ID
join _last_3_months as ll on ll.test_framework_id = s.test_framework_id
    and ll.TEST_KEY = s.TEST_KEY
    and ll.test_label = s.test_label
    and ll.store_brand = s.store_brand
left join _agg_promo_codes as pc on pc.SESSION_ID = pp.SESSION_ID
    and pc.ORDER_ID = pp.ORDER_ID
    and s.test_key = pc.test_key
    and s.test_framework_id = pc.test_framework_id
    and s.test_label = pc.test_label
    and s.store_brand = pc.store_brand
    AND s.ab_test_segment = pc.ab_test_segment
left join _agg_product_segment as sg on sg.SESSION_ID = s.SESSION_ID
    and sg.ORDER_ID = pp.ORDER_ID
    and s.test_key = sg.test_key
    and s.test_framework_id = sg.test_framework_id
    and s.test_label = sg.test_label
    and s.store_brand = sg.store_brand
    AND s.ab_test_segment = sg.ab_test_segment
left join _builder_test_labels as u on u.builder_id = s.test_key
where
    s.test_group is not null
    and TTL_ORDER_RNK = 1
    AND ORDER_SALES_CHANNEL_L1 = 'Online Order'
    AND ORDER_CLASSIFICATION_L1 = 'Product Order'
    and ORDER_LOCAL_DATETIME >= AB_TEST_START_LOCAL_DATETIME
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
    ,TEST_GROUP_DESCRIPTION
    ,AB_TEST_START_LOCAL_DATETIME::date
    ,s.STORE_BRAND
    ,STORE_REGION
    ,STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,TEST_START_LEAD_TENURE_GROUP
    ,TEST_START_VIP_MONTH_TENURE_GROUP
    ,IS_QUIZ_START_ACTION
    ,IS_QUIZ_COMPLETE_ACTION
    ,IS_LEAD_REGISTRATION_ACTION
    ,IS_QUIZ_REGISTRATION_ACTION
    ,IS_SPEEDY_REGISTRATION_ACTION
    ,IS_SKIP_QUIZ_REGISTRATION_ACTION
    ,case when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
     when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
     when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end
    ,MEMBERSHIP_PRICE
    ,MEMBERSHIP_TYPE
    --      ,'Temp N/A' as IS_MOBILE_APP_USER
    --      ,'Temp N/A' as IS_FREE_TRIAL
    --      ,'Temp N/A' as IS_CROSS_PROMO
    --      ,'Temp N/A' as IS_MALE_GATEWAY
    ,IS_MALE_SESSION
    ,IS_MALE_CUSTOMER
    ,IS_MALE_SESSION_CUSTOMER
    ,s.PLATFORM
    ,s.OS
    --      ,'Temp N/A' as BROWSER
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
    ,IS_PREPAID_CREDITCARD
    ,payment_type_adjusted
    ,digital_wallet_payment_type_adjusted
    ,payment_method_type_adjusted
    ,credit_card_type_adjusted
    ,CUSTOMER_SELECTED_SHIPPING_TYPE
    ,CUSTOMER_SELECTED_SHIPPING_SERVICE
    ,pc.promo_codes
    ,promo_names
    ,sg.product_segment

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
    ,IS_QUIZ_START_ACTION
    ,IS_QUIZ_COMPLETE_ACTION
    ,IS_LEAD_REGISTRATION_ACTION
    ,IS_QUIZ_REGISTRATION_ACTION
    ,IS_SPEEDY_REGISTRATION_ACTION
    ,IS_SKIP_QUIZ_REGISTRATION_ACTION
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
    ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN IS_SKIP_MONTH_ACTION
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
    ,IS_PREPAID_CREDITCARD
    ,payment_type_adjusted AS payment_type
    ,digital_wallet_payment_type_adjusted AS digital_wallet_payment_type
    ,payment_method_type_adjusted AS payment_method_type
    ,credit_card_type_adjusted AS credit_card_type
    ,CUSTOMER_SELECTED_SHIPPING_TYPE
    ,CUSTOMER_SELECTED_SHIPPING_SERVICE
     ,promo_codes
     ,promo_names
     ,sg.product_segment
    ,count(distinct case when membership_order_type_L1 = 'Activating VIP' then pp.ORDER_ID end) AS orders_activating
    ,count(distinct case when membership_order_type_L1 <> 'Activating VIP' then pp.ORDER_ID end) AS orders_nonactivating
    ,count(distinct pp.ORDER_ID) AS orders_total
    ,count(distinct case when membership_order_type_L1 = 'Activating VIP' and ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_activating
    ,count(distinct case when membership_order_type_L1 <> 'Activating VIP' and ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_nonactivating
    ,count(distinct case when ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_units end) as units_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_units end) as units_nonactivating
    ,sum(ttl_units) as units_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_gross_revenue end) as ttl_product_gross_revenue_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_gross_revenue end) as ttl_product_gross_revenue_nonactivating
    ,sum(ttl_product_gross_revenue) as product_revenue_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_gross_revenue_excl_shipping end) as product_revenue_excl_shipping_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_gross_revenue_excl_shipping end) as product_revenue_excl_shipping_nonactivating
    ,sum(ttl_product_gross_revenue_excl_shipping) as product_revenue_excl_shipping_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_margin_pre_return end) as product_margin_pre_return_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_margin_pre_return end) as product_margin_pre_return_nonactivating
    ,sum(ttl_product_margin_pre_return) AS product_margin_pre_return_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_margin_pre_return_excl_shipping end) as product_margin_pre_return_excl_shipping_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_margin_pre_return_excl_shipping end) as product_margin_pre_return_excl_shipping_nonactivating
    ,sum(ttl_product_margin_pre_return_excl_shipping) AS product_margin_pre_return_excl_shipping_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_cash_gross_revenue end) as cash_gross_revenue_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_cash_gross_revenue end) as cash_gross_revenue_nonactivating
    ,sum(ttl_cash_gross_revenue) AS cash_gross_revenue_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then TTL_SHIPPING_REVENUE end) as shipping_revenue_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then TTL_SHIPPING_REVENUE end) as shipping_revenue_nonactivating
    ,sum(TTL_SHIPPING_REVENUE) AS shipping_revenue_total
    ,$last_refreshed_datetime_hq
--'individual country and total platform'
-- select s.test_key,s.test_label,s.ab_test_segment,count(*),count(distinct pp.order_id), count(*)-count(distinct pp.order_id) as diff
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
left join _agg_promo_codes as pc on pc.SESSION_ID = pp.SESSION_ID
    and pc.ORDER_ID = pp.ORDER_ID
    and s.test_key = pc.test_key
    and s.test_framework_id = pc.test_framework_id
    and s.test_label = pc.test_label
    and s.store_brand = pc.store_brand
    AND s.ab_test_segment = pc.ab_test_segment
left join _agg_product_segment as sg on sg.SESSION_ID = s.SESSION_ID
    and sg.ORDER_ID = pp.ORDER_ID
    and s.test_key = sg.test_key
    and s.test_framework_id = sg.test_framework_id
    and s.test_label = sg.test_label
    and s.store_brand = sg.store_brand
    AND s.ab_test_segment = sg.ab_test_segment
left join _builder_test_labels as u on u.builder_id = s.test_key
where
    s.test_group is not null
    and TTL_ORDER_RNK = 1
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
    ,STORE_REGION
    ,STORE_COUNTRY
    ,TEST_START_MEMBERSHIP_STATE
    ,TEST_START_LEAD_TENURE_GROUP
    ,TEST_START_VIP_MONTH_TENURE_GROUP
    ,IS_QUIZ_START_ACTION
    ,IS_QUIZ_COMPLETE_ACTION
    ,IS_LEAD_REGISTRATION_ACTION
    ,IS_QUIZ_REGISTRATION_ACTION
    ,IS_SPEEDY_REGISTRATION_ACTION
    ,IS_SKIP_QUIZ_REGISTRATION_ACTION
    ,case when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
         when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
         when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end
    ,MEMBERSHIP_PRICE
    ,MEMBERSHIP_TYPE
    --     ,'Temp N/A' as IS_MOBILE_APP_USER
    --      ,'Temp N/A' as IS_FREE_TRIAL
    --      ,'Temp N/A' as IS_CROSS_PROMO
    --      ,'Temp N/A' as IS_MALE_GATEWAY
    ,IS_MALE_SESSION
    ,IS_MALE_CUSTOMER
    ,IS_MALE_SESSION_CUSTOMER
    --      ,'Total' as PLATFORM
    --      ,'Total' as OS
    --      ,'Temp N/A' as BROWSER
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
    ,IS_PREPAID_CREDITCARD
    ,payment_type_adjusted
    ,digital_wallet_payment_type_adjusted
    ,payment_method_type_adjusted
    ,credit_card_type_adjusted
    ,CUSTOMER_SELECTED_SHIPPING_TYPE
    ,CUSTOMER_SELECTED_SHIPPING_SERVICE
    ,promo_codes
    ,promo_names
    ,sg.product_segment

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
    ,IS_QUIZ_START_ACTION
    ,IS_QUIZ_COMPLETE_ACTION
    ,IS_LEAD_REGISTRATION_ACTION
    ,IS_QUIZ_REGISTRATION_ACTION
    ,IS_SPEEDY_REGISTRATION_ACTION
    ,IS_SKIP_QUIZ_REGISTRATION_ACTION
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
    ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN IS_SKIP_MONTH_ACTION
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
    ,IS_PREPAID_CREDITCARD
    ,payment_type_adjusted AS payment_type
    ,digital_wallet_payment_type_adjusted AS digital_wallet_payment_type
    ,payment_method_type_adjusted AS payment_method_type
    ,credit_card_type_adjusted AS credit_card_type
    ,CUSTOMER_SELECTED_SHIPPING_TYPE
    ,CUSTOMER_SELECTED_SHIPPING_SERVICE
     ,promo_codes
    ,promo_names
     ,sg.product_segment
    ,count(distinct case when membership_order_type_L1 = 'Activating VIP' then pp.ORDER_ID end) AS orders_activating
    ,count(distinct case when membership_order_type_L1 <> 'Activating VIP' then pp.ORDER_ID end) AS orders_nonactivating
    ,count(distinct pp.ORDER_ID) AS orders_total
    ,count(distinct case when membership_order_type_L1 = 'Activating VIP' and ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_activating
    ,count(distinct case when membership_order_type_L1 <> 'Activating VIP' and ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_nonactivating
    ,count(distinct case when ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_units end) as units_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_units end) as units_nonactivating
    ,sum(ttl_units) as units_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_gross_revenue end) as ttl_product_gross_revenue_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_gross_revenue end) as ttl_product_gross_revenue_nonactivating
    ,sum(ttl_product_gross_revenue) as product_revenue_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_gross_revenue_excl_shipping end) as product_revenue_excl_shipping_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_gross_revenue_excl_shipping end) as product_revenue_excl_shipping_nonactivating
    ,sum(ttl_product_gross_revenue_excl_shipping) as product_revenue_excl_shipping_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_margin_pre_return end) as product_margin_pre_return_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_margin_pre_return end) as product_margin_pre_return_nonactivating
    ,sum(ttl_product_margin_pre_return) AS product_margin_pre_return_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_margin_pre_return_excl_shipping end) as product_margin_pre_return_excl_shipping_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_margin_pre_return_excl_shipping end) as product_margin_pre_return_excl_shipping_nonactivating
    ,sum(ttl_product_margin_pre_return_excl_shipping) AS product_margin_pre_return_excl_shipping_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_cash_gross_revenue end) as cash_gross_revenue_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_cash_gross_revenue end) as cash_gross_revenue_nonactivating
    ,sum(ttl_cash_gross_revenue) AS cash_gross_revenue_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then TTL_SHIPPING_REVENUE end) as shipping_revenue_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then TTL_SHIPPING_REVENUE end) as shipping_revenue_nonactivating
    ,sum(TTL_SHIPPING_REVENUE) AS shipping_revenue_total
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
left join _agg_promo_codes as pc on pc.SESSION_ID = pp.SESSION_ID
    and pc.ORDER_ID = pp.ORDER_ID
    and s.test_key = pc.test_key
    and s.test_framework_id = pc.test_framework_id
    and s.test_label = pc.test_label
    and s.store_brand = pc.store_brand
    AND s.ab_test_segment = pc.ab_test_segment
left join _agg_product_segment as sg on sg.SESSION_ID = s.SESSION_ID
    and sg.ORDER_ID = pp.ORDER_ID
    and s.test_key = sg.test_key
    and s.test_framework_id = sg.test_framework_id
    and s.test_label = sg.test_label
    and s.store_brand = sg.store_brand
    AND s.ab_test_segment = sg.ab_test_segment
left join _builder_test_labels as u on u.builder_id = s.test_key
where
    s.test_group is not null
    and TTL_ORDER_RNK = 1
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
    ,IS_QUIZ_START_ACTION
    ,IS_QUIZ_COMPLETE_ACTION
    ,IS_LEAD_REGISTRATION_ACTION
    ,IS_QUIZ_REGISTRATION_ACTION
    ,IS_SPEEDY_REGISTRATION_ACTION
    ,IS_SKIP_QUIZ_REGISTRATION_ACTION
    ,case when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
         when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
         when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end
    ,MEMBERSHIP_PRICE
    ,MEMBERSHIP_TYPE
    --        ,'Temp N/A' as IS_MOBILE_APP_USER
    --      ,'Temp N/A' as IS_FREE_TRIAL
    --      ,'Temp N/A' as IS_CROSS_PROMO
    --      ,'Temp N/A' as IS_MALE_GATEWAY
    ,IS_MALE_SESSION
    ,IS_MALE_CUSTOMER
    ,IS_MALE_SESSION_CUSTOMER
    ,s.PLATFORM
    ,s.OS
    --      ,'Temp N/A' as BROWSER
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
    ,IS_PREPAID_CREDITCARD
    ,payment_type_adjusted
    ,digital_wallet_payment_type_adjusted
    ,payment_method_type_adjusted
    ,credit_card_type_adjusted
    ,CUSTOMER_SELECTED_SHIPPING_TYPE
    ,CUSTOMER_SELECTED_SHIPPING_SERVICE
    ,promo_codes
    ,promo_names
    ,sg.product_segment

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
    ,IS_QUIZ_START_ACTION
    ,IS_QUIZ_COMPLETE_ACTION
    ,IS_LEAD_REGISTRATION_ACTION
    ,IS_QUIZ_REGISTRATION_ACTION
    ,IS_SPEEDY_REGISTRATION_ACTION
    ,IS_SKIP_QUIZ_REGISTRATION_ACTION
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
    ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN IS_SKIP_MONTH_ACTION
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
    ,IS_PREPAID_CREDITCARD
    ,payment_type_adjusted AS payment_type
    ,digital_wallet_payment_type_adjusted AS digital_wallet_payment_type
    ,payment_method_type_adjusted AS payment_method_type
    ,credit_card_type_adjusted AS credit_card_type
    ,CUSTOMER_SELECTED_SHIPPING_TYPE
    ,CUSTOMER_SELECTED_SHIPPING_SERVICE
     ,promo_codes
    ,promo_names
    ,sg.product_segment
    ,count(distinct case when membership_order_type_L1 = 'Activating VIP' then pp.ORDER_ID end) AS orders_activating
    ,count(distinct case when membership_order_type_L1 <> 'Activating VIP' then pp.ORDER_ID end) AS orders_nonactivating
    ,count(distinct pp.ORDER_ID) AS orders_total
    ,count(distinct case when membership_order_type_L1 = 'Activating VIP' and ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_activating
    ,count(distinct case when membership_order_type_L1 <> 'Activating VIP' and ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_nonactivating
    ,count(distinct case when ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_units end) as units_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_units end) as units_nonactivating
    ,sum(ttl_units) as units_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_gross_revenue end) as ttl_product_gross_revenue_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_gross_revenue end) as ttl_product_gross_revenue_nonactivating
    ,sum(ttl_product_gross_revenue) as product_revenue_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_gross_revenue_excl_shipping end) as product_revenue_excl_shipping_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_gross_revenue_excl_shipping end) as product_revenue_excl_shipping_nonactivating
    ,sum(ttl_product_gross_revenue_excl_shipping) as product_revenue_excl_shipping_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_margin_pre_return end) as product_margin_pre_return_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_margin_pre_return end) as product_margin_pre_return_nonactivating
    ,sum(ttl_product_margin_pre_return) AS product_margin_pre_return_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_margin_pre_return_excl_shipping end) as product_margin_pre_return_excl_shipping_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_margin_pre_return_excl_shipping end) as product_margin_pre_return_excl_shipping_nonactivating
    ,sum(ttl_product_margin_pre_return_excl_shipping) AS product_margin_pre_return_excl_shipping_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_cash_gross_revenue end) as cash_gross_revenue_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_cash_gross_revenue end) as cash_gross_revenue_nonactivating
    ,sum(ttl_cash_gross_revenue) AS cash_gross_revenue_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then TTL_SHIPPING_REVENUE end) as shipping_revenue_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then TTL_SHIPPING_REVENUE end) as shipping_revenue_nonactivating
    ,sum(TTL_SHIPPING_REVENUE) AS shipping_revenue_total
    ,$last_refreshed_datetime_hq
--'individual region and total platform'
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
left join (select test_key
             ,count(distinct case when STORE_REGION = 'NA' then STORE_COUNTRY end) as na_store_count
             ,count(distinct case when STORE_REGION = 'EU' then STORE_COUNTRY end) as eu_store_count
        from reporting_base_prod.SHARED.SESSION_ORDER_AB_TEST_DETAIL group by 1) as t1 on t1.TEST_KEY = s.test_key
left join _agg_promo_codes as pc on pc.SESSION_ID = pp.SESSION_ID
    and pc.ORDER_ID = pp.ORDER_ID
    and s.test_key = pc.test_key
    and s.test_framework_id = pc.test_framework_id
    and s.test_label = pc.test_label
    and s.store_brand = pc.store_brand
    AND s.ab_test_segment = pc.ab_test_segment
left join _agg_product_segment as sg on sg.SESSION_ID = s.SESSION_ID
    and sg.ORDER_ID = pp.ORDER_ID
    and s.test_key = sg.test_key
    and s.test_framework_id = sg.test_framework_id
    and s.test_label = sg.test_label
    and s.store_brand = sg.store_brand
    AND s.ab_test_segment = sg.ab_test_segment
left join _builder_test_labels as u on u.builder_id = s.test_key
where
    s.test_group is not null
    and TTL_ORDER_RNK = 1
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
    ,IS_QUIZ_START_ACTION
    ,IS_QUIZ_COMPLETE_ACTION
    ,IS_LEAD_REGISTRATION_ACTION
    ,IS_QUIZ_REGISTRATION_ACTION
    ,IS_SPEEDY_REGISTRATION_ACTION
    ,IS_SKIP_QUIZ_REGISTRATION_ACTION
    ,case when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
         when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
         when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end
    ,MEMBERSHIP_PRICE
    ,MEMBERSHIP_TYPE
    --        ,'Temp N/A' as IS_MOBILE_APP_USER
    --      ,'Temp N/A' as IS_FREE_TRIAL
    --      ,'Temp N/A' as IS_CROSS_PROMO
    --      ,'Temp N/A' as IS_MALE_GATEWAY
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
    ,IS_PREPAID_CREDITCARD
    ,payment_type_adjusted
    ,digital_wallet_payment_type_adjusted
    ,payment_method_type_adjusted
    ,credit_card_type_adjusted
    --shipping type
    ,CUSTOMER_SELECTED_SHIPPING_TYPE
    ,CUSTOMER_SELECTED_SHIPPING_SERVICE
    ,promo_codes
    ,promo_names
    ,sg.product_segment

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
    ,IS_QUIZ_START_ACTION
    ,IS_QUIZ_COMPLETE_ACTION
    ,IS_LEAD_REGISTRATION_ACTION
    ,IS_QUIZ_REGISTRATION_ACTION
    ,IS_SPEEDY_REGISTRATION_ACTION
    ,IS_SKIP_QUIZ_REGISTRATION_ACTION
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
    ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN IS_SKIP_MONTH_ACTION
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
    ,IS_PREPAID_CREDITCARD
    ,payment_type_adjusted AS payment_type
    ,digital_wallet_payment_type_adjusted AS digital_wallet_payment_type
    ,payment_method_type_adjusted AS payment_method_type
    ,credit_card_type_adjusted AS credit_card_type
    ,CUSTOMER_SELECTED_SHIPPING_TYPE
    ,CUSTOMER_SELECTED_SHIPPING_SERVICE
     ,promo_codes
    ,promo_names
     ,sg.product_segment
    ,count(distinct case when membership_order_type_L1 = 'Activating VIP' then pp.ORDER_ID end) AS orders_activating
    ,count(distinct case when membership_order_type_L1 <> 'Activating VIP' then pp.ORDER_ID end) AS orders_nonactivating
    ,count(distinct pp.ORDER_ID) AS orders_total
    ,count(distinct case when membership_order_type_L1 = 'Activating VIP' and ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_activating
    ,count(distinct case when membership_order_type_L1 <> 'Activating VIP' and ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_nonactivating
    ,count(distinct case when ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_units end) as units_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_units end) as units_nonactivating
    ,sum(ttl_units) as units_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_gross_revenue end) as ttl_product_gross_revenue_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_gross_revenue end) as ttl_product_gross_revenue_nonactivating
    ,sum(ttl_product_gross_revenue) as product_revenue_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_gross_revenue_excl_shipping end) as product_revenue_excl_shipping_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_gross_revenue_excl_shipping end) as product_revenue_excl_shipping_nonactivating
    ,sum(ttl_product_gross_revenue_excl_shipping) as product_revenue_excl_shipping_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_margin_pre_return end) as product_margin_pre_return_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_margin_pre_return end) as product_margin_pre_return_nonactivating
    ,sum(ttl_product_margin_pre_return) AS product_margin_pre_return_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_margin_pre_return_excl_shipping end) as product_margin_pre_return_excl_shipping_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_margin_pre_return_excl_shipping end) as product_margin_pre_return_excl_shipping_nonactivating
    ,sum(ttl_product_margin_pre_return_excl_shipping) AS product_margin_pre_return_excl_shipping_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_cash_gross_revenue end) as cash_gross_revenue_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_cash_gross_revenue end) as cash_gross_revenue_nonactivating
    ,sum(ttl_cash_gross_revenue) AS cash_gross_revenue_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then TTL_SHIPPING_REVENUE end) as shipping_revenue_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then TTL_SHIPPING_REVENUE end) as shipping_revenue_nonactivating
    ,sum(TTL_SHIPPING_REVENUE) AS shipping_revenue_total
    ,$last_refreshed_datetime_hq
--'total region and individual platform'
FROM reporting_base_prod.SHARED.session_order_ab_test_detail s
join reporting_base_prod.shared.order_line_psource_payment as pp on pp.SESSION_ID = s.SESSION_ID
join _last_3_months as ll on ll.test_framework_id = s.test_framework_id
    and ll.TEST_KEY = s.TEST_KEY
    and ll.test_label = s.test_label
    and ll.store_brand = s.store_brand
left join (select test_key,count(distinct STORE_REGION) as region_count from reporting_base_prod.SHARED.SESSION_ORDER_AB_TEST_DETAIL group by 1) as t1 on t1.TEST_KEY = s.test_key
left join _agg_promo_codes as pc on pc.SESSION_ID = pp.SESSION_ID
    and pc.ORDER_ID = pp.ORDER_ID
    and s.test_key = pc.test_key
    and s.test_framework_id = pc.test_framework_id
    and s.test_label = pc.test_label
    and s.store_brand = pc.store_brand
    AND s.ab_test_segment = pc.ab_test_segment
left join _agg_product_segment as sg on sg.SESSION_ID = s.SESSION_ID
    and sg.ORDER_ID = pp.ORDER_ID
    and s.test_key = sg.test_key
    and s.test_framework_id = sg.test_framework_id
    and s.test_label = sg.test_label
    and s.store_brand = sg.store_brand
    AND s.ab_test_segment = sg.ab_test_segment
left join _builder_test_labels as u on u.builder_id = s.test_key
where
    s.test_group is not null
    and TTL_ORDER_RNK = 1
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
    ,IS_QUIZ_START_ACTION
    ,IS_QUIZ_COMPLETE_ACTION
    ,IS_LEAD_REGISTRATION_ACTION
    ,IS_QUIZ_REGISTRATION_ACTION
    ,IS_SPEEDY_REGISTRATION_ACTION
    ,IS_SKIP_QUIZ_REGISTRATION_ACTION
    ,case when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
         when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
         when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end
    ,MEMBERSHIP_PRICE
    ,MEMBERSHIP_TYPE
    --        ,'Temp N/A' as IS_MOBILE_APP_USER
    --      ,'Temp N/A' as IS_FREE_TRIAL
    --      ,'Temp N/A' as IS_CROSS_PROMO
    --      ,'Temp N/A' as IS_MALE_GATEWAY
    ,IS_MALE_SESSION
    ,IS_MALE_CUSTOMER
    ,IS_MALE_SESSION_CUSTOMER
    ,s.PLATFORM
    ,s.OS
    --      ,'Temp N/A' as BROWSER
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
    ,IS_PREPAID_CREDITCARD
    ,payment_type_adjusted
    ,digital_wallet_payment_type_adjusted
    ,payment_method_type_adjusted
    ,credit_card_type_adjusted
    ,CUSTOMER_SELECTED_SHIPPING_TYPE
    ,CUSTOMER_SELECTED_SHIPPING_SERVICE
    ,promo_codes
    ,promo_names
    ,sg.product_segment

union all

SELECT
    'total region and total platform' as version
    ,s.TEST_KEY
    ,s.TEST_FRAMEWORK_ID
    ,coalesce(u.builder_test_label_adj,s.TEST_LABEL) as test_label
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
    ,IS_QUIZ_START_ACTION
    ,IS_QUIZ_COMPLETE_ACTION
    ,IS_LEAD_REGISTRATION_ACTION
    ,IS_QUIZ_REGISTRATION_ACTION
    ,IS_SPEEDY_REGISTRATION_ACTION
    ,IS_SKIP_QUIZ_REGISTRATION_ACTION
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
    ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN IS_SKIP_MONTH_ACTION
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
    ,IS_PREPAID_CREDITCARD
    ,payment_type_adjusted AS payment_type
    ,digital_wallet_payment_type_adjusted AS digital_wallet_payment_type
    ,payment_method_type_adjusted AS payment_method_type
    ,credit_card_type_adjusted AS credit_card_type
    ,CUSTOMER_SELECTED_SHIPPING_TYPE
    ,CUSTOMER_SELECTED_SHIPPING_SERVICE
     ,promo_codes
    ,promo_names
    ,sg.product_segment
    ,count(distinct case when membership_order_type_L1 = 'Activating VIP' then pp.ORDER_ID end) AS orders_activating
    ,count(distinct case when membership_order_type_L1 <> 'Activating VIP' then pp.ORDER_ID end) AS orders_nonactivating
    ,count(distinct pp.ORDER_ID) AS orders_total
    ,count(distinct case when membership_order_type_L1 = 'Activating VIP' and ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_activating
    ,count(distinct case when membership_order_type_L1 <> 'Activating VIP' and ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_nonactivating
    ,count(distinct case when ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_units end) as units_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_units end) as units_nonactivating
    ,sum(ttl_units) as units_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_gross_revenue end) as ttl_product_gross_revenue_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_gross_revenue end) as ttl_product_gross_revenue_nonactivating
    ,sum(ttl_product_gross_revenue) as product_revenue_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_gross_revenue_excl_shipping end) as product_revenue_excl_shipping_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_gross_revenue_excl_shipping end) as product_revenue_excl_shipping_nonactivating
    ,sum(ttl_product_gross_revenue_excl_shipping) as product_revenue_excl_shipping_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_margin_pre_return end) as product_margin_pre_return_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_margin_pre_return end) as product_margin_pre_return_nonactivating
    ,sum(ttl_product_margin_pre_return) AS product_margin_pre_return_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_margin_pre_return_excl_shipping end) as product_margin_pre_return_excl_shipping_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_margin_pre_return_excl_shipping end) as product_margin_pre_return_excl_shipping_nonactivating
    ,sum(ttl_product_margin_pre_return_excl_shipping) AS product_margin_pre_return_excl_shipping_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_cash_gross_revenue end) as cash_gross_revenue_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_cash_gross_revenue end) as cash_gross_revenue_nonactivating
    ,sum(ttl_cash_gross_revenue) AS cash_gross_revenue_total
    ,sum(case when membership_order_type_L1 = 'Activating VIP' then TTL_SHIPPING_REVENUE end) as shipping_revenue_activating
    ,sum(case when membership_order_type_L1 <> 'Activating VIP' then TTL_SHIPPING_REVENUE end) as shipping_revenue_nonactivating
    ,sum(TTL_SHIPPING_REVENUE) AS shipping_revenue_total
    ,$last_refreshed_datetime_hq
--'total region and total platform'
FROM REPORTING_BASE_PROD.SHARED.session_order_ab_test_detail s
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
left join (select test_key,count(distinct STORE_REGION) as region_count from REPORTING_BASE_PROD.SHARED.SESSION_ORDER_AB_TEST_DETAIL group by 1) as t1 on t1.TEST_KEY = s.test_key
left join _agg_promo_codes as pc on pc.SESSION_ID = pp.SESSION_ID
    and pc.ORDER_ID = pp.ORDER_ID
    and s.test_key = pc.test_key
    and s.test_framework_id = pc.test_framework_id
    and s.test_label = pc.test_label
    and s.store_brand = pc.store_brand
    AND s.ab_test_segment = pc.ab_test_segment
left join _agg_product_segment as sg on sg.SESSION_ID = s.SESSION_ID
    and sg.ORDER_ID = pp.ORDER_ID
    and s.test_key = sg.test_key
    and s.test_framework_id = sg.test_framework_id
    and s.test_label = sg.test_label
    and s.store_brand = sg.store_brand
    AND s.ab_test_segment = sg.ab_test_segment
left join _builder_test_labels as u on u.builder_id = s.test_key
where
    s.test_group is not null
    and TTL_ORDER_RNK = 1
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
   ,TEST_START_MEMBERSHIP_STATE
   ,TEST_START_LEAD_TENURE_GROUP
   ,TEST_START_VIP_MONTH_TENURE_GROUP
   ,IS_QUIZ_START_ACTION
   ,IS_QUIZ_COMPLETE_ACTION
   ,IS_LEAD_REGISTRATION_ACTION
   ,IS_QUIZ_REGISTRATION_ACTION
   ,IS_SPEEDY_REGISTRATION_ACTION
   ,IS_SKIP_QUIZ_REGISTRATION_ACTION
   ,case when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_QUIZ_REGISTRATION_ACTION = TRUE then 'Quiz'
         when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SPEEDY_REGISTRATION_ACTION = TRUE then 'Speedy Sign-Up'
         when IS_LEAD_REGISTRATION_ACTION = TRUE and IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 'Skip Quiz' else null end
   ,MEMBERSHIP_PRICE
   ,MEMBERSHIP_TYPE
--        ,'Temp N/A' as IS_MOBILE_APP_USER
--      ,'Temp N/A' as IS_FREE_TRIAL
--      ,'Temp N/A' as IS_CROSS_PROMO
--      ,'Temp N/A' as IS_MALE_GATEWAY
   ,IS_MALE_SESSION
   ,IS_MALE_CUSTOMER
   ,IS_MALE_SESSION_CUSTOMER
--      ,'Total' as PLATFORM
--      ,'Total' as OS
--      ,'Temp N/A' as BROWSER
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
   ,IS_PREPAID_CREDITCARD
   ,payment_type_adjusted
   ,digital_wallet_payment_type_adjusted
   ,payment_method_type_adjusted
   ,credit_card_type_adjusted
   ,CUSTOMER_SELECTED_SHIPPING_TYPE
   ,CUSTOMER_SELECTED_SHIPPING_SERVICE
   ,promo_codes
    ,promo_names
    ,sg.product_segment;

BEGIN;  --comment

DELETE FROM REPORTING_PROD.SHARED.PAYMENT_SHIPPING_TYPE_AB_TEST_FINAL  --comment
WHERE AB_TEST_START_LOCAL_DATE>=$process_from_date;

DELETE FROM REPORTING_PROD.shared.PAYMENT_SHIPPING_TYPE_AB_TEST_FINAL
where
    TEST_KEY not in (select test_key from _last_3_months)
        and test_label not in (select test_label from _last_3_months);

INSERT INTO REPORTING_PROD.SHARED.PAYMENT_SHIPPING_TYPE_AB_TEST_FINAL  --comment
(
    VERSION,
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
    IS_QUIZ_START_ACTION,
    IS_QUIZ_COMPLETE_ACTION,
    IS_LEAD_REGISTRATION_ACTION,
    IS_QUIZ_REGISTRATION_ACTION,
    IS_SPEEDY_REGISTRATION_ACTION,
    IS_SKIP_QUIZ_REGISTRATION_ACTION,
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
    IS_PREPAID_CREDITCARD,
    PAYMENT_TYPE,
    DIGITAL_WALLET_PAYMENT_TYPE,
    PAYMENT_METHOD_TYPE,
    CREDIT_CARD_TYPE,
    CUSTOMER_SELECTED_SHIPPING_TYPE,
    CUSTOMER_SELECTED_SHIPPING_SERVICE,
    PROMO_CODES,
    promo_names,
    PRODUCT_SEGMENT,
    ORDERS_ACTIVATING,
    ORDERS_NONACTIVATING,
    ORDERS_TOTAL,
    FAILED_ORDERS_ACTIVATING,
    FAILED_ORDERS_NONACTIVATING,
    FAILED_ORDERS_TOTAL,
    UNITS_ACTIVATING,
    UNITS_NONACTIVATING,
    UNITS_TOTAL,
    TTL_PRODUCT_GROSS_REVENUE_ACTIVATING,
    TTL_PRODUCT_GROSS_REVENUE_NONACTIVATING,
    PRODUCT_REVENUE_TOTAL,
    PRODUCT_REVENUE_EXCL_SHIPPING_ACTIVATING,
    PRODUCT_REVENUE_EXCL_SHIPPING_NONACTIVATING,
    PRODUCT_REVENUE_EXCL_SHIPPING_TOTAL,
    PRODUCT_MARGIN_PRE_RETURN_ACTIVATING,
    PRODUCT_MARGIN_PRE_RETURN_NONACTIVATING,
    PRODUCT_MARGIN_PRE_RETURN_TOTAL,
    PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING_ACTIVATING,
    PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING_NONACTIVATING,
    PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING_TOTAL,
    CASH_GROSS_REVENUE_ACTIVATING,
    CASH_GROSS_REVENUE_NONACTIVATING,
    CASH_GROSS_REVENUE_TOTAL,
    SHIPPING_REVENUE_ACTIVATING,
    SHIPPING_REVENUE_NONACTIVATING,
    SHIPPING_REVENUE_TOTAL,
    LAST_REFRESHED_DATETIME_HQ
)
select
    VERSION,
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
    IS_QUIZ_START_ACTION,
    IS_QUIZ_COMPLETE_ACTION,
    IS_LEAD_REGISTRATION_ACTION,
    IS_QUIZ_REGISTRATION_ACTION,
    IS_SPEEDY_REGISTRATION_ACTION,
    IS_SKIP_QUIZ_REGISTRATION_ACTION,
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
    IS_PREPAID_CREDITCARD,
    PAYMENT_TYPE,
    DIGITAL_WALLET_PAYMENT_TYPE,
    PAYMENT_METHOD_TYPE,
    CREDIT_CARD_TYPE,
    CUSTOMER_SELECTED_SHIPPING_TYPE,
    CUSTOMER_SELECTED_SHIPPING_SERVICE,
    PROMO_CODES,
    promo_names,
    PRODUCT_SEGMENT,
    ORDERS_ACTIVATING,
    ORDERS_NONACTIVATING,
    ORDERS_TOTAL,
    FAILED_ORDERS_ACTIVATING,
    FAILED_ORDERS_NONACTIVATING,
    FAILED_ORDERS_TOTAL,
    UNITS_ACTIVATING,
    UNITS_NONACTIVATING,
    UNITS_TOTAL,
    TTL_PRODUCT_GROSS_REVENUE_ACTIVATING,
    TTL_PRODUCT_GROSS_REVENUE_NONACTIVATING,
    PRODUCT_REVENUE_TOTAL,
    PRODUCT_REVENUE_EXCL_SHIPPING_ACTIVATING,
    PRODUCT_REVENUE_EXCL_SHIPPING_NONACTIVATING,
    PRODUCT_REVENUE_EXCL_SHIPPING_TOTAL,
    PRODUCT_MARGIN_PRE_RETURN_ACTIVATING,
    PRODUCT_MARGIN_PRE_RETURN_NONACTIVATING,
    PRODUCT_MARGIN_PRE_RETURN_TOTAL,
    PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING_ACTIVATING,
    PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING_NONACTIVATING,
    PRODUCT_MARGIN_PRE_RETURN_EXCL_SHIPPING_TOTAL,
    CASH_GROSS_REVENUE_ACTIVATING,
    CASH_GROSS_REVENUE_NONACTIVATING,
    CASH_GROSS_REVENUE_TOTAL,
    SHIPPING_REVENUE_ACTIVATING,
    SHIPPING_REVENUE_NONACTIVATING,
    SHIPPING_REVENUE_TOTAL,
    LAST_REFRESHED_DATETIME_HQ
from _PAYMENT_SHIPPING_TYPE_AB_TEST_FINAL
;

COMMIT;  --comment
