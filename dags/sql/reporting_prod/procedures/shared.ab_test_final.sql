
set last_refreshed_datetime_hq = (select max(CONVERT_TIMEZONE('America/Los_Angeles',session_local_datetime)::datetime) from reporting_base_prod.shared.session_order_ab_test_detail);
set process_from_date = (select MAX(convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATE))::date-7 --comment
                         from REPORTING_PROD.shared.ab_test_final); --comment

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
--     ,TEST_LABEL
--     ,TEST_FRAMEWORK_ID
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

CREATE OR REPLACE TEMP TABLE _ab_test_final AS --comment
-- CREATE OR REPLACE TRANSIENT TABLE reporting_prod.shared.ab_test_final AS --comment
SELECT
    'individual country and individual platform' as version
     ,d.TEST_KEY
     ,d.TEST_FRAMEWORK_ID
     ,coalesce(u.builder_test_label_adj,d.test_label) as test_label
     ,TEST_FRAMEWORK_DESCRIPTION
     ,TEST_FRAMEWORK_TICKET
     ,d.CAMPAIGN_CODE
     ,TEST_TYPE
     ,test_activated_datetime
     ,case when TEST_GROUP ilike 'Control%' THEN 'Control' else 'Variant' end as TEST_GROUP_TYPE
     ,TEST_GROUP
     ,GROUP_NAME
     ,TRAFFIC_SPLIT_TYPE
     ,TEST_GROUP_DESCRIPTION
     ,AB_TEST_START_LOCAL_DATETIME::date as ab_test_start_local_date
     ,d.STORE_BRAND
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
     ,NULL as MEMBERSHIP_PRICE
     ,MEMBERSHIP_TYPE
     ,NULL as IS_MOBILE_APP_USER
     ,NULL as IS_FREE_TRIAL
     ,NULL as IS_CROSS_PROMO
     ,NULL as IS_MALE_GATEWAY
     ,NULL as IS_MALE_SESSION_ACTION
     ,IS_MALE_SESSION
     ,IS_MALE_CUSTOMER
     ,IS_MALE_SESSION_CUSTOMER
     ,PLATFORM
     ,OS
     ,NULL as BROWSER
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then CHANNEL else 'Null - Non Prospect' end as CHANNEL
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then SUBCHANNEL else 'Null - Non Prospect' end as SUBCHANNEL
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then DM_GATEWAY_ID else null end as DM_GATEWAY_ID
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then GATEWAY_NAME else 'Null - Non Prospect' end as GATEWAY_NAME
     ,coalesce(is_login_action,false)::BOOLEAN AS is_login_action
     ,coalesce(IS_SHOPPING_GRID_VISIT,false)::BOOLEAN as IS_SHOPPING_GRID_VISIT
     ,coalesce(IS_PDP_VISIT,null)::BOOLEAN AS IS_PDP_VISIT
     ,coalesce(IS_ATB_ACTION,null)::BOOLEAN AS IS_ATB_ACTION
     ,coalesce(IS_CART_VISIT,null)::BOOLEAN AS IS_CART_VISIT
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
     ,count(*) as sessions
     ,count(distinct CUSTOMER_ID) as customers
     ,count(distinct VISITOR_ID) as visitors
     ,COUNT(distinct case when test_type in ('membership','Sorting Hat, Persistent','Membership Detail','Customer Detail') then customer_id
                          when test_type in ('session','Session Detail','Sorting Hat, Non Persistent','Builder') then session_id end) as test_starts_actual
     ,sum(skip_quiz_actions) as skip_quiz_actions
     ,sum(quiz_starts) as quiz_starts
     ,sum(quiz_completes) as quiz_completes
     ,sum(lead_registrations) as lead_registrations
     ,sum(quiz_lead_registrations) as quiz_lead_registrations
     ,sum(speedy_lead_registrations) as speedy_lead_registrations
     ,sum(skip_quiz_lead_registrations) as skip_quiz_lead_registrations
     ,SUM(vip_activations_1hr) as vip_activations_1hr
     ,SUM(vip_activations_3hr) as vip_activations_3hr
     ,sum(vip_activations_24hr) as vip_activations_24hr
     ,SUM(vip_activations) AS vip_activations
     ,sum(VIP_ACTIVATIONS_SAME_SESSION) as VIP_ACTIVATIONS_SAME_SESSION
     ,sum(VIP_ACTIVATIONS_BY_LEAD_REG) as VIP_ACTIVATIONS_BY_LEAD_REG
     ,SUM(units_activating) AS units_activating
     ,sum(product_subtotal_excl_tariff_activating) as product_subtotal_excl_tariff_activating
     ,sum(product_subtotal_incl_tariff_activating) as product_subtotal_incl_tariff_activating
     ,SUM(product_discount_activating) as product_discount_activating
     ,SUM(shipping_discount_activating) as shipping_discount_activating
     ,SUM(total_discount_activating) as total_discount_activating
     ,SUM(shipping_revenue_activating) as shipping_revenue_activating
     ,SUM(tariff_surcharge_activating) as tariff_surcharge_activating
     ,SUM(tax_activating) as tax_activating
     ,SUM(product_gross_revenue_activating) AS product_gross_revenue_activating
     ,SUM(product_gross_revenue_excl_shipping_activating) AS product_gross_revenue_excl_shipping_activating
     ,sum(product_margin_pre_return_activating) AS product_margin_pre_return_activating
     ,sum(cash_gross_revenue_activating) AS cash_gross_revenue_activating
     ,sum(cash_credit_activating) AS cash_credit_activating
     ,sum(noncash_credit_activating) AS noncash_credit_activating
     ,sum(product_cost_activating) AS product_cost_activating
     ,sum(landed_cost_activating)  AS landed_cost_activating
     ,sum(shipping_cost_activating)  AS shipping_cost_activating
     ,sum(total_cogs_activating) AS total_cogs_activating
     ,sum(prepaid_creditcard_orders_activating) as prepaid_creditcard_orders_activating
     ,sum(prepaid_creditcard_failed_attempts_activating) as prepaid_creditcard_failed_attempts_activating
     ,sum(failed_orders_activating) as failed_orders_activating
     ,SUM(orders_nonactivating) AS orders_nonactivating
     ,SUM(units_nonactivating) AS units_nonactivating
     ,SUM(product_subtotal_excl_tariff_nonactivating) as product_subtotal_excl_tariff_nonactivating
     ,SUM(product_subtotal_incl_tariff_nonactivating) as product_subtotal_incl_tariff_nonactivating
     ,SUM(product_discount_nonactivating) as product_discount_nonactivating
     ,SUM(shipping_discount_nonactivating) as shipping_discount_nonactivating
     ,SUM(total_discount_nonactivating) as total_discount_nonactivating
     ,SUM(shipping_revenue_nonactivating) as shipping_revenue_nonactivating
     ,SUM(tariff_surcharge_nonactivating) as tariff_surcharge_nonactivating
     ,SUM(tax_nonactivating) as tax_nonactivating
     ,SUM(product_gross_revenue_nonactivating) AS product_gross_revenue_nonactivating
     ,SUM(product_gross_revenue_excl_shipping_nonactivating) AS product_gross_revenue_excl_shipping_nonactivating
     ,SUM(product_margin_pre_return_nonactivating) AS product_margin_pre_return_nonactivating
     ,SUM(cash_gross_revenue_nonactivating) AS cash_gross_revenue_nonactivating
     ,SUM(cash_credit_nonactivating) AS cash_credit_nonactivating
     ,SUM(noncash_credit_nonactivating) AS noncash_credit_nonactivating
     ,SUM(product_cost_nonactivating) AS product_cost_nonactivating
     ,SUM(landed_cost_nonactivating) AS landed_cost_nonactivating
     ,SUM(shipping_cost_nonactivating) AS shipping_cost_nonactivating
     ,SUM(total_cogs_nonactivating) AS total_cogs_nonactivating
     ,SUM(membership_credit_redeemed_amount_nonactivating) as membership_credit_redeemed_amount_nonactivating
     ,SUM(membership_credits_redeemed_count_nonactivating) as membership_credits_token_redeemed_count_nonactivating
     ,SUM(prepaid_creditcard_orders_nonactivating) as prepaid_creditcard_orders_nonactivating
     ,SUM(prepaid_creditcard_failed_attempts_nonactivating) as prepaid_creditcard_failed_attempts_nonactivating
     ,SUM(failed_orders_nonactivating) as failed_orders_nonactivating
     ,SUM(orders_total) AS orders_total
     ,SUM(units_total) AS units_total
     ,SUM(product_subtotal_excl_tariff_total) as product_subtotal_excl_tariff_total
     ,SUM(product_subtotal_incl_tariff_total) as product_subtotal_incl_tariff_total
     ,SUM(product_discount_total) as product_discount_total
     ,SUM(shipping_discount_total) as shipping_discount_total
     ,SUM(total_discount_total) as total_discount_total
     ,SUM(shipping_revenue_total) as shipping_revenue_total
     ,SUM(tariff_surcharge_total) as tariff_surcharge_total
     ,SUM(tax_total) as tax_total
     ,SUM(product_gross_revenue_total) AS product_gross_revenue_total
     ,SUM(product_gross_revenue_excl_shipping_total) AS product_gross_revenue_excl_shipping_total
     ,SUM(product_margin_pre_return_total) AS product_margin_pre_return_total
     ,SUM(cash_gross_revenue_total) AS cash_gross_revenue_total
     ,SUM(cash_credit_amount_total) AS cash_credit_amount_total
     ,SUM(noncash_credit_amount_total) AS noncash_credit_amount_total
     ,SUM(product_cost_total) AS product_cost_total
     ,SUM(landed_cost_total) AS landed_cost_total
     ,SUM(shipping_cost_total) AS shipping_cost_total
     ,SUM(total_cogs_total) AS total_cogs_total
     ,SUM(prepaid_creditcard_orders_total) as prepaid_creditcard_orders_total
     ,SUM(prepaid_creditcard_failed_attempts_total) as prepaid_creditcard_failed_attempts_total
     ,SUM(failed_orders_total) as failed_orders_total
     ,SUM(membership_cancellations) as membership_cancellations
     ,SUM(online_membership_cancellations) as online_membership_cancellations
     ,SUM(membership_pauses) as membership_pauses
     ,SUM(distinct_monthly_vip_customers) as distinct_monthly_vip_customers
     ,SUM(login_actions) as login_actions
     ,count(case when IS_SHOPPING_GRID_VISIT = true then session_id end) as shopping_grid_actions
     ,count(case when IS_PDP_VISIT = true then session_id end) as pdp_actions
     ,count(case when is_atb_action = true then session_id end) as atb_actions
     ,sum(cart_actions) as cart_actions
     ,SUM(skip_actions) as skip_actions
     ,SUM(flag_1_actions) as flag_1_actions
     ,SUM(flag_2_actions) as flag_2_actions
     ,SUM(flag_3_actions) as flag_3_actions
     ,SUM(CUSTOM_FLAG_ACTIONS) AS custom_flag_actions
     ,SUM(custom_flag_2_actions) AS custom_flag_2_actions
     ,$last_refreshed_datetime_hq as last_refreshed_datetime_hq
--'individual country and individual platform'
-- select test_label,count(*),count(distinct session_id)
FROM reporting_base_prod.shared.session_order_ab_test_detail as d
join _last_3_months as ll on ll.test_framework_id = d.test_framework_id
    and ll.TEST_KEY = d.TEST_KEY
    and ll.test_label = d.test_label
    and ll.store_brand = d.store_brand
left join _builder_test_labels as u on u.builder_id = d.test_key
where
    test_group is not null
    and convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME)::date>=$process_from_date --comment
group by
    d.TEST_KEY
       ,d.TEST_FRAMEWORK_ID
       ,coalesce(u.builder_test_label_adj,d.test_label)
       ,TEST_FRAMEWORK_DESCRIPTION
       ,TEST_FRAMEWORK_TICKET
       ,d.CAMPAIGN_CODE
       ,TEST_TYPE
       ,test_activated_datetime
       ,case when TEST_GROUP ilike 'Control%' THEN 'Control' else 'Variant' end
       ,TEST_GROUP
       ,GROUP_NAME
       ,TRAFFIC_SPLIT_TYPE
       ,TEST_GROUP_DESCRIPTION
       ,AB_TEST_START_LOCAL_DATETIME::date
       ,d.STORE_BRAND
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
--      ,MEMBERSHIP_PRICE
       ,MEMBERSHIP_TYPE
--      ,IS_MOBILE_APP_USER
--      ,IS_FREE_TRIAL
--      ,IS_CROSS_PROMO
--      ,IS_MALE_GATEWAY
--      ,IS_MALE_SESSION_ACTION
       ,IS_MALE_SESSION
       ,IS_MALE_CUSTOMER
       ,IS_MALE_SESSION_CUSTOMER
       ,PLATFORM
       ,OS
--      ,BROWSER
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then CHANNEL else 'Null - Non Prospect' end
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then SUBCHANNEL else 'Null - Non Prospect' end
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then DM_GATEWAY_ID else null end
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then GATEWAY_NAME else 'Null - Non Prospect' end
       ,coalesce(is_login_action,false)::BOOLEAN
        ,coalesce(IS_SHOPPING_GRID_VISIT,false)::BOOLEAN
       ,coalesce(IS_PDP_VISIT,null)::BOOLEAN
       ,coalesce(IS_ATB_ACTION,null)::BOOLEAN
       ,coalesce(IS_CART_VISIT,null)::BOOLEAN
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

union all

SELECT
    'individual country and total platform' as version
     ,d.TEST_KEY
     ,d.TEST_FRAMEWORK_ID
     ,coalesce(u.builder_test_label_adj,d.test_label) as test_label
     ,TEST_FRAMEWORK_DESCRIPTION
     ,TEST_FRAMEWORK_TICKET
     ,d.CAMPAIGN_CODE
     ,TEST_TYPE
     ,test_activated_datetime
     ,case when d.TEST_GROUP ilike 'Control%' THEN 'Control' else 'Variant' end as TEST_GROUP_TYPE
     ,d.TEST_GROUP
     ,GROUP_NAME
     ,TRAFFIC_SPLIT_TYPE
     ,sc.TEST_GROUP_DESCRIPTION
     ,AB_TEST_START_LOCAL_DATETIME::date as ab_test_start_local_date
     ,d.STORE_BRAND
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
     ,NULL as MEMBERSHIP_PRICE
     ,MEMBERSHIP_TYPE
     ,NULL as IS_MOBILE_APP_USER
     ,NULL as IS_FREE_TRIAL
     ,NULL as IS_CROSS_PROMO
     ,NULL as IS_MALE_GATEWAY
     ,NULL as IS_MALE_SESSION_ACTION
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
     ,coalesce(is_login_action,false)::BOOLEAN AS is_login_action
     ,coalesce(IS_SHOPPING_GRID_VISIT,false)::BOOLEAN as IS_SHOPPING_GRID_VISIT
     ,coalesce(IS_PDP_VISIT,null)::BOOLEAN AS IS_PDP_VISIT
     ,coalesce(IS_ATB_ACTION,null)::BOOLEAN AS IS_ATB_ACTION
     ,coalesce(IS_CART_VISIT,null)::BOOLEAN AS IS_CART_VISIT
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
     ,count(*) as sessions
     ,count(distinct CUSTOMER_ID) as customers
     ,count(distinct VISITOR_ID) as visitors
     ,COUNT(distinct case when test_type in ('membership','Sorting Hat, Persistent','Membership Detail','Customer Detail') then customer_id
                          when test_type in ('session','Session Detail','Sorting Hat, Non Persistent','Builder') then session_id end) as test_starts_actual
     ,sum(skip_quiz_actions) as skip_quiz_actions
     ,sum(quiz_starts) as quiz_starts
     ,sum(quiz_completes) as quiz_completes
     ,sum(lead_registrations) as lead_registrations
     ,sum(quiz_lead_registrations) as quiz_lead_registrations
     ,sum(speedy_lead_registrations) as speedy_lead_registrations
     ,sum(skip_quiz_lead_registrations) as skip_quiz_lead_registrations
     ,SUM(vip_activations_1hr) as vip_activations_1hr
     ,SUM(vip_activations_3hr) as vip_activations_3hr
     ,sum(vip_activations_24hr) as vip_activations_24hr
     ,SUM(vip_activations) AS vip_activations
     ,sum(VIP_ACTIVATIONS_SAME_SESSION) as VIP_ACTIVATIONS_SAME_SESSION
     ,sum(VIP_ACTIVATIONS_BY_LEAD_REG) as VIP_ACTIVATIONS_BY_LEAD_REG
     ,SUM(units_activating) AS units_activating
     ,sum(product_subtotal_excl_tariff_activating) as product_subtotal_excl_tariff_activating
     ,sum(product_subtotal_incl_tariff_activating) as product_subtotal_incl_tariff_activating
     ,SUM(product_discount_activating) as product_discount_activating
     ,SUM(shipping_discount_activating) as shipping_discount_activating
     ,SUM(total_discount_activating) as total_discount_activating
     ,SUM(shipping_revenue_activating) as shipping_revenue_activating
     ,SUM(tariff_surcharge_activating) as tariff_surcharge_activating
     ,SUM(tax_activating) as tax_activating
     ,SUM(product_gross_revenue_activating) AS product_gross_revenue_activating
     ,SUM(product_gross_revenue_excl_shipping_activating) AS product_gross_revenue_excl_shipping_activating
     ,sum(product_margin_pre_return_activating) AS product_margin_pre_return_activating
     ,sum(cash_gross_revenue_activating) AS cash_gross_revenue_activating
     ,sum(cash_credit_activating) AS cash_credit_activating
     ,sum(noncash_credit_activating) AS noncash_credit_activating
     ,sum(product_cost_activating) AS product_cost_activating
     ,sum(landed_cost_activating)  AS landed_cost_activating
     ,sum(shipping_cost_activating)  AS shipping_cost_activating
     ,sum(total_cogs_activating) AS total_cogs_activating
     ,sum(prepaid_creditcard_orders_activating) as prepaid_creditcard_orders_activating
     ,sum(prepaid_creditcard_failed_attempts_activating) as prepaid_creditcard_failed_attempts_activating
     ,sum(failed_orders_activating) as failed_orders_activating
     ,SUM(orders_nonactivating) AS orders_nonactivating
     ,SUM(units_nonactivating) AS units_nonactivating
     ,SUM(product_subtotal_excl_tariff_nonactivating) as product_subtotal_excl_tariff_nonactivating
     ,SUM(product_subtotal_incl_tariff_nonactivating) as product_subtotal_incl_tariff_nonactivating
     ,SUM(product_discount_nonactivating) as product_discount_nonactivating
     ,SUM(shipping_discount_nonactivating) as shipping_discount_nonactivating
     ,SUM(total_discount_nonactivating) as total_discount_nonactivating
     ,SUM(shipping_revenue_nonactivating) as shipping_revenue_nonactivating
     ,SUM(tariff_surcharge_nonactivating) as tariff_surcharge_nonactivating
     ,SUM(tax_nonactivating) as tax_nonactivating
     ,SUM(product_gross_revenue_nonactivating) AS product_gross_revenue_nonactivating
     ,SUM(product_gross_revenue_excl_shipping_nonactivating) AS product_gross_revenue_excl_shipping_nonactivating
     ,SUM(product_margin_pre_return_nonactivating) AS product_margin_pre_return_nonactivating
     ,SUM(cash_gross_revenue_nonactivating) AS cash_gross_revenue_nonactivating
     ,SUM(cash_credit_nonactivating) AS cash_credit_nonactivating
     ,SUM(noncash_credit_nonactivating) AS noncash_credit_nonactivating
     ,SUM(product_cost_nonactivating) AS product_cost_nonactivating
     ,SUM(landed_cost_nonactivating) AS landed_cost_nonactivating
     ,SUM(shipping_cost_nonactivating) AS shipping_cost_nonactivating
     ,SUM(total_cogs_nonactivating) AS total_cogs_nonactivating
     ,SUM(membership_credit_redeemed_amount_nonactivating) as membership_credit_redeemed_amount_nonactivating
     ,SUM(membership_credits_redeemed_count_nonactivating) as membership_credits_token_redeemed_count_nonactivating
     ,SUM(prepaid_creditcard_orders_nonactivating) as prepaid_creditcard_orders_nonactivating
     ,SUM(prepaid_creditcard_failed_attempts_nonactivating) as prepaid_creditcard_failed_attempts_nonactivating
     ,SUM(failed_orders_nonactivating) as failed_orders_nonactivating
     ,SUM(orders_total) AS orders_total
     ,SUM(units_total) AS units_total
     ,SUM(product_subtotal_excl_tariff_total) as product_subtotal_excl_tariff_total
     ,SUM(product_subtotal_incl_tariff_total) as product_subtotal_incl_tariff_total
     ,SUM(product_discount_total) as product_discount_total
     ,SUM(shipping_discount_total) as shipping_discount_total
     ,SUM(total_discount_total) as total_discount_total
     ,SUM(shipping_revenue_total) as shipping_revenue_total
     ,SUM(tariff_surcharge_total) as tariff_surcharge_total
     ,SUM(tax_total) as tax_total
     ,SUM(product_gross_revenue_total) AS product_gross_revenue_total
     ,SUM(product_gross_revenue_excl_shipping_total) AS product_gross_revenue_excl_shipping_total
     ,SUM(product_margin_pre_return_total) AS product_margin_pre_return_total
     ,SUM(cash_gross_revenue_total) AS cash_gross_revenue_total
     ,SUM(cash_credit_amount_total) AS cash_credit_amount_total
     ,SUM(noncash_credit_amount_total) AS noncash_credit_amount_total
     ,SUM(product_cost_total) AS product_cost_total
     ,SUM(landed_cost_total) AS landed_cost_total
     ,SUM(shipping_cost_total) AS shipping_cost_total
     ,SUM(total_cogs_total) AS total_cogs_total
     ,SUM(prepaid_creditcard_orders_total) as prepaid_creditcard_orders_total
     ,SUM(prepaid_creditcard_failed_attempts_total) as prepaid_creditcard_failed_attempts_total
     ,SUM(failed_orders_total) as failed_orders_total
     ,SUM(membership_cancellations) as membership_cancellations
     ,SUM(online_membership_cancellations) as online_membership_cancellations
     ,SUM(membership_pauses) as membership_pauses
     ,SUM(distinct_monthly_vip_customers) as distinct_monthly_vip_customers
     ,SUM(login_actions) as login_actions
     ,count(case when IS_SHOPPING_GRID_VISIT = true then session_id end) as shopping_grid_actions
     ,count(case when IS_PDP_VISIT = true then session_id end) as pdp_actions
     ,count(case when is_atb_action = true then session_id end) as atb_actions
     ,sum(cart_actions) as cart_actions
     ,SUM(skip_actions) as skip_actions
     ,SUM(flag_1_actions) as flag_1_actions
     ,SUM(flag_2_actions) as flag_2_actions
     ,SUM(flag_3_actions) as flag_3_actions
     ,SUM(CUSTOM_FLAG_ACTIONS) AS custom_flag_actions
     ,SUM(custom_flag_2_actions) AS custom_flag_2_actions
     ,$last_refreshed_datetime_hq as last_refreshed_datetime_hq
--'individual country and total platform'
FROM reporting_base_prod.shared.session_order_ab_test_detail as d
join _last_3_months as ll on ll.test_framework_id = d.test_framework_id
    and ll.TEST_KEY = d.TEST_KEY
    and ll.test_label = d.test_label
    and ll.store_brand = d.store_brand
left join _total_descriptions as sc on d.test_key = sc.test_key
    and d.test_framework_id = sc.test_framework_id
    and d.test_label = sc.test_label
    and d.store_brand = sc.store_brand
    AND d.test_group = sc.test_group
left join _builder_test_labels as u on u.builder_id = d.test_key
where
    d.test_group is not null
    and (d.TEST_KEY ilike '%(User%'
        or (test_type in ('membership','session') and d.test_key in (select distinct test_key from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA where DEVICE in ('Desktop & Mobile','Desktop & App','Mobile & App','Desktop, Mobile & App')))
        or (test_type in ('Builder','Session Detail','Customer Detail','Membership Detail') and d.test_key in (select distinct test_key from lake_view.sharepoint.ab_test_metadata_import_oof where test_platforms in ('Desktop & Mobile','Mobile & App','ALL','Desktop and Mobile')))
        or (test_type in ('Builder','Session Detail','Customer Detail','Membership Detail') and d.test_key in (select distinct concat(test_key,' (PSRC)') as test_key from lake_view.sharepoint.ab_test_metadata_import_oof where test_platforms in ('Desktop & Mobile','Mobile & App','ALL','Desktop and Mobile')))
        or (test_type ilike 'Sorting Hat%' and d.test_key in (select distinct meta_original_metadata_test_id from _sorting_hat_metadata_ids where test_platforms in ('Desktop & Mobile','Mobile & App','ALL','Desktop and Mobile'))))
    and convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME)::date>=$process_from_date --comment
group by
    d.TEST_KEY
       ,d.TEST_FRAMEWORK_ID
       ,coalesce(u.builder_test_label_adj,d.test_label)
       ,TEST_FRAMEWORK_DESCRIPTION
       ,TEST_FRAMEWORK_TICKET
       ,d.CAMPAIGN_CODE
       ,TEST_TYPE
       ,test_activated_datetime
       ,case when d.TEST_GROUP ilike 'Control%' THEN 'Control' else 'Variant' end
       ,d.TEST_GROUP
       ,GROUP_NAME
       ,TRAFFIC_SPLIT_TYPE
       ,sc.TEST_GROUP_DESCRIPTION
       ,AB_TEST_START_LOCAL_DATETIME::date
       ,d.STORE_BRAND
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
--      ,'Temp N/A' as MEMBERSHIP_PRICE
       ,MEMBERSHIP_TYPE
--      ,'Temp N/A' as IS_MOBILE_APP_USER
--      ,'Temp N/A' as IS_FREE_TRIAL
--      ,'Temp N/A' as IS_CROSS_PROMO
--      ,'Temp N/A' as IS_MALE_GATEWAY
--      ,'Temp N/A' as IS_MALE_SESSION_ACTION
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
       ,coalesce(is_login_action,false)::BOOLEAN
       ,coalesce(IS_SHOPPING_GRID_VISIT,false)::BOOLEAN
       ,coalesce(IS_PDP_VISIT,null)::BOOLEAN
       ,coalesce(IS_ATB_ACTION,null)::BOOLEAN
       ,coalesce(IS_CART_VISIT,null)::BOOLEAN
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

union all

SELECT
    'individual region and individual platform' as version
     ,d.TEST_KEY
     ,d.TEST_FRAMEWORK_ID
     ,coalesce(u.builder_test_label_adj,d.test_label)
     ,TEST_FRAMEWORK_DESCRIPTION
     ,TEST_FRAMEWORK_TICKET
     ,d.CAMPAIGN_CODE
     ,TEST_TYPE
     ,test_activated_datetime
     ,case when TEST_GROUP ilike 'Control%' THEN 'Control' else 'Variant' end as TEST_GROUP_TYPE
     ,TEST_GROUP
     ,GROUP_NAME
     ,TRAFFIC_SPLIT_TYPE
     ,TEST_GROUP_DESCRIPTION
     ,AB_TEST_START_LOCAL_DATETIME::date as ab_test_start_local_date
     ,d.STORE_BRAND
     ,concat(d.STORE_REGION,' TTL') as STORE_REGION
     ,concat(d.STORE_REGION,' TTL') as STORE_COUNTRY
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
     ,NULL as MEMBERSHIP_PRICE
     ,MEMBERSHIP_TYPE
     ,NULL as IS_MOBILE_APP_USER
     ,NULL as IS_FREE_TRIAL
     ,NULL as IS_CROSS_PROMO
     ,NULL as IS_MALE_GATEWAY
     ,NULL as IS_MALE_SESSION_ACTION
     ,IS_MALE_SESSION
     ,IS_MALE_CUSTOMER
     ,IS_MALE_SESSION_CUSTOMER
     ,PLATFORM
     ,OS
     ,NULL as BROWSER
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then CHANNEL else 'Null - Non Prospect' end as CHANNEL
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then SUBCHANNEL else 'Null - Non Prospect' end as SUBCHANNEL
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then DM_GATEWAY_ID else null end as DM_GATEWAY_ID
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then GATEWAY_NAME else 'Null - Non Prospect' end as GATEWAY_NAME
     ,coalesce(is_login_action,false)::BOOLEAN AS is_login_action
     ,coalesce(IS_SHOPPING_GRID_VISIT,false)::BOOLEAN as IS_SHOPPING_GRID_VISIT
     ,coalesce(IS_PDP_VISIT,null)::BOOLEAN AS IS_PDP_VISIT
     ,coalesce(IS_ATB_ACTION,null)::BOOLEAN AS IS_ATB_ACTION
     ,coalesce(IS_CART_VISIT,null)::BOOLEAN AS IS_CART_VISIT
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
     ,count(*) as sessions
     ,count(distinct CUSTOMER_ID) as customers
     ,count(distinct VISITOR_ID) as visitors
     ,COUNT(distinct case when test_type in ('membership','Sorting Hat, Persistent','Membership Detail','Customer Detail') then customer_id
                          when test_type in ('session','Session Detail','Sorting Hat, Non Persistent','Builder') then session_id end) as test_starts_actual
     ,sum(skip_quiz_actions) as skip_quiz_actions
     ,sum(quiz_starts) as quiz_starts
     ,sum(quiz_completes) as quiz_completes
     ,sum(lead_registrations) as lead_registrations
     ,sum(quiz_lead_registrations) as quiz_lead_registrations
     ,sum(speedy_lead_registrations) as speedy_lead_registrations
     ,sum(skip_quiz_lead_registrations) as skip_quiz_lead_registrations
     ,SUM(vip_activations_1hr) as vip_activations_1hr
     ,SUM(vip_activations_3hr) as vip_activations_3hr
     ,sum(vip_activations_24hr) as vip_activations_24hr
     ,SUM(vip_activations) AS vip_activations
     ,sum(VIP_ACTIVATIONS_SAME_SESSION) as VIP_ACTIVATIONS_SAME_SESSION
     ,sum(VIP_ACTIVATIONS_BY_LEAD_REG) as VIP_ACTIVATIONS_BY_LEAD_REG
     ,SUM(units_activating) AS units_activating
     ,sum(product_subtotal_excl_tariff_activating) as product_subtotal_excl_tariff_activating
     ,sum(product_subtotal_incl_tariff_activating) as product_subtotal_incl_tariff_activating
     ,SUM(product_discount_activating) as product_discount_activating
     ,SUM(shipping_discount_activating) as shipping_discount_activating
     ,SUM(total_discount_activating) as total_discount_activating
     ,SUM(shipping_revenue_activating) as shipping_revenue_activating
     ,SUM(tariff_surcharge_activating) as tariff_surcharge_activating
     ,SUM(tax_activating) as tax_activating
     ,SUM(product_gross_revenue_activating) AS product_gross_revenue_activating
     ,SUM(product_gross_revenue_excl_shipping_activating) AS product_gross_revenue_excl_shipping_activating
     ,sum(product_margin_pre_return_activating) AS product_margin_pre_return_activating
     ,sum(cash_gross_revenue_activating) AS cash_gross_revenue_activating
     ,sum(cash_credit_activating) AS cash_credit_activating
     ,sum(noncash_credit_activating) AS noncash_credit_activating
     ,sum(product_cost_activating) AS product_cost_activating
     ,sum(landed_cost_activating)  AS landed_cost_activating
     ,sum(shipping_cost_activating)  AS shipping_cost_activating
     ,sum(total_cogs_activating) AS total_cogs_activating
     ,sum(prepaid_creditcard_orders_activating) as prepaid_creditcard_orders_activating
     ,sum(prepaid_creditcard_failed_attempts_activating) as prepaid_creditcard_failed_attempts_activating
     ,sum(failed_orders_activating) as failed_orders_activating
     ,SUM(orders_nonactivating) AS orders_nonactivating
     ,SUM(units_nonactivating) AS units_nonactivating
     ,SUM(product_subtotal_excl_tariff_nonactivating) as product_subtotal_excl_tariff_nonactivating
     ,SUM(product_subtotal_incl_tariff_nonactivating) as product_subtotal_incl_tariff_nonactivating
     ,SUM(product_discount_nonactivating) as product_discount_nonactivating
     ,SUM(shipping_discount_nonactivating) as shipping_discount_nonactivating
     ,SUM(total_discount_nonactivating) as total_discount_nonactivating
     ,SUM(shipping_revenue_nonactivating) as shipping_revenue_nonactivating
     ,SUM(tariff_surcharge_nonactivating) as tariff_surcharge_nonactivating
     ,SUM(tax_nonactivating) as tax_nonactivating
     ,SUM(product_gross_revenue_nonactivating) AS product_gross_revenue_nonactivating
     ,SUM(product_gross_revenue_excl_shipping_nonactivating) AS product_gross_revenue_excl_shipping_nonactivating
     ,SUM(product_margin_pre_return_nonactivating) AS product_margin_pre_return_nonactivating
     ,SUM(cash_gross_revenue_nonactivating) AS cash_gross_revenue_nonactivating
     ,SUM(cash_credit_nonactivating) AS cash_credit_nonactivating
     ,SUM(noncash_credit_nonactivating) AS noncash_credit_nonactivating
     ,SUM(product_cost_nonactivating) AS product_cost_nonactivating
     ,SUM(landed_cost_nonactivating) AS landed_cost_nonactivating
     ,SUM(shipping_cost_nonactivating) AS shipping_cost_nonactivating
     ,SUM(total_cogs_nonactivating) AS total_cogs_nonactivating
     ,SUM(membership_credit_redeemed_amount_nonactivating) as membership_credit_redeemed_amount_nonactivating
     ,SUM(membership_credits_redeemed_count_nonactivating) as membership_credits_token_redeemed_count_nonactivating
     ,SUM(prepaid_creditcard_orders_nonactivating) as prepaid_creditcard_orders_nonactivating
     ,SUM(prepaid_creditcard_failed_attempts_nonactivating) as prepaid_creditcard_failed_attempts_nonactivating
     ,SUM(failed_orders_nonactivating) as failed_orders_nonactivating
     ,SUM(orders_total) AS orders_total
     ,SUM(units_total) AS units_total
     ,SUM(product_subtotal_excl_tariff_total) as product_subtotal_excl_tariff_total
     ,SUM(product_subtotal_incl_tariff_total) as product_subtotal_incl_tariff_total
     ,SUM(product_discount_total) as product_discount_total
     ,SUM(shipping_discount_total) as shipping_discount_total
     ,SUM(total_discount_total) as total_discount_total
     ,SUM(shipping_revenue_total) as shipping_revenue_total
     ,SUM(tariff_surcharge_total) as tariff_surcharge_total
     ,SUM(tax_total) as tax_total
     ,SUM(product_gross_revenue_total) AS product_gross_revenue_total
     ,SUM(product_gross_revenue_excl_shipping_total) AS product_gross_revenue_excl_shipping_total
     ,SUM(product_margin_pre_return_total) AS product_margin_pre_return_total
     ,SUM(cash_gross_revenue_total) AS cash_gross_revenue_total
     ,SUM(cash_credit_amount_total) AS cash_credit_amount_total
     ,SUM(noncash_credit_amount_total) AS noncash_credit_amount_total
     ,SUM(product_cost_total) AS product_cost_total
     ,SUM(landed_cost_total) AS landed_cost_total
     ,SUM(shipping_cost_total) AS shipping_cost_total
     ,SUM(total_cogs_total) AS total_cogs_total
     ,SUM(prepaid_creditcard_orders_total) as prepaid_creditcard_orders_total
     ,SUM(prepaid_creditcard_failed_attempts_total) as prepaid_creditcard_failed_attempts_total
     ,SUM(failed_orders_total) as failed_orders_total
     ,SUM(membership_cancellations) as membership_cancellations
     ,SUM(online_membership_cancellations) as online_membership_cancellations
     ,SUM(membership_pauses) as membership_pauses
     ,SUM(distinct_monthly_vip_customers) as distinct_monthly_vip_customers
     ,SUM(login_actions) as login_actions
     ,count(case when IS_SHOPPING_GRID_VISIT = true then session_id end) as shopping_grid_actions
     ,count(case when IS_PDP_VISIT = true then session_id end) as pdp_actions
     ,count(case when is_atb_action = true then session_id end) as atb_actions
     ,sum(cart_actions) as cart_actions
     ,SUM(skip_actions) as skip_actions
     ,SUM(flag_1_actions) as flag_1_actions
     ,SUM(flag_2_actions) as flag_2_actions
     ,SUM(flag_3_actions) as flag_3_actions
     ,SUM(CUSTOM_FLAG_ACTIONS) AS custom_flag_actions
     ,SUM(custom_flag_2_actions) AS custom_flag_2_actions
     ,$last_refreshed_datetime_hq as last_refreshed_datetime_hq
--'individual region and individual platform'
FROM reporting_base_prod.shared.session_order_ab_test_detail as d
join _last_3_months as ll on ll.test_framework_id = d.test_framework_id
    and ll.TEST_KEY = d.TEST_KEY
    and ll.test_label = d.test_label
    and ll.store_brand = d.store_brand
left join (select test_key
             ,count(distinct case when STORE_REGION = 'NA' then STORE_COUNTRY end) as na_store_count
             ,count(distinct case when STORE_REGION = 'EU' then STORE_COUNTRY end) as eu_store_count
        from REPORTING_BASE_PROD.SHARED.SESSION_ORDER_AB_TEST_DETAIL group by 1) as t1 on t1.TEST_KEY = d.test_key
left join _builder_test_labels as u on u.builder_id = d.test_key
where
    test_group is not null
    and d.STORE_BRAND not in ('FabKids','ShoeDazzle','Yitty')
    and ((t1.na_store_count > 0 or t1.eu_store_count > 1) and (t1.na_store_count <> 1 or t1.eu_store_count <> 0))
    and convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME)::date>=$process_from_date --comment
group by
    d.TEST_KEY
       ,d.TEST_FRAMEWORK_ID
       ,coalesce(u.builder_test_label_adj,d.test_label)
       ,TEST_FRAMEWORK_DESCRIPTION
       ,TEST_FRAMEWORK_TICKET
       ,d.CAMPAIGN_CODE
       ,TEST_TYPE
       ,test_activated_datetime
       ,case when TEST_GROUP ilike 'Control%' THEN 'Control' else 'Variant' end
       ,TEST_GROUP
       ,GROUP_NAME
       ,TRAFFIC_SPLIT_TYPE
       ,TEST_GROUP_DESCRIPTION
       ,AB_TEST_START_LOCAL_DATETIME::date
       ,d.STORE_BRAND
       ,concat(d.STORE_REGION,' TTL')
       ,concat(d.STORE_REGION,' TTL')
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
--      ,'Temp N/A' as MEMBERSHIP_PRICE
       ,MEMBERSHIP_TYPE
--      ,'Temp N/A' as IS_MOBILE_APP_USER
--      ,'Temp N/A' as IS_FREE_TRIAL
--      ,'Temp N/A' as IS_CROSS_PROMO
--      ,'Temp N/A' as IS_MALE_GATEWAY
--      ,'Temp N/A' as IS_MALE_SESSION_ACTION
       ,IS_MALE_SESSION
       ,IS_MALE_CUSTOMER
       ,IS_MALE_SESSION_CUSTOMER
       ,PLATFORM
       ,OS
--      ,'Temp N/A' as BROWSER
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then CHANNEL else 'Null - Non Prospect' end
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then SUBCHANNEL else 'Null - Non Prospect' end
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then DM_GATEWAY_ID else null end
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then GATEWAY_NAME else 'Null - Non Prospect' end
       ,coalesce(is_login_action,false)::BOOLEAN
       ,coalesce(IS_SHOPPING_GRID_VISIT,false)::BOOLEAN
       ,coalesce(IS_PDP_VISIT,null)::BOOLEAN
       ,coalesce(IS_ATB_ACTION,null)::BOOLEAN
       ,coalesce(IS_CART_VISIT,null)::BOOLEAN
       ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN
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

union all

SELECT
    'individual region and total platform' as version
     ,d.TEST_KEY
     ,d.TEST_FRAMEWORK_ID
     ,coalesce(u.builder_test_label_adj,d.test_label) as test_label
     ,TEST_FRAMEWORK_DESCRIPTION
     ,TEST_FRAMEWORK_TICKET
     ,d.CAMPAIGN_CODE
     ,TEST_TYPE
     ,test_activated_datetime
     ,case when d.TEST_GROUP ilike 'Control%' THEN 'Control' else 'Variant' end as TEST_GROUP_TYPE
     ,d.TEST_GROUP
     ,GROUP_NAME
     ,TRAFFIC_SPLIT_TYPE
     ,sc.TEST_GROUP_DESCRIPTION
     ,AB_TEST_START_LOCAL_DATETIME::date as ab_test_start_local_date
     ,d.STORE_BRAND
     ,concat(d.STORE_REGION,' TTL')  as STORE_REGION
     ,concat(d.STORE_REGION,' TTL') as STORE_COUNTRY
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
     ,NULL as MEMBERSHIP_PRICE
     ,MEMBERSHIP_TYPE
     ,NULL as IS_MOBILE_APP_USER
     ,NULL as IS_FREE_TRIAL
     ,NULL as IS_CROSS_PROMO
     ,NULL as IS_MALE_GATEWAY
     ,NULL as IS_MALE_SESSION_ACTION
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
     ,coalesce(is_login_action,false)::BOOLEAN AS is_login_action
     ,coalesce(IS_SHOPPING_GRID_VISIT,false)::BOOLEAN as IS_SHOPPING_GRID_VISIT
     ,coalesce(IS_PDP_VISIT,null)::BOOLEAN AS IS_PDP_VISIT
     ,coalesce(IS_ATB_ACTION,null)::BOOLEAN AS IS_ATB_ACTION
     ,coalesce(IS_CART_VISIT,null)::BOOLEAN AS IS_CART_VISIT
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
     ,count(*) as sessions
     ,count(distinct CUSTOMER_ID) as customers
     ,count(distinct VISITOR_ID) as visitors
     ,COUNT(distinct case when test_type in ('membership','Sorting Hat, Persistent','Membership Detail','Customer Detail') then customer_id
                          when test_type in ('session','Session Detail','Sorting Hat, Non Persistent','Builder') then session_id end) as test_starts_actual
     ,sum(skip_quiz_actions) as skip_quiz_actions
     ,sum(quiz_starts) as quiz_starts
     ,sum(quiz_completes) as quiz_completes
     ,sum(lead_registrations) as lead_registrations
     ,sum(quiz_lead_registrations) as quiz_lead_registrations
     ,sum(speedy_lead_registrations) as speedy_lead_registrations
     ,sum(skip_quiz_lead_registrations) as skip_quiz_lead_registrations
     ,SUM(vip_activations_1hr) as vip_activations_1hr
     ,SUM(vip_activations_3hr) as vip_activations_3hr
     ,sum(vip_activations_24hr) as vip_activations_24hr
     ,SUM(vip_activations) AS vip_activations
     ,sum(VIP_ACTIVATIONS_SAME_SESSION) as VIP_ACTIVATIONS_SAME_SESSION
     ,sum(VIP_ACTIVATIONS_BY_LEAD_REG) as VIP_ACTIVATIONS_BY_LEAD_REG
     ,SUM(units_activating) AS units_activating
     ,sum(product_subtotal_excl_tariff_activating) as product_subtotal_excl_tariff_activating
     ,sum(product_subtotal_incl_tariff_activating) as product_subtotal_incl_tariff_activating
     ,SUM(product_discount_activating) as product_discount_activating
     ,SUM(shipping_discount_activating) as shipping_discount_activating
     ,SUM(total_discount_activating) as total_discount_activating
     ,SUM(shipping_revenue_activating) as shipping_revenue_activating
     ,SUM(tariff_surcharge_activating) as tariff_surcharge_activating
     ,SUM(tax_activating) as tax_activating
     ,SUM(product_gross_revenue_activating) AS product_gross_revenue_activating
     ,SUM(product_gross_revenue_excl_shipping_activating) AS product_gross_revenue_excl_shipping_activating
     ,sum(product_margin_pre_return_activating) AS product_margin_pre_return_activating
     ,sum(cash_gross_revenue_activating) AS cash_gross_revenue_activating
     ,sum(cash_credit_activating) AS cash_credit_activating
     ,sum(noncash_credit_activating) AS noncash_credit_activating
     ,sum(product_cost_activating) AS product_cost_activating
     ,sum(landed_cost_activating)  AS landed_cost_activating
     ,sum(shipping_cost_activating)  AS shipping_cost_activating
     ,sum(total_cogs_activating) AS total_cogs_activating
     ,sum(prepaid_creditcard_orders_activating) as prepaid_creditcard_orders_activating
     ,sum(prepaid_creditcard_failed_attempts_activating) as prepaid_creditcard_failed_attempts_activating
     ,sum(failed_orders_activating) as failed_orders_activating
     ,SUM(orders_nonactivating) AS orders_nonactivating
     ,SUM(units_nonactivating) AS units_nonactivating
     ,SUM(product_subtotal_excl_tariff_nonactivating) as product_subtotal_excl_tariff_nonactivating
     ,SUM(product_subtotal_incl_tariff_nonactivating) as product_subtotal_incl_tariff_nonactivating
     ,SUM(product_discount_nonactivating) as product_discount_nonactivating
     ,SUM(shipping_discount_nonactivating) as shipping_discount_nonactivating
     ,SUM(total_discount_nonactivating) as total_discount_nonactivating
     ,SUM(shipping_revenue_nonactivating) as shipping_revenue_nonactivating
     ,SUM(tariff_surcharge_nonactivating) as tariff_surcharge_nonactivating
     ,SUM(tax_nonactivating) as tax_nonactivating
     ,SUM(product_gross_revenue_nonactivating) AS product_gross_revenue_nonactivating
     ,SUM(product_gross_revenue_excl_shipping_nonactivating) AS product_gross_revenue_excl_shipping_nonactivating
     ,SUM(product_margin_pre_return_nonactivating) AS product_margin_pre_return_nonactivating
     ,SUM(cash_gross_revenue_nonactivating) AS cash_gross_revenue_nonactivating
     ,SUM(cash_credit_nonactivating) AS cash_credit_nonactivating
     ,SUM(noncash_credit_nonactivating) AS noncash_credit_nonactivating
     ,SUM(product_cost_nonactivating) AS product_cost_nonactivating
     ,SUM(landed_cost_nonactivating) AS landed_cost_nonactivating
     ,SUM(shipping_cost_nonactivating) AS shipping_cost_nonactivating
     ,SUM(total_cogs_nonactivating) AS total_cogs_nonactivating
     ,SUM(membership_credit_redeemed_amount_nonactivating) as membership_credit_redeemed_amount_nonactivating
     ,SUM(membership_credits_redeemed_count_nonactivating) as membership_credits_token_redeemed_count_nonactivating
     ,SUM(prepaid_creditcard_orders_nonactivating) as prepaid_creditcard_orders_nonactivating
     ,SUM(prepaid_creditcard_failed_attempts_nonactivating) as prepaid_creditcard_failed_attempts_nonactivating
     ,SUM(failed_orders_nonactivating) as failed_orders_nonactivating
     ,SUM(orders_total) AS orders_total
     ,SUM(units_total) AS units_total
     ,SUM(product_subtotal_excl_tariff_total) as product_subtotal_excl_tariff_total
     ,SUM(product_subtotal_incl_tariff_total) as product_subtotal_incl_tariff_total
     ,SUM(product_discount_total) as product_discount_total
     ,SUM(shipping_discount_total) as shipping_discount_total
     ,SUM(total_discount_total) as total_discount_total
     ,SUM(shipping_revenue_total) as shipping_revenue_total
     ,SUM(tariff_surcharge_total) as tariff_surcharge_total
     ,SUM(tax_total) as tax_total
     ,SUM(product_gross_revenue_total) AS product_gross_revenue_total
     ,SUM(product_gross_revenue_excl_shipping_total) AS product_gross_revenue_excl_shipping_total
     ,SUM(product_margin_pre_return_total) AS product_margin_pre_return_total
     ,SUM(cash_gross_revenue_total) AS cash_gross_revenue_total
     ,SUM(cash_credit_amount_total) AS cash_credit_amount_total
     ,SUM(noncash_credit_amount_total) AS noncash_credit_amount_total
     ,SUM(product_cost_total) AS product_cost_total
     ,SUM(landed_cost_total) AS landed_cost_total
     ,SUM(shipping_cost_total) AS shipping_cost_total
     ,SUM(total_cogs_total) AS total_cogs_total
     ,SUM(prepaid_creditcard_orders_total) as prepaid_creditcard_orders_total
     ,SUM(prepaid_creditcard_failed_attempts_total) as prepaid_creditcard_failed_attempts_total
     ,SUM(failed_orders_total) as failed_orders_total
     ,SUM(membership_cancellations) as membership_cancellations
     ,SUM(online_membership_cancellations) as online_membership_cancellations
     ,SUM(membership_pauses) as membership_pauses
     ,SUM(distinct_monthly_vip_customers) as distinct_monthly_vip_customers
     ,SUM(login_actions) as login_actions
     ,count(case when IS_SHOPPING_GRID_VISIT = true then session_id end) as shopping_grid_actions
     ,count(case when IS_PDP_VISIT = true then session_id end) as pdp_actions
     ,count(case when is_atb_action = true then session_id end) as atb_actions
     ,sum(cart_actions) as cart_actions
     ,SUM(skip_actions) as skip_actions
     ,SUM(flag_1_actions) as flag_1_actions
     ,SUM(flag_2_actions) as flag_2_actions
     ,SUM(flag_3_actions) as flag_3_actions
     ,SUM(CUSTOM_FLAG_ACTIONS) AS custom_flag_actions
     ,SUM(custom_flag_2_actions) AS custom_flag_2_actions
     ,$last_refreshed_datetime_hq as last_refreshed_datetime_hq
--'individual region and total platform'
FROM reporting_base_prod.shared.session_order_ab_test_detail as d
join _last_3_months as ll on ll.test_framework_id = d.test_framework_id
    and ll.TEST_KEY = d.TEST_KEY
    and ll.test_label = d.test_label
    and ll.store_brand = d.store_brand
left join _total_descriptions as sc on d.test_key = sc.test_key
    and d.test_framework_id = sc.test_framework_id
    and d.test_label = sc.test_label
    and d.store_brand = sc.store_brand
    AND d.test_group = sc.test_group
left join (select test_key
             ,count(distinct case when STORE_REGION = 'NA' then STORE_COUNTRY end) as na_store_count
             ,count(distinct case when STORE_REGION = 'EU' then STORE_COUNTRY end) as eu_store_count
        from REPORTING_BASE_PROD.SHARED.SESSION_ORDER_AB_TEST_DETAIL group by 1) as t1 on t1.TEST_KEY = d.test_key
left join _builder_test_labels as u on u.builder_id = d.test_key
where
    d.test_group is not null
    and d.STORE_BRAND not in ('FabKids','ShoeDazzle','Yitty')
    and (d.TEST_KEY ilike '%(User%'
        or (test_type in ('membership','session') and d.test_key in (select distinct test_key from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA where DEVICE in ('Desktop & Mobile','Desktop & App','Mobile & App','Desktop, Mobile & App')))
        or (test_type in ('Builder','Session Detail','Customer Detail','Membership Detail') and d.test_key in (select distinct test_key from lake_view.sharepoint.ab_test_metadata_import_oof where test_platforms in ('Desktop & Mobile','Mobile & App','ALL','Desktop and Mobile')))
        or (test_type in ('Builder','Session Detail','Customer Detail','Membership Detail') and d.test_key in (select distinct concat(test_key,' (PSRC)') as test_key from lake_view.sharepoint.ab_test_metadata_import_oof where test_platforms in ('Desktop & Mobile','Mobile & App','ALL','Desktop and Mobile')))
        or (test_type ilike 'Sorting Hat%' and d.test_key in (select distinct meta_original_metadata_test_id from _sorting_hat_metadata_ids where test_platforms in ('Desktop & Mobile','Mobile & App','ALL','Desktop and Mobile'))))
    and ((t1.na_store_count > 0 or t1.eu_store_count > 1) and (t1.na_store_count <> 1 or t1.eu_store_count <> 0))
    and convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME)::date>=$process_from_date --comment
group by
    d.TEST_KEY
       ,d.TEST_FRAMEWORK_ID
       ,coalesce(u.builder_test_label_adj,d.test_label)
       ,TEST_FRAMEWORK_DESCRIPTION
       ,TEST_FRAMEWORK_TICKET
       ,d.CAMPAIGN_CODE
       ,TEST_TYPE
       ,test_activated_datetime
       ,case when d.TEST_GROUP ilike 'Control%' THEN 'Control' else 'Variant' end
       ,d.TEST_GROUP
       ,GROUP_NAME
       ,TRAFFIC_SPLIT_TYPE
       ,sc.TEST_GROUP_DESCRIPTION
       ,AB_TEST_START_LOCAL_DATETIME::date
       ,d.STORE_BRAND
       ,concat(d.STORE_REGION,' TTL')
       ,concat(d.STORE_REGION,' TTL')
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
--      ,'Temp N/A' as MEMBERSHIP_PRICE
       ,MEMBERSHIP_TYPE
--      ,'Temp N/A' as IS_MOBILE_APP_USER
--      ,'Temp N/A' as IS_FREE_TRIAL
--      ,'Temp N/A' as IS_CROSS_PROMO
--      ,'Temp N/A' as IS_MALE_GATEWAY
--      ,'Temp N/A' as IS_MALE_SESSION_ACTION
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
       ,coalesce(is_login_action,false)::BOOLEAN
       ,coalesce(IS_SHOPPING_GRID_VISIT,false)::BOOLEAN
       ,coalesce(IS_PDP_VISIT,null)::BOOLEAN
       ,coalesce(IS_ATB_ACTION,null)::BOOLEAN
       ,coalesce(IS_CART_VISIT,null)::BOOLEAN
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

union all

SELECT
    'total region and individual platform' as version
     ,d.TEST_KEY
     ,d.TEST_FRAMEWORK_ID
     ,coalesce(u.builder_test_label_adj,d.test_label) as test_label
     ,TEST_FRAMEWORK_DESCRIPTION
     ,TEST_FRAMEWORK_TICKET
     ,d.CAMPAIGN_CODE
     ,TEST_TYPE
     ,test_activated_datetime
     ,case when TEST_GROUP ilike 'Control%' THEN 'Control' else 'Variant' end as TEST_GROUP_TYPE
     ,TEST_GROUP
     ,GROUP_NAME
     ,TRAFFIC_SPLIT_TYPE
     ,TEST_GROUP_DESCRIPTION
     ,AB_TEST_START_LOCAL_DATETIME::date as ab_test_start_local_date
     ,d.STORE_BRAND
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
     ,NULL as MEMBERSHIP_PRICE
     ,MEMBERSHIP_TYPE
     ,NULL as IS_MOBILE_APP_USER
     ,NULL as IS_FREE_TRIAL
     ,NULL as IS_CROSS_PROMO
     ,NULL as IS_MALE_GATEWAY
     ,NULL as IS_MALE_SESSION_ACTION
     ,IS_MALE_SESSION
     ,IS_MALE_CUSTOMER
     ,IS_MALE_SESSION_CUSTOMER
     ,PLATFORM
     ,OS
     ,NULL as BROWSER
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then CHANNEL else 'Null - Non Prospect' end as CHANNEL
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then SUBCHANNEL else 'Null - Non Prospect' end as SUBCHANNEL
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then DM_GATEWAY_ID else null end as DM_GATEWAY_ID
     ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then GATEWAY_NAME else 'Null - Non Prospect' end as GATEWAY_NAME
     ,coalesce(is_login_action,false)::BOOLEAN AS is_login_action
     ,coalesce(IS_SHOPPING_GRID_VISIT,false)::BOOLEAN as IS_SHOPPING_GRID_VISIT
     ,coalesce(IS_PDP_VISIT,null)::BOOLEAN AS IS_PDP_VISIT
     ,coalesce(IS_ATB_ACTION,null)::BOOLEAN AS IS_ATB_ACTION
     ,coalesce(IS_CART_VISIT,null)::BOOLEAN AS IS_CART_VISIT
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
     ,count(*) as sessions
     ,count(distinct CUSTOMER_ID) as customers
     ,count(distinct VISITOR_ID) as visitors
     ,COUNT(distinct case when test_type in ('membership','Sorting Hat, Persistent','Membership Detail','Customer Detail') then customer_id
                          when test_type in ('session','Session Detail','Sorting Hat, Non Persistent','Builder') then session_id end) as test_starts_actual
     ,sum(skip_quiz_actions) as skip_quiz_actions
     ,sum(quiz_starts) as quiz_starts
     ,sum(quiz_completes) as quiz_completes
     ,sum(lead_registrations) as lead_registrations
     ,sum(quiz_lead_registrations) as quiz_lead_registrations
     ,sum(speedy_lead_registrations) as speedy_lead_registrations
     ,sum(skip_quiz_lead_registrations) as skip_quiz_lead_registrations
     ,SUM(vip_activations_1hr) as vip_activations_1hr
     ,SUM(vip_activations_3hr) as vip_activations_3hr
     ,sum(vip_activations_24hr) as vip_activations_24hr
     ,SUM(vip_activations) AS vip_activations
     ,sum(VIP_ACTIVATIONS_SAME_SESSION) as VIP_ACTIVATIONS_SAME_SESSION
     ,sum(VIP_ACTIVATIONS_BY_LEAD_REG) as VIP_ACTIVATIONS_BY_LEAD_REG
     ,SUM(units_activating) AS units_activating
     ,sum(product_subtotal_excl_tariff_activating) as product_subtotal_excl_tariff_activating
     ,sum(product_subtotal_incl_tariff_activating) as product_subtotal_incl_tariff_activating
     ,SUM(product_discount_activating) as product_discount_activating
     ,SUM(shipping_discount_activating) as shipping_discount_activating
     ,SUM(total_discount_activating) as total_discount_activating
     ,SUM(shipping_revenue_activating) as shipping_revenue_activating
     ,SUM(tariff_surcharge_activating) as tariff_surcharge_activating
     ,SUM(tax_activating) as tax_activating
     ,SUM(product_gross_revenue_activating) AS product_gross_revenue_activating
     ,SUM(product_gross_revenue_excl_shipping_activating) AS product_gross_revenue_excl_shipping_activating
     ,sum(product_margin_pre_return_activating) AS product_margin_pre_return_activating
     ,sum(cash_gross_revenue_activating) AS cash_gross_revenue_activating
     ,sum(cash_credit_activating) AS cash_credit_activating
     ,sum(noncash_credit_activating) AS noncash_credit_activating
     ,sum(product_cost_activating) AS product_cost_activating
     ,sum(landed_cost_activating)  AS landed_cost_activating
     ,sum(shipping_cost_activating)  AS shipping_cost_activating
     ,sum(total_cogs_activating) AS total_cogs_activating
     ,sum(prepaid_creditcard_orders_activating) as prepaid_creditcard_orders_activating
     ,sum(prepaid_creditcard_failed_attempts_activating) as prepaid_creditcard_failed_attempts_activating
     ,sum(failed_orders_activating) as failed_orders_activating
     ,SUM(orders_nonactivating) AS orders_nonactivating
     ,SUM(units_nonactivating) AS units_nonactivating
     ,SUM(product_subtotal_excl_tariff_nonactivating) as product_subtotal_excl_tariff_nonactivating
     ,SUM(product_subtotal_incl_tariff_nonactivating) as product_subtotal_incl_tariff_nonactivating
     ,SUM(product_discount_nonactivating) as product_discount_nonactivating
     ,SUM(shipping_discount_nonactivating) as shipping_discount_nonactivating
     ,SUM(total_discount_nonactivating) as total_discount_nonactivating
     ,SUM(shipping_revenue_nonactivating) as shipping_revenue_nonactivating
     ,SUM(tariff_surcharge_nonactivating) as tariff_surcharge_nonactivating
     ,SUM(tax_nonactivating) as tax_nonactivating
     ,SUM(product_gross_revenue_nonactivating) AS product_gross_revenue_nonactivating
     ,SUM(product_gross_revenue_excl_shipping_nonactivating) AS product_gross_revenue_excl_shipping_nonactivating
     ,SUM(product_margin_pre_return_nonactivating) AS product_margin_pre_return_nonactivating
     ,SUM(cash_gross_revenue_nonactivating) AS cash_gross_revenue_nonactivating
     ,SUM(cash_credit_nonactivating) AS cash_credit_nonactivating
     ,SUM(noncash_credit_nonactivating) AS noncash_credit_nonactivating
     ,SUM(product_cost_nonactivating) AS product_cost_nonactivating
     ,SUM(landed_cost_nonactivating) AS landed_cost_nonactivating
     ,SUM(shipping_cost_nonactivating) AS shipping_cost_nonactivating
     ,SUM(total_cogs_nonactivating) AS total_cogs_nonactivating
     ,SUM(membership_credit_redeemed_amount_nonactivating) as membership_credit_redeemed_amount_nonactivating
     ,SUM(membership_credits_redeemed_count_nonactivating) as membership_credits_token_redeemed_count_nonactivating
     ,SUM(prepaid_creditcard_orders_nonactivating) as prepaid_creditcard_orders_nonactivating
     ,SUM(prepaid_creditcard_failed_attempts_nonactivating) as prepaid_creditcard_failed_attempts_nonactivating
     ,SUM(failed_orders_nonactivating) as failed_orders_nonactivating
     ,SUM(orders_total) AS orders_total
     ,SUM(units_total) AS units_total
     ,SUM(product_subtotal_excl_tariff_total) as product_subtotal_excl_tariff_total
     ,SUM(product_subtotal_incl_tariff_total) as product_subtotal_incl_tariff_total
     ,SUM(product_discount_total) as product_discount_total
     ,SUM(shipping_discount_total) as shipping_discount_total
     ,SUM(total_discount_total) as total_discount_total
     ,SUM(shipping_revenue_total) as shipping_revenue_total
     ,SUM(tariff_surcharge_total) as tariff_surcharge_total
     ,SUM(tax_total) as tax_total
     ,SUM(product_gross_revenue_total) AS product_gross_revenue_total
     ,SUM(product_gross_revenue_excl_shipping_total) AS product_gross_revenue_excl_shipping_total
     ,SUM(product_margin_pre_return_total) AS product_margin_pre_return_total
     ,SUM(cash_gross_revenue_total) AS cash_gross_revenue_total
     ,SUM(cash_credit_amount_total) AS cash_credit_amount_total
     ,SUM(noncash_credit_amount_total) AS noncash_credit_amount_total
     ,SUM(product_cost_total) AS product_cost_total
     ,SUM(landed_cost_total) AS landed_cost_total
     ,SUM(shipping_cost_total) AS shipping_cost_total
     ,SUM(total_cogs_total) AS total_cogs_total
     ,SUM(prepaid_creditcard_orders_total) as prepaid_creditcard_orders_total
     ,SUM(prepaid_creditcard_failed_attempts_total) as prepaid_creditcard_failed_attempts_total
     ,SUM(failed_orders_total) as failed_orders_total
     ,SUM(membership_cancellations) as membership_cancellations
     ,SUM(online_membership_cancellations) as online_membership_cancellations
     ,SUM(membership_pauses) as membership_pauses
     ,SUM(distinct_monthly_vip_customers) as distinct_monthly_vip_customers
     ,SUM(login_actions) as login_actions
     ,count(case when IS_SHOPPING_GRID_VISIT = true then session_id end) as shopping_grid_actions
     ,count(case when IS_PDP_VISIT = true then session_id end) as pdp_actions
     ,count(case when is_atb_action = true then session_id end) as atb_actions
     ,sum(cart_actions) as cart_actions
     ,SUM(skip_actions) as skip_actions
     ,SUM(flag_1_actions) as flag_1_actions
     ,SUM(flag_2_actions) as flag_2_actions
     ,SUM(flag_3_actions) as flag_3_actions
     ,SUM(CUSTOM_FLAG_ACTIONS) AS custom_flag_actions
     ,SUM(custom_flag_2_actions) AS custom_flag_2_actions
     ,$last_refreshed_datetime_hq as last_refreshed_datetime_hq
--'total region and individual platform'
FROM reporting_base_prod.shared.session_order_ab_test_detail as d
join _last_3_months as ll on ll.test_framework_id = d.test_framework_id
    and ll.TEST_KEY = d.TEST_KEY
    and ll.test_label = d.test_label
    and ll.store_brand = d.store_brand
left join (select test_key,count(distinct STORE_REGION) as region_count from REPORTING_BASE_PROD.SHARED.SESSION_ORDER_AB_TEST_DETAIL group by 1) as t1 on t1.TEST_KEY = d.test_key
left join _builder_test_labels as u on u.builder_id = d.test_key
where
    test_group is not null
    and d.store_brand not in ('FabKids','ShoeDazzle','Yitty')
    and t1.region_count > 1
    and convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME)::date>=$process_from_date --comment
group by
    d.TEST_KEY
       ,d.TEST_FRAMEWORK_ID
       ,coalesce(u.builder_test_label_adj,d.test_label)
       ,TEST_FRAMEWORK_DESCRIPTION
       ,TEST_FRAMEWORK_TICKET
       ,d.CAMPAIGN_CODE
       ,TEST_TYPE
       ,test_activated_datetime
       ,case when TEST_GROUP ilike 'Control%' THEN 'Control' else 'Variant' end
       ,TEST_GROUP
       ,GROUP_NAME
       ,TRAFFIC_SPLIT_TYPE
       ,TEST_GROUP_DESCRIPTION
       ,AB_TEST_START_LOCAL_DATETIME::date
       ,d.STORE_BRAND
--      ,'NA+EU TTL' as STORE_REGION
--      ,'NA+EU TTL' as STORE_COUNTRY
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
--      ,'Temp N/A' as MEMBERSHIP_PRICE
       ,MEMBERSHIP_TYPE
--      ,'Temp N/A' as IS_MOBILE_APP_USER
--      ,'Temp N/A' as IS_FREE_TRIAL
--      ,'Temp N/A' as IS_CROSS_PROMO
--      ,'Temp N/A' as IS_MALE_GATEWAY
--      ,'Temp N/A' as IS_MALE_SESSION_ACTION
       ,IS_MALE_SESSION
       ,IS_MALE_CUSTOMER
       ,IS_MALE_SESSION_CUSTOMER
       ,PLATFORM
       ,OS
--      ,'Temp N/A' as BROWSER
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then CHANNEL else 'Null - Non Prospect' end
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then SUBCHANNEL else 'Null - Non Prospect' end
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then DM_GATEWAY_ID else null end
       ,case when TEST_START_MEMBERSHIP_STATE = 'Prospect' then GATEWAY_NAME else 'Null - Non Prospect' end
       ,coalesce(is_login_action,false)::BOOLEAN
       ,coalesce(IS_SHOPPING_GRID_VISIT,false)::BOOLEAN
       ,coalesce(IS_PDP_VISIT,null)::BOOLEAN
       ,coalesce(IS_ATB_ACTION,null)::BOOLEAN
       ,coalesce(IS_CART_VISIT,null)::BOOLEAN
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

union all

SELECT
    'total region and total platform' as version
     ,d.TEST_KEY
     ,d.TEST_FRAMEWORK_ID
     ,coalesce(u.builder_test_label_adj,d.test_label) as test_label
     ,TEST_FRAMEWORK_DESCRIPTION
     ,TEST_FRAMEWORK_TICKET
     ,d.CAMPAIGN_CODE
     ,TEST_TYPE
     ,test_activated_datetime
     ,case when d.TEST_GROUP ilike 'Control%' THEN 'Control' else 'Variant' end as TEST_GROUP_TYPE
     ,d.TEST_GROUP
     ,GROUP_NAME
     ,TRAFFIC_SPLIT_TYPE
     ,sc.TEST_GROUP_DESCRIPTION
     ,AB_TEST_START_LOCAL_DATETIME::date as ab_test_start_local_date
     ,d.STORE_BRAND
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
     ,NULL as MEMBERSHIP_PRICE
     ,MEMBERSHIP_TYPE
     ,NULL as IS_MOBILE_APP_USER
     ,NULL as IS_FREE_TRIAL
     ,NULL as IS_CROSS_PROMO
     ,NULL as IS_MALE_GATEWAY
     ,NULL as IS_MALE_SESSION_ACTION
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
     ,coalesce(is_login_action,false)::BOOLEAN AS is_login_action
     ,coalesce(IS_SHOPPING_GRID_VISIT,false)::BOOLEAN as IS_SHOPPING_GRID_VISIT
     ,coalesce(IS_PDP_VISIT,null)::BOOLEAN AS IS_PDP_VISIT
     ,coalesce(IS_ATB_ACTION,null)::BOOLEAN AS IS_ATB_ACTION
     ,coalesce(IS_CART_VISIT,null)::BOOLEAN AS IS_CART_VISIT
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
     ,count(*) as sessions
     ,count(distinct CUSTOMER_ID) as customers
     ,count(distinct VISITOR_ID) as visitors
     ,COUNT(distinct case when test_type in ('membership','Sorting Hat, Persistent','Membership Detail','Customer Detail') then customer_id
                          when test_type in ('session','Session Detail','Sorting Hat, Non Persistent','Builder') then session_id end) as test_starts_actual
     ,sum(skip_quiz_actions) as skip_quiz_actions
     ,sum(quiz_starts) as quiz_starts
     ,sum(quiz_completes) as quiz_completes
     ,sum(lead_registrations) as lead_registrations
     ,sum(quiz_lead_registrations) as quiz_lead_registrations
     ,sum(speedy_lead_registrations) as speedy_lead_registrations
     ,sum(skip_quiz_lead_registrations) as skip_quiz_lead_registrations
     ,SUM(vip_activations_1hr) as vip_activations_1hr
     ,SUM(vip_activations_3hr) as vip_activations_3hr
     ,sum(vip_activations_24hr) as vip_activations_24hr
     ,SUM(vip_activations) AS vip_activations
     ,sum(VIP_ACTIVATIONS_SAME_SESSION) as VIP_ACTIVATIONS_SAME_SESSION
     ,sum(VIP_ACTIVATIONS_BY_LEAD_REG) as VIP_ACTIVATIONS_BY_LEAD_REG
     ,SUM(units_activating) AS units_activating
     ,sum(product_subtotal_excl_tariff_activating) as product_subtotal_excl_tariff_activating
     ,sum(product_subtotal_incl_tariff_activating) as product_subtotal_incl_tariff_activating
     ,SUM(product_discount_activating) as product_discount_activating
     ,SUM(shipping_discount_activating) as shipping_discount_activating
     ,SUM(total_discount_activating) as total_discount_activating
     ,SUM(shipping_revenue_activating) as shipping_revenue_activating
     ,SUM(tariff_surcharge_activating) as tariff_surcharge_activating
     ,SUM(tax_activating) as tax_activating
     ,SUM(product_gross_revenue_activating) AS product_gross_revenue_activating
     ,SUM(product_gross_revenue_excl_shipping_activating) AS product_gross_revenue_excl_shipping_activating
     ,sum(product_margin_pre_return_activating) AS product_margin_pre_return_activating
     ,sum(cash_gross_revenue_activating) AS cash_gross_revenue_activating
     ,sum(cash_credit_activating) AS cash_credit_activating
     ,sum(noncash_credit_activating) AS noncash_credit_activating
     ,sum(product_cost_activating) AS product_cost_activating
     ,sum(landed_cost_activating)  AS landed_cost_activating
     ,sum(shipping_cost_activating)  AS shipping_cost_activating
     ,sum(total_cogs_activating) AS total_cogs_activating
     ,sum(prepaid_creditcard_orders_activating) as prepaid_creditcard_orders_activating
     ,sum(prepaid_creditcard_failed_attempts_activating) as prepaid_creditcard_failed_attempts_activating
     ,sum(failed_orders_activating) as failed_orders_activating
     ,SUM(orders_nonactivating) AS orders_nonactivating
     ,SUM(units_nonactivating) AS units_nonactivating
     ,SUM(product_subtotal_excl_tariff_nonactivating) as product_subtotal_excl_tariff_nonactivating
     ,SUM(product_subtotal_incl_tariff_nonactivating) as product_subtotal_incl_tariff_nonactivating
     ,SUM(product_discount_nonactivating) as product_discount_nonactivating
     ,SUM(shipping_discount_nonactivating) as shipping_discount_nonactivating
     ,SUM(total_discount_nonactivating) as total_discount_nonactivating
     ,SUM(shipping_revenue_nonactivating) as shipping_revenue_nonactivating
     ,SUM(tariff_surcharge_nonactivating) as tariff_surcharge_nonactivating
     ,SUM(tax_nonactivating) as tax_nonactivating
     ,SUM(product_gross_revenue_nonactivating) AS product_gross_revenue_nonactivating
     ,SUM(product_gross_revenue_excl_shipping_nonactivating) AS product_gross_revenue_excl_shipping_nonactivating
     ,SUM(product_margin_pre_return_nonactivating) AS product_margin_pre_return_nonactivating
     ,SUM(cash_gross_revenue_nonactivating) AS cash_gross_revenue_nonactivating
     ,SUM(cash_credit_nonactivating) AS cash_credit_nonactivating
     ,SUM(noncash_credit_nonactivating) AS noncash_credit_nonactivating
     ,SUM(product_cost_nonactivating) AS product_cost_nonactivating
     ,SUM(landed_cost_nonactivating) AS landed_cost_nonactivating
     ,SUM(shipping_cost_nonactivating) AS shipping_cost_nonactivating
     ,SUM(total_cogs_nonactivating) AS total_cogs_nonactivating
     ,SUM(membership_credit_redeemed_amount_nonactivating) as membership_credit_redeemed_amount_nonactivating
     ,SUM(membership_credits_redeemed_count_nonactivating) as membership_credits_token_redeemed_count_nonactivating
     ,SUM(prepaid_creditcard_orders_nonactivating) as prepaid_creditcard_orders_nonactivating
     ,SUM(prepaid_creditcard_failed_attempts_nonactivating) as prepaid_creditcard_failed_attempts_nonactivating
     ,SUM(failed_orders_nonactivating) as failed_orders_nonactivating
     ,SUM(orders_total) AS orders_total
     ,SUM(units_total) AS units_total
     ,SUM(product_subtotal_excl_tariff_total) as product_subtotal_excl_tariff_total
     ,SUM(product_subtotal_incl_tariff_total) as product_subtotal_incl_tariff_total
     ,SUM(product_discount_total) as product_discount_total
     ,SUM(shipping_discount_total) as shipping_discount_total
     ,SUM(total_discount_total) as total_discount_total
     ,SUM(shipping_revenue_total) as shipping_revenue_total
     ,SUM(tariff_surcharge_total) as tariff_surcharge_total
     ,SUM(tax_total) as tax_total
     ,SUM(product_gross_revenue_total) AS product_gross_revenue_total
     ,SUM(product_gross_revenue_excl_shipping_total) AS product_gross_revenue_excl_shipping_total
     ,SUM(product_margin_pre_return_total) AS product_margin_pre_return_total
     ,SUM(cash_gross_revenue_total) AS cash_gross_revenue_total
     ,SUM(cash_credit_amount_total) AS cash_credit_amount_total
     ,SUM(noncash_credit_amount_total) AS noncash_credit_amount_total
     ,SUM(product_cost_total) AS product_cost_total
     ,SUM(landed_cost_total) AS landed_cost_total
     ,SUM(shipping_cost_total) AS shipping_cost_total
     ,SUM(total_cogs_total) AS total_cogs_total
     ,SUM(prepaid_creditcard_orders_total) as prepaid_creditcard_orders_total
     ,SUM(prepaid_creditcard_failed_attempts_total) as prepaid_creditcard_failed_attempts_total
     ,SUM(failed_orders_total) as failed_orders_total
     ,SUM(membership_cancellations) as membership_cancellations
     ,SUM(online_membership_cancellations) as online_membership_cancellations
     ,SUM(membership_pauses) as membership_pauses
     ,SUM(distinct_monthly_vip_customers) as distinct_monthly_vip_customers
     ,SUM(login_actions) as login_actions
     ,count(case when IS_SHOPPING_GRID_VISIT = true then session_id end) as shopping_grid_actions
     ,count(case when IS_PDP_VISIT = true then session_id end) as pdp_actions
     ,count(case when is_atb_action = true then session_id end) as atb_actions
     ,sum(cart_actions) as cart_actions
     ,SUM(skip_actions) as skip_actions
     ,SUM(flag_1_actions) as flag_1_actions
     ,SUM(flag_2_actions) as flag_2_actions
     ,SUM(flag_3_actions) as flag_3_actions
     ,SUM(CUSTOM_FLAG_ACTIONS) AS custom_flag_actions
     ,SUM(custom_flag_2_actions) AS custom_flag_2_actions
     ,$last_refreshed_datetime_hq as last_refreshed_datetime_hq
--'total region and total platform'
FROM reporting_base_prod.shared.session_order_ab_test_detail as d
join _last_3_months as ll on ll.test_framework_id = d.test_framework_id
    and ll.TEST_KEY = d.TEST_KEY
    and ll.test_label = d.test_label
    and ll.store_brand = d.store_brand
left join (select test_key,count(distinct STORE_REGION) as region_count from REPORTING_BASE_PROD.SHARED.SESSION_ORDER_AB_TEST_DETAIL group by 1) as t1 on t1.TEST_KEY = d.test_key
left join _total_descriptions as sc on d.test_key = sc.test_key
    and d.test_framework_id = sc.test_framework_id
    and d.test_label = sc.test_label
    and d.store_brand = sc.store_brand
    AND d.test_group = sc.test_group
left join _builder_test_labels as u on u.builder_id = d.test_key
where
    d.test_group is not null
  and d.STORE_BRAND not in ('FabKids','ShoeDazzle','Yitty')
  and (d.TEST_KEY ilike '%(User%'
        or (test_type in ('membership','session') and d.test_key in (select distinct test_key from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA where DEVICE in ('Desktop & Mobile','Desktop & App','Mobile & App','Desktop, Mobile & App')))
        or (test_type in ('Builder','Session Detail','Customer Detail','Membership Detail') and d.test_key in (select distinct test_key from lake_view.sharepoint.ab_test_metadata_import_oof where test_platforms in ('Desktop & Mobile','Mobile & App','ALL','Desktop and Mobile')))
        or (test_type in ('Builder','Session Detail','Customer Detail','Membership Detail') and d.test_key in (select distinct concat(test_key,' (PSRC)') as test_key from lake_view.sharepoint.ab_test_metadata_import_oof where test_platforms in ('Desktop & Mobile','Mobile & App','ALL','Desktop and Mobile')))
        or (test_type ilike 'Sorting Hat%' and d.test_key in (select distinct meta_original_metadata_test_id from _sorting_hat_metadata_ids where test_platforms in ('Desktop & Mobile','Mobile & App','ALL','Desktop and Mobile'))))
  and t1.region_count > 1
  and convert_timezone('America/Los_Angeles',AB_TEST_START_LOCAL_DATETIME)::date>=$process_from_date --comment
group by
    d.TEST_KEY
       ,d.TEST_FRAMEWORK_ID
       ,coalesce(u.builder_test_label_adj,d.test_label)
       ,TEST_FRAMEWORK_DESCRIPTION
       ,TEST_FRAMEWORK_TICKET
       ,d.CAMPAIGN_CODE
       ,TEST_TYPE
       ,test_activated_datetime
       ,case when d.TEST_GROUP ilike 'Control%' THEN 'Control' else 'Variant' end
       ,d.TEST_GROUP
       ,GROUP_NAME
       ,TRAFFIC_SPLIT_TYPE
       ,sc.TEST_GROUP_DESCRIPTION
       ,AB_TEST_START_LOCAL_DATETIME::date
       ,d.STORE_BRAND
--      ,'NA+EU TTL' as STORE_REGION
--      ,'NA+EU TTL' as STORE_COUNTRY
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
--      ,'Temp N/A' as MEMBERSHIP_PRICE
       ,MEMBERSHIP_TYPE
--      ,'Temp N/A' as IS_MOBILE_APP_USER
--      ,'Temp N/A' as IS_FREE_TRIAL
--      ,'Temp N/A' as IS_CROSS_PROMO
--      ,'Temp N/A' as IS_MALE_GATEWAY
--      ,'Temp N/A' as IS_MALE_SESSION_ACTION
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
       ,coalesce(is_login_action,false)::BOOLEAN
       ,coalesce(IS_SHOPPING_GRID_VISIT,false)::BOOLEAN
       ,coalesce(IS_PDP_VISIT,null)::BOOLEAN
       ,coalesce(IS_ATB_ACTION,null)::BOOLEAN
       ,coalesce(IS_CART_VISIT,null)::BOOLEAN
       ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN
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
      ,CUSTOM_FLAG_2_DESCRIPTION;

BEGIN;--comment

DELETE FROM REPORTING_PROD.shared.ab_test_final --comment
WHERE AB_TEST_START_LOCAL_DATE>=$process_from_date;

DELETE FROM REPORTING_PROD.shared.ab_test_final
where
    TEST_KEY not in (select test_key from _last_3_months)
        and test_label not in (select test_label from _last_3_months);

INSERT INTO REPORTING_PROD.shared.ab_test_final --comment
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
 IS_QUIZ_START_ACTION, --DA-22858
 IS_QUIZ_COMPLETE_ACTION, --DA-22858
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
 IS_MALE_SESSION_ACTION,
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
 IS_LOGIN_ACTION,
 IS_SHOPPING_GRID_VISIT,
 IS_PDP_VISIT,
 IS_ATB_ACTION,
 IS_CART_VISIT,
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
 SESSIONS,
 CUSTOMERS,
 VISITORS,
 TEST_STARTS_ACTUAL,
 SKIP_QUIZ_ACTIONS,
 QUIZ_STARTS,
 QUIZ_COMPLETES,
 LEAD_REGISTRATIONS,
 QUIZ_LEAD_REGISTRATIONS,
 SPEEDY_LEAD_REGISTRATIONS,
 SKIP_QUIZ_LEAD_REGISTRATIONS,
 VIP_ACTIVATIONS_1HR,
 VIP_ACTIVATIONS_3HR,
 VIP_ACTIVATIONS_24HR,
 VIP_ACTIVATIONS,
 VIP_ACTIVATIONS_SAME_SESSION,
 VIP_ACTIVATIONS_BY_LEAD_REG,
 UNITS_ACTIVATING,
 PRODUCT_SUBTOTAL_EXCL_TARIFF_ACTIVATING,
 PRODUCT_SUBTOTAL_INCL_TARIFF_ACTIVATING,
 PRODUCT_DISCOUNT_ACTIVATING,
 SHIPPING_DISCOUNT_ACTIVATING,
 TOTAL_DISCOUNT_ACTIVATING,
 SHIPPING_REVENUE_ACTIVATING,
 TARIFF_SURCHARGE_ACTIVATING,
 TAX_ACTIVATING,
 PRODUCT_GROSS_REVENUE_ACTIVATING,
 PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_ACTIVATING,
 PRODUCT_MARGIN_PRE_RETURN_ACTIVATING,
 CASH_GROSS_REVENUE_ACTIVATING,
 CASH_CREDIT_ACTIVATING,
 NONCASH_CREDIT_ACTIVATING,
 PRODUCT_COST_ACTIVATING,
 LANDED_COST_ACTIVATING,
 SHIPPING_COST_ACTIVATING,
 TOTAL_COGS_ACTIVATING,
 PREPAID_CREDITCARD_ORDERS_ACTIVATING,
 PREPAID_CREDITCARD_FAILED_ATTEMPTS_ACTIVATING,
 FAILED_ORDERS_ACTIVATING,
 ORDERS_NONACTIVATING,
 UNITS_NONACTIVATING,
 PRODUCT_SUBTOTAL_EXCL_TARIFF_NONACTIVATING,
 PRODUCT_SUBTOTAL_INCL_TARIFF_NONACTIVATING,
 PRODUCT_DISCOUNT_NONACTIVATING,
 SHIPPING_DISCOUNT_NONACTIVATING,
 TOTAL_DISCOUNT_NONACTIVATING,
 SHIPPING_REVENUE_NONACTIVATING,
 TARIFF_SURCHARGE_NONACTIVATING,
 TAX_NONACTIVATING,
 PRODUCT_GROSS_REVENUE_NONACTIVATING,
 PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_NONACTIVATING,
 PRODUCT_MARGIN_PRE_RETURN_NONACTIVATING,
 CASH_GROSS_REVENUE_NONACTIVATING,
 CASH_CREDIT_NONACTIVATING,
 NONCASH_CREDIT_NONACTIVATING,
 PRODUCT_COST_NONACTIVATING,
 LANDED_COST_NONACTIVATING,
 SHIPPING_COST_NONACTIVATING,
 TOTAL_COGS_NONACTIVATING,
 MEMBERSHIP_CREDIT_REDEEMED_AMOUNT_NONACTIVATING,
 MEMBERSHIP_CREDITS_TOKEN_REDEEMED_COUNT_NONACTIVATING,
 PREPAID_CREDITCARD_ORDERS_NONACTIVATING,
 PREPAID_CREDITCARD_FAILED_ATTEMPTS_NONACTIVATING,
 FAILED_ORDERS_NONACTIVATING,
 ORDERS_TOTAL,
 UNITS_TOTAL,
 PRODUCT_SUBTOTAL_EXCL_TARIFF_TOTAL,
 PRODUCT_SUBTOTAL_INCL_TARIFF_TOTAL,
 PRODUCT_DISCOUNT_TOTAL,
 SHIPPING_DISCOUNT_TOTAL,
 TOTAL_DISCOUNT_TOTAL,
 SHIPPING_REVENUE_TOTAL,
 TARIFF_SURCHARGE_TOTAL,
 TAX_TOTAL,
 PRODUCT_GROSS_REVENUE_TOTAL,
 PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_TOTAL,
 PRODUCT_MARGIN_PRE_RETURN_TOTAL,
 CASH_GROSS_REVENUE_TOTAL,
 CASH_CREDIT_AMOUNT_TOTAL,
 NONCASH_CREDIT_AMOUNT_TOTAL,
 PRODUCT_COST_TOTAL,
 LANDED_COST_TOTAL,
 SHIPPING_COST_TOTAL,
 TOTAL_COGS_TOTAL,
 PREPAID_CREDITCARD_ORDERS_TOTAL,
 PREPAID_CREDITCARD_FAILED_ATTEMPTS_TOTAL,
 FAILED_ORDERS_TOTAL,
 MEMBERSHIP_CANCELLATIONS,
 ONLINE_MEMBERSHIP_CANCELLATIONS,
 MEMBERSHIP_PAUSES,
 DISTINCT_MONTHLY_VIP_CUSTOMERS,
 LOGIN_ACTIONS,
 SHOPPING_GRID_ACTIONS,
 PDP_ACTIONS,
 ATB_ACTIONS,
 CART_ACTIONS,
 SKIP_ACTIONS,
 FLAG_1_ACTIONS,
 FLAG_2_ACTIONS,
 FLAG_3_ACTIONS,
 custom_flag_actions,
  custom_flag_2_actions,
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
    IS_QUIZ_START_ACTION, --DA-22858
    IS_QUIZ_COMPLETE_ACTION, --DA-22858
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
    IS_MALE_SESSION_ACTION,
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
    IS_LOGIN_ACTION,
    IS_SHOPPING_GRID_VISIT,
    IS_PDP_VISIT,
    IS_ATB_ACTION,
    IS_CART_VISIT,
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
    SESSIONS,
    CUSTOMERS,
    VISITORS,
    TEST_STARTS_ACTUAL,
    SKIP_QUIZ_ACTIONS,
    QUIZ_STARTS,
    QUIZ_COMPLETES,
    LEAD_REGISTRATIONS,
    QUIZ_LEAD_REGISTRATIONS,
    SPEEDY_LEAD_REGISTRATIONS,
    SKIP_QUIZ_LEAD_REGISTRATIONS,
    VIP_ACTIVATIONS_1HR,
    VIP_ACTIVATIONS_3HR,
    VIP_ACTIVATIONS_24HR,
    VIP_ACTIVATIONS,
    VIP_ACTIVATIONS_SAME_SESSION,
    VIP_ACTIVATIONS_BY_LEAD_REG,
    UNITS_ACTIVATING,
    PRODUCT_SUBTOTAL_EXCL_TARIFF_ACTIVATING,
    PRODUCT_SUBTOTAL_INCL_TARIFF_ACTIVATING,
    PRODUCT_DISCOUNT_ACTIVATING,
    SHIPPING_DISCOUNT_ACTIVATING,
    TOTAL_DISCOUNT_ACTIVATING,
    SHIPPING_REVENUE_ACTIVATING,
    TARIFF_SURCHARGE_ACTIVATING,
    TAX_ACTIVATING,
    PRODUCT_GROSS_REVENUE_ACTIVATING,
    PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_ACTIVATING,
    PRODUCT_MARGIN_PRE_RETURN_ACTIVATING,
    CASH_GROSS_REVENUE_ACTIVATING,
    CASH_CREDIT_ACTIVATING,
    NONCASH_CREDIT_ACTIVATING,
    PRODUCT_COST_ACTIVATING,
    LANDED_COST_ACTIVATING,
    SHIPPING_COST_ACTIVATING,
    TOTAL_COGS_ACTIVATING,
    PREPAID_CREDITCARD_ORDERS_ACTIVATING,
    PREPAID_CREDITCARD_FAILED_ATTEMPTS_ACTIVATING,
    FAILED_ORDERS_ACTIVATING,
    ORDERS_NONACTIVATING,
    UNITS_NONACTIVATING,
    PRODUCT_SUBTOTAL_EXCL_TARIFF_NONACTIVATING,
    PRODUCT_SUBTOTAL_INCL_TARIFF_NONACTIVATING,
    PRODUCT_DISCOUNT_NONACTIVATING,
    SHIPPING_DISCOUNT_NONACTIVATING,
    TOTAL_DISCOUNT_NONACTIVATING,
    SHIPPING_REVENUE_NONACTIVATING,
    TARIFF_SURCHARGE_NONACTIVATING,
    TAX_NONACTIVATING,
    PRODUCT_GROSS_REVENUE_NONACTIVATING,
    PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_NONACTIVATING,
    PRODUCT_MARGIN_PRE_RETURN_NONACTIVATING,
    CASH_GROSS_REVENUE_NONACTIVATING,
    CASH_CREDIT_NONACTIVATING,
    NONCASH_CREDIT_NONACTIVATING,
    PRODUCT_COST_NONACTIVATING,
    LANDED_COST_NONACTIVATING,
    SHIPPING_COST_NONACTIVATING,
    TOTAL_COGS_NONACTIVATING,
    MEMBERSHIP_CREDIT_REDEEMED_AMOUNT_NONACTIVATING,
    MEMBERSHIP_CREDITS_TOKEN_REDEEMED_COUNT_NONACTIVATING,
    PREPAID_CREDITCARD_ORDERS_NONACTIVATING,
    PREPAID_CREDITCARD_FAILED_ATTEMPTS_NONACTIVATING,
    FAILED_ORDERS_NONACTIVATING,
    ORDERS_TOTAL,
    UNITS_TOTAL,
    PRODUCT_SUBTOTAL_EXCL_TARIFF_TOTAL,
    PRODUCT_SUBTOTAL_INCL_TARIFF_TOTAL,
    PRODUCT_DISCOUNT_TOTAL,
    SHIPPING_DISCOUNT_TOTAL,
    TOTAL_DISCOUNT_TOTAL,
    SHIPPING_REVENUE_TOTAL,
    TARIFF_SURCHARGE_TOTAL,
    TAX_TOTAL,
    PRODUCT_GROSS_REVENUE_TOTAL,
    PRODUCT_GROSS_REVENUE_EXCL_SHIPPING_TOTAL,
    PRODUCT_MARGIN_PRE_RETURN_TOTAL,
    CASH_GROSS_REVENUE_TOTAL,
    CASH_CREDIT_AMOUNT_TOTAL,
    NONCASH_CREDIT_AMOUNT_TOTAL,
    PRODUCT_COST_TOTAL,
    LANDED_COST_TOTAL,
    SHIPPING_COST_TOTAL,
    TOTAL_COGS_TOTAL,
    PREPAID_CREDITCARD_ORDERS_TOTAL,
    PREPAID_CREDITCARD_FAILED_ATTEMPTS_TOTAL,
    FAILED_ORDERS_TOTAL,
    MEMBERSHIP_CANCELLATIONS,
    ONLINE_MEMBERSHIP_CANCELLATIONS,
    MEMBERSHIP_PAUSES,
    DISTINCT_MONTHLY_VIP_CUSTOMERS,
    LOGIN_ACTIONS,
    SHOPPING_GRID_ACTIONS,
    PDP_ACTIONS,
    ATB_ACTIONS,
    CART_ACTIONS,
    SKIP_ACTIONS,
    FLAG_1_ACTIONS,
    FLAG_2_ACTIONS,
    FLAG_3_ACTIONS,
    custom_flag_actions,
    custom_flag_2_actions,
    LAST_REFRESHED_DATETIME_HQ
from _ab_test_final;

COMMIT; --comment









---- validation script
-- select
--     TEST_KEY
--      ,'1 REPORTING_BASE.shared.session_ab_test_cms_framework' as table_version
--      ,count(session_id) as session
--      ,count(distinct session_id) as sessions_distinct
-- from REPORTING_BASE.shared.session_ab_test_cms_framework
-- group by 1,2
--
-- union all
--
-- select
--     TEST_KEY
--      ,'1 REPORTING_BASE.shared.session_ab_test_cms_framework' as table_version
--      ,count(session_id) as session
--      ,count(distinct session_id) as sessions_distinct
-- from REPORTING_BASE.shared.session_ab_test_sorting_hat
-- group by 1,2
--
-- union all
--
-- select
--     TEST_KEY
--      ,'1 REPORTING_BASE.shared.session_ab_test_cms_framework' as table_version
--      ,count(session_id) as session
--      ,count(distinct session_id) as sessions_distinct
-- from REPORTING_BASE.shared.session_ab_test_non_framework
-- group by 1,2
--
-- union all
--
-- select
--     TEST_KEY
--      ,'2 reporting_base.shared.session_order_ab_test_detail' as table_version
--      ,count(*) as session
--      ,count(distinct session_id) as sessions_distinct
-- from reporting_base.shared.session_order_ab_test_detail
-- group by 1,2
--
-- union all
--
-- select
--     TEST_KEY
--      ,'3a reporting.shared.ab_test_final' as table_version
--      ,sum(sessions) as session
--      ,sum(sessions) as sessions_distinct
-- from reporting.shared.ab_test_final
-- where version = 'individual country and individual platform'
-- group by 1,2
--
-- union all
--
-- select
--     TEST_KEY
--      ,'3b reporting.shared.ab_test_final' as table_version
--      ,sum(sessions) as session
--      ,sum(sessions) as sessions_distinct
-- from reporting.shared.ab_test_final
-- where version = 'individual country and total platform'
-- group by 1,2
--
-- union all
--
-- select
--     TEST_KEY
--      ,'3c reporting.shared.ab_test_final' as table_version
--      ,sum(sessions) as session
--      ,sum(sessions) as sessions_distinct
-- from reporting.shared.ab_test_final
-- where version = 'individual region and individual platform'
-- group by 1,2
--
-- union all
--
-- select
--     TEST_KEY
--      ,'3d reporting.shared.ab_test_final' as table_version
--      ,sum(sessions) as session
--      ,sum(sessions) as sessions_distinct
-- from reporting.shared.ab_test_final
-- where version = 'individual region and total platform'
-- group by 1,2
--
-- union all
--
-- select
--     TEST_KEY
--      ,'3e reporting.shared.ab_test_final' as table_version
--      ,sum(sessions) as session
--      ,sum(sessions) as sessions_distinct
-- from reporting.shared.ab_test_final
-- where version = 'total region and individual platform'
-- group by 1,2
--
-- union all
--
-- select
--     TEST_KEY
--      ,'3f reporting.shared.ab_test_final' as table_version
--      ,sum(sessions) as session
--      ,sum(sessions) as sessions_distinct
-- from reporting.shared.ab_test_final
-- where version = 'total region and total platform'
-- group by 1,2
-- order by 1,2
