--
set last_load_datetime = (select max(a.session_local_datetime)::date-7 as last_load_datetime
                          from reporting_base_prod.shared.session_order_ab_test_detail a
                          join reporting_base_prod.shared.session b on a.session_id = b.session_id);

create or replace temp table _sessions_last7days as --comment
select session_id
from reporting_base_prod.shared.session
where session_local_datetime > $last_load_datetime;

CREATE OR REPLACE TEMPORARY TABLE _last_3_months as
select distinct TEST_KEY,test_label,test_framework_id,STORE_BRAND,CMS_MIN_TEST_START_DATETIME_HQ,max(AB_TEST_START_LOCAL_DATETIME) as max_AB_TEST_START_LOCAL_DATETIME
from REPORTING_BASE_PROD.SHARED.SESSION_AB_TEST_METADATA
group by 1,2,3,4,5

union

select distinct TEST_KEY,test_label,test_framework_id,STORE_BRAND,CMS_MIN_TEST_START_DATETIME_HQ,current_date as max_AB_TEST_START_LOCAL_DATETIME
from REPORTING_PROD.SHARED.AB_TEST_CMS_METADATA_FILTERED
where CAMPAIGN_CODE in ('MYVIPSNOOZEOPTION','SAVEOFFERSEGMENTATIONLITE')
group by 1,2,3,4,5

union

select distinct TEST_KEY,test_label,test_framework_id,STORE_BRAND,CMS_MIN_TEST_START_DATETIME_HQ,current_date as max_AB_TEST_START_LOCAL_DATETIME
from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA
where TEST_KEY in ('a6f9577471e740beae1d711f87dd4340','51b1213e2202403fb6a11461b2c9349c')
group by 1,2,3,4,5;

delete from  _last_3_months
    where max_AB_TEST_START_LOCAL_DATETIME <= CURRENT_DATE - INTERVAL '3 MONTH';

CREATE OR REPLACE TEMPORARY TABLE _oof_tests as
select
    oof.*
     ,coalesce(w.datetime_added,'9999-12-01'::TIMESTAMP_TZ(9)) as adjusted_end_datetime_adj
from lake_view.sharepoint.ab_test_metadata_import_oof as oof
join lake_consolidated_view.ultra_merchant.test_metadata as m on edw_prod.stg.udf_unconcat_brand(m.TEST_METADATA_ID) = oof.test_key
    and test_type ilike 'Sorting Hat%'
left join lake_consolidated.ultra_merchant.test_winner as w on w.TEST_METADATA_ID = m.TEST_METADATA_ID

union all

select *
     ,case when adjusted_end_datetime = '1900-01-01' then '9999-12-01' else adjusted_end_datetime end as adjusted_end_datetime_adj
from lake_view.sharepoint.ab_test_metadata_import_oof
where test_type ilike '%Detail' or test_type = 'Builder'

union all

select
    test_type,ticket,status
    ,'Postreg Personalization AB Test (April 2024) - AGG' test_label
    ,test_outcome,test_outcome_description,brand,region,yty_exclusion,adjusted_activated_datetime,adjusted_end_datetime,test_platforms,membership_state,gender,test_start_location,test_split
    ,'Postreg Personalization AB Test (April 2024) - AGG' as test_key
    ,'Control Agg' control_description
    ,'Control Agg' control_start
    ,control_end
    ,'Variant A Agg' variant_a_description
    ,'Variant A Agg' variant_a_start
    ,variant_a_end
    ,'Variant B Agg' variant_b_description
    ,'Variant B Agg' variant_b_start
    ,variant_b_end
    ,variant_c_description,variant_c_start,variant_c_end,variant_d_description,variant_d_start,variant_d_end
    ,flag_1_description,flag_1_name,flag_1_value,flag_2_description,flag_2_name,flag_2_value,flag_3_description,flag_3_name,flag_3_value,meta_row_hash,meta_create_datetime,meta_update_datetime
    ,case when adjusted_end_datetime = '1900-01-01' then '9999-12-01' else adjusted_end_datetime end as adjusted_end_datetime_adj
from lake_view.sharepoint.ab_test_metadata_import_oof
where test_type = 'Builder'
    and test_label in
            (
                'Postreg - Womens standard_sitewide_offer - Personalization AB Test (April 2024) | Postreg - Womens standard_sitewide_offer - Personalization AB Test (April 2024) - Control | Postreg - Womens standard_sitewide_offer - Personalization AB Test (April 2024) - Perso Toggle Test | Postreg - Womens standard_sitewide_offer - Personalization AB Test (April 2024) - No Toggle'
                ,'Postreg - Womens standard_2legging_offer - Personalization AB Test (April 2024) | Postreg - Womens standard_2legging_offer - Personalization AB Test (April 2024) - Control | Postreg - Womens standard_2legging_offer - Personalization AB Test (April 2024) - Perso Toggle Test | Postreg - Womens standard_2legging_offer - Personalization AB Test (April 2024) - No Toggle'
                ,'Postreg - Womens Default - Personalization AB Test (April 2024) | Postreg - Womens Default - Personalization AB Test (April 2024) - Control | Postreg - Womens Default - Personalization AB Test (April 2024) - Perso Toggle Test | Postreg - Womens Default - Personalization AB Test (April 2024) - No Toggle'
            );

CREATE OR REPLACE TEMPORARY TABLE _other_test_group_descriptions as
select distinct
   p.test_framework_id
  ,p.test_label
  ,p.test_key
  ,p.store_brand
  ,p.test_group
  ,case when test_key not ilike '%(User - Combo Groups)' and test_group = 'Control' then 'search spring'
        when test_key not ilike '%(User - Combo Groups)' and test_group = 'Variant' then 'constructor'
        when test_key ilike '%(User - Combo Groups)' and test_group = 'Control' then 'constructor > search spring'
        when test_key ilike '%(User - Combo Groups)' and test_group = 'Variant' then 'search spring > constructor' end as test_group_description
from reporting_base_prod.shared.SESSION_AB_TEST_METADATA p
where CAMPAIGN_CODE = 'FLUSSSVCSTR'

union all

select distinct
   p.test_framework_id
  ,p.test_label
  ,p.test_key
  ,p.store_brand
  ,p.test_group
  ,TEST_VARIATION_NAME as test_group_description
from reporting_base_prod.shared.SESSION_AB_TEST_BUILDER p
where TEST_TYPE = 'Builder' and builder_data_source = 'API';

create or replace temp table _session_order_ab_test_detail as --comment
-- CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.session_order_ab_test_detail as --comment
SELECT
    m.SESSION_ID
     ,'ALL' AS version
     ,m.TEST_KEY
     ,cast(m.AB_TEST_SEGMENT as varchar) as AB_TEST_SEGMENT
     ,cast(m.TEST_FRAMEWORK_ID as smallint) as TEST_FRAMEWORK_ID
     ,m.test_label
     ,m.TEST_FRAMEWORK_DESCRIPTION
     ,m.TEST_FRAMEWORK_TICKET
     ,m.CAMPAIGN_CODE
     ,m.TEST_TYPE
     ,m.CMS_MIN_TEST_START_DATETIME_HQ as test_activated_datetime
     ,DEVICE_TYPE as cms_device_type
     ,coalesce(m.test_group,g.TEST_GROUP) as test_group
     ,case when m.test_type not in ('membership','session') and GROUP_NAME is null then 'Non CMS'
         when (m.TEST_KEY = 'FLUSSSVCSTR_v2' or m.test_key ilike 'FLUSSSVCSTR_v3%') then '50/50' else GROUP_NAME end as GROUP_NAME
    ,case when m.test_type in ('session','membership') then coalesce(cc.test_group_description,trim(g.TEST_GROUP_DESCRIPTION))
           when m.test_type not in ('session','membership') and coalesce(m.test_group,g.TEST_GROUP) = 'Control' then COALESCE(trim(oof.control_description),cc.test_group_description)
           when m.test_type not in ('session','membership') and coalesce(m.test_group,g.TEST_GROUP) in ('Variant A','Variant') then COALESCE(trim(oof.variant_a_description),cc.test_group_description)
           when m.test_type not in ('session','membership') and coalesce(m.test_group,g.TEST_GROUP) = 'Variant B' then COALESCE(trim(oof.variant_b_description),cc.test_group_description)
           when m.test_type not in ('session','membership') and coalesce(m.test_group,g.TEST_GROUP) = 'Variant C' then COALESCE(trim(oof.variant_c_description),cc.test_group_description)
           when m.test_type not in ('session','membership') and coalesce(m.test_group,g.TEST_GROUP)= 'Variant D' then COALESCE(trim(oof.variant_d_description),cc.test_group_description)
            else null end as test_group_description
     ,case when TRAFFIC_SPLIT_TYPE is null then 'Non CMS' end as TRAFFIC_SPLIT_TYPE
     --datetimes
     ,AB_TEST_START_LOCAL_DATETIME
     ,AB_TEST_START_LOCAL_DATE
     ,AB_TEST_START_LOCAL_MONTH_DATE
     ,m.SESSION_LOCAL_DATETIME
     ,m.REGISTRATION_LOCAL_DATETIME
     --ids
     ,VISITOR_ID
     ,m.CUSTOMER_ID
     ,MEMBERSHIP_ID
     --store
     ,m.STORE_ID
     ,m.STORE_BRAND
     ,STORE_REGION
     ,STORE_COUNTRY
     ,STORE_NAME
     ,m.SPECIALTY_COUNTRY_CODE
     ,customer_state
     --lead and vip tenures
     ,m.TEST_START_MEMBERSHIP_STATE
     ,m.TEST_START_LEAD_DAILY_TENURE
     ,case when TEST_START_MEMBERSHIP_STATE <> 'Lead' then 'Non Lead'
           ELSE test_start_lead_tenure_group END AS  test_start_lead_tenure_group
     ,m.TEST_START_VIP_MONTH_TENURE
     ,case when TEST_START_MEMBERSHIP_STATE <> 'VIP' then 'Non VIP'
           ELSE TEST_START_VIP_MONTH_TENURE_GROUP END AS  TEST_START_VIP_MONTH_TENURE_GROUP
     --membership attributes
     ,m.HDYH
     ,IS_QUIZ_START_ACTION
     ,IS_QUIZ_COMPLETE_ACTION
     ,IS_LEAD_REGISTRATION_ACTION
     ,IS_QUIZ_REGISTRATION_ACTION
     ,IS_SPEEDY_REGISTRATION_ACTION
     ,IS_SKIP_QUIZ_REGISTRATION_ACTION
     ,coalesce(LEAD_REG_TYPE,REGISTRATION_TYPE) as LEAD_REG_TYPE
     ,m.MEMBERSHIP_PRICE
     ,m.MEMBERSHIP_TYPE
     ,case when FIRST_MOBILE_APP_SESSION_LOCAL_DATETIME >= AB_TEST_START_LOCAL_DATETIME then TRUE else FALSE end as IS_MOBILE_APP_USER
     ,IS_FREE_TRIAL
     ,IS_CROSS_PROMO
     ,m.CUMULATIVE_PRODUCT_GROSS_PROFIT_DECILE
     ,m.CUMULATIVE_CASH_GROSS_PROFIT_DECILE
     -- flags for FL
     ,IS_YITTY_GATEWAY
     ,IS_MALE_GATEWAY
     ,IS_MALE_SESSION_ACTION
     ,IS_MALE_SESSION
     ,case when IS_MALE_CUSTOMER = TRUE then TRUE else FALSE end AS IS_MALE_CUSTOMER
     ,case when IS_MALE_SESSION = TRUE or IS_MALE_CUSTOMER = TRUE then TRUE else FALSE end AS IS_MALE_SESSION_CUSTOMER
     --tech
     ,m.PLATFORM
     ,m.PLATFORM_RAW
     ,case when platform = 'App' and m.os = 'Unknown' and app.os in ('iOS','Mac OS','Mac OSX') then 'Apple'
           when platform = 'App' and m.os = 'Unknown' and app.os not in ('iOS','Mac OS','Mac OSX') then 'Android'
           else m.os end as OS
     ,m.BROWSER
     --acquisition
     ,CHANNEL
     ,SUBCHANNEL
     ,GATEWAY_TYPE
     ,GATEWAY_SUB_TYPE
     ,DM_GATEWAY_ID
     ,GATEWAY_CODE
     ,GATEWAY_NAME
     ,DM_SITE_ID
     --session actions
     ,coalesce(is_login_action,false)::BOOLEAN is_login_action
     ,null as IS_SHOPPING_GRID_VISIT
     ,null IS_PDP_VISIT
     ,null IS_ATB_ACTION
     ,null as IS_CART_VISIT
     ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN IS_SKIP_MONTH_ACTION
     ,is_migrated_session
     --session flags
     ,f.FLAG_1_NAME
     ,f.FLAG_1_DESCRIPTION
     ,f.FLAG_2_NAME
     ,f.FLAG_2_DESCRIPTION
     ,f.FLAG_3_NAME
     ,f.FLAG_3_DESCRIPTION
     ,f.FLAG_4_NAME as CUSTOM_FLAG_NAME
     ,f.FLAG_4_DESCRIPTION as CUSTOM_FLAG_DESCRIPTION
     ,f.FLAG_5_NAME as CUSTOM_FLAG_2_NAME
     ,f.FLAG_5_DESCRIPTION as CUSTOM_FLAG_2_DESCRIPTION
     --quiz / reg funnel
     ,sum(case when IS_SKIP_QUIZ_ACTION = TRUE then 1 else 0 end) AS skip_quiz_actions
     ,sum(DISTINCT case when m.IS_QUIZ_START_ACTION = TRUE then 1 else 0 end) AS quiz_starts
     ,sum(DISTINCT case when m.IS_QUIZ_COMPLETE_ACTION = TRUE then 1 else 0 end) AS quiz_completes
     ,sum(DISTINCT CASE WHEN m.IS_LEAD_REGISTRATION_ACTION = TRUE THEN 1 else 0 END) AS lead_registrations
     ,sum(case when IS_QUIZ_REGISTRATION_ACTION = TRUE then 1 else 0 end) as quiz_lead_registrations
     ,sum(case when IS_SPEEDY_REGISTRATION_ACTION = TRUE then 1 else 0 end) as speedy_lead_registrations
     ,sum(case when IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 1 else 0 end) as skip_quiz_lead_registrations
     --activating
     ,COALESCE(SUM(o.vip_activations_1hr),0) AS vip_activations_1hr
     ,COALESCE(SUM(o.vip_activations_3hr),0) AS vip_activations_3hr
     ,COALESCE(SUM(o.vip_activations_24hr),0) AS vip_activations_24hr
     ,COALESCE(SUM(o.vip_activations),0) AS vip_activations
     ,COALESCE(sum(case when order_version = 'same session' then vip_activations end),0) as VIP_ACTIVATIONS_SAME_SESSION
     ,COALESCE(sum(case when order_version = 'by lead reg' then vip_activations end),0) as VIP_ACTIVATIONS_BY_LEAD_REG
     ,COALESCE(SUM(o.units_activating),0) AS units_activating
     ,coalesce(sum(o.product_subtotal_excl_tariff_activating),0) as product_subtotal_excl_tariff_activating
     ,coalesce(sum(o.product_subtotal_incl_tariff_activating),0) as product_subtotal_incl_tariff_activating
     ,coalesce(sum(o.product_discount_activating),0) as product_discount_activating
     ,coalesce(sum(o.shipping_discount_activating),0) as shipping_discount_activating
     ,coalesce(sum(o.total_discount_activating),0) as total_discount_activating
     ,coalesce(sum(o.shipping_revenue_activating),0) as shipping_revenue_activating
     ,coalesce(sum(o.tariff_surcharge_activating),0) as tariff_surcharge_activating
     ,coalesce(sum(o.tax_activating),0) as tax_activating
     ,COALESCE(SUM(o.product_gross_revenue_activating),0) AS product_gross_revenue_activating
     ,COALESCE(SUM(o.product_gross_revenue_excl_shipping_activating),0) AS product_gross_revenue_excl_shipping_activating
     ,COALESCE(SUM(o.product_margin_pre_return_activating),0) AS product_margin_pre_return_activating
     ,COALESCE(SUM(o.cash_gross_revenue_activating),0) AS cash_gross_revenue_activating
     ,COALESCE(SUM(o.cash_credit_activating),0) AS cash_credit_activating
     ,COALESCE(SUM(o.noncash_credit_activating),0) AS noncash_credit_activating
     ,COALESCE(SUM(o.product_cost_activating),0) AS product_cost_activating
     ,COALESCE(SUM(o.landed_cost_activating),0)  AS landed_cost_activating
     ,COALESCE(SUM(o.shipping_cost_activating),0)  AS shipping_cost_activating
     ,COALESCE(SUM(o.total_cogs_activating),0)  AS total_cogs_activating
     ,COALESCE(SUM(o.prepaid_creditcard_orders_activating),0) AS prepaid_creditcard_orders_activating
     ,COALESCE(SUM(o.prepaid_creditcard_failed_attempts_activating),0)  AS prepaid_creditcard_failed_attempts_activating
     ,COALESCE(SUM(o.failed_orders_activating),0)  AS failed_orders_activating
--non-activating
     ,COALESCE(SUM(o.orders_nonactivating),0) AS orders_nonactivating
     ,COALESCE(SUM(o.units_nonactivating),0) AS units_nonactivating
     ,coalesce(sum(o.product_subtotal_excl_tariff_nonactivating),0) as product_subtotal_excl_tariff_nonactivating
     ,coalesce(sum(o.product_subtotal_incl_tariff_nonactivating),0) as product_subtotal_incl_tariff_nonactivating
     ,coalesce(sum(o.product_discount_nonactivating),0) as product_discount_nonactivating
     ,coalesce(sum(o.shipping_discount_nonactivating),0) as shipping_discount_nonactivating
     ,coalesce(sum(o.total_discount_nonactivating),0) as total_discount_nonactivating
     ,coalesce(sum(o.shipping_revenue_nonactivating),0) as shipping_revenue_nonactivating
     ,coalesce(sum(o.tariff_surcharge_nonactivating),0) as tariff_surcharge_nonactivating
     ,coalesce(sum(o.tax_nonactivating),0) as tax_nonactivating
     ,COALESCE(SUM(o.product_gross_revenue_nonactivating),0) AS product_gross_revenue_nonactivating
     ,COALESCE(SUM(o.product_gross_revenue_excl_shipping_nonactivating),0) AS product_gross_revenue_excl_shipping_nonactivating
     ,COALESCE(SUM(o.product_margin_pre_return_nonactivating),0) AS product_margin_pre_return_nonactivating
     ,COALESCE(SUM(o.cash_gross_revenue_nonactivating),0) AS cash_gross_revenue_nonactivating
     ,COALESCE(SUM(o.cash_credit_nonactivating),0) AS cash_credit_nonactivating
     ,COALESCE(SUM(o.noncash_credit_nonactivating),0) AS noncash_credit_nonactivating
     ,COALESCE(SUM(o.product_cost_nonactivating),0) AS product_cost_nonactivating
     ,COALESCE(SUM(o.landed_cost_nonactivating),0)  AS landed_cost_nonactivating
     ,COALESCE(SUM(o.shipping_cost_nonactivating),0)  AS shipping_cost_nonactivating
     ,COALESCE(SUM(o.total_cogs_nonactivating),0)  AS total_cogs_nonactivating
     ,COALESCE(SUM(o.membership_credit_redeemed_amount_nonactivating),0)  as membership_credit_redeemed_amount_nonactivating
     ,COALESCE(SUM(o.membership_credits_token_redeemed_count_nonactivating),0)  as membership_credits_redeemed_count_nonactivating
     ,COALESCE(SUM(o.prepaid_creditcard_orders_nonactivating),0)  as prepaid_creditcard_orders_nonactivating
     ,COALESCE(SUM(o.prepaid_creditcard_failed_attempts_nonactivating),0)  as prepaid_creditcard_failed_attempts_nonactivating
     ,COALESCE(SUM(o.failed_orders_nonactivating),0)  as failed_orders_nonactivating
--total
     ,COALESCE(SUM(o.orders_total),0) AS orders_total
     ,COALESCE(SUM(o.units_total),0) AS units_total
     ,coalesce(sum(o.product_subtotal_excl_tariff_total),0) as product_subtotal_excl_tariff_total
     ,coalesce(sum(o.product_subtotal_incl_tariff_total),0) as product_subtotal_incl_tariff_total
     ,coalesce(sum(o.product_discount_total),0) as product_discount_total
     ,coalesce(sum(o.shipping_discount_total),0) as shipping_discount_total
     ,coalesce(sum(o.total_discount_total),0) as total_discount_total
     ,coalesce(sum(o.shipping_revenue_total),0) as shipping_revenue_total
     ,coalesce(sum(o.tariff_surcharge_total),0) as tariff_surcharge_total
     ,coalesce(sum(o.tax_total),0) as tax_total
     ,COALESCE(SUM(o.product_gross_revenue_total),0) AS product_gross_revenue_total
     ,COALESCE(SUM(o.product_gross_revenue_excl_shipping_total),0) AS product_gross_revenue_excl_shipping_total
     ,COALESCE(SUM(o.product_margin_pre_return_total),0) AS product_margin_pre_return_total
     ,COALESCE(SUM(o.cash_gross_revenue_total),0) AS cash_gross_revenue_total
     ,COALESCE(SUM(o.cash_credit_amount_total),0) AS cash_credit_amount_total
     ,COALESCE(SUM(o.noncash_credit_amount_total),0) AS noncash_credit_amount_total
     ,COALESCE(SUM(o.product_cost_total),0) AS product_cost_total
     ,COALESCE(SUM(o.landed_cost_total),0)  AS landed_cost_total
     ,COALESCE(SUM(o.shipping_cost_total),0)  AS shipping_cost_total
     ,COALESCE(SUM(o.total_cogs_total),0)  AS total_cogs_total
     ,COALESCE(SUM(o.prepaid_creditcard_orders_total),0)  as prepaid_creditcard_orders_total
     ,COALESCE(SUM(o.prepaid_creditcard_failed_attempts_total),0)  as prepaid_creditcard_failed_attempts_total
     ,COALESCE(SUM(o.failed_orders_total),0) as failed_orders_total
     ,COALESCE(SUM(c.cancellations),0) as membership_cancellations
     ,COALESCE(SUM(c.online_cancellations),0) as online_membership_cancellations
     ,COALESCE(SUM(c.membership_pauses),0) as membership_pauses
     ,SUM(case when is_login_action = TRUE then 1 else 0 end) as login_actions
     ,0 as SHOPPING_GRID_ACTIONS
     ,0 as pdp_actions
     ,0 as atb_actions
     ,0 as cart_actions
     ,SUM(case when IS_SKIP_MONTH_ACTION = TRUE then 1 else 0 end) as skip_actions
     ,SUM(case when f.FLAG_1_NAME is not null then 1 else 0 end) as flag_1_actions
     ,SUM(case when f.FLAG_2_NAME is not null then 1 else 0 end) as flag_2_actions
     ,SUM(case when f.FLAG_3_NAME is not null then 1 else 0 end) as flag_3_actions
     ,SUM(case when f.FLAG_4_NAME is not null then 1 else 0 end) as custom_flag_actions
     ,SUM(case when f.FLAG_5_NAME is not null then 1 else 0 end) as custom_flag_2_actions
     ,count(distinct case when m.rnk_monthly = 1 and TEST_START_MEMBERSHIP_STATE = 'VIP' then m.SESSION_ID end) as distinct_monthly_vip_customers
-- select m.test_key,m.test_label,count(*)
FROM reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
join _sessions_last7days sld on sld.session_id = m.session_id --comment
JOIN _last_3_months AS l3 on l3.TEST_KEY = m.TEST_KEY
        and l3.STORE_BRAND = m.STORE_BRAND
        and l3.test_label = m.test_label
        and l3.test_framework_id = m.TEST_FRAMEWORK_ID
left join reporting_base_prod.shared.mobile_app_session_os_updated app on app.SESSION_ID = m.SESSION_ID
left join reporting_prod.shared.AB_TEST_CMS_GROUPS as g on
    g.TEST_KEY = m.TEST_KEY
    and g.test_label = m.test_label
    and m.test_type in ('session','membership')
    and g.DEVICE_TYPE = m.PLATFORM
    and try_cast(m.AB_TEST_SEGMENT as integer) between g.TEST_VALUE_START and g.TEST_VALUE_END
    and g.store_brand = m.STORE_BRAND
    and g.statuscode = 113
left join _oof_tests as oof on
    m.test_key  = oof.test_key
    and m.test_label  = oof.test_label
LEFT JOIN reporting_base_prod.SHARED.SESSION_AB_TEST_ORDERS AS o ON
    o.session_id = m.session_id
    and cast(o.AB_TEST_SEGMENT as varchar) = cast(m.AB_TEST_SEGMENT as varchar)
    and o.TEST_KEY = m.test_key
    and o.test_label = m.test_label
left join reporting_base_prod.SHARED.SESSION_AB_TEST_MEMBERSHIP_CANCELS as c on
    m.session_id = c.min_session_id
    and cast(c.AB_TEST_SEGMENT as varchar) = cast(m.AB_TEST_SEGMENT as varchar)
    and c.TEST_KEY = m.test_key
    and c.test_label = m.test_label
left join reporting_base_prod.shared.SESSION_AB_TEST_FLAGS as f on
    f.SESSION_ID = m.SESSION_ID
    and cast(f.AB_TEST_SEGMENT as varchar) = cast(m.AB_TEST_SEGMENT as varchar)
    and f.TEST_KEY = m.test_key
    and f.test_label = m.test_label
left join _other_test_group_descriptions as cc on cc.test_framework_id = m.test_framework_id
    and cc.TEST_KEY = m.TEST_KEY
    and cc.test_label = m.test_label
    and cc.store_brand = m.store_brand
    and cc.test_group = m.test_group
left join edw_prod.DATA_MODEL.dim_customer as dc on dc.CUSTOMER_ID = m.CUSTOMER_ID
--where m.test_key in ('dafd1514a3db41a88d5fbb869da90d81','fe90b100b35540b3a50dd959d9cd2dcc')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62
       ,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86

union all

SELECT
    m.SESSION_ID
     ,'PSRC' AS version
     ,concat(m.TEST_KEY,' (PSRC)') as TEST_KEY
     ,cast(m.AB_TEST_SEGMENT as varchar) as AB_TEST_SEGMENT
     ,cast(m.TEST_FRAMEWORK_ID as smallint) as TEST_FRAMEWORK_ID
     ,m.test_label
     ,m.TEST_FRAMEWORK_DESCRIPTION
     ,m.TEST_FRAMEWORK_TICKET
     ,m.CAMPAIGN_CODE
     ,m.TEST_TYPE
     ,m.CMS_MIN_TEST_START_DATETIME_HQ as test_activated_datetime
     ,DEVICE_TYPE as cms_device_type
     ,coalesce(m.test_group,g.TEST_GROUP) as test_group
     ,case when m.test_type not in ('membership','session') and GROUP_NAME is null then 'Non CMS' else GROUP_NAME end as GROUP_NAME
     ,case when m.test_type in ('session','membership') then coalesce(cc.test_group_description,trim(g.TEST_GROUP_DESCRIPTION))
           when m.test_type not in ('session','membership') and coalesce(m.test_group,g.TEST_GROUP) = 'Control' then COALESCE(trim(oof.control_description),cc.test_group_description)
           when m.test_type not in ('session','membership') and coalesce(m.test_group,g.TEST_GROUP) in ('Variant A','Variant') then COALESCE(trim(oof.variant_a_description),cc.test_group_description)
           when m.test_type not in ('session','membership') and coalesce(m.test_group,g.TEST_GROUP) = 'Variant B' then COALESCE(trim(oof.variant_b_description),cc.test_group_description)
           when m.test_type not in ('session','membership') and coalesce(m.test_group,g.TEST_GROUP) = 'Variant C' then COALESCE(trim(oof.variant_c_description),cc.test_group_description)
           when m.test_type not in ('session','membership') and coalesce(m.test_group,g.TEST_GROUP)= 'Variant D' then COALESCE(trim(oof.variant_d_description),cc.test_group_description)
            else null end as test_group_description
     ,case when TRAFFIC_SPLIT_TYPE is null then 'Non CMS' end as TRAFFIC_SPLIT_TYPE
     --datetimes
     ,AB_TEST_START_LOCAL_DATETIME
     ,AB_TEST_START_LOCAL_DATE
     ,AB_TEST_START_LOCAL_MONTH_DATE
     ,m.SESSION_LOCAL_DATETIME
     ,m.REGISTRATION_LOCAL_DATETIME
     --ids
     ,VISITOR_ID
     ,m.CUSTOMER_ID
     ,MEMBERSHIP_ID
     --store
     ,m.STORE_ID
     ,m.STORE_BRAND
     ,STORE_REGION
     ,STORE_COUNTRY
     ,STORE_NAME
     ,m.SPECIALTY_COUNTRY_CODE
     ,customer_state
     --lead and vip tenures
     ,m.TEST_START_MEMBERSHIP_STATE
     ,m.TEST_START_LEAD_DAILY_TENURE
     ,case when TEST_START_MEMBERSHIP_STATE <> 'Lead' then 'Non Lead'
           ELSE test_start_lead_tenure_group END AS  test_start_lead_tenure_group
     ,m.TEST_START_VIP_MONTH_TENURE
     ,case when TEST_START_MEMBERSHIP_STATE <> 'VIP' then 'Non VIP'
           ELSE TEST_START_VIP_MONTH_TENURE_GROUP END AS  TEST_START_VIP_MONTH_TENURE_GROUP
     --membership attributes
     ,m.HDYH
     ,IS_QUIZ_START_ACTION
     ,IS_QUIZ_COMPLETE_ACTION
     ,IS_LEAD_REGISTRATION_ACTION
     ,IS_QUIZ_REGISTRATION_ACTION
     ,IS_SPEEDY_REGISTRATION_ACTION
     ,IS_SKIP_QUIZ_REGISTRATION_ACTION
     ,coalesce(LEAD_REG_TYPE,REGISTRATION_TYPE) as LEAD_REG_TYPE
     ,m.MEMBERSHIP_PRICE
     ,m.MEMBERSHIP_TYPE
     ,case when FIRST_MOBILE_APP_SESSION_LOCAL_DATETIME >= AB_TEST_START_LOCAL_DATETIME then TRUE else FALSE end as IS_MOBILE_APP_USER
     ,IS_FREE_TRIAL
     ,IS_CROSS_PROMO
     ,m.CUMULATIVE_PRODUCT_GROSS_PROFIT_DECILE
     ,m.CUMULATIVE_CASH_GROSS_PROFIT_DECILE
     -- flags for FL
     ,IS_YITTY_GATEWAY
     ,IS_MALE_GATEWAY
     ,IS_MALE_SESSION_ACTION
     ,IS_MALE_SESSION
     ,case when IS_MALE_CUSTOMER = TRUE then TRUE else FALSE end AS IS_MALE_CUSTOMER
     ,case when IS_MALE_SESSION = TRUE or IS_MALE_CUSTOMER = TRUE then TRUE else FALSE end AS IS_MALE_SESSION_CUSTOMER
     --tech
     ,m.PLATFORM
     ,m.PLATFORM_RAW
     ,case when platform = 'App' and m.os = 'Unknown' and app.os in ('iOS','Mac OS','Mac OSX') then 'Apple'
           when platform = 'App' and m.os = 'Unknown' and app.os not in ('iOS','Mac OS','Mac OSX') then 'Android'
           else m.os end as OS
     ,m.BROWSER
     --acquisition
     ,CHANNEL
     ,SUBCHANNEL
     ,GATEWAY_TYPE
     ,GATEWAY_SUB_TYPE
     ,DM_GATEWAY_ID
     ,GATEWAY_CODE
     ,GATEWAY_NAME
     ,DM_SITE_ID
     --session actions
     ,coalesce(is_login_action,false)::BOOLEAN is_login_action
     ,null as IS_SHOPPING_GRID_VISIT
     ,null IS_PDP_VISIT
     ,null IS_ATB_ACTION
     ,null as IS_CART_VISIT
     ,coalesce(IS_SKIP_MONTH_ACTION,false)::BOOLEAN IS_SKIP_MONTH_ACTION
     ,is_migrated_session
     --session flags
     ,f.FLAG_1_NAME
     ,f.FLAG_1_DESCRIPTION
     ,f.FLAG_2_NAME
     ,f.FLAG_2_DESCRIPTION
     ,f.FLAG_3_NAME
     ,f.FLAG_3_DESCRIPTION
     ,f.FLAG_4_NAME as CUSTOM_FLAG_NAME
     ,f.FLAG_4_DESCRIPTION as CUSTOM_FLAG_DESCRIPTION
     ,f.FLAG_5_NAME as CUSTOM_FLAG_2_NAME
     ,f.FLAG_5_DESCRIPTION as CUSTOM_FLAG_2_DESCRIPTION
     --quiz / reg funnel
     ,sum(case when IS_SKIP_QUIZ_ACTION = TRUE then 1 else 0 end) AS skip_quiz_actions
     ,sum(DISTINCT case when m.IS_QUIZ_START_ACTION = TRUE then 1 else 0 end) AS quiz_starts
     ,sum(DISTINCT case when m.IS_QUIZ_COMPLETE_ACTION = TRUE then 1 else 0 end) AS quiz_completes
     ,sum(DISTINCT CASE WHEN m.IS_LEAD_REGISTRATION_ACTION = TRUE THEN 1 else 0 END) AS lead_registrations
     ,sum(case when IS_QUIZ_REGISTRATION_ACTION = TRUE then 1 else 0 end) as quiz_lead_registrations
     ,sum(case when IS_SPEEDY_REGISTRATION_ACTION = TRUE then 1 else 0 end) as speedy_lead_registrations
     ,sum(case when IS_SKIP_QUIZ_REGISTRATION_ACTION = TRUE then 1 else 0 end) as skip_quiz_lead_registrations
     --activating
     ,COALESCE(SUM(o.vip_activations_1hr),0) AS vip_activations_1hr
     ,COALESCE(SUM(o.vip_activations_3hr),0) AS vip_activations_3hr
     ,COALESCE(SUM(o.vip_activations_24hr),0) AS vip_activations_24hr
     ,COALESCE(SUM(o.vip_activations),0) AS vip_activations
     ,COALESCE(sum(case when order_version = 'same session' then vip_activations end),0) as VIP_ACTIVATIONS_SAME_SESSION
     ,COALESCE(sum(case when order_version = 'by lead reg' then vip_activations end),0) as VIP_ACTIVATIONS_BY_LEAD_REG
     ,COALESCE(SUM(o.units_activating),0) AS units_activating
     ,coalesce(sum(o.product_subtotal_excl_tariff_activating),0) as product_subtotal_excl_tariff_activating
     ,coalesce(sum(o.product_subtotal_incl_tariff_activating),0) as product_subtotal_incl_tariff_activating
     ,coalesce(sum(o.product_discount_activating),0) as product_discount_activating
     ,coalesce(sum(o.shipping_discount_activating),0) as shipping_discount_activating
     ,coalesce(sum(o.total_discount_activating),0) as total_discount_activating
     ,coalesce(sum(o.shipping_revenue_activating),0) as shipping_revenue_activating
     ,coalesce(sum(o.tariff_surcharge_activating),0) as tariff_surcharge_activating
     ,coalesce(sum(o.tax_activating),0) as tax_activating
     ,COALESCE(SUM(o.product_gross_revenue_activating),0) AS product_gross_revenue_activating
     ,COALESCE(SUM(o.product_gross_revenue_excl_shipping_activating),0) AS product_gross_revenue_excl_shipping_activating
     ,COALESCE(SUM(o.product_margin_pre_return_activating),0) AS product_margin_pre_return_activating
     ,COALESCE(SUM(o.cash_gross_revenue_activating),0) AS cash_gross_revenue_activating
     ,COALESCE(SUM(o.cash_credit_activating),0) AS cash_credit_activating
     ,COALESCE(SUM(o.noncash_credit_activating),0) AS noncash_credit_activating
     ,COALESCE(SUM(o.product_cost_activating),0) AS product_cost_activating
     ,COALESCE(SUM(o.landed_cost_activating),0)  AS landed_cost_activating
     ,COALESCE(SUM(o.shipping_cost_activating),0)  AS shipping_cost_activating
     ,COALESCE(SUM(o.total_cogs_activating),0)  AS total_cogs_activating
     ,COALESCE(SUM(o.prepaid_creditcard_orders_activating),0) AS prepaid_creditcard_orders_activating
     ,COALESCE(SUM(o.prepaid_creditcard_failed_attempts_activating),0)  AS prepaid_creditcard_failed_attempts_activating
     ,COALESCE(SUM(o.failed_orders_activating),0)  AS failed_orders_activating
--non-activating
     ,COALESCE(SUM(o.orders_nonactivating),0) AS orders_nonactivating
     ,COALESCE(SUM(o.units_nonactivating),0) AS units_nonactivating
     ,coalesce(sum(o.product_subtotal_excl_tariff_nonactivating),0) as product_subtotal_excl_tariff_nonactivating
     ,coalesce(sum(o.product_subtotal_incl_tariff_nonactivating),0) as product_subtotal_incl_tariff_nonactivating
     ,coalesce(sum(o.product_discount_nonactivating),0) as product_discount_nonactivating
     ,coalesce(sum(o.shipping_discount_nonactivating),0) as shipping_discount_nonactivating
     ,coalesce(sum(o.total_discount_nonactivating),0) as total_discount_nonactivating
     ,coalesce(sum(o.shipping_revenue_nonactivating),0) as shipping_revenue_nonactivating
     ,coalesce(sum(o.tariff_surcharge_nonactivating),0) as tariff_surcharge_nonactivating
     ,coalesce(sum(o.tax_nonactivating),0) as tax_nonactivating
     ,COALESCE(SUM(o.product_gross_revenue_nonactivating),0) AS product_gross_revenue_nonactivating
     ,COALESCE(SUM(o.product_gross_revenue_excl_shipping_nonactivating),0) AS product_gross_revenue_excl_shipping_nonactivating
     ,COALESCE(SUM(o.product_margin_pre_return_nonactivating),0) AS product_margin_pre_return_nonactivating
     ,COALESCE(SUM(o.cash_gross_revenue_nonactivating),0) AS cash_gross_revenue_nonactivating
     ,COALESCE(SUM(o.cash_credit_nonactivating),0) AS cash_credit_nonactivating
     ,COALESCE(SUM(o.noncash_credit_nonactivating),0) AS noncash_credit_nonactivating
     ,COALESCE(SUM(o.product_cost_nonactivating),0) AS product_cost_nonactivating
     ,COALESCE(SUM(o.landed_cost_nonactivating),0)  AS landed_cost_nonactivating
     ,COALESCE(SUM(o.shipping_cost_nonactivating),0)  AS shipping_cost_nonactivating
     ,COALESCE(SUM(o.total_cogs_nonactivating),0)  AS total_cogs_nonactivating
     ,COALESCE(SUM(o.membership_credit_redeemed_amount_nonactivating),0)  as membership_credit_redeemed_amount_nonactivating
     ,COALESCE(SUM(o.membership_credits_token_redeemed_count_nonactivating),0)  as membership_credits_redeemed_count_nonactivating
     ,COALESCE(SUM(o.prepaid_creditcard_orders_nonactivating),0)  as prepaid_creditcard_orders_nonactivating
     ,COALESCE(SUM(o.prepaid_creditcard_failed_attempts_nonactivating),0)  as prepaid_creditcard_failed_attempts_nonactivating
     ,COALESCE(SUM(o.failed_orders_nonactivating),0)  as failed_orders_nonactivating
--total
     ,COALESCE(SUM(o.orders_total),0) AS orders_total
     ,COALESCE(SUM(o.units_total),0) AS units_total
     ,coalesce(sum(o.product_subtotal_excl_tariff_total),0) as product_subtotal_excl_tariff_total
     ,coalesce(sum(o.product_subtotal_incl_tariff_total),0) as product_subtotal_incl_tariff_total
     ,coalesce(sum(o.product_discount_total),0) as product_discount_total
     ,coalesce(sum(o.shipping_discount_total),0) as shipping_discount_total
     ,coalesce(sum(o.total_discount_total),0) as total_discount_total
     ,coalesce(sum(o.shipping_revenue_total),0) as shipping_revenue_total
     ,coalesce(sum(o.tariff_surcharge_total),0) as tariff_surcharge_total
     ,coalesce(sum(o.tax_total),0) as tax_total
     ,COALESCE(SUM(o.product_gross_revenue_total),0) AS product_gross_revenue_total
     ,COALESCE(SUM(o.product_gross_revenue_excl_shipping_total),0) AS product_gross_revenue_excl_shipping_total
     ,COALESCE(SUM(o.product_margin_pre_return_total),0) AS product_margin_pre_return_total
     ,COALESCE(SUM(o.cash_gross_revenue_total),0) AS cash_gross_revenue_total
     ,COALESCE(SUM(o.cash_credit_amount_total),0) AS cash_credit_amount_total
     ,COALESCE(SUM(o.noncash_credit_amount_total),0) AS noncash_credit_amount_total
     ,COALESCE(SUM(o.product_cost_total),0) AS product_cost_total
     ,COALESCE(SUM(o.landed_cost_total),0)  AS landed_cost_total
     ,COALESCE(SUM(o.shipping_cost_total),0)  AS shipping_cost_total
     ,COALESCE(SUM(o.total_cogs_total),0)  AS total_cogs_total
     ,COALESCE(SUM(o.prepaid_creditcard_orders_total),0)  as prepaid_creditcard_orders_total
     ,COALESCE(SUM(o.prepaid_creditcard_failed_attempts_total),0)  as prepaid_creditcard_failed_attempts_total
     ,COALESCE(SUM(o.failed_orders_total),0) as failed_orders_total
     ,COALESCE(SUM(c.cancellations),0) as membership_cancellations
     ,COALESCE(SUM(c.online_cancellations),0) as online_membership_cancellations
     ,COALESCE(SUM(c.membership_pauses),0) as membership_pauses
     ,SUM(case when is_login_action = TRUE then 1 else 0 end) as login_actions
     ,0 as SHOPPING_GRID_ACTIONS
     ,0 as pdp_actions
     ,0 as atb_actions
     ,0 as cart_actions
     ,SUM(case when IS_SKIP_MONTH_ACTION = TRUE then 1 else 0 end) as skip_actions
     ,SUM(case when f.FLAG_1_NAME is not null then 1 else 0 end) as flag_1_actions
     ,SUM(case when f.FLAG_2_NAME is not null then 1 else 0 end) as flag_2_actions
     ,SUM(case when f.FLAG_3_NAME is not null then 1 else 0 end) as flag_3_actions
     ,SUM(case when f.FLAG_4_NAME is not null then 1 else 0 end) as custom_flag_actions
     ,SUM(case when f.FLAG_5_NAME is not null then 1 else 0 end) as custom_flag_2_actions
     ,count(distinct case when m.rnk_monthly = 1 and TEST_START_MEMBERSHIP_STATE = 'VIP' then m.SESSION_ID end) as distinct_monthly_vip_customers
FROM reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as m
join _sessions_last7days sld on sld.session_id = m.session_id --comment
JOIN _last_3_months AS l3 on l3.TEST_KEY = m.TEST_KEY
        and l3.STORE_BRAND = m.STORE_BRAND
        and l3.test_label = m.test_label
        and l3.test_framework_id = m.TEST_FRAMEWORK_ID
left join reporting_base_prod.shared.mobile_app_session_os_updated app on app.SESSION_ID = m.SESSION_ID
left join reporting_prod.shared.AB_TEST_CMS_GROUPS as g on
    g.TEST_KEY = m.TEST_KEY
    and g.test_label = m.test_label
    and m.test_type in ('session','membership')
    and g.DEVICE_TYPE = m.PLATFORM
    and try_cast(m.AB_TEST_SEGMENT as integer) between g.TEST_VALUE_START and g.TEST_VALUE_END
    and g.store_brand = m.STORE_BRAND
    and g.statuscode = 113
left join _oof_tests as oof on
    m.test_key  = oof.test_key
    and m.test_label  = oof.test_label
LEFT JOIN reporting_base_prod.SHARED.session_ab_test_orders_psource AS o ON
    o.session_id = m.session_id
    and cast(o.AB_TEST_SEGMENT as varchar) = cast(m.AB_TEST_SEGMENT as varchar)
    and o.TEST_KEY = m.test_key
    and o.test_label = m.test_label
left join reporting_base_prod.SHARED.SESSION_AB_TEST_MEMBERSHIP_CANCELS as c on
    m.session_id = c.min_session_id
    and cast(c.AB_TEST_SEGMENT as varchar) = cast(m.AB_TEST_SEGMENT as varchar)
    and c.TEST_KEY = m.test_key
    and c.test_label = m.test_label
left join reporting_base_prod.shared.SESSION_AB_TEST_FLAGS as f on
    f.SESSION_ID = m.SESSION_ID
    and cast(f.AB_TEST_SEGMENT as varchar) = cast(m.AB_TEST_SEGMENT as varchar)
    and f.TEST_KEY = m.test_key
    and f.test_label = m.test_label
left join _other_test_group_descriptions as cc on cc.test_framework_id = m.test_framework_id
    and cc.TEST_KEY = m.TEST_KEY
    and cc.test_label = m.test_label
    and cc.store_brand = m.store_brand
    and cc.test_group = m.test_group
left join edw_prod.DATA_MODEL.dim_customer as dc on dc.CUSTOMER_ID = m.CUSTOMER_ID
where
    m.TEST_KEY in (select distinct TEST_KEY from reporting_base_prod.SHARED.session_ab_test_orders_psource)
        and m.TEST_LABEL in (select distinct TEST_LABEL from reporting_base_prod.SHARED.session_ab_test_orders_psource)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62
       ,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86
;

BEGIN; --comment

delete from reporting_base_prod.shared.session_order_ab_test_detail --comment
where session_id in (select session_id from _sessions_last7days);

DELETE FROM reporting_base_prod.shared.session_order_ab_test_detail
where
    TEST_KEY not in (select test_key from _last_3_months)
        and test_label not in (select test_label from _last_3_months);

insert into reporting_base_prod.shared.session_order_ab_test_detail --comment
(
SESSION_ID,
TEST_KEY,
AB_TEST_SEGMENT,
TEST_FRAMEWORK_ID,
TEST_LABEL,
TEST_FRAMEWORK_DESCRIPTION,
TEST_FRAMEWORK_TICKET,
CAMPAIGN_CODE,
TEST_TYPE,
TEST_ACTIVATED_DATETIME,
CMS_DEVICE_TYPE,
TEST_GROUP,
GROUP_NAME,
TEST_GROUP_DESCRIPTION,
TRAFFIC_SPLIT_TYPE,
AB_TEST_START_LOCAL_DATETIME,
AB_TEST_START_LOCAL_DATE,
AB_TEST_START_LOCAL_MONTH_DATE,
SESSION_LOCAL_DATETIME,
REGISTRATION_LOCAL_DATETIME,
VISITOR_ID,
CUSTOMER_ID,
MEMBERSHIP_ID,
STORE_ID,
STORE_BRAND,
STORE_REGION,
STORE_COUNTRY,
STORE_NAME,
SPECIALTY_COUNTRY_CODE,
CUSTOMER_STATE,
TEST_START_MEMBERSHIP_STATE,
TEST_START_LEAD_DAILY_TENURE,
TEST_START_LEAD_TENURE_GROUP,
TEST_START_VIP_MONTH_TENURE,
TEST_START_VIP_MONTH_TENURE_GROUP,
HDYH,
IS_QUIZ_START_ACTION, -- DA-22858
IS_QUIZ_COMPLETE_ACTION, -- DA-22858
IS_LEAD_REGISTRATION_ACTION,
IS_QUIZ_REGISTRATION_ACTION,
IS_SPEEDY_REGISTRATION_ACTION,
IS_SKIP_QUIZ_REGISTRATION_ACTION,
LEAD_REG_TYPE,
MEMBERSHIP_PRICE,
MEMBERSHIP_TYPE,
IS_MOBILE_APP_USER,
IS_FREE_TRIAL,
IS_CROSS_PROMO,
CUMULATIVE_PRODUCT_GROSS_PROFIT_DECILE,
CUMULATIVE_CASH_GROSS_PROFIT_DECILE,
IS_YITTY_GATEWAY,
IS_MALE_GATEWAY,
IS_MALE_SESSION_ACTION,
IS_MALE_SESSION,
IS_MALE_CUSTOMER,
IS_MALE_SESSION_CUSTOMER,
PLATFORM,
PLATFORM_RAW,
OS,
BROWSER,
CHANNEL,
SUBCHANNEL,
GATEWAY_TYPE,
GATEWAY_SUB_TYPE,
DM_GATEWAY_ID,
GATEWAY_CODE,
GATEWAY_NAME,
DM_SITE_ID,
IS_LOGIN_ACTION,
IS_SHOPPING_GRID_VISIT,
IS_PDP_VISIT,
IS_ATB_ACTION,
IS_CART_VISIT,
IS_SKIP_MONTH_ACTION,
IS_MIGRATED_SESSION,
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
MEMBERSHIP_CREDITS_REDEEMED_COUNT_NONACTIVATING,
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
LOGIN_ACTIONS,
SHOPPING_GRID_ACTIONS,
PDP_ACTIONS,
ATB_ACTIONS,
CART_ACTIONS,
SKIP_ACTIONS,
FLAG_1_ACTIONS,
FLAG_2_ACTIONS,
FLAG_3_ACTIONS,
CUSTOM_FLAG_ACTIONS,
CUSTOM_FLAG_2_ACTIONS,
DISTINCT_MONTHLY_VIP_CUSTOMERS
)
select SESSION_ID,
TEST_KEY,
AB_TEST_SEGMENT,
TEST_FRAMEWORK_ID,
TEST_LABEL,
TEST_FRAMEWORK_DESCRIPTION,
TEST_FRAMEWORK_TICKET,
CAMPAIGN_CODE,
TEST_TYPE,
TEST_ACTIVATED_DATETIME,
CMS_DEVICE_TYPE,
TEST_GROUP,
GROUP_NAME,
TEST_GROUP_DESCRIPTION,
TRAFFIC_SPLIT_TYPE,
AB_TEST_START_LOCAL_DATETIME,
AB_TEST_START_LOCAL_DATE,
AB_TEST_START_LOCAL_MONTH_DATE,
SESSION_LOCAL_DATETIME,
REGISTRATION_LOCAL_DATETIME,
VISITOR_ID,
CUSTOMER_ID,
MEMBERSHIP_ID,
STORE_ID,
STORE_BRAND,
STORE_REGION,
STORE_COUNTRY,
STORE_NAME,
SPECIALTY_COUNTRY_CODE,
CUSTOMER_STATE,
TEST_START_MEMBERSHIP_STATE,
TEST_START_LEAD_DAILY_TENURE,
TEST_START_LEAD_TENURE_GROUP,
TEST_START_VIP_MONTH_TENURE,
TEST_START_VIP_MONTH_TENURE_GROUP,
HDYH,
IS_QUIZ_START_ACTION, -- DA-22858
IS_QUIZ_COMPLETE_ACTION, -- DA-22858
IS_LEAD_REGISTRATION_ACTION,
IS_QUIZ_REGISTRATION_ACTION,
IS_SPEEDY_REGISTRATION_ACTION,
IS_SKIP_QUIZ_REGISTRATION_ACTION,
LEAD_REG_TYPE,
MEMBERSHIP_PRICE,
MEMBERSHIP_TYPE,
IS_MOBILE_APP_USER,
IS_FREE_TRIAL,
IS_CROSS_PROMO,
CUMULATIVE_PRODUCT_GROSS_PROFIT_DECILE,
CUMULATIVE_CASH_GROSS_PROFIT_DECILE,
IS_YITTY_GATEWAY,
IS_MALE_GATEWAY,
IS_MALE_SESSION_ACTION,
IS_MALE_SESSION,
IS_MALE_CUSTOMER,
IS_MALE_SESSION_CUSTOMER,
PLATFORM,
PLATFORM_RAW,
OS,
BROWSER,
CHANNEL,
SUBCHANNEL,
GATEWAY_TYPE,
GATEWAY_SUB_TYPE,
DM_GATEWAY_ID,
GATEWAY_CODE,
GATEWAY_NAME,
DM_SITE_ID,
IS_LOGIN_ACTION,
IS_SHOPPING_GRID_VISIT,
IS_PDP_VISIT,
IS_ATB_ACTION,
IS_CART_VISIT,
IS_SKIP_MONTH_ACTION,
IS_MIGRATED_SESSION,
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
MEMBERSHIP_CREDITS_REDEEMED_COUNT_NONACTIVATING,
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
LOGIN_ACTIONS,
SHOPPING_GRID_ACTIONS,
PDP_ACTIONS,
ATB_ACTIONS,
CART_ACTIONS,
SKIP_ACTIONS,
FLAG_1_ACTIONS,
FLAG_2_ACTIONS,
FLAG_3_ACTIONS,
CUSTOM_FLAG_ACTIONS,
custom_flag_2_actions,
DISTINCT_MONTHLY_VIP_CUSTOMERS
from _session_order_ab_test_detail;

-------------------------------------------------------------------------------------------------------
--custom code

DELETE FROM reporting_base_prod.shared.session_order_ab_test_detail
where
    test_group is null
   or SESSION_ID in   --deleting invalid session--gender and lead tenures
      (select distinct SESSION_ID
       from reporting_base_prod.SHARED.session_ab_test_cms_framework
       where (TEST_TYPE in ('membership','session') and is_valid_gender_sessions = FALSE)
          or (TEST_TYPE in ('membership','session') and flag_is_cms_filtered_lead = FALSE)
          or (IS_BOT = TRUE and TEST_START_MEMBERSHIP_STATE = 'Prospect')) --deleting bots
   --deleting yitty sessions from pdp bmig test
   or (test_key = 'pdp' and test_label ilike 'fl bmig%%' and store_brand = 'Yitty')
    or (edw_prod.stg.udf_unconcat_brand(TEST_FRAMEWORK_ID) = 1215 and STORE_COUNTRY in ('FR','DE'));

--deleting non-california customers from FK tariff  test & FL Snooze test
CREATE OR REPLACE TEMPORARY TABLE _ca_customers as
    (select distinct c.CUSTOMER_ID
     from (select distinct customer_id from reporting_base_prod.shared.session_order_ab_test_detail where campaign_code in ('TariffFKKi2866','MYVIPSNOOZEOPTION','ONLINECANCELREDESIGN','CANCELFIRSTORDER')) as c
              join edw_prod.DATA_MODEL.FACT_ORDER as fo on fo.CUSTOMER_ID = c.customer_id
              left join edw_prod.DATA_MODEL.DIM_ADDRESS as a1 on a1.ADDRESS_ID = fo.BILLING_ADDRESS_ID
         and a1.STATE = 'CA'
              left join edw_prod.DATA_MODEL.DIM_ADDRESS as a2 on a2.ADDRESS_ID = fo.SHIPPING_ADDRESS_ID
         and a2.STATE = 'CA'
     where a1.ADDRESS_ID is not null or a2.ADDRESS_ID is not null
     union
     select distinct c.CUSTOMER_ID
     from (select distinct customer_id from reporting_base_prod.shared.session_order_ab_test_detail where campaign_code in ('TariffFKKi2866','MYVIPSNOOZEOPTION','ONLINECANCELREDESIGN','CANCELFIRSTORDER')) as c
              join edw_prod.DATA_MODEL.DIM_CUSTOMER as dc on dc.CUSTOMER_ID = c.customer_id
                where lower(DEFAULT_STATE_PROVINCE) = 'CA');

--WEIRD SESSIONS IN FL PROMO STACKING TEST, NO USER AGENT = NO JS EXECUTED
DELETE FROM reporting_base_prod.shared.session_order_ab_test_detail
WHERE SESSION_ID IN
      (
            select distinct d.SESSION_ID
            from work.dbo.abt_1249_delete_sessions as d
--             from reporting_base_prod.shared.SESSION_ORDER_AB_TEST_DETAIL as d
--             join reporting_base_prod.shared.session_inspection as i on i.session_id = d.SESSION_ID
--             where
--                 TEST_FRAMEWORK_ID = 1249
--                 and d.PLATFORM = 'Desktop'
--                 and d.CHANNEL = 'direct traffic'
--                 and d.IS_MIGRATED_SESSION = false
--                 and is_in_segment = false
--                 and is_spawning_session = false
--                 and is_spawned_session = false
--                 and is_tfg_test_session = false
--                 and is_preview_session = false
--                 and is_internal_bot = false
--                 and is_bot_flagged = false
--                 and ip_flagged = 'NORMAL'
--                 and is_missing_ua_source = true
        );

-- fl image sort ABT custom sql for eligible test starts
--deleting sessions for FL Image Sort ABT where a product wasn't viewed

-- CREATE OR REPLACE TEMPORARY TABLE _products_1253 as
-- select distinct
--     MASTER_PRODUCT_ID as product_id
-- from edw_prod.DATA_MODEL.DIM_PRODUCT as dp
-- where
--     MASTER_PRODUCT_ID in
--         (4610596,8677720,8990332,9509005,11814757,12650035,12651556,13414276
--             ,14040280,14073430,14075425,14218771,14219854,14559955,14279440,14385748) --remove 14230396
-- order by 1;
--
-- CREATE OR REPLACE TEMPORARY TABLE _min_customer_session_ids_1253 as
-- select distinct customer_id,min(session_id) as min_session_id
-- from
--     (select
--         try_to_number(p.PROPERTIES_CUSTOMER_ID) as customer_id
--         ,min(try_to_number(p.PROPERTIES_SESSION_ID)) as session_id
--     from reporting_base_prod.SHARED.SESSION_AB_TEST_CMS_FRAMEWORK as a
--     join LAKE.SEGMENT_FL.JAVASCRIPT_FABLETICS_PRODUCT_LIST_VIEWED as p on a.SESSION_ID = try_to_number(p.PROPERTIES_SESSION_ID)
--     join _products_1253 as d on d.product_id = try_to_number(p.PROPERTIES_PRODUCTS_PRODUCT_ID)
--     where
--         try_to_number(p.PROPERTIES_SESSION_ID) >= 11446067959 --min abt session id
--         and TEST_FRAMEWORK_ID = 1253
--     group by 1
--
--     union
--
--     select
--         try_to_number(p.PROPERTIES_CUSTOMER_ID) as customer_id
--         ,min(try_to_number(p.PROPERTIES_SESSION_ID)) as session_id
--     from reporting_base_prod.SHARED.SESSION_AB_TEST_CMS_FRAMEWORK as a
--     join LAKE.SEGMENT_FL.JAVASCRIPT_FABLETICS_PRODUCT_VIEWED as p on a.SESSION_ID = try_to_number(p.PROPERTIES_SESSION_ID)
--     join _products_1253 as d on d.product_id = try_to_number(p.PROPERTIES_PRODUCT_ID)
--     where
--         try_to_number(p.PROPERTIES_SESSION_ID) >= 11446067959 --min abt session id
--         and TEST_FRAMEWORK_ID = 1253
--     group by 1
--
--     union
--
--     select
--         try_to_number(p.PROPERTIES_CUSTOMER_ID) as customer_id
--         ,min(try_to_number(p.PROPERTIES_SESSION_ID)) as session_id
--     from reporting_base_prod.SHARED.SESSION_AB_TEST_CMS_FRAMEWORK as a
--     join LAKE.SEGMENT_FL.java_FABLETICS_product_added as p on a.SESSION_ID = try_to_number(p.PROPERTIES_SESSION_ID)
--     join _products_1253 as d on d.product_id = try_to_number(p.PROPERTIES_PRODUCT_ID)
--     where
--         try_to_number(p.PROPERTIES_SESSION_ID) >= 11446067959 --min abt session id
--         and TEST_FRAMEWORK_ID = 1253
--     group by 1
--
--     union
--
--     select
--         a.customer_id
--         ,min(p.session_id) as session_id
--     from reporting_base_prod.SHARED.SESSION_AB_TEST_CMS_FRAMEWORK as a
--     join reporting_base_prod.SHARED.ORDER_LINE_PSOURCE_PAYMENT as p on a.SESSION_ID = p.SESSION_ID
--     join _products_1253 as d on d.product_id = p.product_id
--     where
--         p.session_id >= 11446067959 --min abt session id
--         and TEST_FRAMEWORK_ID = 1253
--     group by 1)
-- group by 1;
--
-- CREATE OR REPLACE TEMPORARY TABLE _eligible_test_sessions_1253 as
-- select distinct TEST_FRAMEWORK_ID,s.customer_id,SESSION_ID
-- from reporting_base_prod.SHARED.SESSION_ORDER_AB_TEST_DETAIL as s
-- join _min_customer_session_ids_1253 as min on min.customer_id = s.CUSTOMER_ID
-- where
--     TEST_FRAMEWORK_ID = 1253
--     and s.SESSION_ID >= min_session_id;
--
-- DELETE FROM reporting_base_prod.shared.session_order_ab_test_detail
-- where TEST_FRAMEWORK_ID = 1253 and SESSION_ID not in (select distinct SESSION_ID from _eligible_test_sessions_1253);

-------------------------------------------------------------------------------------------------------
--FL CANCELFIRSTORDER (1245) CUSTOM SQL

-- CREATE OR REPLACE TEMPORARY TABLE _eligible_customers_with_d7_abt_start_from_activation_1245 as --checking days between vip activation and abt start datetime
-- select distinct
--     a.CUSTOMER_ID
--     ,TEST_START_VIP_MONTH_TENURE
--     ,SOURCE_ACTIVATION_LOCAL_DATETIME
--     ,AB_TEST_START_LOCAL_DATETIME
--     ,datediff('day',SOURCE_ACTIVATION_LOCAL_DATETIME,AB_TEST_START_LOCAL_DATETIME) + 1 as days_between_vip_activation_and_abt_test_start
-- from reporting_base_prod.SHARED.SESSION_ORDER_AB_TEST_DETAIL as a
-- join edw_prod.DATA_MODEL.FACT_ACTIVATION as fa on fa.CUSTOMER_ID = a.CUSTOMER_ID
-- where
--     edw_prod.stg.udf_unconcat_brand(TEST_FRAMEWORK_ID) = 1245;
--
-- delete from _eligible_customers_with_d7_abt_start_from_activation_1245 where days_between_vip_activation_and_abt_test_start > 7; --deleting all non newly activated VIPs
--
-- CREATE OR REPLACE TEMPORARY TABLE _flag_1_details_1245 as --min sessions for flag 1 record
-- select distinct s.customer_id,f.TEST_KEY,f.TEST_LABEL,f.AB_TEST_SEGMENT,f.FLAG_1_LOCAL_DATETIME,f.FLAG_1_NAME,f.FLAG_1_DESCRIPTION,min(f.SESSION_ID) as session_id
-- from reporting_base_prod.shared.session_ab_test_flags as f
-- join reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as s on s.SESSION_ID = f.SESSION_ID --just to get customer ID
-- where
--     edw_prod.stg.udf_unconcat_brand(f.TEST_FRAMEWORK_ID) = 1245
--     and CUSTOMER_ID in (select distinct customer_id from _eligible_customers_with_d7_abt_start_from_activation_1245)
-- group by 1,2,3,4,5,6,7;
--
-- CREATE OR REPLACE TEMPORARY TABLE _account_cancel_pageviews_1245 as --min sessions for account/cancel pageview
-- select distinct a.CUSTOMER_ID,TEST_KEY,TEST_LABEL,AB_TEST_SEGMENT,a.STORE_BRAND,min(PROPERTIES_SESSION_ID) as SESSION_ID
-- from reporting_base_prod.SHARED.SESSION_AB_TEST_METADATA as a
-- join LAKE.SEGMENT_FL.JAVASCRIPT_FABLETICS_PAGE as b on try_to_number(b.PROPERTIES_SESSION_ID) = edw_prod.stg.udf_unconcat_brand(a.SESSION_ID)
-- join edw_prod.data_model.dim_store as c on c.store_id = a.store_id
-- where
--     edw_prod.stg.udf_unconcat_brand(TEST_FRAMEWORK_ID) = 1245
--     and PROPERTIES_PAGE_NAME ilike '%/account/cancel%'
--     and try_to_number(PROPERTIES_SESSION_ID) >= 11454462773 --min session for this abt
--     and CUSTOMER_ID in (select distinct customer_id from _eligible_customers_with_d7_abt_start_from_activation_1245)
-- group by 1,2,3,4,5;
--
-- CREATE OR REPLACE TEMPORARY TABLE _eligible_customer_min_sessions_1245 as --getting distinct session ids
-- select CUSTOMER_ID,TEST_KEY,test_label,AB_TEST_SEGMENT,session_id
-- from _flag_1_details_1245
-- union
-- select CUSTOMER_ID,TEST_KEY,test_label,AB_TEST_SEGMENT,session_id
-- from _account_cancel_pageviews_1245;
--
-- CREATE OR REPLACE TEMPORARY TABLE _eligible_sessions_from_customers_1245 as --getting all session IDS after that customer's flag/pageview record
-- select distinct a.CUSTOMER_ID,b.SESSION_ID
-- from _eligible_customer_min_sessions_1245 as a
-- join reporting_base_prod.SHARED.SESSION_ORDER_AB_TEST_DETAIL as b on a.CUSTOMER_ID = b.customer_id
--     and a.TEST_KEY = b.test_key
--     and a.test_label = b.test_label
--     and cast(a.AB_TEST_SEGMENT as varchar) = cast(b.AB_TEST_SEGMENT as varchar)
-- where
--     b.SESSION_ID >= a.SESSION_ID --get all sessions after they had flag record
--     and edw_prod.stg.udf_unconcat_brand(b.TEST_FRAMEWORK_ID) = 1245;
--
-- delete from reporting_base_prod.SHARED.SESSION_ORDER_AB_TEST_DETAIL --deleting ineligible traffic now
--     where
--         edw_prod.stg.udf_unconcat_brand(TEST_FRAMEWORK_ID) = 1245
--         and SESSION_ID not in (select session_id from _eligible_sessions_from_customers_1245);

update reporting_base_prod.SHARED.SESSION_ORDER_AB_TEST_DETAIL
set CUSTOM_FLAG_NAME = 'None', CUSTOM_FLAG_2_NAME = 'None'
where
    TEST_LABEL = 'Personalization Grid | Postreg - Womens standard_2legging_offer Perso AB Test (Feb 2024) - Control | Postreg - Womens standard_2legging_offer Perso AB Test (Feb 2024) - Grid Test'
    and (CUSTOM_FLAG_NAME is null and CUSTOM_FLAG_2_NAME is null);

------------------------------------------------------------------------
--ADD TESTS HERE FOR FUNNEL METRICS

CREATE OR REPLACE TEMPORARY TABLE _stg_funnel_view_sessions as
select
    CAMPAIGN_CODE
    ,TEST_KEY
    ,SESSION_ID
    ,edw_prod.stg.udf_unconcat_brand(SESSION_ID) as meta_original_session_id
    ,AB_TEST_START_LOCAL_DATETIME
    ,convert_timezone(STORE_TIME_ZONE,'UTC',AB_TEST_START_LOCAL_DATETIME::datetime)::datetime as AB_TEST_START_UTC_DATETIME
    ,s.IS_SHOPPING_GRID_VISIT
    ,s.IS_PDP_VISIT
    ,s.IS_ATB_ACTION
-- select distinct CAMPAIGN_CODE,TEST_KEY
from reporting_base_prod.SHARED.SESSION_ORDER_AB_TEST_DETAIL as s
join EDW_PROD.DATA_MODEL.DIM_STORE as t on t.STORE_ID = s.STORE_ID
where
    CAMPAIGN_CODE in ('FLUSSSVCSTR','EcatSwatchesPdp','mobileappstorytelling','REMIX4421JF','mobileappsquareswatches','KlarnaForLeads')
        or TEST_LABEL IN ('AL/DG Sort Algo | Homepage - Postreg FLW Aged Leads/DG US - Sort Algo AB Test (August 2024) - Control | Homepage - Postreg FLW Aged Leads/DG US - Sort Algo AB Test (August 2024) - Test');

/* watermark for faster processing of Segment data */
SET min_watermark = (
    SELECT MIN(AB_TEST_START_UTC_DATETIME)
    FROM _stg_funnel_view_sessions
    );

--GRID VIEWS
update reporting_base_prod.SHARED.SESSION_ORDER_AB_TEST_DETAIL
set IS_SHOPPING_GRID_VISIT = TRUE
where
    TEST_KEY in (select distinct TEST_KEY from _stg_funnel_view_sessions)
    and SESSION_ID in
            (
                select distinct SESSION_ID
                from _stg_funnel_view_sessions as s
                join LAKE.SEGMENT_FL.JAVASCRIPT_FABLETICS_PRODUCT_LIST_VIEWED as p on try_to_number(p.PROPERTIES_SESSION_ID) = s.meta_original_session_id
                where
                    p.TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
                    AND p.TIMESTAMP::datetime >= $min_watermark
                    AND NOT COALESCE(s.IS_SHOPPING_GRID_VISIT,'') ILIKE 'true'

                union

                select distinct SESSION_ID
                from _stg_funnel_view_sessions as s
                join LAKE.SEGMENT_FL.REACT_NATIVE_FABLETICS_PRODUCT_LIST_VIEWED as p on IFNULL(try_to_number(p.PROPERTIES_SESSION_ID)::VARCHAR, '0')::VARCHAR = s.meta_original_session_id
                where
                    p.TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
                    and p.PROPERTIES_SESSION_ID <> '0'
                    AND p.TIMESTAMP::datetime >= $min_watermark
                    AND NOT COALESCE(s.IS_SHOPPING_GRID_VISIT,'') ILIKE 'true'

                union

                select distinct SESSION_ID
                from _stg_funnel_view_sessions as s
                join LAKE.SEGMENT_SXF.JAVASCRIPT_SXF_PRODUCT_LIST_VIEWED as p on try_to_number(p.PROPERTIES_SESSION_ID) = s.meta_original_session_id
                where
                    p.TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
                    AND p.TIMESTAMP::datetime >= $min_watermark
                    AND NOT COALESCE(s.IS_SHOPPING_GRID_VISIT,'') ILIKE 'true'

                union

                select distinct SESSION_ID
                from _stg_funnel_view_sessions as s
                join LAKE.SEGMENT_GFB.JAVASCRIPT_FABKIDS_PRODUCT_LIST_VIEWED as p on try_to_number(p.PROPERTIES_SESSION_ID) = s.meta_original_session_id
                where
                    p.TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
                    AND p.TIMESTAMP::datetime >= $min_watermark
                    AND NOT COALESCE(s.IS_SHOPPING_GRID_VISIT,'') ILIKE 'true'

                union

                select distinct SESSION_ID
                from _stg_funnel_view_sessions as s
                join LAKE.SEGMENT_GFB.JAVASCRIPT_SHOEDAZZLE_PRODUCT_LIST_VIEWED as p on try_to_number(p.PROPERTIES_SESSION_ID) = s.meta_original_session_id
                where
                    p.TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
                    AND p.TIMESTAMP::datetime >= $min_watermark
                    AND NOT COALESCE(s.IS_SHOPPING_GRID_VISIT,'') ILIKE 'true'

                union

                select distinct SESSION_ID
                from _stg_funnel_view_sessions as s
                join LAKE.SEGMENT_GFB.JAVASCRIPT_JUSTFAB_PRODUCT_LIST_VIEWED as p on try_to_number(p.PROPERTIES_SESSION_ID) = s.meta_original_session_id
                where
                    p.TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
                    AND p.TIMESTAMP::datetime >= $min_watermark
                    AND NOT COALESCE(s.IS_SHOPPING_GRID_VISIT,'') ILIKE 'true'

                union

                select distinct SESSION_ID
                from _stg_funnel_view_sessions as s
                join LAKE.SEGMENT_GFB.REACT_NATIVE_JUSTFAB_PRODUCT_LIST_VIEWED as p on IFNULL(try_to_number(p.PROPERTIES_SESSION_ID)::VARCHAR, '0')::VARCHAR = s.meta_original_session_id
                where
                    p.TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
                    and p.PROPERTIES_SESSION_ID <> '0'
                    AND p.TIMESTAMP::datetime >= $min_watermark
                    AND NOT COALESCE(s.IS_SHOPPING_GRID_VISIT,'') ILIKE 'true'
            );

update reporting_base_prod.SHARED.SESSION_ORDER_AB_TEST_DETAIL
set IS_SHOPPING_GRID_VISIT = FALSE
where
    TEST_KEY in (select distinct TEST_KEY from _stg_funnel_view_sessions)
    and IS_SHOPPING_GRID_VISIT is null;

------------------------------------------------------------------------
--PDP VIEWS

update reporting_base_prod.SHARED.SESSION_ORDER_AB_TEST_DETAIL
set IS_PDP_VISIT = TRUE
where
    TEST_KEY in (select distinct TEST_KEY from _stg_funnel_view_sessions)
    and SESSION_ID in
        (
            select distinct SESSION_ID
            from _stg_funnel_view_sessions as s
            join LAKE.SEGMENT_FL.JAVASCRIPT_FABLETICS_PRODUCT_VIEWED as p on try_to_number(p.PROPERTIES_SESSION_ID) = s.meta_original_session_id
            where
                p.TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
                AND p.TIMESTAMP::datetime >= $min_watermark
                AND NOT COALESCE(s.IS_PDP_VISIT,'') ILIKE 'true'

            union

            select distinct SESSION_ID
            from _stg_funnel_view_sessions as s
            join LAKE.SEGMENT_FL.REACT_NATIVE_FABLETICS_PRODUCT_VIEWED as p on IFNULL(try_to_number(p.PROPERTIES_SESSION_ID)::VARCHAR, '0')::VARCHAR = s.meta_original_session_id
            where
                p.TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
                and p.PROPERTIES_SESSION_ID <> '0'
                AND p.TIMESTAMP::datetime >= $min_watermark
                AND NOT COALESCE(s.IS_PDP_VISIT,'') ILIKE 'true'

            union

            select distinct SESSION_ID
            from _stg_funnel_view_sessions as s
            join LAKE.SEGMENT_GFB.JAVASCRIPT_JUSTFAB_PRODUCT_VIEWED as p on try_to_number(p.PROPERTIES_SESSION_ID) = s.meta_original_session_id
            where
                p.TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
                AND p.TIMESTAMP::datetime >= $min_watermark
                AND NOT COALESCE(s.IS_PDP_VISIT,'') ILIKE 'true'

            union

            select distinct SESSION_ID
            from _stg_funnel_view_sessions as s
            join LAKE.SEGMENT_GFB.REACT_NATIVE_JUSTFAB_PRODUCT_VIEWED as p on IFNULL(try_to_number(p.PROPERTIES_SESSION_ID)::VARCHAR, '0')::VARCHAR = s.meta_original_session_id
            where
                p.TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
                and p.PROPERTIES_SESSION_ID <> '0'
                AND p.TIMESTAMP::datetime >= $min_watermark
                AND NOT COALESCE(s.IS_PDP_VISIT,'') ILIKE 'true'

            union

            select distinct SESSION_ID
            from _stg_funnel_view_sessions as s
            join LAKE.SEGMENT_GFB.JAVASCRIPT_FABKIDS_PRODUCT_VIEWED as p on try_to_number(p.PROPERTIES_SESSION_ID) = s.meta_original_session_id
            where
                p.TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
                AND p.TIMESTAMP::datetime >= $min_watermark
                AND NOT COALESCE(s.IS_PDP_VISIT,'') ILIKE 'true'

            union

            select distinct SESSION_ID
            from _stg_funnel_view_sessions as s
            join LAKE.SEGMENT_GFB.JAVASCRIPT_SHOEDAZZLE_PRODUCT_VIEWED as p on try_to_number(p.PROPERTIES_SESSION_ID) = s.meta_original_session_id
            where
                p.TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
                AND p.TIMESTAMP::datetime >= $min_watermark
                AND NOT COALESCE(s.IS_PDP_VISIT,'') ILIKE 'true'

            union

            select distinct SESSION_ID
            from _stg_funnel_view_sessions as s
            join LAKE.SEGMENT_SXF.JAVASCRIPT_SXF_PRODUCT_VIEWED as p on try_to_number(p.PROPERTIES_SESSION_ID) = s.meta_original_session_id
            where
                p.TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
                AND p.TIMESTAMP::datetime >= $min_watermark
                AND NOT COALESCE(s.IS_PDP_VISIT,'') ILIKE 'true'
        );

update reporting_base_prod.SHARED.SESSION_ORDER_AB_TEST_DETAIL
set IS_PDP_VISIT = FALSE
where
    TEST_KEY in (select distinct TEST_KEY from _stg_funnel_view_sessions)
    and IS_PDP_VISIT is null;

------------------------------------------------------------------------
--ATB

update reporting_base_prod.SHARED.SESSION_ORDER_AB_TEST_DETAIL
set is_atb_action = TRUE
where
    TEST_KEY in (select distinct TEST_KEY from _stg_funnel_view_sessions)
    and SESSION_ID in
        (
            select distinct SESSION_ID
            from _stg_funnel_view_sessions as s
            join LAKE.SEGMENT_FL.JAVA_FABLETICS_PRODUCT_ADDED as p on try_to_number(p.PROPERTIES_SESSION_ID) = s.meta_original_session_id
            where
                p.TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
                AND p.TIMESTAMP::datetime >= $min_watermark
                AND NOT COALESCE(s.IS_ATB_ACTION,'') ILIKE 'true'

            union

            select distinct SESSION_ID
            from _stg_funnel_view_sessions as s
            join LAKE.SEGMENT_FL.JAVA_FABLETICS_ECOM_MOBILE_APP_PRODUCT_ADDED as p on IFNULL(try_to_number(p.PROPERTIES_SESSION_ID)::VARCHAR, '0')::VARCHAR = s.meta_original_session_id
            where
                p.TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
                and p.PROPERTIES_SESSION_ID <> '0'
                AND p.TIMESTAMP::datetime >= $min_watermark
                AND NOT COALESCE(s.IS_ATB_ACTION,'') ILIKE 'true'

            union

            select distinct SESSION_ID
            from _stg_funnel_view_sessions as s
            join LAKE.SEGMENT_GFB.JAVA_JUSTFAB_PRODUCT_ADDED as p on try_to_number(p.PROPERTIES_SESSION_ID) = s.meta_original_session_id
            where
                p.TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
                AND p.TIMESTAMP::datetime >= $min_watermark
                AND NOT COALESCE(s.IS_ATB_ACTION,'') ILIKE 'true'

            union

            select distinct SESSION_ID
            from _stg_funnel_view_sessions as s
            join LAKE.SEGMENT_GFB.REACT_NATIVE_JUSTFAB_ADD_TO_CART as p on IFNULL(try_to_number(p.PROPERTIES_SESSION_ID)::VARCHAR, '0')::VARCHAR = s.meta_original_session_id
            where
                p.TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
                and p.PROPERTIES_SESSION_ID <> '0'
                AND p.TIMESTAMP::datetime >= $min_watermark
                AND NOT COALESCE(s.IS_ATB_ACTION,'') ILIKE 'true'

            union

            select distinct SESSION_ID
            from _stg_funnel_view_sessions as s
            join LAKE.SEGMENT_GFB.JAVA_FABKIDS_PRODUCT_ADDED as p on try_to_number(p.PROPERTIES_SESSION_ID) = s.meta_original_session_id
            where
                p.TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
                AND p.TIMESTAMP::datetime >= $min_watermark
                AND NOT COALESCE(s.IS_ATB_ACTION,'') ILIKE 'true'

            union

            select distinct SESSION_ID
            from _stg_funnel_view_sessions as s
            join LAKE.SEGMENT_GFB.JAVA_SHOEDAZZLE_PRODUCT_ADDED as p on try_to_number(p.PROPERTIES_SESSION_ID) = s.meta_original_session_id
            where
                p.TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
                AND p.TIMESTAMP::datetime >= $min_watermark
                AND NOT COALESCE(s.IS_ATB_ACTION,'') ILIKE 'true'

            union

            select distinct SESSION_ID
            from _stg_funnel_view_sessions as s
            join LAKE.SEGMENT_SXF.JAVA_SXF_PRODUCT_ADDED as p on try_to_number(p.PROPERTIES_SESSION_ID) = s.meta_original_session_id
            where
                p.TIMESTAMP::datetime >= AB_TEST_START_UTC_DATETIME
                AND p.TIMESTAMP::datetime >= $min_watermark
                AND NOT COALESCE(s.IS_ATB_ACTION,'') ILIKE 'true'
        );

update reporting_base_prod.SHARED.SESSION_ORDER_AB_TEST_DETAIL
set is_atb_action = FALSE
where
    TEST_KEY in (select distinct TEST_KEY from _stg_funnel_view_sessions)
    and is_atb_action is null;

-------------------------------------------------------------------------------------------------------

COMMIT; --comment


-- select
--     version,test_key,test_label,test_group,count(*) as sessions,count(distinct session_id) as sessions_distinct,sessions - sessions_distinct as diff
-- from reporting_base_prod.shared.session_order_ab_test_detail
-- where test_label in ('dafd1514a3db41a88d5fbb869da90d81','fe90b100b35540b3a50dd959d9cd2dcc')
-- group by 1,2,3,4
-- order by 7 desc

