

CREATE OR REPLACE TEMPORARY TABLE _orders_by_session as
select
    'same session' as order_version
    ,s.SESSION_ID
     ,s.test_key
     ,s.test_label
     ,s.AB_TEST_SEGMENT
     ,s.CUSTOMER_ID

--activating (24)
     ,SUM(CASE WHEN membership_order_type_L1 = 'Activating VIP' and datediff(hour,pp.REGISTRATION_LOCAL_DATETIME,ORDER_LOCAL_DATETIME) <= 1 THEN 1 ELSE 0 END) AS vip_activations_1hr
     ,SUM(CASE WHEN membership_order_type_L1 = 'Activating VIP' and datediff(hour,pp.REGISTRATION_LOCAL_DATETIME,ORDER_LOCAL_DATETIME) <= 3 THEN 1 ELSE 0 END) AS vip_activations_3hr
     ,SUM(CASE WHEN membership_order_type_L1 = 'Activating VIP' and datediff(hour,pp.REGISTRATION_LOCAL_DATETIME,ORDER_LOCAL_DATETIME) <= 24 THEN 1 ELSE 0 END) AS vip_activations_24hr
     ,SUM(CASE WHEN membership_order_type_L1 = 'Activating VIP' then 1 else 0 end) AS vip_activations
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then TTL_UNITS end) as units_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_subtotal_excl_tariff end) as product_subtotal_excl_tariff_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_subtotal_incl_tariff end) as product_subtotal_incl_tariff_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_discount end) as product_discount_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_shipping_discount end) as shipping_discount_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_discount + ttl_shipping_discount end) as total_discount_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then TTL_SHIPPING_REVENUE end) as shipping_revenue_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_tariff end) as tariff_surcharge_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_tax end) as tax_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_gross_revenue end) as product_gross_revenue_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_gross_revenue_excl_shipping end) as product_gross_revenue_excl_shipping_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_margin_pre_return end) as product_margin_pre_return_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then TTL_CASH_GROSS_REVENUE end) as cash_gross_revenue_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then TTL_CASH_CREDIT end) as cash_credit_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_non_cash_credit end) as noncash_credit_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_cost end) as product_cost_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_landed_cost end) as landed_cost_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_shipping_cost end) as shipping_cost_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_cost + ttl_landed_cost + ttl_shipping_cost end) as total_cogs_activating
     ,count(distinct case when membership_order_type_L1 = 'Activating VIP' and IS_PREPAID_CREDITCARD = TRUE then pp.order_id end) as prepaid_creditcard_orders_activating
     ,count(distinct case when membership_order_type_L1 = 'Activating VIP' and IS_PREPAID_CREDITCARD_FAILURE = TRUE then pp.order_id end) as prepaid_creditcard_failed_attempts_activating
     ,count(distinct case when membership_order_type_L1 = 'Activating VIP' and ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_activating

--nonactivating
     ,count(distinct case when membership_order_type_L1 <> 'Activating VIP' then pp.ORDER_ID end) as orders_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then TTL_UNITS end) as units_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_subtotal_excl_tariff end) as product_subtotal_excl_tariff_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_subtotal_incl_tariff end) as product_subtotal_incl_tariff_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_discount end) as product_discount_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_shipping_discount end) as shipping_discount_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_discount + ttl_shipping_discount end) as total_discount_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then TTL_SHIPPING_REVENUE end) as shipping_revenue_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_tariff end) as tariff_surcharge_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_tax end) as tax_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_gross_revenue end) as product_gross_revenue_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_gross_revenue_excl_shipping end) as product_gross_revenue_excl_shipping_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_margin_pre_return end) as product_margin_pre_return_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then TTL_CASH_GROSS_REVENUE end) as cash_gross_revenue_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then TTL_CASH_CREDIT end) as cash_credit_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_non_cash_credit end) as noncash_credit_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_cost end) as product_cost_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_landed_cost end) as landed_cost_nonactivating
     ,sum(case when membership_order_type_L1 <>'Activating VIP' then ttl_shipping_cost end) as shipping_cost_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_cost + ttl_landed_cost + ttl_shipping_cost end) as total_cogs_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_membership_credit_redeemed end) as membership_credit_redeemed_amount_nonactivating
--      ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_token_redeemed end) as membership_token_redeemed_amount_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_credits_token_redeemed_count end) as membership_credits_token_redeemed_count_nonactivating
     ,count(distinct case when membership_order_type_L1 <> 'Activating VIP' and IS_PREPAID_CREDITCARD = TRUE then pp.order_id end) as prepaid_creditcard_orders_nonactivating
     ,count(distinct case when membership_order_type_L1 <> 'Activating VIP' and IS_PREPAID_CREDITCARD_FAILURE = TRUE then pp.order_id end) as prepaid_creditcard_failed_attempts_nonactivating
     ,count(distinct case when membership_order_type_L1 <> 'Activating VIP' and ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_nonactivating

--nonactivating + activating
     ,count(distinct pp.ORDER_ID) as orders_total
     ,sum(TTL_UNITS) as units_total
     ,sum(ttl_subtotal_excl_tariff) as product_subtotal_excl_tariff_total
     ,sum(ttl_product_subtotal_incl_tariff) as product_subtotal_incl_tariff_total
     ,sum(ttl_product_discount) as product_discount_total
     ,sum(ttl_shipping_discount) as shipping_discount_total
     ,sum(ttl_product_discount + ttl_shipping_discount) as total_discount_total
     ,sum(TTL_SHIPPING_REVENUE) as shipping_revenue_total
     ,sum(ttl_tariff) as tariff_surcharge_total
     ,sum(ttl_tax) as tax_total
     ,sum(ttl_product_gross_revenue) as product_gross_revenue_total
     ,sum(ttl_product_gross_revenue_excl_shipping) as product_gross_revenue_excl_shipping_total
     ,sum(ttl_product_margin_pre_return) as product_margin_pre_return_total
     ,sum(TTL_CASH_GROSS_REVENUE) as cash_gross_revenue_total
     ,sum(ttl_cash_credit) as cash_credit_amount_total
     ,sum(ttl_non_cash_credit) as noncash_credit_amount_total
     ,sum(ttl_product_cost) as product_cost_total
     ,sum(ttl_landed_cost) as landed_cost_total
     ,sum(ttl_shipping_cost) as shipping_cost_total
     ,sum(ttl_product_cost + ttl_landed_cost + ttl_shipping_cost) as total_cogs_total
     ,count(distinct case when IS_PREPAID_CREDITCARD = TRUE then pp.order_id end) as prepaid_creditcard_orders_total
     ,count(distinct case when IS_PREPAID_CREDITCARD_FAILURE = TRUE then pp.order_id end) as prepaid_creditcard_failed_attempts_total
     ,count(distinct case when ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_total
FROM REPORTING_BASE_PROD.SHARED.SESSION_AB_TEST_METADATA s
join reporting_base_prod.shared.order_line_psource_payment as pp on pp.SESSION_ID = s.SESSION_ID
where
        TTL_ORDER_RNK = 1
  AND ORDER_SALES_CHANNEL_L1 = 'Online Order'
  and ORDER_CLASSIFICATION_L1 = 'Product Order'
  and ORDER_LOCAL_DATETIME >= AB_TEST_START_LOCAL_DATETIME
group by
    s.SESSION_ID
    ,s.test_key
    ,s.test_label
    ,s.AB_TEST_SEGMENT
    ,s.CUSTOMER_ID;


CREATE OR REPLACE TEMPORARY TABLE _vips_from_lead_registrations as
select
    'by lead reg' as order_version
    ,s.SESSION_ID
     ,s.test_key
     ,s.test_label
     ,s.AB_TEST_SEGMENT
     ,s.CUSTOMER_ID
     ,SUM(CASE WHEN membership_order_type_L1 = 'Activating VIP' and datediff(hour,pp.REGISTRATION_LOCAL_DATETIME,ORDER_LOCAL_DATETIME) <= 1 THEN 1 ELSE 0 END) AS vip_activations_1hr
     ,SUM(CASE WHEN membership_order_type_L1 = 'Activating VIP' and datediff(hour,pp.REGISTRATION_LOCAL_DATETIME,ORDER_LOCAL_DATETIME) <= 3 THEN 1 ELSE 0 END) AS vip_activations_3hr
     ,SUM(CASE WHEN membership_order_type_L1 = 'Activating VIP' and datediff(hour,pp.REGISTRATION_LOCAL_DATETIME,ORDER_LOCAL_DATETIME) <= 24 THEN 1 ELSE 0 END) AS vip_activations_24hr
     ,SUM(CASE WHEN membership_order_type_L1 = 'Activating VIP' then 1 else 0 end) AS vip_activations
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then TTL_UNITS end) as units_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_subtotal_excl_tariff end) as product_subtotal_excl_tariff_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_subtotal_incl_tariff end) as product_subtotal_incl_tariff_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_discount end) as product_discount_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_shipping_discount end) as shipping_discount_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_discount + ttl_shipping_discount end) as total_discount_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then TTL_SHIPPING_REVENUE end) as shipping_revenue_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_tariff end) as tariff_surcharge_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_tax end) as tax_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_gross_revenue end) as product_gross_revenue_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_gross_revenue_excl_shipping end) as product_gross_revenue_excl_shipping_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_margin_pre_return end) as product_margin_pre_return_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then TTL_CASH_GROSS_REVENUE end) as cash_gross_revenue_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then TTL_CASH_CREDIT end) as cash_credit_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_non_cash_credit end) as noncash_credit_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_cost end) as product_cost_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_landed_cost end) as landed_cost_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_shipping_cost end) as shipping_cost_activating
     ,sum(case when membership_order_type_L1 = 'Activating VIP' then ttl_product_cost + ttl_landed_cost + ttl_shipping_cost end) as total_cogs_activating
     ,count(distinct case when membership_order_type_L1 = 'Activating VIP' and IS_PREPAID_CREDITCARD = TRUE then pp.order_id end) as prepaid_creditcard_orders_activating
     ,count(distinct case when membership_order_type_L1 = 'Activating VIP' and IS_PREPAID_CREDITCARD_FAILURE = TRUE then pp.order_id end) as prepaid_creditcard_failed_attempts_activating
     ,count(distinct case when membership_order_type_L1 = 'Activating VIP' and ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_activating

--nonactivating
     ,count(distinct case when membership_order_type_L1 <> 'Activating VIP' then pp.ORDER_ID end) as orders_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then TTL_UNITS end) as units_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_subtotal_excl_tariff end) as product_subtotal_excl_tariff_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_subtotal_incl_tariff end) as product_subtotal_incl_tariff_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_discount end) as product_discount_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_shipping_discount end) as shipping_discount_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_discount + ttl_shipping_discount end) as total_discount_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then TTL_SHIPPING_REVENUE end) as shipping_revenue_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_tariff end) as tariff_surcharge_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_tax end) as tax_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_gross_revenue end) as product_gross_revenue_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_gross_revenue_excl_shipping end) as product_gross_revenue_excl_shipping_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_margin_pre_return end) as product_margin_pre_return_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then TTL_CASH_GROSS_REVENUE end) as cash_gross_revenue_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then TTL_CASH_CREDIT end) as cash_credit_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_non_cash_credit end) as noncash_credit_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_cost end) as product_cost_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_landed_cost end) as landed_cost_nonactivating
     ,sum(case when membership_order_type_L1 <>'Activating VIP' then ttl_shipping_cost end) as shipping_cost_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_product_cost + ttl_landed_cost + ttl_shipping_cost end) as total_cogs_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_membership_credit_redeemed end) as membership_credit_redeemed_amount_nonactivating
     ,sum(case when membership_order_type_L1 <> 'Activating VIP' then ttl_credits_token_redeemed_count end) as membership_credits_redeemed_count_nonactivating
     ,count(distinct case when membership_order_type_L1 <> 'Activating VIP' and IS_PREPAID_CREDITCARD = TRUE then pp.order_id end) as prepaid_creditcard_orders_nonactivating
     ,count(distinct case when membership_order_type_L1 <> 'Activating VIP' and IS_PREPAID_CREDITCARD_FAILURE = TRUE then pp.order_id end) as prepaid_creditcard_failed_attempts_nonactivating
     ,count(distinct case when membership_order_type_L1 <> 'Activating VIP' and ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_nonactivating

--nonactivating + activating
     ,count(distinct pp.ORDER_ID) as orders_total
     ,sum(TTL_UNITS) as units_total
     ,sum(ttl_subtotal_excl_tariff) as product_subtotal_excl_tariff_total
     ,sum(ttl_product_subtotal_incl_tariff) as product_subtotal_incl_tariff_total
     ,sum(ttl_product_discount) as product_discount_total
     ,sum(ttl_shipping_discount) as shipping_discount_total
     ,sum(ttl_product_discount + ttl_shipping_discount) as total_discount_total
     ,sum(TTL_SHIPPING_REVENUE) as shipping_revenue_total
     ,sum(ttl_tariff) as tariff_surcharge_total
     ,sum(ttl_tax) as tax_total
     ,sum(ttl_product_gross_revenue) as product_gross_revenue_total
     ,sum(ttl_product_gross_revenue_excl_shipping) as product_gross_revenue_excl_shipping_total
     ,sum(ttl_product_margin_pre_return) as product_margin_pre_return_total
     ,sum(TTL_CASH_GROSS_REVENUE) as cash_gross_revenue_total
     ,sum(ttl_cash_credit) as cash_credit_amount_total
     ,sum(ttl_non_cash_credit) as noncash_credit_amount_total
     ,sum(ttl_product_cost) as product_cost_total
     ,sum(ttl_landed_cost) as landed_cost_total
     ,sum(ttl_shipping_cost) as shipping_cost_total
     ,sum(ttl_product_cost + ttl_landed_cost + ttl_shipping_cost) as total_cogs_total
     ,count(distinct case when IS_PREPAID_CREDITCARD = TRUE then pp.order_id end) as prepaid_creditcard_orders_total
     ,count(distinct case when IS_PREPAID_CREDITCARD_FAILURE = TRUE then pp.order_id end) as prepaid_creditcard_failed_attempts_total
     ,count(distinct case when ORDER_STATUS not in ('Success','Pending') then pp.order_id end) as failed_orders_total
FROM REPORTING_BASE_PROD.SHARED.SESSION_AB_TEST_METADATA s
join edw_prod.DATA_MODEL.FACT_ACTIVATION as f on f.CUSTOMER_ID = s.CUSTOMER_ID
join reporting_base_prod.shared.order_line_psource_payment as pp on pp.SESSION_ID = f.SESSION_ID
    and TTL_ORDER_RNK = 1
    AND ORDER_SALES_CHANNEL_L1 = 'Online Order'
where
    s.SESSION_ID not in (select distinct SESSION_ID from _orders_by_session)
    and s.SESSION_ID <> f.SESSION_ID
    and IS_LEAD_REGISTRATION_ACTION = TRUE
    and TEST_START_MEMBERSHIP_STATE = 'Prospect'
    and SOURCE_ACTIVATION_LOCAL_DATETIME >= s.AB_TEST_START_LOCAL_DATETIME
group by
    s.SESSION_ID
    ,s.test_key
    ,s.test_label
    ,s.AB_TEST_SEGMENT
    ,s.CUSTOMER_ID;

CREATE OR REPLACE TRANSIENT TABLE reporting_base_prod.shared.session_ab_test_orders as

select * from _orders_by_session

union

select * from _vips_from_lead_registrations;

ALTER TABLE reporting_base_prod.shared.session_ab_test_orders SET DATA_RETENTION_TIME_IN_DAYS = 0;


-- select
--     test_key
--     ,test_label
--     ,AB_TEST_SEGMENT
--     ,count(*),count(distinct SESSION_ID)
-- from reporting_base_prod.shared.session_ab_test_orders group by 1,2,3
