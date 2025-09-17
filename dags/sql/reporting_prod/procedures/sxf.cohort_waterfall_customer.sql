SET execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);
USE EDW_PROD;

CREATE OR REPLACE TEMPORARY TABLE _retail_price AS
(
    SELECT old.customer_id
        ,DATE_TRUNC('MONTH',DATE(old.order_local_datetime)) order_month
        ,fa.vip_cohort_month_date
        ,SUM(initial_retail_price_excl_vat) initial_retail_price_excl_vat
        ,SUM(iff(is_activating = 'Activating VIP', initial_retail_price_excl_vat,0)) Activating_initial_retail_price_excl_vat
    FROM REPORTING_PROD.SXF.VIEW_ORDER_LINE_RECOGNIZED_DATASET old
    JOIN EDW_PROD.DATA_MODEL_SXF.FACT_ORDER fo ON fo.order_id = old.order_id
    LEFT JOIN EDW_PROD.DATA_MODEL_SXF.FACT_ACTIVATION fa ON fa.activation_key = fo.activation_key
    GROUP BY old.customer_id
        ,order_month
        ,vip_cohort_month_date
);


CREATE OR REPLACE TEMPORARY TABLE _all_credits AS
SELECT DISTINCT
    fo.order_id,
    fo.customer_id,
    fo.order_local_datetime,
    fa.vip_cohort_month_date,
    COALESCE(oc.store_credit_id, oc.membership_token_id, oc.gift_certificate_id) AS credit_id
FROM EDW_PROD.DATA_MODEL_SXF.FACT_ORDER fo
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE ds ON ds.store_id = fo.store_id
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_ORDER_STATUS os ON os.order_status_key = fo.order_status_key
JOIN EDW_PROD.DATA_MODEL_SXF.DIM_ORDER_SALES_CHANNEL osc ON osc.order_sales_channel_key = fo.order_sales_channel_key
LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.ORDER_CREDIT AS oc ON oc.order_id = fo.order_id
LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.STORE_CREDIT AS sc ON sc.store_credit_id = oc.store_credit_id
LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.MEMBERSHIP_STORE_CREDIT msc ON sc.store_credit_id = msc.store_credit_id
--LEFT JOIN LAKE_VIEW.ULTRA_MERCHANT.ORDER_CREDIT_DELETE_LOG ocdl ON ocdl.order_credit_id = oc.order_credit_id
LEFT JOIN EDW_PROD.DATA_MODEL_SXF.FACT_ACTIVATION fa ON fa.activation_key = fo.activation_key
WHERE credit_id IS NOT NULL --AND ocdl.order_credit_id IS NULL
    AND store_brand = 'Savage X'
    AND LOWER(osc.order_classification_l1) = 'product order'
    AND LOWER(os.order_status) IN ('success', 'pending');


CREATE OR REPLACE TEMPORARY TABLE _orders_with_credit_redemptions AS
SELECT cc.customer_id
        ,DATE_TRUNC('MONTH',DATE(order_local_datetime)) redeemed_month
        ,vip_cohort_month_date
        ,COUNT(DISTINCT order_id) order_count_with_credit_redemption
FROM _all_credits cc
LEFT JOIN EDW_PROD.DATA_MODEL_SXF.DIM_CREDIT dc ON dc.credit_id = cc.credit_id and dc.customer_id = cc.customer_id
WHERE dc.credit_type NOT ilike 'Variable Credit'
GROUP BY cc.customer_id,
    redeemed_month,
    vip_cohort_month_date;


CREATE OR REPLACE TEMPORARY TABLE _vip_product_count AS
(
    SELECT
        edw_prod.stg.udf_unconcat_brand(customer_id) customer_id,
        vip_cohort_month_date,
        SUM(clvm.product_order_count) vip_purchase_count
    FROM EDW_PROD.ANALYTICS_BASE.CUSTOMER_LIFETIME_VALUE_MONTHLY clvm
    JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE ds ON ds.store_id = clvm.store_id
    WHERE
        ds.store_brand = 'Savage X'
        AND (clvm.is_bop_vip = TRUE OR clvm.month_date = clvm.vip_cohort_month_date)
    GROUP BY
            edw_prod.stg.udf_unconcat_brand(customer_id),
            vip_cohort_month_date
);

CREATE OR REPLACE TEMPORARY TABLE _cohort_waterfall_activating AS
(
    SELECT
        edw_prod.stg.udf_unconcat_brand(customer_id) customer_id,
        activation_key,
        SUM(nvl(activating_product_order_count,0)) AS activating_product_order_count,
        SUM(nvl(activating_product_order_unit_count,0)) AS activating_product_order_unit_count,
        SUM(nvl(activating_product_order_product_discount_amount,0)) AS activating_product_order_product_discount_amount,
        SUM(nvl(activating_product_order_subtotal_excl_tariff_amount,0)) AS activating_product_order_subtotal_excl_tariff_amount,
        SUM(nvl(activating_product_order_shipping_revenue_amount,0)) AS activating_product_order_shipping_revenue_amount,
          SUM(nvl(activating_product_gross_revenue,0)) AS activating_product_gross_revenue,
        SUM(nvl(activating_product_order_landed_product_cost_amount,0)) AS activating_product_order_landed_product_cost_amount
    FROM EDW_PROD.ANALYTICS_BASE.CUSTOMER_LIFETIME_VALUE_LTD clv
    JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE ds ON ds.store_id = clv.store_id
    WHERE ds.store_brand = 'Savage X'
    GROUP BY
             edw_prod.stg.udf_unconcat_brand(customer_id),
             activation_key
);

CREATE OR REPLACE TEMPORARY TABLE _emp_opt_in AS
(
    SELECT DISTINCT
    	m.customer_id
        ,TO_DATE(DATE_TRUNC('MONTH', cd.datetime_modified)) month_opted_in
    FROM LAKE_SXF_VIEW.ULTRA_MERCHANT.MEMBERSHIP m
    JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE ds ON ds.store_id = m.store_id
    JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.MEMBERSHIP_TYPE mt ON m.membership_type_id = mt.membership_type_id
    LEFT JOIN LAKE_SXF_VIEW.ULTRA_MERCHANT.CUSTOMER_DETAIL cd ON cd.customer_id = m.customer_id
    WHERE
        ds.store_brand = 'Savage X'
        AND ds.store_full_name NOT ILIKE '%(DM)%'
        AND cd.name = 'nmp_opt_in_true'
        AND UPPER(cd.value) IN ('TRUE', 'YES')
);

CREATE OR REPLACE TEMPORARY TABLE _omni_classification AS
(
    SELECT customer_id
        ,activation_key
        ,order_channel
        ,ROW_NUMBER() OVER(PARTITION BY customer_id,activation_key ORDER BY order_local_datetime_start DESC) row_num
    FROM REPORTING_PROD.SXF.VIEW_OMNI_CLASSIFICATION
    WHERE activation_key IS NOT NULL
         qualify row_num = 1
);


CREATE OR REPLACE TEMPORARY TABLE _repeat_produdct_order_metrics AS (
select customer_id
      ,activation_key
      ,date_trunc('month',order_local_datetime)::date as order_month
      ,count(Order_id) as repeat_vip_product_order_count
from EDW_PROD.DATA_MODEL_SXF.FACT_ORDER fo
join EDW_PROD.DATA_MODEL_SXF.DIM_ORDER_SALES_CHANNEL dosc on fo.order_sales_channel_key = dosc.order_sales_channel_key
join EDW_PROD.DATA_MODEL_SXF.DIM_STORE ds on ds.store_id = fo.store_id
        where ds.store_brand_abbr = 'SX'
        and ds.STORE_FULL_NAME not like '%(DM)%'
        and ORDER_CLASSIFICATION_L2 = 'Product Order'
        and ORDER_MEMBERSHIP_CLASSIFICATION_KEY = '4'  -- Repeat Vip
        and order_status_key = 1 -- Success
group by 1,2,3
);

CREATE OR REPLACE TEMPORARY TABLE _cohort_waterfall_customer_final_output_stg AS
(
    SELECT
        edw_prod.stg.udf_unconcat_brand(clvm.customer_id) customer_id
        ,clvm.vip_cohort_month_date
        ,clvm.membership_type
        ,clvm.month_date
        ,CASE
            WHEN clvm.vip_cohort_month_date = clvm.month_date AND clvm.is_reactivated_vip = FALSE THEN 'Activation'
            WHEN clvm.vip_cohort_month_date = clvm.month_date AND clvm.is_reactivated_vip = TRUE THEN 'Reactivation'
            WHEN clvm.is_bop_vip = TRUE THEN 'BOP'
            ELSE 'Cancelled'
        END AS membership_monthly_status
        ,ds.store_brand
        ,ds.store_region
        ,ds.store_country
        ,ds.store_name
        ,COALESCE(vip_ds.store_type,'Unsure') AS activation_store_type
        ,COALESCE(vip_ds.store_full_name,'Unsure') AS activation_store_full_name
        ,CASE
            WHEN omc.order_channel ILIKE 'Omni' THEN 'Omni Channel'
            WHEN omc.order_channel ILIKE 'Online' THEN 'Online Purchaser Only'
            WHEN omc.order_channel ILIKE 'Retail' THEN 'Retail Purchaser Only'
            ELSE 'Unsure'
        END AS omni_channel
        ,CASE
            WHEN clvm.is_reactivated_vip = TRUE THEN 'Reactivated Membership'
            WHEN clvm.is_reactivated_vip =FALSE THEN 'First Membership'
            ELSE 'Unsure'
        END AS membership_sequence
        ,CASE
            WHEN vp.vip_purchase_count > 1 THEN 'Repeat Purchaser'
            WHEN vp.vip_purchase_count = 1 THEN 'Activating Purchaser Only'
            ELSE 'Unsure'
         END AS repeat_purchaser
        ,(iff(clvm.vip_cohort_month_date = clvm.month_date, cwa.activating_product_order_unit_count, 0)) AS activating_product_order_unit_count
        ,(iff(clvm.vip_cohort_month_date = clvm.month_date, cwa.activating_product_order_count, 0)) AS activating_product_order_count
        ,(iff(clvm.vip_cohort_month_date = clvm.month_date, cwa.activating_product_order_product_discount_amount, 0)) AS activating_product_order_product_discount_amount
        ,(iff(clvm.vip_cohort_month_date = clvm.month_date, cwa.activating_product_order_subtotal_excl_tariff_amount, 0)) AS activating_product_order_subtotal_excl_tariff_amount
        ,(iff(clvm.vip_cohort_month_date = clvm.month_date, cwa.activating_product_order_shipping_revenue_amount, 0)) AS activating_product_order_shipping_revenue_amount
        ,(iff(clvm.vip_cohort_month_date = clvm.month_date, cwa.activating_product_gross_revenue, 0)) AS activating_product_gross_revenue
        ,(iff(clvm.vip_cohort_month_date = clvm.month_date, cwa.activating_product_order_landed_product_cost_amount, 0)) AS activating_product_order_landed_product_cost_amount
        ,(iff(clvm.vip_cohort_month_date <> clvm.month_date, clvm.product_order_unit_count, (clvm.product_order_unit_count - cwa.activating_product_order_unit_count))) AS nonactivating_product_order_unit_count
        ,(iff(clvm.vip_cohort_month_date <> clvm.month_date, clvm.product_order_count, (NVL(clvm.product_order_count, 0) - cwa.activating_product_order_count))) AS nonactivating_product_order_count
        ,(iff(clvm.vip_cohort_month_date <> clvm.month_date, clvm.product_order_product_discount_amount, (NVL(clvm.product_order_product_discount_amount, 0) - cwa.activating_product_order_product_discount_amount))) AS nonactivating_product_order_product_discount_amount
        ,(iff(clvm.vip_cohort_month_date <> clvm.month_date, clvm.product_order_subtotal_excl_tariff_amount, (NVL(clvm.product_order_subtotal_excl_tariff_amount , 0) - cwa.activating_product_order_subtotal_excl_tariff_amount))) AS nonactivating_product_order_subtotal_excl_tariff_amount
        ,(iff(clvm.vip_cohort_month_date <> clvm.month_date, clvm.product_order_shipping_revenue_amount, (NVL(clvm.product_order_shipping_revenue_amount, 0) - cwa.activating_product_order_shipping_revenue_amount))) AS nonactivating_product_order_shipping_revenue_amount
        ,(iff(clvm.vip_cohort_month_date <> clvm.month_date, clvm.product_gross_revenue, (NVL(clvm.product_gross_revenue, 0) - cwa.activating_product_gross_revenue))) AS nonactivating_product_gross_revenue
        ,(iff(clvm.vip_cohort_month_date <> clvm.month_date, clvm.product_order_landed_product_cost_amount, (NVL(clvm.product_order_landed_product_cost_amount, 0) - cwa.activating_product_order_landed_product_cost_amount))) AS nonactivating_product_order_landed_product_cost_amount
        ,((clvm.is_cancel = TRUE)::int) AS vip_cancellation_count
        ,((clvm.is_bop_vip = TRUE)::int) AS bop_vip_count
        ,((clvm.vip_cohort_month_date = clvm.month_date)::int) AS activation_count
        ,((clvm.is_bop_vip = TRUE AND clvm.is_failed_billing = TRUE)::int) AS failed_billing_count
        ,((clvm.is_merch_purchaser = TRUE AND clvm.is_bop_vip = TRUE)::int) AS merch_purchase_count
        ,((clvm.is_bop_vip = TRUE AND (clvm.is_merch_purchaser = TRUE OR (clvm.is_successful_billing = TRUE AND clvm.is_pending_billing = FALSE)))::int) AS purchase_count
        ,((clvm.vip_cohort_month_date = clvm.month_date AND clvm.is_reactivated_vip = TRUE)::int) AS reactivation_count
        ,((clvm.is_bop_vip = TRUE AND clvm.is_skip = TRUE)::int) AS skip_count
        ,((clvm.is_bop_vip = TRUE AND clvm.is_passive_cancel = TRUE AND clvm.is_cancel = TRUE)::int) AS vip_passive_cancellation_count
        ,((clvm.is_successful_billing = TRUE)::int) AS is_successful_billing_count
        ,(ifnull(clvm.online_product_order_count, 0)) AS online_product_order_count
        ,(ifnull(clvm.retail_product_order_count, 0)) AS retail_product_order_count
        ,(ifnull(clvm.mobile_app_product_order_count, 0)) AS mobile_app_product_order_count
        ,(ifnull(clvm.online_product_order_unit_count, 0)) AS online_product_order_unit_count
        ,(ifnull(clvm.retail_product_order_unit_count, 0)) AS retail_product_order_unit_count
        ,(ifnull(clvm.mobile_app_product_order_unit_count, 0)) AS mobile_app_product_order_unit_count
        ,(ifnull(clvm.online_product_order_subtotal_excl_tariff_amount, 0)) AS online_product_order_subtotal_excl_tariff_amount
        ,(ifnull(clvm.retail_product_order_subtotal_excl_tariff_amount, 0)) AS retail_product_order_subtotal_excl_tariff_amount
        ,(ifnull(clvm.mobile_app_product_order_subtotal_excl_tariff_amount, 0)) AS mobile_app_product_order_subtotal_excl_tariff_amount
        ,(ifnull(clvm.online_product_order_product_discount_amount, 0)) AS online_product_order_product_discount_amount
        ,(ifnull(clvm.retail_product_order_product_discount_amount, 0)) AS retail_product_order_product_discount_amount
        ,(ifnull(clvm.mobile_app_product_order_product_discount_amount, 0)) AS mobile_app_product_order_product_discount_amount
        ,(ifnull(clvm.online_product_order_shipping_revenue_amount, 0)) AS online_product_order_shipping_revenue_amount
        ,(ifnull(clvm.retail_product_order_shipping_revenue_amount, 0)) AS retail_product_order_shipping_revenue_amount
        ,(ifnull(clvm.mobile_app_product_order_shipping_revenue_amount, 0)) AS mobile_app_product_order_shipping_revenue_amount
        ,(ifnull(clvm.product_order_direct_cogs_amount, 0)) AS product_order_direct_cogs_amount
        ,(ifnull(clvm.online_product_order_direct_cogs_amount, 0)) AS online_product_order_direct_cogs_amount
        ,(ifnull(clvm.retail_product_order_direct_cogs_amount, 0)) AS retail_product_order_direct_cogs_amount
        ,(ifnull(clvm.mobile_app_product_order_direct_cogs_amount, 0)) AS mobile_app_product_order_direct_cogs_amount
        ,(ifnull(clvm.product_order_selling_expenses_amount, 0)) AS product_order_selling_expenses_amount
        ,(ifnull(clvm.online_product_order_selling_expenses_amount, 0)) AS online_product_order_selling_expenses_amount
        ,(ifnull(clvm.retail_product_order_selling_expenses_amount, 0)) AS retail_product_order_selling_expenses_amount
        ,(ifnull(clvm.mobile_app_product_order_selling_expenses_amount, 0)) AS mobile_app_product_order_selling_expenses_amount
        ,(ifnull(clvm.product_order_cash_refund_amount_and_chargeback_amount, 0)) AS product_order_cash_refund_amount_and_chargeback_amount
        ,(ifnull(clvm.online_product_order_cash_refund_chargeback_amount, 0)) AS online_product_order_cash_refund_chargeback_amount
        ,(ifnull(clvm.retail_product_order_cash_refund_chargeback_amount, 0)) AS retail_product_order_cash_refund_chargeback_amount
        ,(ifnull(clvm.mobile_app_product_order_cash_refund_chargeback_amount, 0)) AS mobile_app_product_order_cash_refund_chargeback_amount
        ,(ifnull(clvm.product_order_cash_credit_refund_amount, 0)) AS product_order_cash_credit_refund_amount
        ,(ifnull(clvm.online_product_order_cash_credit_refund_amount, 0)) AS online_product_order_cash_credit_refund_amount
        ,(ifnull(clvm.retail_product_order_cash_credit_refund_amount, 0)) AS retail_product_order_cash_credit_refund_amount
        ,(ifnull(clvm.mobile_app_product_order_cash_credit_refund_amount, 0)) AS mobile_app_product_order_cash_credit_refund_amount
        ,(ifnull(clvm.product_order_reship_exchange_order_count, 0)) AS product_order_reship_exchange_order_count
        ,(ifnull(clvm.online_product_order_reship_exchange_order_count, 0)) AS online_product_order_reship_exchange_order_count
        ,(ifnull(clvm.retail_product_order_reship_exchange_order_count, 0)) AS retail_product_order_reship_exchange_order_count
        ,(ifnull(clvm.mobile_app_product_order_reship_exchange_order_count, 0)) AS mobile_app_product_order_reship_exchange_order_count
        ,(ifnull(clvm.product_order_reship_exchange_unit_count, 0)) AS product_order_reship_exchange_unit_count
        ,(ifnull(clvm.online_product_order_reship_exchange_unit_count, 0)) AS online_product_order_reship_exchange_unit_count
        ,(ifnull(clvm.retail_product_order_reship_exchange_unit_count, 0)) AS retail_product_order_reship_exchange_unit_count
        ,(ifnull(clvm.mobile_app_product_order_reship_exchange_unit_count, 0)) AS mobile_app_product_order_reship_exchange_unit_count
        ,(ifnull(clvm.product_order_reship_exchange_direct_cogs_amount, 0)) AS product_order_reship_exchange_direct_cogs_amount
        ,(ifnull(clvm.online_product_order_reship_exchange_direct_cogs_amount, 0)) AS online_product_order_reship_exchange_direct_cogs_amount
        ,(ifnull(clvm.retail_product_order_reship_exchange_direct_cogs_amount, 0)) AS retail_product_order_reship_exchange_direct_cogs_amount
        ,(ifnull(clvm.mobile_app_product_order_reship_exchange_direct_cogs_amount, 0)) AS mobile_app_product_order_reship_exchange_direct_cogs_amount
        ,(ifnull(clvm.product_order_return_cogs_amount, 0)) AS product_order_return_cogs_amount
        ,(ifnull(clvm.online_product_order_return_cogs_amount, 0)) AS online_product_order_return_cogs_amount
        ,(ifnull(clvm.retail_product_order_return_cogs_amount, 0)) AS retail_product_order_return_cogs_amount
        ,(ifnull(clvm.mobile_app_product_order_return_cogs_amount, 0)) AS mobile_app_product_order_return_cogs_amount
        ,(ifnull(clvm.product_order_return_unit_count, 0)) AS product_order_return_unit_count
        ,(ifnull(clvm.online_product_order_return_unit_count, 0)) AS online_product_order_return_unit_count
        ,(ifnull(clvm.retail_product_order_return_unit_count, 0)) AS retail_product_order_return_unit_count
        ,(ifnull(clvm.mobile_app_product_order_return_unit_count, 0)) AS mobile_app_product_order_return_unit_count
        ,(ifnull(clvm.product_order_amount_to_pay, 0)) AS product_order_amount_to_pay
        ,(ifnull(clvm.online_product_order_amount_to_pay, 0)) AS online_product_order_amount_to_pay
        ,(ifnull(clvm.retail_product_order_amount_to_pay, 0)) AS retail_product_order_amount_to_pay
        ,(ifnull(clvm.mobile_app_product_order_amount_to_pay, 0)) AS mobile_app_product_order_amount_to_pay
        ,(ifnull(clvm.product_gross_revenue_excl_shipping, 0)) AS product_gross_revenue_excl_shipping
        ,(ifnull(clvm.online_product_gross_revenue_excl_shipping, 0)) AS online_product_gross_revenue_excl_shipping
        ,(ifnull(clvm.retail_product_gross_revenue_excl_shipping, 0)) AS retail_product_gross_revenue_excl_shipping
        ,(ifnull(clvm.mobile_app_product_gross_revenue_excl_shipping, 0)) AS mobile_app_product_gross_revenue_excl_shipping
        ,(ifnull(clvm.product_gross_revenue, 0)) AS product_gross_revenue
        ,(ifnull(clvm.online_product_gross_revenue, 0)) AS online_product_gross_revenue
        ,(ifnull(clvm.retail_product_gross_revenue, 0)) AS retail_product_gross_revenue
        ,(ifnull(clvm.mobile_app_product_gross_revenue, 0)) AS mobile_app_product_gross_revenue
        ,(ifnull(clvm.product_net_revenue, 0)) AS product_net_revenue
        ,(ifnull(clvm.online_product_net_revenue, 0)) AS online_product_net_revenue
        ,(ifnull(clvm.retail_product_net_revenue, 0)) AS retail_product_net_revenue
        ,(ifnull(clvm.mobile_app_product_net_revenue, 0)) AS mobile_app_product_net_revenue
        ,(ifnull(clvm.product_margin_pre_return, 0)) AS product_margin_pre_return
        ,(ifnull(clvm.online_product_margin_pre_return, 0)) AS online_product_margin_pre_return
        ,(ifnull(clvm.retail_product_margin_pre_return, 0)) AS retail_product_margin_pre_return
        ,(ifnull(clvm.product_margin_pre_return_excl_shipping, 0)) AS product_margin_pre_return_excl_shipping
        ,(ifnull(clvm.online_product_margin_pre_return_excl_shipping, 0)) AS online_product_margin_pre_return_excl_shipping
        ,(ifnull(clvm.retail_product_margin_pre_return_excl_shipping, 0)) AS retail_product_margin_pre_return_excl_shipping
        ,(ifnull(clvm.product_gross_profit, 0)) AS product_gross_profit
        ,(ifnull(clvm.online_product_gross_profit, 0)) AS online_product_gross_profit
        ,(ifnull(clvm.retail_product_gross_profit, 0)) AS retail_product_gross_profit
        ,(ifnull(clvm.mobile_app_product_gross_profit, 0)) AS mobile_app_product_gross_profit
        ,(ifnull(clvm.product_variable_contribution_profit, 0)) AS product_variable_contribution_profit
        ,(ifnull(clvm.online_product_variable_contribution_profit, 0)) AS online_product_variable_contribution_profit
        ,(ifnull(clvm.retail_product_variable_contribution_profit, 0)) AS retail_product_variable_contribution_profit
        ,(ifnull(clvm.mobile_app_product_variable_contribution_profit, 0)) AS mobile_app_product_variable_contribution_profit
        ,(ifnull(clvm.product_order_cash_gross_revenue_amount, 0)) AS product_order_cash_gross_revenue_amount
        ,(ifnull(clvm.online_product_order_cash_gross_revenue_amount, 0)) AS online_product_order_cash_gross_revenue_amount
        ,(ifnull(clvm.retail_product_order_cash_gross_revenue_amount, 0)) AS retail_product_order_cash_gross_revenue_amount
        ,(ifnull(clvm.mobile_app_product_order_cash_gross_revenue_amount, 0)) AS mobile_app_product_order_cash_gross_revenue_amount
        ,(ifnull(clvm.product_order_cash_net_revenue, 0)) AS product_order_cash_net_revenue
        ,(ifnull(clvm.online_product_order_cash_net_revenue, 0)) AS online_product_order_cash_net_revenue
        ,(ifnull(clvm.retail_product_order_cash_net_revenue, 0)) AS retail_product_order_cash_net_revenue
        ,(ifnull(clvm.mobile_app_product_order_cash_net_revenue, 0)) AS mobile_app_product_order_cash_net_revenue
        ,(ifnull(clvm.billing_cash_gross_revenue, 0)) AS billing_cash_gross_revenue
        ,(ifnull(clvm.billing_cash_net_revenue, 0)) AS billing_cash_net_revenue
        ,(ifnull(clvm.cash_gross_revenue, 0)) AS cash_gross_revenue
        ,(ifnull(clvm.cash_net_revenue, 0)) AS cash_net_revenue
        ,(ifnull(clvm.cash_gross_profit, 0)) AS cash_gross_profit
        ,(ifnull(clvm.cash_variable_contribution_profit, 0)) AS cash_variable_contribution_profit
        ,(ifnull(clvm.monthly_billed_credit_cash_transaction_amount, 0)) AS monthly_billed_credit_cash_transaction_amount
        ,(ifnull(clvm.membership_fee_cash_transaction_amount, 0)) AS membership_fee_cash_transaction_amount
        ,(ifnull(clvm.gift_card_transaction_amount, 0)) AS gift_card_transaction_amount
        ,(ifnull(clvm.legacy_credit_cash_transaction_amount, 0)) AS legacy_credit_cash_transaction_amount
        ,(ifnull(clvm.monthly_billed_credit_cash_refund_chargeback_amount, 0)) AS monthly_billed_credit_cash_refund_chargeback_amount
        ,(ifnull(clvm.membership_fee_cash_refund_chargeback_amount, 0)) AS membership_fee_cash_refund_chargeback_amount
        ,(ifnull(clvm.gift_card_cash_refund_chargeback_amount, 0)) AS gift_card_cash_refund_chargeback_amount
        ,(ifnull(clvm.legacy_credit_cash_refund_chargeback_amount, 0)) AS legacy_credit_cash_refund_chargeback_amount
        ,(ifnull(clvm.cumulative_cash_gross_profit, 0)) AS cumulative_cash_gross_profit
        ,(ifnull(clvm.cumulative_product_gross_profit, 0)) AS cumulative_product_gross_profit
        ,(ifnull(clvm.cumulative_cash_gross_profit_decile, 0)) AS cumulative_cash_gross_profit_decile
        ,(ifnull(clvm.cumulative_product_gross_profit_decile, 0)) AS cumulative_product_gross_profit_decile
        ,(ifnull(clvm.billed_cash_credit_cancelled_equivalent_count,0)) AS billed_cash_credit_cancelled_equivalent_count
        ,(ifnull(clvm.billed_cash_credit_redeemed_equivalent_count,0)) AS billed_cash_credit_redeemed_equivalent_count
        ,(ifnull(clvltd.customer_acquisition_cost,0)) AS media_spend
        ,(CASE WHEN e.customer_id IS NOT NULL THEN 1 ELSE 0 END) AS cumulative_total_emp_opt_in
        ,(CASE WHEN e.customer_id IS NOT NULL AND (clvm.is_bop_vip = TRUE OR clvm.month_date = clvm.vip_cohort_month_date) THEN 1 ELSE 0 END ) AS cumulative_active_emp_opt_in
        ,(CASE WHEN e2.customer_id IS NOT NULL THEN 1 ELSE 0 END) AS total_emp_opt_in
        ,IFNULL(cr.order_count_with_credit_redemption,0) order_count_with_credit_redemption
        ,IFNULL(rp.initial_retail_price_excl_vat,0) initial_retail_price_excl_vat
        ,IFNULL(rp.Activating_initial_retail_price_excl_vat,0) Activating_initial_retail_price_excl_vat
        ,((clvm.is_bop_vip = TRUE AND clvm.is_login = TRUE)::int) AS login_count
        ,IFNULL(rpom.repeat_vip_product_order_count,0) repeat_vip_product_order_count
        ,((clvm.is_bop_vip = TRUE AND clvm.is_skip = TRUE AND clvm.product_order_count > 0 )::int) AS skip_then_purchased
        ,((clvm.is_bop_vip = TRUE AND clvm.is_successful_billing = TRUE AND clvm.product_order_count > 0 )::int) AS Billed_then_purchased
    FROM EDW_PROD.ANALYTICS_BASE.CUSTOMER_LIFETIME_VALUE_MONTHLY AS clvm
    JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE AS ds
        ON ds.store_id = clvm.store_id
    JOIN _cohort_waterfall_activating AS cwa
        ON CONCAT(cwa.customer_id,'30') = clvm.customer_id
        AND cwa.activation_key = clvm.activation_key
    LEFT JOIN EDW_PROD.DATA_MODEL_SXF.FACT_ACTIVATION fa
        ON CONCAT(fa.customer_id,'30') = clvm.customer_id and fa.activation_key = clvm.activation_key
    LEFT JOIN EDW_PROD.DATA_MODEL_SXF.DIM_STORE AS vip_ds
        ON vip_ds.store_id = fa.sub_store_id
    LEFT JOIN EDW_PROD.ANALYTICS_BASE.CUSTOMER_LIFETIME_VALUE_LTD as clvltd
        ON clvm.customer_id = clvltd.customer_id
        AND clvm.vip_cohort_month_date = clvltd.vip_cohort_month_date
        AND clvm.month_date = clvltd.vip_cohort_month_date
    LEFT JOIN _omni_classification omc
        ON CONCAT(omc.customer_id,'30') = clvm.customer_id
        AND omc.activation_key = clvm.activation_key
    LEFT JOIN _vip_product_count vp
        ON CONCAT(vp.customer_id,'30') = clvm.customer_id
        AND vp.vip_cohort_month_date = clvm.vip_cohort_month_date
    LEFT JOIN _emp_opt_in e
        ON CONCAT(e.customer_id,'30') = clvm.customer_id
        AND clvm.month_date >= e.month_opted_in
    LEFT JOIN _emp_opt_in e2
        ON CONCAT(e2.customer_id,'30') = clvm.customer_id
        AND clvm.month_date = e2.month_opted_in
    LEFT JOIN _orders_with_credit_redemptions cr
        ON CONCAT(cr.customer_id,'30') = clvm.customer_id
        AND cr.redeemed_month = clvm.month_date
        AND cr.vip_cohort_month_date = clvm.vip_cohort_month_date
    LEFT JOIN _retail_price rp
        ON CONCAT(rp.customer_id,'30') = clvm.customer_id
        AND rp.order_month = clvm.month_date
        AND rp.vip_cohort_month_date = clvm.vip_cohort_month_date
   LEFT JOIN _repeat_produdct_order_metrics rpom
        ON CONCAT(rpom.customer_id,'30') = clvm.customer_id
        AND rpom.order_month = clvm.month_date
        AND rpom.activation_key = clvm.activation_key
    WHERE clvm.vip_cohort_month_date >= '2018-01-01'
        AND ds.store_brand = 'Savage X'
);

TRUNCATE TABLE REPORTING_PROD.SXF.COHORT_WATERFALL_CUSTOMER;

INSERT INTO REPORTING_PROD.SXF.COHORT_WATERFALL_CUSTOMER
(
     customer_id
    ,vip_cohort_month_date
    ,membership_type
    ,month_date
    ,membership_monthly_status
    ,store_brand
    ,store_region
    ,store_country
    ,store_name
    ,activation_store_type
    ,activation_store_full_name
    ,omni_channel
    ,membership_sequence
    ,repeat_purchaser
    ,activating_product_order_unit_count
    ,activating_product_order_count
    ,activating_product_order_product_discount_amount
    ,activating_product_order_subtotal_excl_tariff_amount
    ,activating_product_order_shipping_revenue_amount
    ,activating_product_gross_revenue
    ,activating_product_order_landed_product_cost_amount
    ,nonactivating_product_order_unit_count
    ,nonactivating_product_order_count
    ,nonactivating_product_order_product_discount_amount
    ,nonactivating_product_order_subtotal_excl_tariff_amount
    ,nonactivating_product_order_shipping_revenue_amount
    ,nonactivating_product_gross_revenue
    ,nonactivating_product_order_landed_product_cost_amount
    ,vip_cancellation_count
    ,bop_vip_count
    ,activation_count
    ,failed_billing_count
    ,merch_purchase_count
    ,purchase_count
    ,reactivation_count
    ,skip_count
    ,vip_passive_cancellation_count
    ,is_successful_billing_count
    ,online_product_order_count
    ,retail_product_order_count
    ,mobile_app_product_order_count
    ,online_product_order_unit_count
    ,retail_product_order_unit_count
    ,mobile_app_product_order_unit_count
    ,online_product_order_subtotal_excl_tariff_amount
    ,retail_product_order_subtotal_excl_tariff_amount
    ,mobile_app_product_order_subtotal_excl_tariff_amount
    ,online_product_order_product_discount_amount
    ,retail_product_order_product_discount_amount
    ,mobile_app_product_order_product_discount_amount
    ,online_product_order_shipping_revenue_amount
    ,retail_product_order_shipping_revenue_amount
    ,mobile_app_product_order_shipping_revenue_amount
    ,product_order_direct_cogs_amount
    ,online_product_order_direct_cogs_amount
    ,retail_product_order_direct_cogs_amount
    ,mobile_app_product_order_direct_cogs_amount
    ,product_order_selling_expenses_amount
    ,online_product_order_selling_expenses_amount
    ,retail_product_order_selling_expenses_amount
    ,mobile_app_product_order_selling_expenses_amount
    ,product_order_cash_refund_amount_and_chargeback_amount
    ,online_product_order_cash_refund_chargeback_amount
    ,retail_product_order_cash_refund_chargeback_amount
    ,mobile_app_product_order_cash_refund_chargeback_amount
    ,product_order_cash_credit_refund_amount
    ,online_product_order_cash_credit_refund_amount
    ,retail_product_order_cash_credit_refund_amount
    ,mobile_app_product_order_cash_credit_refund_amount
    ,product_order_reship_exchange_order_count
    ,online_product_order_reship_exchange_order_count
    ,retail_product_order_reship_exchange_order_count
    ,mobile_app_product_order_reship_exchange_order_count
    ,product_order_reship_exchange_unit_count
    ,online_product_order_reship_exchange_unit_count
    ,retail_product_order_reship_exchange_unit_count
    ,mobile_app_product_order_reship_exchange_unit_count
    ,product_order_reship_exchange_direct_cogs_amount
    ,online_product_order_reship_exchange_direct_cogs_amount
    ,retail_product_order_reship_exchange_direct_cogs_amount
    ,mobile_app_product_order_reship_exchange_direct_cogs_amount
    ,product_order_return_cogs_amount
    ,online_product_order_return_cogs_amount
    ,retail_product_order_return_cogs_amount
    ,mobile_app_product_order_return_cogs_amount
    ,product_order_return_unit_count
    ,online_product_order_return_unit_count
    ,retail_product_order_return_unit_count
    ,mobile_app_product_order_return_unit_count
    ,product_order_amount_to_pay
    ,online_product_order_amount_to_pay
    ,retail_product_order_amount_to_pay
    ,mobile_app_product_order_amount_to_pay
    ,product_gross_revenue_excl_shipping
    ,online_product_gross_revenue_excl_shipping
    ,retail_product_gross_revenue_excl_shipping
    ,mobile_app_product_gross_revenue_excl_shipping
    ,product_gross_revenue
    ,online_product_gross_revenue
    ,retail_product_gross_revenue
    ,mobile_app_product_gross_revenue
    ,product_net_revenue
    ,online_product_net_revenue
    ,retail_product_net_revenue
    ,mobile_app_product_net_revenue
    ,product_margin_pre_return
    ,online_product_margin_pre_return
    ,retail_product_margin_pre_return
    ,product_margin_pre_return_excl_shipping
    ,online_product_margin_pre_return_excl_shipping
    ,retail_product_margin_pre_return_excl_shipping
    ,product_gross_profit
    ,online_product_gross_profit
    ,retail_product_gross_profit
    ,mobile_app_product_gross_profit
    ,product_variable_contribution_profit
    ,online_product_variable_contribution_profit
    ,retail_product_variable_contribution_profit
    ,mobile_app_product_variable_contribution_profit
    ,product_order_cash_gross_revenue_amount
    ,online_product_order_cash_gross_revenue_amount
    ,retail_product_order_cash_gross_revenue_amount
    ,mobile_app_product_order_cash_gross_revenue_amount
    ,product_order_cash_net_revenue
    ,online_product_order_cash_net_revenue
    ,retail_product_order_cash_net_revenue
    ,mobile_app_product_order_cash_net_revenue
    ,billing_cash_gross_revenue
    ,billing_cash_net_revenue
    ,cash_gross_revenue
    ,cash_net_revenue
    ,cash_gross_profit
    ,cash_variable_contribution_profit
    ,monthly_billed_credit_cash_transaction_amount
    ,membership_fee_cash_transaction_amount
    ,gift_card_transaction_amount
    ,legacy_credit_cash_transaction_amount
    ,monthly_billed_credit_cash_refund_chargeback_amount
    ,membership_fee_cash_refund_chargeback_amount
    ,gift_card_cash_refund_chargeback_amount
    ,legacy_credit_cash_refund_chargeback_amount
    ,cumulative_cash_gross_profit
    ,cumulative_product_gross_profit
    ,cumulative_cash_gross_profit_decile
    ,cumulative_product_gross_profit_decile
    ,billed_cash_credit_cancelled_equivalent_count
    ,billed_cash_credit_redeemed_equivalent_count
    ,media_spend
    ,cumulative_total_emp_opt_in
    ,cumulative_active_emp_opt_in
    ,total_emp_opt_in
    ,order_count_with_credit_redemption
    ,initial_retail_price_excl_vat
    ,Activating_initial_retail_price_excl_vat
    ,login_count
    ,repeat_vip_product_order_count
    ,skip_then_purchased
    ,Billed_then_purchased
    ,meta_create_datetime
    ,meta_update_datetime
)
SELECT DISTINCT
    customer_id
    ,vip_cohort_month_date
    ,membership_type
    ,month_date
    ,membership_monthly_status
    ,store_brand
    ,store_region
    ,store_country
    ,store_name
    ,activation_store_type
    ,activation_store_full_name
    ,omni_channel
    ,membership_sequence
    ,repeat_purchaser
    ,activating_product_order_unit_count
    ,activating_product_order_count
    ,activating_product_order_product_discount_amount
    ,activating_product_order_subtotal_excl_tariff_amount
    ,activating_product_order_shipping_revenue_amount
    ,activating_product_gross_revenue
    ,activating_product_order_landed_product_cost_amount
    ,nonactivating_product_order_unit_count
    ,nonactivating_product_order_count
    ,nonactivating_product_order_product_discount_amount
    ,nonactivating_product_order_subtotal_excl_tariff_amount
    ,nonactivating_product_order_shipping_revenue_amount
    ,nonactivating_product_gross_revenue
    ,nonactivating_product_order_landed_product_cost_amount
    ,vip_cancellation_count
    ,bop_vip_count
    ,activation_count
    ,failed_billing_count
    ,merch_purchase_count
    ,purchase_count
    ,reactivation_count
    ,skip_count
    ,vip_passive_cancellation_count
    ,is_successful_billing_count
    ,online_product_order_count
    ,retail_product_order_count
    ,mobile_app_product_order_count
    ,online_product_order_unit_count
    ,retail_product_order_unit_count
    ,mobile_app_product_order_unit_count
    ,online_product_order_subtotal_excl_tariff_amount
    ,retail_product_order_subtotal_excl_tariff_amount
    ,mobile_app_product_order_subtotal_excl_tariff_amount
    ,online_product_order_product_discount_amount
    ,retail_product_order_product_discount_amount
    ,mobile_app_product_order_product_discount_amount
    ,online_product_order_shipping_revenue_amount
    ,retail_product_order_shipping_revenue_amount
    ,mobile_app_product_order_shipping_revenue_amount
    ,product_order_direct_cogs_amount
    ,online_product_order_direct_cogs_amount
    ,retail_product_order_direct_cogs_amount
    ,mobile_app_product_order_direct_cogs_amount
    ,product_order_selling_expenses_amount
    ,online_product_order_selling_expenses_amount
    ,retail_product_order_selling_expenses_amount
    ,mobile_app_product_order_selling_expenses_amount
    ,product_order_cash_refund_amount_and_chargeback_amount
    ,online_product_order_cash_refund_chargeback_amount
    ,retail_product_order_cash_refund_chargeback_amount
    ,mobile_app_product_order_cash_refund_chargeback_amount
    ,product_order_cash_credit_refund_amount
    ,online_product_order_cash_credit_refund_amount
    ,retail_product_order_cash_credit_refund_amount
    ,mobile_app_product_order_cash_credit_refund_amount
    ,product_order_reship_exchange_order_count
    ,online_product_order_reship_exchange_order_count
    ,retail_product_order_reship_exchange_order_count
    ,mobile_app_product_order_reship_exchange_order_count
    ,product_order_reship_exchange_unit_count
    ,online_product_order_reship_exchange_unit_count
    ,retail_product_order_reship_exchange_unit_count
    ,mobile_app_product_order_reship_exchange_unit_count
    ,product_order_reship_exchange_direct_cogs_amount
    ,online_product_order_reship_exchange_direct_cogs_amount
    ,retail_product_order_reship_exchange_direct_cogs_amount
    ,mobile_app_product_order_reship_exchange_direct_cogs_amount
    ,product_order_return_cogs_amount
    ,online_product_order_return_cogs_amount
    ,retail_product_order_return_cogs_amount
    ,mobile_app_product_order_return_cogs_amount
    ,product_order_return_unit_count
    ,online_product_order_return_unit_count
    ,retail_product_order_return_unit_count
    ,mobile_app_product_order_return_unit_count
    ,product_order_amount_to_pay
    ,online_product_order_amount_to_pay
    ,retail_product_order_amount_to_pay
    ,mobile_app_product_order_amount_to_pay
    ,product_gross_revenue_excl_shipping
    ,online_product_gross_revenue_excl_shipping
    ,retail_product_gross_revenue_excl_shipping
    ,mobile_app_product_gross_revenue_excl_shipping
    ,product_gross_revenue
    ,online_product_gross_revenue
    ,retail_product_gross_revenue
    ,mobile_app_product_gross_revenue
    ,product_net_revenue
    ,online_product_net_revenue
    ,retail_product_net_revenue
    ,mobile_app_product_net_revenue
    ,product_margin_pre_return
    ,online_product_margin_pre_return
    ,retail_product_margin_pre_return
    ,product_margin_pre_return_excl_shipping
    ,online_product_margin_pre_return_excl_shipping
    ,retail_product_margin_pre_return_excl_shipping
    ,product_gross_profit
    ,online_product_gross_profit
    ,retail_product_gross_profit
    ,mobile_app_product_gross_profit
    ,product_variable_contribution_profit
    ,online_product_variable_contribution_profit
    ,retail_product_variable_contribution_profit
    ,mobile_app_product_variable_contribution_profit
    ,product_order_cash_gross_revenue_amount
    ,online_product_order_cash_gross_revenue_amount
    ,retail_product_order_cash_gross_revenue_amount
    ,mobile_app_product_order_cash_gross_revenue_amount
    ,product_order_cash_net_revenue
    ,online_product_order_cash_net_revenue
    ,retail_product_order_cash_net_revenue
    ,mobile_app_product_order_cash_net_revenue
    ,billing_cash_gross_revenue
    ,billing_cash_net_revenue
    ,cash_gross_revenue
    ,cash_net_revenue
    ,cash_gross_profit
    ,cash_variable_contribution_profit
    ,monthly_billed_credit_cash_transaction_amount
    ,membership_fee_cash_transaction_amount
    ,gift_card_transaction_amount
    ,legacy_credit_cash_transaction_amount
    ,monthly_billed_credit_cash_refund_chargeback_amount
    ,membership_fee_cash_refund_chargeback_amount
    ,gift_card_cash_refund_chargeback_amount
    ,legacy_credit_cash_refund_chargeback_amount
    ,cumulative_cash_gross_profit
    ,cumulative_product_gross_profit
    ,cumulative_cash_gross_profit_decile
    ,cumulative_product_gross_profit_decile
    ,billed_cash_credit_cancelled_equivalent_count
    ,billed_cash_credit_redeemed_equivalent_count
    ,media_spend
    ,cumulative_total_emp_opt_in
    ,cumulative_active_emp_opt_in
    ,total_emp_opt_in
    ,order_count_with_credit_redemption
    ,initial_retail_price_excl_vat
    ,Activating_initial_retail_price_excl_vat
    ,login_count
    ,repeat_vip_product_order_count
    ,skip_then_purchased
    ,Billed_then_purchased
    ,$execution_start_time
    ,$execution_start_time
FROM _cohort_waterfall_customer_final_output_stg
ORDER BY
    vip_cohort_month_date ASC,
    customer_id ASC,
    month_date ASC;
