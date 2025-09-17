SET execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMPORARY TABLE _view_custom_segment AS
SELECT DISTINCT *
FROM REPORTING_PROD.SXF.VIEW_CUSTOM_SEGMENT
ORDER BY vip_cohort ASC, customer_id ASC;

INSERT OVERWRITE INTO REPORTING_PROD.SXF.CUSTOM_SEGMENT_WATERFALL
(
     vip_cohort_month_date
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
    ,custom_segment_category
    ,custom_segment
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
    ,initial_cohort_count
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
    ,meta_create_datetime
    ,meta_update_datetime
)
SELECT
     cwc.vip_cohort_month_date
    ,cwc.membership_type
    ,cwc.month_date
    ,cwc.membership_monthly_status
    ,cwc.store_brand
    ,cwc.store_region
    ,cwc.store_country
    ,cwc.store_name
    ,cwc.activation_store_type
    ,cwc.activation_store_full_name
    ,cwc.omni_channel
    ,cwc.membership_sequence
    ,cwc.repeat_purchaser
    ,cs.custom_segment_category
    ,cs.custom_segment
    ,SUM(cwc.activating_product_order_unit_count) AS activating_product_order_unit_count
    ,SUM(cwc.activating_product_order_count) AS activating_product_order_count
    ,SUM(cwc.activating_product_order_product_discount_amount) AS activating_product_order_product_discount_amount
    ,SUM(cwc.activating_product_order_subtotal_excl_tariff_amount) AS activating_product_order_subtotal_excl_tariff_amount
    ,SUM(cwc.activating_product_order_shipping_revenue_amount) AS activating_product_order_shipping_revenue_amount
    ,SUM(cwc.activating_product_gross_revenue) AS activating_product_gross_revenue
    ,SUM(cwc.activating_product_order_landed_product_cost_amount) AS activating_product_order_landed_product_cost_amount
    ,SUM(cwc.nonactivating_product_order_unit_count) AS nonactivating_product_order_unit_count
    ,SUM(cwc.nonactivating_product_order_count) AS nonactivating_product_order_count
    ,SUM(cwc.nonactivating_product_order_product_discount_amount) AS nonactivating_product_order_product_discount_amount
    ,SUM(cwc.nonactivating_product_order_subtotal_excl_tariff_amount) AS nonactivating_product_order_subtotal_excl_tariff_amount
    ,SUM(cwc.nonactivating_product_order_shipping_revenue_amount) AS nonactivating_product_order_shipping_revenue_amount
    ,SUM(cwc.nonactivating_product_gross_revenue) AS nonactivating_product_gross_revenue
    ,SUM(cwc.nonactivating_product_order_landed_product_cost_amount) AS nonactivating_product_order_landed_product_cost_amount
    ,SUM(cwc.vip_cancellation_count) AS vip_cancellation_count
    ,SUM(cwc.bop_vip_count) AS bop_vip_count
    ,SUM(cwc.activation_count) AS activation_count
    ,SUM(cwc.failed_billing_count) AS failed_billing_count
    ,SUM(cwc.merch_purchase_count) AS merch_purchase_count
    ,SUM(cwc.purchase_count) AS purchase_count
    ,SUM(cwc.reactivation_count) AS reactivation_count
    ,SUM(cwc.skip_count) AS skip_count
    ,SUM(cwc.vip_passive_cancellation_count) AS vip_passive_cancellation_count
    ,SUM(cwc.is_successful_billing_count) AS is_successful_billing_count
    ,COUNT(DISTINCT cwc.customer_id) AS initial_cohort_count
    ,SUM(ifnull(cwc.online_product_order_count, 0)) AS online_product_order_count
    ,SUM(ifnull(cwc.retail_product_order_count, 0)) AS retail_product_order_count
    ,SUM(ifnull(cwc.mobile_app_product_order_count, 0)) AS mobile_app_product_order_count
    ,SUM(ifnull(cwc.online_product_order_unit_count, 0)) AS online_product_order_unit_count
    ,SUM(ifnull(cwc.retail_product_order_unit_count, 0)) AS retail_product_order_unit_count
    ,SUM(ifnull(cwc.mobile_app_product_order_unit_count, 0)) AS mobile_app_product_order_unit_count
    ,SUM(ifnull(cwc.online_product_order_subtotal_excl_tariff_amount, 0)) AS online_product_order_subtotal_excl_tariff_amount
    ,SUM(ifnull(cwc.retail_product_order_subtotal_excl_tariff_amount, 0)) AS retail_product_order_subtotal_excl_tariff_amount
    ,SUM(ifnull(cwc.mobile_app_product_order_subtotal_excl_tariff_amount, 0)) AS mobile_app_product_order_subtotal_excl_tariff_amount
    ,SUM(ifnull(cwc.online_product_order_product_discount_amount, 0)) AS online_product_order_product_discount_amount
    ,SUM(ifnull(cwc.retail_product_order_product_discount_amount, 0)) AS retail_product_order_product_discount_amount
    ,SUM(ifnull(cwc.mobile_app_product_order_product_discount_amount, 0)) AS mobile_app_product_order_product_discount_amount
    ,SUM(ifnull(cwc.online_product_order_shipping_revenue_amount, 0)) AS online_product_order_shipping_revenue_amount
    ,SUM(ifnull(cwc.retail_product_order_shipping_revenue_amount, 0)) AS retail_product_order_shipping_revenue_amount
    ,SUM(ifnull(cwc.mobile_app_product_order_shipping_revenue_amount, 0)) AS mobile_app_product_order_shipping_revenue_amount
    ,SUM(ifnull(cwc.product_order_direct_cogs_amount, 0)) AS product_order_direct_cogs_amount
    ,SUM(ifnull(cwc.online_product_order_direct_cogs_amount, 0)) AS online_product_order_direct_cogs_amount
    ,SUM(ifnull(cwc.retail_product_order_direct_cogs_amount, 0)) AS retail_product_order_direct_cogs_amount
    ,SUM(ifnull(cwc.mobile_app_product_order_direct_cogs_amount, 0)) AS mobile_app_product_order_direct_cogs_amount
    ,SUM(ifnull(cwc.product_order_selling_expenses_amount, 0)) AS product_order_selling_expenses_amount
    ,SUM(ifnull(cwc.online_product_order_selling_expenses_amount, 0)) AS online_product_order_selling_expenses_amount
    ,SUM(ifnull(cwc.retail_product_order_selling_expenses_amount, 0)) AS retail_product_order_selling_expenses_amount
    ,SUM(ifnull(cwc.mobile_app_product_order_selling_expenses_amount, 0)) AS mobile_app_product_order_selling_expenses_amount
    ,SUM(ifnull(cwc.product_order_cash_refund_amount_and_chargeback_amount, 0)) AS product_order_cash_refund_amount_and_chargeback_amount
    ,SUM(ifnull(cwc.online_product_order_cash_refund_chargeback_amount, 0)) AS online_product_order_cash_refund_chargeback_amount
    ,SUM(ifnull(cwc.retail_product_order_cash_refund_chargeback_amount, 0)) AS retail_product_order_cash_refund_chargeback_amount
    ,SUM(ifnull(cwc.mobile_app_product_order_cash_refund_chargeback_amount, 0)) AS mobile_app_product_order_cash_refund_chargeback_amount
    ,SUM(ifnull(cwc.product_order_cash_credit_refund_amount, 0)) AS product_order_cash_credit_refund_amount
    ,SUM(ifnull(cwc.online_product_order_cash_credit_refund_amount, 0)) AS online_product_order_cash_credit_refund_amount
    ,SUM(ifnull(cwc.retail_product_order_cash_credit_refund_amount, 0)) AS retail_product_order_cash_credit_refund_amount
    ,SUM(ifnull(cwc.mobile_app_product_order_cash_credit_refund_amount, 0)) AS mobile_app_product_order_cash_credit_refund_amount
    ,SUM(ifnull(cwc.product_order_reship_exchange_order_count, 0)) AS product_order_reship_exchange_order_count
    ,SUM(ifnull(cwc.online_product_order_reship_exchange_order_count, 0)) AS online_product_order_reship_exchange_order_count
    ,SUM(ifnull(cwc.retail_product_order_reship_exchange_order_count, 0)) AS retail_product_order_reship_exchange_order_count
    ,SUM(ifnull(cwc.mobile_app_product_order_reship_exchange_order_count, 0)) AS mobile_app_product_order_reship_exchange_order_count
    ,SUM(ifnull(cwc.product_order_reship_exchange_unit_count, 0)) AS product_order_reship_exchange_unit_count
    ,SUM(ifnull(cwc.online_product_order_reship_exchange_unit_count, 0)) AS online_product_order_reship_exchange_unit_count
    ,SUM(ifnull(cwc.retail_product_order_reship_exchange_unit_count, 0)) AS retail_product_order_reship_exchange_unit_count
    ,SUM(ifnull(cwc.mobile_app_product_order_reship_exchange_unit_count, 0)) AS mobile_app_product_order_reship_exchange_unit_count
    ,SUM(ifnull(cwc.product_order_reship_exchange_direct_cogs_amount, 0)) AS product_order_reship_exchange_direct_cogs_amount
    ,SUM(ifnull(cwc.online_product_order_reship_exchange_direct_cogs_amount, 0)) AS online_product_order_reship_exchange_direct_cogs_amount
    ,SUM(ifnull(cwc.retail_product_order_reship_exchange_direct_cogs_amount, 0)) AS retail_product_order_reship_exchange_direct_cogs_amount
    ,SUM(ifnull(cwc.mobile_app_product_order_reship_exchange_direct_cogs_amount, 0)) AS mobile_app_product_order_reship_exchange_direct_cogs_amount
    ,SUM(ifnull(cwc.product_order_return_cogs_amount, 0)) AS product_order_return_cogs_amount
    ,SUM(ifnull(cwc.online_product_order_return_cogs_amount, 0)) AS online_product_order_return_cogs_amount
    ,SUM(ifnull(cwc.retail_product_order_return_cogs_amount, 0)) AS retail_product_order_return_cogs_amount
    ,SUM(ifnull(cwc.mobile_app_product_order_return_cogs_amount, 0)) AS mobile_app_product_order_return_cogs_amount
    ,SUM(ifnull(cwc.product_order_return_unit_count, 0)) AS product_order_return_unit_count
    ,SUM(ifnull(cwc.online_product_order_return_unit_count, 0)) AS online_product_order_return_unit_count
    ,SUM(ifnull(cwc.retail_product_order_return_unit_count, 0)) AS retail_product_order_return_unit_count
    ,SUM(ifnull(cwc.mobile_app_product_order_return_unit_count, 0)) AS mobile_app_product_order_return_unit_count
    ,SUM(ifnull(cwc.product_order_amount_to_pay, 0)) AS product_order_amount_to_pay
    ,SUM(ifnull(cwc.online_product_order_amount_to_pay, 0)) AS online_product_order_amount_to_pay
    ,SUM(ifnull(cwc.retail_product_order_amount_to_pay, 0)) AS retail_product_order_amount_to_pay
    ,SUM(ifnull(cwc.mobile_app_product_order_amount_to_pay, 0)) AS mobile_app_product_order_amount_to_pay
    ,SUM(ifnull(cwc.product_gross_revenue_excl_shipping, 0)) AS product_gross_revenue_excl_shipping
    ,SUM(ifnull(cwc.online_product_gross_revenue_excl_shipping, 0)) AS online_product_gross_revenue_excl_shipping
    ,SUM(ifnull(cwc.retail_product_gross_revenue_excl_shipping, 0)) AS retail_product_gross_revenue_excl_shipping
    ,SUM(ifnull(cwc.mobile_app_product_gross_revenue_excl_shipping, 0)) AS mobile_app_product_gross_revenue_excl_shipping
    ,SUM(ifnull(cwc.product_gross_revenue, 0)) AS product_gross_revenue
    ,SUM(ifnull(cwc.online_product_gross_revenue, 0)) AS online_product_gross_revenue
    ,SUM(ifnull(cwc.retail_product_gross_revenue, 0)) AS retail_product_gross_revenue
    ,SUM(ifnull(cwc.mobile_app_product_gross_revenue, 0)) AS mobile_app_product_gross_revenue
    ,SUM(ifnull(cwc.product_net_revenue, 0)) AS product_net_revenue
    ,SUM(ifnull(cwc.online_product_net_revenue, 0)) AS online_product_net_revenue
    ,SUM(ifnull(cwc.retail_product_net_revenue, 0)) AS retail_product_net_revenue
    ,SUM(ifnull(cwc.mobile_app_product_net_revenue, 0)) AS mobile_app_product_net_revenue
    ,SUM(ifnull(cwc.product_margin_pre_return, 0)) AS product_margin_pre_return
    ,SUM(ifnull(cwc.online_product_margin_pre_return, 0)) AS online_product_margin_pre_return
    ,SUM(ifnull(cwc.retail_product_margin_pre_return, 0)) AS retail_product_margin_pre_return
    ,SUM(ifnull(cwc.product_margin_pre_return_excl_shipping, 0)) AS product_margin_pre_return_excl_shipping
    ,SUM(ifnull(cwc.online_product_margin_pre_return_excl_shipping, 0)) AS online_product_margin_pre_return_excl_shipping
    ,SUM(ifnull(cwc.retail_product_margin_pre_return_excl_shipping, 0)) AS retail_product_margin_pre_return_excl_shipping
    ,SUM(ifnull(cwc.product_gross_profit, 0)) AS product_gross_profit
    ,SUM(ifnull(cwc.online_product_gross_profit, 0)) AS online_product_gross_profit
    ,SUM(ifnull(cwc.retail_product_gross_profit, 0)) AS retail_product_gross_profit
    ,SUM(ifnull(cwc.mobile_app_product_gross_profit, 0)) AS mobile_app_product_gross_profit
    ,SUM(ifnull(cwc.product_variable_contribution_profit, 0)) AS product_variable_contribution_profit
    ,SUM(ifnull(cwc.online_product_variable_contribution_profit, 0)) AS online_product_variable_contribution_profit
    ,SUM(ifnull(cwc.retail_product_variable_contribution_profit, 0)) AS retail_product_variable_contribution_profit
    ,SUM(ifnull(cwc.mobile_app_product_variable_contribution_profit, 0)) AS mobile_app_product_variable_contribution_profit
    ,SUM(ifnull(cwc.product_order_cash_gross_revenue_amount, 0)) AS product_order_cash_gross_revenue_amount
    ,SUM(ifnull(cwc.online_product_order_cash_gross_revenue_amount, 0)) AS online_product_order_cash_gross_revenue_amount
    ,SUM(ifnull(cwc.retail_product_order_cash_gross_revenue_amount, 0)) AS retail_product_order_cash_gross_revenue_amount
    ,SUM(ifnull(cwc.mobile_app_product_order_cash_gross_revenue_amount, 0)) AS mobile_app_product_order_cash_gross_revenue_amount
    ,SUM(ifnull(cwc.product_order_cash_net_revenue, 0)) AS product_order_cash_net_revenue
    ,SUM(ifnull(cwc.online_product_order_cash_net_revenue, 0)) AS online_product_order_cash_net_revenue
    ,SUM(ifnull(cwc.retail_product_order_cash_net_revenue, 0)) AS retail_product_order_cash_net_revenue
    ,SUM(ifnull(cwc.mobile_app_product_order_cash_net_revenue, 0)) AS mobile_app_product_order_cash_net_revenue
    ,SUM(ifnull(cwc.billing_cash_gross_revenue, 0)) AS billing_cash_gross_revenue
    ,SUM(ifnull(cwc.billing_cash_net_revenue, 0)) AS billing_cash_net_revenue
    ,SUM(ifnull(cwc.cash_gross_revenue, 0)) AS cash_gross_revenue
    ,SUM(ifnull(cwc.cash_net_revenue, 0)) AS cash_net_revenue
    ,SUM(ifnull(cwc.cash_gross_profit, 0)) AS cash_gross_profit
    ,SUM(ifnull(cwc.cash_variable_contribution_profit, 0)) AS cash_variable_contribution_profit
    ,SUM(ifnull(cwc.monthly_billed_credit_cash_transaction_amount, 0)) AS monthly_billed_credit_cash_transaction_amount
    ,SUM(ifnull(cwc.membership_fee_cash_transaction_amount, 0)) AS membership_fee_cash_transaction_amount
    ,SUM(ifnull(cwc.gift_card_transaction_amount, 0)) AS gift_card_transaction_amount
    ,SUM(ifnull(cwc.legacy_credit_cash_transaction_amount, 0)) AS legacy_credit_cash_transaction_amount
    ,SUM(ifnull(cwc.monthly_billed_credit_cash_refund_chargeback_amount, 0)) AS monthly_billed_credit_cash_refund_chargeback_amount
    ,SUM(ifnull(cwc.membership_fee_cash_refund_chargeback_amount, 0)) AS membership_fee_cash_refund_chargeback_amount
    ,SUM(ifnull(cwc.gift_card_cash_refund_chargeback_amount, 0)) AS gift_card_cash_refund_chargeback_amount
    ,SUM(ifnull(cwc.legacy_credit_cash_refund_chargeback_amount, 0)) AS legacy_credit_cash_refund_chargeback_amount
    ,SUM(ifnull(cwc.cumulative_cash_gross_profit, 0)) AS cumulative_cash_gross_profit
    ,SUM(ifnull(cwc.cumulative_product_gross_profit, 0)) AS cumulative_product_gross_profit
    ,SUM(ifnull(cwc.cumulative_cash_gross_profit_decile, 0)) AS cumulative_cash_gross_profit_decile
    ,SUM(ifnull(cwc.cumulative_product_gross_profit_decile, 0)) AS cumulative_product_gross_profit_decile
    ,SUM(ifnull(cwc.billed_cash_credit_cancelled_equivalent_count,0)) AS billed_cash_credit_cancelled_equivalent_count
    ,SUM(ifnull(cwc.billed_cash_credit_redeemed_equivalent_count,0)) AS billed_cash_credit_redeemed_equivalent_count
    ,SUM(ifnull(cwc.media_spend,0)) AS media_spend
    ,SUM(ifnull(cwc.cumulative_total_emp_opt_in,0)) AS cumulative_total_emp_opt_in
    ,SUM(ifnull(cwc.cumulative_active_emp_opt_in,0)) AS cumulative_active_emp_opt_in
    ,SUM(ifnull(cwc.total_emp_opt_in,0)) AS total_emp_opt_in
    ,SUM(ifnull(cwc.order_count_with_credit_redemption,0)) AS order_count_with_credit_redemption
    ,$execution_start_time AS meta_update_time
    ,$execution_start_time AS meta_create_time
FROM REPORTING_PROD.SXF.COHORT_WATERFALL_CUSTOMER cwc
JOIN _view_custom_segment cs
    ON cs.customer_id = cwc.customer_id
    AND cs.vip_cohort = cwc.vip_cohort_month_date
GROUP BY
         cwc.vip_cohort_month_date,
         cwc.membership_type,
         cwc.month_date,
         cwc.membership_monthly_status,
         cwc.store_brand,
         cwc.store_region,
         cwc.store_country,
         cwc.store_name,
         cwc.activation_store_type,
         cwc.activation_store_full_name,
         cwc.omni_channel,
         cwc.membership_sequence,
         cwc.repeat_purchaser,
         cs.custom_segment_category,
         cs.custom_segment;
