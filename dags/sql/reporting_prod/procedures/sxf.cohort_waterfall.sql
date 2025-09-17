SET execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);

TRUNCATE TABLE REPORTING_PROD.SXF.COHORT_WATERFALL;

INSERT INTO REPORTING_PROD.SXF.COHORT_WATERFALL
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
    ,initial_retail_price_excl_vat
    ,Activating_initial_retail_price_excl_vat
    ,login_count
    ,repeat_vip_product_order_count
    ,skip_then_purchased
    ,Billed_then_purchased
    ,meta_create_datetime
    ,meta_update_datetime
)
SELECT
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
    ,SUM(activating_product_order_unit_count) AS activating_product_order_unit_count
    ,SUM(activating_product_order_count) AS activating_product_order_count
    ,SUM(activating_product_order_product_discount_amount) AS activating_product_order_product_discount_amount
    ,SUM(activating_product_order_subtotal_excl_tariff_amount) AS activating_product_order_subtotal_excl_tariff_amount
    ,SUM(activating_product_order_shipping_revenue_amount) AS activating_product_order_shipping_revenue_amount
    ,SUM(activating_product_gross_revenue) AS activating_product_gross_revenue
    ,SUM(activating_product_order_landed_product_cost_amount) AS activating_product_order_landed_product_cost_amount
    ,SUM(nonactivating_product_order_unit_count) AS nonactivating_product_order_unit_count
    ,SUM(nonactivating_product_order_count) AS nonactivating_product_order_count
    ,SUM(nonactivating_product_order_product_discount_amount) AS nonactivating_product_order_product_discount_amount
    ,SUM(nonactivating_product_order_subtotal_excl_tariff_amount) AS nonactivating_product_order_subtotal_excl_tariff_amount
    ,SUM(nonactivating_product_order_shipping_revenue_amount) AS nonactivating_product_order_shipping_revenue_amount
    ,SUM(nonactivating_product_gross_revenue) AS nonactivating_product_gross_revenue
    ,SUM(nonactivating_product_order_landed_product_cost_amount) AS nonactivating_product_order_landed_product_cost_amount
    ,SUM(vip_cancellation_count) AS vip_cancellation_count
    ,SUM(bop_vip_count) AS bop_vip_count
    ,SUM(activation_count) AS activation_count
    ,SUM(failed_billing_count) AS failed_billing_count
    ,SUM(merch_purchase_count) AS merch_purchase_count
    ,SUM(purchase_count) AS purchase_count
    ,SUM(reactivation_count) AS reactivation_count
    ,SUM(skip_count) AS skip_count
    ,SUM(vip_passive_cancellation_count) AS vip_passive_cancellation_count
    ,SUM(is_successful_billing_count) AS is_successful_billing_count
    ,COUNT(DISTINCT customer_id) AS initial_cohort_count
    ,SUM(ifnull(online_product_order_count, 0)) AS online_product_order_count
    ,SUM(ifnull(retail_product_order_count, 0)) AS retail_product_order_count
    ,SUM(ifnull(mobile_app_product_order_count, 0)) AS mobile_app_product_order_count
    ,SUM(ifnull(online_product_order_unit_count, 0)) AS online_product_order_unit_count
    ,SUM(ifnull(retail_product_order_unit_count, 0)) AS retail_product_order_unit_count
    ,SUM(ifnull(mobile_app_product_order_unit_count, 0)) AS mobile_app_product_order_unit_count
    ,SUM(ifnull(online_product_order_subtotal_excl_tariff_amount, 0)) AS online_product_order_subtotal_excl_tariff_amount
    ,SUM(ifnull(retail_product_order_subtotal_excl_tariff_amount, 0)) AS retail_product_order_subtotal_excl_tariff_amount
    ,SUM(ifnull(mobile_app_product_order_subtotal_excl_tariff_amount, 0)) AS mobile_app_product_order_subtotal_excl_tariff_amount
    ,SUM(ifnull(online_product_order_product_discount_amount, 0)) AS online_product_order_product_discount_amount
    ,SUM(ifnull(retail_product_order_product_discount_amount, 0)) AS retail_product_order_product_discount_amount
    ,SUM(ifnull(mobile_app_product_order_product_discount_amount, 0)) AS mobile_app_product_order_product_discount_amount
    ,SUM(ifnull(online_product_order_shipping_revenue_amount, 0)) AS online_product_order_shipping_revenue_amount
    ,SUM(ifnull(retail_product_order_shipping_revenue_amount, 0)) AS retail_product_order_shipping_revenue_amount
    ,SUM(ifnull(mobile_app_product_order_shipping_revenue_amount, 0)) AS mobile_app_product_order_shipping_revenue_amount
    ,SUM(ifnull(product_order_direct_cogs_amount, 0)) AS product_order_direct_cogs_amount
    ,SUM(ifnull(online_product_order_direct_cogs_amount, 0)) AS online_product_order_direct_cogs_amount
    ,SUM(ifnull(retail_product_order_direct_cogs_amount, 0)) AS retail_product_order_direct_cogs_amount
    ,SUM(ifnull(mobile_app_product_order_direct_cogs_amount, 0)) AS mobile_app_product_order_direct_cogs_amount
    ,SUM(ifnull(product_order_selling_expenses_amount, 0)) AS product_order_selling_expenses_amount
    ,SUM(ifnull(online_product_order_selling_expenses_amount, 0)) AS online_product_order_selling_expenses_amount
    ,SUM(ifnull(retail_product_order_selling_expenses_amount, 0)) AS retail_product_order_selling_expenses_amount
    ,SUM(ifnull(mobile_app_product_order_selling_expenses_amount, 0)) AS mobile_app_product_order_selling_expenses_amount
    ,SUM(ifnull(product_order_cash_refund_amount_and_chargeback_amount, 0)) AS product_order_cash_refund_amount_and_chargeback_amount
    ,SUM(ifnull(online_product_order_cash_refund_chargeback_amount, 0)) AS online_product_order_cash_refund_chargeback_amount
    ,SUM(ifnull(retail_product_order_cash_refund_chargeback_amount, 0)) AS retail_product_order_cash_refund_chargeback_amount
    ,SUM(ifnull(mobile_app_product_order_cash_refund_chargeback_amount, 0)) AS mobile_app_product_order_cash_refund_chargeback_amount
    ,SUM(ifnull(product_order_cash_credit_refund_amount, 0)) AS product_order_cash_credit_refund_amount
    ,SUM(ifnull(online_product_order_cash_credit_refund_amount, 0)) AS online_product_order_cash_credit_refund_amount
    ,SUM(ifnull(retail_product_order_cash_credit_refund_amount, 0)) AS retail_product_order_cash_credit_refund_amount
    ,SUM(ifnull(mobile_app_product_order_cash_credit_refund_amount, 0)) AS mobile_app_product_order_cash_credit_refund_amount
    ,SUM(ifnull(product_order_reship_exchange_order_count, 0)) AS product_order_reship_exchange_order_count
    ,SUM(ifnull(online_product_order_reship_exchange_order_count, 0)) AS online_product_order_reship_exchange_order_count
    ,SUM(ifnull(retail_product_order_reship_exchange_order_count, 0)) AS retail_product_order_reship_exchange_order_count
    ,SUM(ifnull(mobile_app_product_order_reship_exchange_order_count, 0)) AS mobile_app_product_order_reship_exchange_order_count
    ,SUM(ifnull(product_order_reship_exchange_unit_count, 0)) AS product_order_reship_exchange_unit_count
    ,SUM(ifnull(online_product_order_reship_exchange_unit_count, 0)) AS online_product_order_reship_exchange_unit_count
    ,SUM(ifnull(retail_product_order_reship_exchange_unit_count, 0)) AS retail_product_order_reship_exchange_unit_count
    ,SUM(ifnull(mobile_app_product_order_reship_exchange_unit_count, 0)) AS mobile_app_product_order_reship_exchange_unit_count
    ,SUM(ifnull(product_order_reship_exchange_direct_cogs_amount, 0)) AS product_order_reship_exchange_direct_cogs_amount
    ,SUM(ifnull(online_product_order_reship_exchange_direct_cogs_amount, 0)) AS online_product_order_reship_exchange_direct_cogs_amount
    ,SUM(ifnull(retail_product_order_reship_exchange_direct_cogs_amount, 0)) AS retail_product_order_reship_exchange_direct_cogs_amount
    ,SUM(ifnull(mobile_app_product_order_reship_exchange_direct_cogs_amount, 0)) AS mobile_app_product_order_reship_exchange_direct_cogs_amount
    ,SUM(ifnull(product_order_return_cogs_amount, 0)) AS product_order_return_cogs_amount
    ,SUM(ifnull(online_product_order_return_cogs_amount, 0)) AS online_product_order_return_cogs_amount
    ,SUM(ifnull(retail_product_order_return_cogs_amount, 0)) AS retail_product_order_return_cogs_amount
    ,SUM(ifnull(mobile_app_product_order_return_cogs_amount, 0)) AS mobile_app_product_order_return_cogs_amount
    ,SUM(ifnull(product_order_return_unit_count, 0)) AS product_order_return_unit_count
    ,SUM(ifnull(online_product_order_return_unit_count, 0)) AS online_product_order_return_unit_count
    ,SUM(ifnull(retail_product_order_return_unit_count, 0)) AS retail_product_order_return_unit_count
    ,SUM(ifnull(mobile_app_product_order_return_unit_count, 0)) AS mobile_app_product_order_return_unit_count
    ,SUM(ifnull(product_order_amount_to_pay, 0)) AS product_order_amount_to_pay
    ,SUM(ifnull(online_product_order_amount_to_pay, 0)) AS online_product_order_amount_to_pay
    ,SUM(ifnull(retail_product_order_amount_to_pay, 0)) AS retail_product_order_amount_to_pay
    ,SUM(ifnull(mobile_app_product_order_amount_to_pay, 0)) AS mobile_app_product_order_amount_to_pay
    ,SUM(ifnull(product_gross_revenue_excl_shipping, 0)) AS product_gross_revenue_excl_shipping
    ,SUM(ifnull(online_product_gross_revenue_excl_shipping, 0)) AS online_product_gross_revenue_excl_shipping
    ,SUM(ifnull(retail_product_gross_revenue_excl_shipping, 0)) AS retail_product_gross_revenue_excl_shipping
    ,SUM(ifnull(mobile_app_product_gross_revenue_excl_shipping, 0)) AS mobile_app_product_gross_revenue_excl_shipping
    ,SUM(ifnull(product_gross_revenue, 0)) AS product_gross_revenue
    ,SUM(ifnull(online_product_gross_revenue, 0)) AS online_product_gross_revenue
    ,SUM(ifnull(retail_product_gross_revenue, 0)) AS retail_product_gross_revenue
    ,SUM(ifnull(mobile_app_product_gross_revenue, 0)) AS mobile_app_product_gross_revenue
    ,SUM(ifnull(product_net_revenue, 0)) AS product_net_revenue
    ,SUM(ifnull(online_product_net_revenue, 0)) AS online_product_net_revenue
    ,SUM(ifnull(retail_product_net_revenue, 0)) AS retail_product_net_revenue
    ,SUM(ifnull(mobile_app_product_net_revenue, 0)) AS mobile_app_product_net_revenue
    ,SUM(ifnull(product_margin_pre_return, 0)) AS product_margin_pre_return
    ,SUM(ifnull(online_product_margin_pre_return, 0)) AS online_product_margin_pre_return
    ,SUM(ifnull(retail_product_margin_pre_return, 0)) AS retail_product_margin_pre_return
    ,SUM(ifnull(product_margin_pre_return_excl_shipping, 0)) AS product_margin_pre_return_excl_shipping
    ,SUM(ifnull(online_product_margin_pre_return_excl_shipping, 0)) AS online_product_margin_pre_return_excl_shipping
    ,SUM(ifnull(retail_product_margin_pre_return_excl_shipping, 0)) AS retail_product_margin_pre_return_excl_shipping
    ,SUM(ifnull(product_gross_profit, 0)) AS product_gross_profit
    ,SUM(ifnull(online_product_gross_profit, 0)) AS online_product_gross_profit
    ,SUM(ifnull(retail_product_gross_profit, 0)) AS retail_product_gross_profit
    ,SUM(ifnull(mobile_app_product_gross_profit, 0)) AS mobile_app_product_gross_profit
    ,SUM(ifnull(product_variable_contribution_profit, 0)) AS product_variable_contribution_profit
    ,SUM(ifnull(online_product_variable_contribution_profit, 0)) AS online_product_variable_contribution_profit
    ,SUM(ifnull(retail_product_variable_contribution_profit, 0)) AS retail_product_variable_contribution_profit
    ,SUM(ifnull(mobile_app_product_variable_contribution_profit, 0)) AS mobile_app_product_variable_contribution_profit
    ,SUM(ifnull(product_order_cash_gross_revenue_amount, 0)) AS product_order_cash_gross_revenue_amount
    ,SUM(ifnull(online_product_order_cash_gross_revenue_amount, 0)) AS online_product_order_cash_gross_revenue_amount
    ,SUM(ifnull(retail_product_order_cash_gross_revenue_amount, 0)) AS retail_product_order_cash_gross_revenue_amount
    ,SUM(ifnull(mobile_app_product_order_cash_gross_revenue_amount, 0)) AS mobile_app_product_order_cash_gross_revenue_amount
    ,SUM(ifnull(product_order_cash_net_revenue, 0)) AS product_order_cash_net_revenue
    ,SUM(ifnull(online_product_order_cash_net_revenue, 0)) AS online_product_order_cash_net_revenue
    ,SUM(ifnull(retail_product_order_cash_net_revenue, 0)) AS retail_product_order_cash_net_revenue
    ,SUM(ifnull(mobile_app_product_order_cash_net_revenue, 0)) AS mobile_app_product_order_cash_net_revenue
    ,SUM(ifnull(billing_cash_gross_revenue, 0)) AS billing_cash_gross_revenue
    ,SUM(ifnull(billing_cash_net_revenue, 0)) AS billing_cash_net_revenue
    ,SUM(ifnull(cash_gross_revenue, 0)) AS cash_gross_revenue
    ,SUM(ifnull(cash_net_revenue, 0)) AS cash_net_revenue
    ,SUM(ifnull(cash_gross_profit, 0)) AS cash_gross_profit
    ,SUM(ifnull(cash_variable_contribution_profit, 0)) AS cash_variable_contribution_profit
    ,SUM(ifnull(monthly_billed_credit_cash_transaction_amount, 0)) AS monthly_billed_credit_cash_transaction_amount
    ,SUM(ifnull(membership_fee_cash_transaction_amount, 0)) AS membership_fee_cash_transaction_amount
    ,SUM(ifnull(gift_card_transaction_amount, 0)) AS gift_card_transaction_amount
    ,SUM(ifnull(legacy_credit_cash_transaction_amount, 0)) AS legacy_credit_cash_transaction_amount
    ,SUM(ifnull(monthly_billed_credit_cash_refund_chargeback_amount, 0)) AS monthly_billed_credit_cash_refund_chargeback_amount
    ,SUM(ifnull(membership_fee_cash_refund_chargeback_amount, 0)) AS membership_fee_cash_refund_chargeback_amount
    ,SUM(ifnull(gift_card_cash_refund_chargeback_amount, 0)) AS gift_card_cash_refund_chargeback_amount
    ,SUM(ifnull(legacy_credit_cash_refund_chargeback_amount, 0)) AS legacy_credit_cash_refund_chargeback_amount
    ,SUM(ifnull(cumulative_cash_gross_profit, 0)) AS cumulative_cash_gross_profit
    ,SUM(ifnull(cumulative_product_gross_profit, 0)) AS cumulative_product_gross_profit
    ,SUM(ifnull(cumulative_cash_gross_profit_decile, 0)) AS cumulative_cash_gross_profit_decile
    ,SUM(ifnull(cumulative_product_gross_profit_decile, 0)) AS cumulative_product_gross_profit_decile
    ,SUM(ifnull(billed_cash_credit_cancelled_equivalent_count,0)) AS billed_cash_credit_cancelled_equivalent_count
    ,SUM(ifnull(billed_cash_credit_redeemed_equivalent_count,0)) AS billed_cash_credit_redeemed_equivalent_count
    ,SUM(ifnull(media_spend,0)) AS media_spend
    ,SUM(ifnull(cumulative_total_emp_opt_in,0)) AS cumulative_total_emp_opt_in
    ,SUM(ifnull(cumulative_active_emp_opt_in,0)) AS cumulative_active_emp_opt_in
    ,SUM(ifnull(total_emp_opt_in,0)) AS total_emp_opt_in
    ,SUM(ifnull(order_count_with_credit_redemption,0)) AS order_count_with_credit_redemption
    ,sum(ifnull(initial_retail_price_excl_vat,0)) AS initial_retail_price_excl_vat
    ,sum(ifnull(Activating_initial_retail_price_excl_vat,0)) AS Activating_initial_retail_price_excl_vat
    ,sum(ifnull(login_count,0)) AS login_count
    ,sum(ifnull(repeat_vip_product_order_count,0)) AS repeat_vip_product_order_count
    ,sum(ifnull(skip_then_purchased,0)) AS skip_then_purchased
    ,sum(ifnull(Billed_then_purchased,0)) AS Billed_then_purchased
    ,$execution_start_time AS meta_update_time
    ,$execution_start_time AS meta_create_time
FROM REPORTING_PROD.SXF.COHORT_WATERFALL_CUSTOMER
GROUP BY
    vip_cohort_month_date,
    membership_type,
    month_date,
    membership_monthly_status,
    store_brand,
    store_region,
    store_country,
    store_name,
    activation_store_type,
    activation_store_full_name,
    omni_channel,
    membership_sequence,
    repeat_purchaser
ORDER BY vip_cohort_month_date ASC, month_date ASC;
