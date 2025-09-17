CREATE OR REPLACE VIEW reporting.daily_cash_month_tot AS
    WITH _bop_vips_first_of_month AS (
        SELECT date,
               date_object,
               currency_object,
               currency_type,
               report_mapping,
               order_membership_classification_key,
               bop_vips
        FROM reporting.daily_cash_base_calc
        WHERE DAY(date) = 1
    )
    SELECT
/*Daily Cash Version*/
        date_trunc('month',dcbc.date) AS month_date
        ,dcbc.date_object
        ,dcbc.currency_object
        ,dcbc.currency_type
/*Segment*/
        ,dcbc.report_mapping
/*Measure Filter*/
        ,dcbc.order_membership_classification_key
/*Measures*/
        ,SUM(dcbc.product_order_subtotal_excl_tariff_amount) AS product_order_subtotal_excl_tariff_amount_month_tot
        ,SUM(dcbc.product_order_product_discount_amount) AS product_order_product_discount_amount_month_tot
        ,SUM(dcbc.product_order_shipping_revenue_amount) AS product_order_shipping_revenue_amount_month_tot
        ,SUM(dcbc.product_order_noncash_credit_redeemed_amount) AS product_order_noncash_credit_redeemed_amount_month_tot
        ,SUM(dcbc.product_order_count) AS product_order_count_month_tot
        ,SUM(dcbc.product_order_count_excl_seeding) AS product_order_count_excl_seeding_month_tot
        ,SUM(dcbc.product_margin_pre_return_excl_seeding) AS product_margin_pre_return_excl_seeding_month_tot
        ,SUM(dcbc.product_order_unit_count) AS product_order_unit_count_month_tot
        ,SUM(dcbc.product_order_air_vip_price) AS product_order_air_vip_price_month_tot
        ,SUM(dcbc.product_order_retail_unit_price) AS product_order_retail_unit_price_month_tot
        ,SUM(dcbc.product_order_price_offered_amount) AS product_order_price_offered_amount_month_tot
        ,SUM(dcbc.product_order_landed_product_cost_amount) AS product_order_landed_product_cost_amount_month_tot
        ,SUM(dcbc.oracle_product_order_landed_product_cost_amount) AS oracle_product_order_landed_product_cost_amount_month_tot
        ,SUM(dcbc.product_order_shipping_cost_amount) AS product_order_shipping_cost_amount_month_tot
        ,SUM(dcbc.product_order_shipping_supplies_cost_amount) AS product_order_shipping_supplies_cost_amount_month_tot
        ,SUM(dcbc.product_order_direct_cogs_amount) AS product_order_direct_cogs_amount_month_tot
        ,SUM(dcbc.product_order_cash_refund_amount) AS product_order_cash_refund_amount_month_tot
        ,SUM(dcbc.product_order_cash_chargeback_amount) AS product_order_cash_chargeback_amount_month_tot
        ,SUM(dcbc.product_order_cost_product_returned_resaleable_amount) AS product_order_cost_product_returned_resaleable_amount_month_tot
        ,SUM(dcbc.product_order_cost_product_returned_damaged_amount) AS product_order_cost_product_returned_damaged_amount_month_tot
        ,SUM(dcbc.product_order_return_shipping_cost_amount) AS product_order_return_shipping_cost_amount_month_tot
        ,SUM(dcbc.product_order_reship_order_count) AS product_order_reship_order_count_month_tot
        ,SUM(dcbc.product_order_reship_unit_count) AS product_order_reship_unit_count_month_tot
        ,SUM(dcbc.product_order_reship_direct_cogs_amount) AS product_order_reship_direct_cogs_amount_month_tot
        ,SUM(dcbc.product_order_reship_product_cost_amount) AS product_order_reship_product_cost_amount_month_tot
        ,SUM(dcbc.oracle_product_order_reship_product_cost_amount) AS oracle_product_order_reship_product_cost_amount_month_tot
        ,SUM(dcbc.product_order_reship_shipping_cost_amount) AS product_order_reship_shipping_cost_amount_month_tot
        ,SUM(dcbc.product_order_reship_shipping_supplies_cost_amount) AS product_order_reship_shipping_supplies_cost_amount_month_tot
        ,SUM(dcbc.product_order_exchange_order_count) AS product_order_exchange_order_count_month_tot
        ,SUM(dcbc.product_order_exchange_unit_count) AS product_order_exchange_unit_count_month_tot
        ,SUM(dcbc.product_order_exchange_product_cost_amount) AS product_order_exchange_product_cost_amount_month_tot
        ,SUM(dcbc.oracle_product_order_exchange_product_cost_amount) AS oracle_product_order_exchange_product_cost_amount_month_tot
        ,SUM(dcbc.product_order_exchange_shipping_cost_amount) AS product_order_exchange_shipping_cost_amount_month_tot
        ,SUM(dcbc.product_order_exchange_shipping_supplies_cost_amount) AS product_order_exchange_shipping_supplies_cost_amount_month_tot
        ,SUM(dcbc.product_gross_revenue) AS product_gross_revenue_month_tot
        ,SUM(dcbc.product_gross_revenue_excl_shipping) AS product_gross_revenue_excl_shipping_month_tot
        ,SUM(dcbc.product_net_revenue) AS product_net_revenue_month_tot
        ,SUM(dcbc.product_margin_pre_return) AS product_margin_pre_return_month_tot
        ,SUM(dcbc.product_gross_profit) AS product_gross_profit_month_tot
        ,SUM(dcbc.product_order_cash_gross_revenue_amount) AS product_order_cash_gross_revenue_amount_month_tot
        ,SUM(dcbc.cash_gross_revenue) AS cash_gross_revenue_month_tot
        ,SUM(dcbc.cash_net_revenue) AS cash_net_revenue_month_tot
        ,SUM(dcbc.cash_net_revenue_budgeted_fx) AS cash_net_revenue_budgeted_fx_month_tot
        ,SUM(dcbc.cash_gross_profit) AS cash_gross_profit_month_tot
        ,SUM(dcbc.billed_credit_cash_transaction_count) AS billed_credit_cash_transaction_count_month_tot
        ,SUM(dcbc.on_retry_billed_credit_cash_transaction_count) AS on_retry_billed_credit_cash_transaction_count_month_tot
        ,SUM(dcbc.billing_cash_refund_amount) AS billing_cash_refund_amount_month_tot
        ,SUM(dcbc.billing_cash_chargeback_amount) AS billing_cash_chargeback_amount_month_tot
        ,SUM(dcbc.billed_credit_cash_transaction_amount) AS billed_credit_cash_transaction_amount_month_tot
        ,SUM(dcbc.billed_credit_cash_refund_chargeback_amount) AS billed_credit_cash_refund_chargeback_amount_month_tot
        ,SUM(dcbc.billed_cash_credit_redeemed_amount) AS billed_cash_credit_redeemed_amount_month_tot
        ,SUM(dcbc.product_order_misc_cogs_amount) AS product_order_misc_cogs_amount_month_tot
        ,SUM(dcbc.product_order_cash_gross_profit) AS product_order_cash_gross_profit_month_tot
        ,SUM(dcbc.billed_cash_credit_redeemed_same_month_amount) AS billed_cash_credit_redeemed_same_month_amount_month_tot
        ,SUM(dcbc.billing_order_transaction_count) AS billing_order_transaction_count_month_tot
        ,SUM(dcbc.membership_fee_cash_transaction_amount) AS membership_fee_cash_transaction_amount_month_tot
        ,SUM(dcbc.gift_card_transaction_amount) AS gift_card_transaction_amount_month_tot
        ,SUM(dcbc.legacy_credit_cash_transaction_amount) AS legacy_credit_cash_transaction_amount_month_tot
        ,SUM(dcbc.membership_fee_cash_refund_chargeback_amount) AS membership_fee_cash_refund_chargeback_amount_month_tot
        ,SUM(dcbc.billed_cash_credit_issued_equivalent_count) AS billed_cash_credit_issued_equivalent_count_month_tot
        ,SUM(dcbc.refund_cash_credit_issued_amount) AS refund_cash_credit_issued_amount_month_tot
        ,SUM(dcbc.refund_cash_credit_redeemed_amount) AS refund_cash_credit_redeemed_amount_month_tot
        ,SUM(dcbc.refund_cash_credit_cancelled_amount) AS refund_cash_credit_cancelled_amount_month_tot
        ,SUM(dcbc.billed_credit_cash_refund_count) AS billed_credit_cash_refund_count_month_tot
        ,SUM(dcbc.cash_variable_contribution_profit) AS cash_variable_contribution_profit_month_tot
        ,SUM(dcbc.billed_cash_credit_redeemed_equivalent_count) AS billed_cash_credit_redeemed_equivalent_count_month_tot
        ,SUM(dcbc.billed_cash_credit_cancelled_equivalent_count) AS billed_cash_credit_cancelled_equivalent_count_month_tot
        ,SUM(dcbc.product_order_product_subtotal_amount) AS product_order_product_subtotal_amount_month_tot
        ,SUM(dcbc.leads) AS leads_month_tot
        ,SUM(dcbc.primary_leads) AS primary_leads_month_tot
        ,SUM(dcbc.reactivated_leads) AS reactivated_leads_month_tot
        ,SUM(dcbc.new_vips) AS new_vips_month_tot
        ,SUM(dcbc.reactivated_vips) AS reactivated_vips_month_tot
        ,SUM(dcbc.vips_from_reactivated_leads_m1) AS vips_from_reactivated_leads_m1_month_tot
        ,SUM(dcbc.paid_vips) AS paid_vips_month_tot
        ,SUM(dcbc.unpaid_vips) AS unpaid_vips_month_tot
        ,SUM(dcbc.new_vips_m1) AS new_vips_m1_month_tot
        ,SUM(dcbc.paid_vips_m1) AS paid_vips_m1_month_tot
        ,SUM(dcbc.cancels) AS cancels_month_tot
        ,SUM(dcbc.m1_cancels) AS m1_cancels_month_tot
        ,SUM(ifnull(bvfom.bop_vips,0)) AS bop_vips_month_tot
        ,SUM(dcbc.media_spend) AS media_spend_month_tot
        ,SUM(dcbc.product_order_return_unit_count) AS product_order_return_unit_count_month_tot
        ,SUM(dcbc.merch_purchase_count) AS merch_purchase_count_month_tot
        ,SUM(dcbc.merch_purchase_hyperion_count) AS merch_purchase_hyperion_count_month_tot
        ,SUM(dcbc.other_cash_credit_redeemed_amount) AS other_cash_credit_redeemed_amount_month_tot
        ,SUM(dcbc.gift_card_cash_refund_chargeback_amount) AS gift_card_cash_refund_chargeback_amount_month_tot
        ,SUM(dcbc.legacy_credit_cash_refund_chargeback_amount) AS legacy_credit_cash_refund_chargeback_amount_month_tot
        ,SUM(dcbc.noncash_credit_issued_amount) AS noncash_credit_issued_amount_month_tot
        ,SUM(dcbc.product_order_cash_credit_refund_amount) AS product_order_cash_credit_refund_amount_month_tot
        ,SUM(dcbc.product_order_noncash_credit_refund_amount) AS product_order_noncash_credit_refund_amount_month_tot
        ,SUM(dcbc.product_order_non_token_subtotal_excl_tariff_amount) AS product_order_non_token_subtotal_excl_tariff_amount_month_tot
        ,SUM(dcbc.product_order_non_token_unit_count) AS product_order_non_token_unit_count_month_tot
        ,MAX(dcbc.meta_update_datetime) AS meta_update_datetime
        ,MAX(dcbc.meta_create_datetime) AS meta_create_datetime
    FROM reporting.daily_cash_base_calc AS dcbc
    LEFT JOIN _bop_vips_first_of_month AS bvfom
        ON dcbc.date = bvfom.date
        AND dcbc.date_object = bvfom.date_object
        AND dcbc.currency_object = bvfom.currency_object
        AND dcbc.currency_type = bvfom.currency_type
        AND dcbc.report_mapping = bvfom.report_mapping
        AND dcbc.order_membership_classification_key = bvfom.order_membership_classification_key
    GROUP BY month_date, dcbc.date_object, dcbc.currency_object, dcbc.currency_type, dcbc.report_mapping, dcbc.order_membership_classification_key
