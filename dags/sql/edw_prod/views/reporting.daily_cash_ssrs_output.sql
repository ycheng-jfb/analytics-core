CREATE OR REPLACE VIEW reporting.daily_cash_ssrs_output AS
WITH _max_date AS (
    /*
        Update report_date in reference.config_ssrs_date_param table for report = 'Daily Cash'
        to generate the report for that particular date
        If report_date in reference.config_ssrs_date_param is NULL for report = 'Daily Cash' then
        the latest report will be generated
     */
    SELECT COALESCE(report_date, (SELECT MAX(date) FROM reporting.daily_cash_final_output WHERE report_date_type = 'To Date')) AS max_date
    FROM reference.config_ssrs_date_param
    WHERE report = 'Daily Cash'
),
_daily_cash_final_output AS (
    SELECT dcfo.*
    FROM reporting.daily_cash_final_output dcfo
    JOIN (SELECT DISTINCT report_mapping FROM reference.finance_segment_mapping WHERE is_daily_cash_file = TRUE) fsm on dcfo.report_mapping = fsm.report_mapping
    WHERE report_date_type = 'To Date'
),
_variables AS(
    SELECT (SELECT max_date FROM _max_date) AS report_date, 'Report Date' AS source, (SELECT max_date FROM _max_date) AS full_date

    UNION ALL

    SELECT full_date AS report_date,
        CASE month_date WHEN DATE_TRUNC('MONTH', (SELECT max_date FROM _max_date)) THEN 'Daily_TM'
            WHEN DATEADD('YEAR', -1, DATE_TRUNC('MONTH', (SELECT max_date FROM _max_date))) THEN 'Daily_LY'
            ELSE 'Daily_LM' END AS source,
        (SELECT max_date FROM _max_date) AS full_date
    FROM data_model.dim_date
    WHERE full_date BETWEEN DATEADD('MONTH', -1, DATE_TRUNC('MONTH', (SELECT max_date FROM _max_date))) AND (SELECT max_date FROM _max_date)
        OR month_date = DATEADD('YEAR', -1, DATE_TRUNC('MONTH', (SELECT max_date FROM _max_date)))

    UNION ALL

    SELECT (SELECT max_date FROM _max_date) AS report_date, 'MTD' AS source, (SELECT max_date FROM _max_date) AS full_date

    UNION ALL

    SELECT DATE_TRUNC('MONTH', (SELECT max_date FROM _max_date)) AS report_month, 'Report Month' AS source, (SELECT max_date FROM _max_date) AS full_date

    UNION ALL

    SELECT full_date AS report_date,
        CASE full_date WHEN DATEADD('MONTH', -1, DATE_TRUNC('MONTH', (SELECT max_date FROM _max_date))) THEN 'LMMD'
            ELSE 'LYMD' END AS source,
        (SELECT max_date FROM _max_date) AS full_date
    FROM data_model.dim_date
    WHERE full_date = DATEADD('MONTH', -1, DATE_TRUNC('MONTH', (SELECT max_date FROM _max_date)))
        OR full_date = DATEADD('YEAR', -1, DATE_TRUNC('MONTH', (SELECT max_date FROM _max_date)))
),
_daily AS(
    SELECT date,
        date_object,
        currency_object,
        currency_type,
        business_unit,
        store_brand,
        report_mapping,
        is_daily_cash_usd,
        is_daily_cash_eur,
        v.source AS data_source,
        cash_gross_revenue,
        cash_net_revenue,
        cash_gross_profit,
        cash_gross_profit/NULLIFZERO(cash_net_revenue) AS cash_gross_profit_percent_of_net_cash,
        cash_net_revenue_budgeted_fx AS cash_net_revenue_with_budgeted_fx_rates,
        activating_cash_gross_revenue,
        activating_cash_net_revenue,
        nonactivating_cash_gross_revenue,
        nonactivating_cash_net_revenue,
        COALESCE(product_order_cash_refund_amount, 0) + COALESCE(billing_cash_refund_amount, 0) AS product_order_and_billing_cash_refund,
        COALESCE(product_order_cash_chargeback_amount, 0) + COALESCE(billing_cash_chargeback_amount, 0) AS product_order_and_billing_chargebacks,
        (COALESCE(product_order_cash_refund_amount, 0) + COALESCE(billing_cash_refund_amount, 0) + COALESCE(product_order_cash_chargeback_amount, 0)
            + COALESCE(billing_cash_chargeback_amount, 0))/NULLIFZERO(cash_gross_revenue) AS refund_chargebacks_as_percent_of_cash_gross_rev,
        activating_product_gross_revenue,
        activating_product_net_revenue,
        activating_shipping_revenue,
        activating_product_order_count,
        activating_product_order_count_excl_seeding,
        activating_product_margin_pre_return_excl_seeding,
        activating_product_gross_revenue/NULLIFZERO(activating_product_order_count) AS activating_aov,
        activating_unit_count,
        activating_unit_count/NULLIFZERO(activating_product_order_count) AS activating_upt,
        activating_product_gross_revenue_excl_shipping/NULLIFZERO(activating_unit_count) AS activating_aur,
        (COALESCE(activating_product_order_landed_product_cost_amount, 0) + COALESCE(activating_product_order_reship_product_cost_amount, 0)
            + COALESCE(activating_product_order_exchange_product_cost_amount, 0))
            /NULLIFZERO(COALESCE(activating_unit_count, 0) + COALESCE(activating_product_order_exchange_unit_count, 0)
            + COALESCE(activating_product_order_reship_unit_count, 0)) AS activating_auc_incl_reship_exch,
        activating_product_order_product_discount_amount/NULLIFZERO(COALESCE(activating_product_order_product_subtotal_amount, 0) - COALESCE(activating_product_order_noncash_credit_redeemed_amount, 0)) AS activating_discount_percent,
        (activating_product_order_air_vip_price - (activating_product_order_subtotal_excl_tariff_amount - activating_product_order_product_discount_amount))/NULLIFZERO(activating_product_order_air_vip_price) AS planning_activating_discount_percent,
        activating_product_gross_profit,
        activating_product_gross_profit/NULLIFZERO(activating_product_order_count) AS activating_product_gross_profit_per_order,
        nonactivating_product_gross_profit/NULLIFZERO(nonactivating_product_order_count) AS nonactivating_product_gross_profit_per_order,
        activating_product_gross_profit/NULLIFZERO(activating_product_net_revenue) AS activating_product_gross_profit_percent,
        activating_product_order_cash_gross_revenue,
        activating_product_margin_pre_return,
        activating_product_margin_pre_return/NULLIFZERO(activating_product_order_count) AS activating_product_margin_pre_return_per_order,
        (COALESCE(product_margin_pre_return, 0) + COALESCE(activating_product_margin_pre_return, 0))/NULLIFZERO(nonactivating_product_order_count) AS nonactivating_product_margin_pre_return_per_order,
        activating_product_margin_pre_return/NULLIFZERO(activating_product_gross_revenue) AS activating_product_margin_pre_return_percent,
        nonactivating_product_gross_revenue,
        nonactivating_product_net_revenue,
        nonactivating_product_order_cash_gross_revenue/NULLIFZERO(nonactivating_product_gross_revenue) AS nonactivating_cash_revenue_as_percent_of_product_gross_revenue,
        nonactivating_shipping_revenue,
        nonactivating_product_order_count,
        nonactivating_product_order_count_excl_seeding,
        nonactivating_product_margin_pre_return_excl_seeding,
        nonactivating_product_gross_revenue/NULLIFZERO(nonactivating_product_order_count) AS nonactivating_aov,
        nonactivating_unit_count,
        nonactivating_unit_count/NULLIFZERO(nonactivating_product_order_count) AS nononactivating_upt,
        nonactivating_product_gross_revenue_excl_shipping/NULLIFZERO(nonactivating_unit_count) AS nonactivating_aur,
        (COALESCE(nonactivating_product_order_landed_product_cost_amount, 0) + COALESCE(nonactivating_product_order_reship_product_cost_amount, 0)
            + COALESCE(nonactivating_product_order_exchange_product_cost_amount, 0))
            /NULLIFZERO(COALESCE(nonactivating_unit_count, 0) + COALESCE(nonactivating_product_order_exchange_unit_count, 0)
            + COALESCE(nonactivating_product_order_reship_unit_count, 0)) AS nonactivating_auc_incl_reship_exch,
        nonactivating_product_order_product_discount_amount/NULLIFZERO(COALESCE(nonactivating_product_order_product_subtotal_amount, 0) - COALESCE(nonactivating_product_order_noncash_credit_redeemed_amount, 0)) AS nonactivating_discount_percent,
        (COALESCE(nonactivating_product_order_air_price, 0) - (COALESCE(nonactivating_product_order_subtotal_excl_tariff_amount, 0) - COALESCE(nonactivating_product_order_product_discount_amount, 0)))/NULLIFZERO(nonactivating_product_order_air_price) AS planning_nonactivating_discount_percent,
        nonactivating_product_gross_profit,
        nonactivating_product_gross_profit/NULLIFZERO(nonactivating_product_net_revenue) AS nonactivating_product_gross_profit_percent,
        nonactivating_product_order_cash_gross_profit AS nonactivating_product_order_cash_gross_profit,
        nonactivating_product_order_cash_gross_profit/NULLIFZERO(nonactivating_product_order_count) AS nonactivating_product_order_cash_gross_profit_per_order,
        nonactivating_product_order_cash_gross_revenue,
        guest_product_order_count/NULLIFZERO(nonactivating_product_order_count) AS guest_orders_as_percent_of_non_activating,
        guest_product_net_revenue,
        guest_product_order_count,
        guest_product_order_count_excl_seeding,
        guest_product_margin_pre_return_excl_seeding,
        guest_product_gross_revenue/NULLIFZERO(guest_product_order_count) AS guest_aov,
        guest_product_gross_profit,
        guest_product_gross_revenue,
        guest_shipping_revenue,
        guest_unit_count,
        guest_unit_count/NULLIFZERO(guest_product_order_count) AS guest_upt,
        guest_product_gross_revenue_excl_shipping/NULLIFZERO(guest_unit_count) AS guest_aur,
        guest_product_order_product_discount_amount/NULLIFZERO(COALESCE(guest_product_gross_revenue, 0) + COALESCE(guest_product_order_product_discount_amount, 0)) AS guest_discount_percent,
        (COALESCE(guest_product_order_retail_unit_price, 0) - (COALESCE(guest_product_order_subtotal_excl_tariff_amount, 0) - COALESCE(guest_product_order_product_discount_amount, 0)))/NULLIFZERO(guest_product_order_retail_unit_price) AS planning_guest_discount_percent,
        guest_product_gross_profit/NULLIFZERO(guest_product_net_revenue) AS guest_product_gross_profit_percent,
        (COALESCE(repeat_vip_product_order_air_vip_price, 0) - (COALESCE(repeat_vip_product_order_subtotal_excl_tariff_amount, 0) - COALESCE(repeat_vip_product_order_product_discount_amount, 0)))/NULLIFZERO(repeat_vip_product_order_air_vip_price) AS planning_repeat_vip_discount_percent,
        billed_credit_cash_transaction_amount,
        billed_credit_cash_refund_chargeback_amount,
        billed_cash_credit_redeemed_amount,
        same_month_billed_credit_redeemed AS same_month_billed_credit_redeemed,
        COALESCE(billed_credit_cash_transaction_amount, 0) - COALESCE(billed_credit_cash_refund_chargeback_amount, 0) - COALESCE(billed_cash_credit_redeemed_amount, 0) AS billed_credit_net_billings,
        (COALESCE(billed_credit_cash_transaction_amount, 0) - COALESCE(billed_credit_cash_refund_chargeback_amount, 0) - COALESCE(billed_cash_credit_redeemed_amount, 0))/NULLIFZERO(cash_net_revenue) AS net_credit_billings_as_percent_of_cash_net_revenue, --Need to modify
        COALESCE(billed_credit_cash_transaction_count, 0) - COALESCE(billed_credits_successful_on_retry, 0) AS billed_credits_successful_on_first_try,
        COALESCE(billed_credits_successful_on_retry, 0) AS billed_credits_successful_on_retry,
        billed_credit_cash_transaction_count,
        membership_fee_cash_transaction_amount,
        gift_card_transaction_amount,
        legacy_credit_cash_transaction_amount,
        membership_fee_cash_transaction_amount + gift_card_transaction_amount + legacy_credit_cash_transaction_amount AS other_billing_cash_transaction_amount,
        membership_fee_cash_refund_chargeback_amount,
        COALESCE(membership_fee_cash_transaction_amount, 0) - COALESCE(membership_fee_cash_refund_chargeback_amount, 0) AS membership_fee_net_cash_transaction_amount,
        COALESCE(product_order_landed_product_cost_amount, 0) + COALESCE(product_order_exchange_product_cost_amount, 0) + COALESCE(product_order_reship_product_cost_amount, 0) AS outbound_product_cost_incl_reship_exch,
        COALESCE(oracle_product_order_landed_product_cost_amount, 0) + COALESCE(oracle_product_order_exchange_product_cost_amount, 0) + COALESCE(oracle_product_order_reship_product_cost_amount, 0) AS oracle_outbound_product_cost_incl_reship_exch,
        COALESCE(product_order_count, 0) + COALESCE(product_order_reship_order_count, 0) + COALESCE(product_order_exchange_order_count, 0) AS product_order_count_incl_reship_exch,
        COALESCE(unit_count, 0) + COALESCE(product_order_reship_unit_count, 0) + COALESCE(product_order_exchange_unit_count, 0) AS unit_count_incl_reship_exch,
        product_order_return_unit_count,
        (COALESCE(product_order_landed_product_cost_amount, 0) + COALESCE(product_order_exchange_product_cost_amount, 0) + COALESCE(product_order_reship_product_cost_amount, 0))/NULLIFZERO(COALESCE(unit_count, 0) + COALESCE(product_order_reship_unit_count, 0) + COALESCE(product_order_exchange_unit_count, 0)) AS auc_incl_reship_exch,
        product_cost_returned_resaleable_incl_reship_exch,
        product_order_cost_product_returned_damaged_amount AS product_cost_returned_damaged_incl_reship_exch,
        outbound_product_cost_incl_reship_exch - COALESCE(product_cost_returned_resaleable_incl_reship_exch,0) AS product_cost_net_of_returns_incl_reship_exch,
        COALESCE(product_order_shipping_cost_amount, 0) + COALESCE(product_order_reship_shipping_cost_amount, 0) + COALESCE(product_order_exchange_shipping_cost_amount, 0) AS shipped_cost_incl_reship_exch,
        COALESCE(product_order_shipping_supplies_cost_amount, 0) + COALESCE(product_order_exchange_shipping_supplies_cost_amount, 0) + COALESCE(product_order_reship_shipping_supplies_cost_amount, 0) AS shipping_supplies_cost_incl_reship_exch,
        return_shipping_costs_incl_reship_exch,
        misc_cogs_amount AS misc_cogs_amount,
        COALESCE(product_order_landed_product_cost_amount,0) + COALESCE(product_order_exchange_product_cost_amount,0) + COALESCE(product_order_reship_product_cost_amount,0) + shipped_cost_incl_reship_exch + COALESCE(return_shipping_costs_incl_reship_exch,0) + shipping_supplies_cost_incl_reship_exch + COALESCE(misc_cogs_amount,0) - COALESCE(product_cost_returned_resaleable_incl_reship_exch,0) AS total_cogs,
        COALESCE(oracle_product_order_landed_product_cost_amount,0) + COALESCE(oracle_product_order_exchange_product_cost_amount,0) + COALESCE(oracle_product_order_reship_product_cost_amount,0) + shipped_cost_incl_reship_exch + COALESCE(return_shipping_costs_incl_reship_exch,0) + shipping_supplies_cost_incl_reship_exch + COALESCE(misc_cogs_amount,0) - COALESCE(product_cost_returned_resaleable_incl_reship_exch,0) AS oracle_total_cogs,
        product_order_cash_credit_refund_amount AS product_order_cash_credit_refund_amount,
        activating_product_order_cash_credit_refund_amount AS activating_product_order_cash_credit_refund_amount,
        nonactivating_product_order_cash_credit_refund_amount AS nonactivating_product_order_cash_credit_refund_amount,
        COALESCE(product_order_cash_credit_refund_amount,0) - COALESCE(refund_cash_credit_redeemed_amount,0) AS product_order_cash_credit_refund_net_billings,
        COALESCE(membership_fee_cash_transaction_amount,0)
            + COALESCE(gift_card_transaction_amount,0)
            + COALESCE(legacy_credit_cash_transaction_amount,0)
            - COALESCE(other_cash_credit_redeemed_amount,0)
            - COALESCE(gift_card_cash_refund_chargeback_amount,0)
            - COALESCE(membership_fee_cash_refund_chargeback_amount,0)
            - COALESCE(legacy_credit_cash_refund_chargeback_amount,0) AS gift_card_membership_fee_legacy_credit_net_billings,
        noncash_credit_issued_amount AS noncash_credit_issued_amount,
        product_order_noncash_credit_redeemed_amount AS product_order_noncash_credit_redeemed_amount,
        NULL AS pending_product_order_count,
        NULL AS pending_order_cash_transaction_amount,
        NULL AS pending_order_cash_margin_pre_return_amount,
        NULL AS skip_count,
        NULL AS skip_rate,
        merch_purchase_count_mtd/NULLIFZERO(bom_vips) AS merch_purchase_rate,
        merch_purchase_hyperion_count_mtd/NULLIFZERO(bom_vips) AS merch_purchase_rate_hyperion,
        COALESCE(bop_vips, 0) + COALESCE(new_vips, 0) - COALESCE(cancels, 0) AS eop_vips,
        reactivated_vip_product_order_count,
        reactivated_vip_product_order_count_excl_seeding,
        reactivated_vip_product_margin_pre_return_excl_seeding,
        nonactivating_product_order_product_discount_amount
            /NULLIFZERO(COALESCE(nonactivating_product_order_non_token_subtotal_excl_tariff_amount, 0) - COALESCE(nonactivating_product_order_noncash_credit_redeemed_amount, 0))
                          AS nonactivating_cash_discount_percent,
        nonactivating_product_order_non_token_unit_count/NULLIFZERO(nonactivating_unit_count) AS nonactivating_non_token_unit_percent,
        guest_product_order_count/NULLIFZERO(nonactivating_product_order_count) AS nonactivating_guest_product_order_percent
    FROM _daily_cash_final_output dcfo
    JOIN _variables v ON v.report_date = dcfo.date
        AND v.source IN ('Daily_TM', 'Daily_LM', 'Daily_LY')
),
_mtd AS (
    SELECT date,
        date_object,
        currency_object,
        currency_type,
        business_unit,
        store_brand,
        report_mapping,
        is_daily_cash_usd,
        is_daily_cash_eur,
        source AS data_source,
        cash_gross_revenue_mtd AS cash_gross_revenue,
        cash_net_revenue_mtd AS cash_net_revenue,
        cash_gross_profit_mtd AS cash_gross_profit,
        cash_gross_profit_mtd/NULLIFZERO(cash_net_revenue_mtd) AS cash_gross_profit_percent_of_net_cash,
        cash_net_revenue_budgeted_fx_mtd AS cash_net_revenue_with_budgeted_fx_rates,
        activating_cash_gross_revenue_mtd AS activating_cash_gross_revenue,
        activating_cash_net_revenue_mtd AS activating_cash_net_revenue,
        nonactivating_cash_gross_revenue_mtd AS nonactivating_cash_gross_revenue,
        nonactivating_cash_net_revenue_mtd AS nonactivating_cash_net_revenue,
        COALESCE(product_order_cash_refund_amount_mtd, 0) + COALESCE(billing_cash_refund_amount_mtd, 0) AS product_order_and_billing_cash_refund,
        COALESCE(product_order_cash_chargeback_amount_mtd, 0) + COALESCE(billing_cash_chargeback_amount_mtd, 0) AS product_order_and_billing_chargebacks,
        (COALESCE(product_order_cash_refund_amount_mtd, 0) + COALESCE(billing_cash_refund_amount_mtd, 0) + COALESCE(product_order_cash_chargeback_amount_mtd, 0)
            + COALESCE(billing_cash_chargeback_amount_mtd, 0))/NULLIFZERO(cash_gross_revenue_mtd) AS refund_chargebacks_as_percent_of_cash_gross_rev,
        activating_product_gross_revenue_mtd AS activating_product_gross_revenue,
        activating_product_net_revenue_mtd AS activating_product_net_revenue,
        activating_shipping_revenue_mtd AS activating_shipping_revenue,
        activating_product_order_count_mtd AS activating_product_order_count,
        activating_product_order_count_excl_seeding_mtd AS activating_product_order_count_excl_seeding,
        activating_product_margin_pre_return_excl_seeding_mtd AS activating_product_margin_pre_return_excl_seeding,
        activating_product_gross_revenue_mtd/NULLIFZERO(activating_product_order_count_mtd) AS activating_aov,
        activating_unit_count_mtd AS activating_unit_count,
        activating_unit_count_mtd/NULLIFZERO(activating_product_order_count_mtd) AS activating_upt,
        activating_product_gross_revenue_excl_shipping_mtd/NULLIFZERO(activating_unit_count_mtd) AS activating_aur,
        (COALESCE(activating_product_order_landed_product_cost_amount_mtd, 0) + COALESCE(activating_product_order_reship_product_cost_amount_mtd, 0)
            + COALESCE(activating_product_order_exchange_product_cost_amount_mtd, 0))
            /NULLIFZERO(COALESCE(activating_unit_count_mtd, 0) + COALESCE(activating_product_order_exchange_unit_count_mtd, 0)
            + COALESCE(activating_product_order_reship_unit_count_mtd, 0)) AS activating_auc_incl_reship_exch,
        activating_product_order_product_discount_amount_mtd/NULLIFZERO(COALESCE(activating_product_order_product_subtotal_amount_mtd, 0) - COALESCE(activating_product_order_noncash_credit_redeemed_amount_mtd, 0)) AS activating_discount_percent,
        (COALESCE(activating_product_order_air_vip_price_mtd, 0) - (COALESCE(activating_product_order_subtotal_excl_tariff_amount_mtd, 0) - COALESCE(activating_product_order_product_discount_amount_mtd, 0)))/NULLIFZERO(activating_product_order_air_vip_price_mtd) AS planning_activating_discount_percent,
        activating_product_gross_profit_mtd AS activating_product_gross_profit,
        activating_product_gross_profit_mtd/NULLIFZERO(activating_product_order_count_mtd) AS activating_product_gross_profit_per_order,
        nonactivating_product_gross_profit_mtd/NULLIFZERO(nonactivating_product_order_count_mtd) AS nonactivating_product_gross_profit_per_order,
        activating_product_gross_profit_mtd/NULLIFZERO(activating_product_net_revenue_mtd) AS activating_product_gross_profit_percent,
        activating_product_order_cash_gross_revenue_mtd AS activating_product_order_cash_gross_revenue,
        activating_product_margin_pre_return_mtd AS activating_product_margin_pre_return,
        activating_product_margin_pre_return_mtd/NULLIFZERO(activating_product_order_count_mtd) AS activating_product_margin_pre_return_per_order,
        (COALESCE(product_margin_pre_return_mtd, 0) - (COALESCE(activating_product_margin_pre_return_mtd, 0)))/NULLIFZERO(nonactivating_product_order_count_mtd) AS nonactivating_product_margin_pre_return_per_order,
        activating_product_margin_pre_return_mtd/NULLIFZERO(activating_product_gross_revenue_mtd) AS activating_product_margin_pre_return_percent,
        nonactivating_product_gross_revenue_mtd AS nonactivating_product_gross_revenue,
        nonactivating_product_net_revenue_mtd AS nonactivating_product_net_revenue,
        nonactivating_product_order_cash_gross_revenue_mtd/NULLIFZERO(nonactivating_product_gross_revenue_mtd) AS nonactivating_cash_revenue_as_percent_of_product_gross_revenue,
        nonactivating_shipping_revenue_mtd AS nonactivating_shipping_revenue,
        nonactivating_product_order_count_mtd AS nonactivating_product_order_count,
        nonactivating_product_order_count_excl_seeding_mtd AS nonactivating_product_order_count_excl_seeding,
        nonactivating_product_margin_pre_return_excl_seeding_mtd AS nonactivating_product_margin_pre_return_excl_seeding,
        nonactivating_product_gross_revenue_mtd/NULLIFZERO(nonactivating_product_order_count_mtd) AS nonactivating_aov,
        nonactivating_unit_count_mtd AS nonactivating_unit_count,
        nonactivating_unit_count_mtd/NULLIFZERO(nonactivating_product_order_count_mtd) AS nononactivating_upt,
        nonactivating_product_gross_revenue_excl_shipping_mtd/NULLIFZERO(nonactivating_unit_count_mtd) AS nonactivating_aur,
        (COALESCE(nonactivating_product_order_landed_product_cost_amount_mtd, 0) + COALESCE(nonactivating_product_order_reship_product_cost_amount_mtd, 0)
            + COALESCE(nonactivating_product_order_exchange_product_cost_amount_mtd, 0))
            /NULLIFZERO(COALESCE(nonactivating_unit_count_mtd, 0) + COALESCE(nonactivating_product_order_exchange_unit_count_mtd, 0)
            + COALESCE(nonactivating_product_order_reship_unit_count_mtd, 0)) AS nonactivating_auc_incl_reship_exch,
        nonactivating_product_order_product_discount_amount_mtd/NULLIFZERO(COALESCE(nonactivating_product_order_product_subtotal_amount_mtd, 0) - COALESCE(nonactivating_product_order_noncash_credit_redeemed_amount_mtd, 0)) AS nonactivating_discount_percent,
        (COALESCE(nonactivating_product_order_air_price_mtd, 0) - (COALESCE(nonactivating_product_order_subtotal_excl_tariff_amount_mtd, 0) - COALESCE(nonactivating_product_order_product_discount_amount_mtd, 0)))/NULLIFZERO(nonactivating_product_order_air_price_mtd) AS planning_nonactivating_discount_percent,
        nonactivating_product_gross_profit_mtd AS nonactivating_product_gross_profit,
        nonactivating_product_gross_profit_mtd/NULLIFZERO(nonactivating_product_net_revenue_mtd) AS nonactivating_product_gross_profit_percent,
        nonactivating_product_order_cash_gross_profit_mtd AS nonactivating_product_order_cash_gross_profit,
        nonactivating_product_order_cash_gross_profit_mtd/NULLIFZERO(nonactivating_product_order_count_mtd) AS nonactivating_product_order_cash_gross_profit_per_order,
        nonactivating_product_order_cash_gross_revenue_mtd AS nonactivating_product_order_cash_gross_revenue,
        guest_product_order_count_mtd/NULLIFZERO(nonactivating_product_order_count_mtd) AS guest_orders_as_percent_of_non_activating,
        guest_product_net_revenue_mtd AS guest_product_net_revenue,
        guest_product_order_count_mtd AS guest_product_order_count,
        guest_product_order_count_excl_seeding_mtd AS guest_product_order_count_excl_seeding,
        guest_product_margin_pre_return_excl_seeding_mtd AS guest_product_margin_pre_return_excl_seeding,
        guest_product_gross_revenue_mtd/NULLIFZERO(guest_product_order_count_mtd) AS guest_aov,
        guest_product_gross_profit_mtd AS guest_product_gross_profit,
        guest_product_gross_revenue_mtd AS guest_product_gross_revenue,
        guest_shipping_revenue_mtd AS guest_shipping_revenue,
        guest_unit_count_mtd AS guest_unit_count,
        guest_unit_count_mtd/NULLIFZERO(guest_product_order_count_mtd) AS guest_upt,
        guest_product_gross_revenue_excl_shipping_mtd/NULLIFZERO(guest_unit_count_mtd) AS guest_aur,
        guest_product_order_product_discount_amount_mtd/NULLIFZERO(COALESCE(guest_product_gross_revenue_mtd, 0) + COALESCE(guest_product_order_product_discount_amount_mtd, 0)) AS guest_discount_percent,
        (COALESCE(guest_product_order_retail_unit_price_mtd, 0) - (COALESCE(guest_product_order_subtotal_excl_tariff_amount_mtd, 0) - COALESCE(guest_product_order_product_discount_amount_mtd, 0)))/NULLIFZERO(guest_product_order_retail_unit_price_mtd) AS planning_guest_discount_percent,
        guest_product_gross_profit_mtd/NULLIFZERO(guest_product_net_revenue_mtd) AS guest_product_gross_profit_percent,
        (COALESCE(repeat_vip_product_order_air_vip_price_mtd, 0) - (COALESCE(repeat_vip_product_order_subtotal_excl_tariff_amount_mtd, 0) - COALESCE(repeat_vip_product_order_product_discount_amount_mtd, 0)))/NULLIFZERO(repeat_vip_product_order_air_vip_price_mtd) AS planning_repeat_vip_discount_percent,
        billed_credit_cash_transaction_amount_mtd AS billed_credit_cash_transaction_amount,
        billed_credit_cash_refund_chargeback_amount_mtd AS billed_credit_cash_refund_chargeback_amount,
        billed_cash_credit_redeemed_amount_mtd AS billed_cash_credit_redeemed_amount,
        same_month_billed_credit_redeemed_mtd AS same_month_billed_credit_redeemed,
        COALESCE(billed_credit_cash_transaction_amount_mtd, 0) - COALESCE(billed_credit_cash_refund_chargeback_amount_mtd, 0) - COALESCE(billed_cash_credit_redeemed_amount_mtd, 0) AS billed_credit_net_billings,
        (COALESCE(billed_credit_cash_transaction_amount_mtd, 0) - COALESCE(billed_credit_cash_refund_chargeback_amount_mtd, 0) - COALESCE(billed_cash_credit_redeemed_amount_mtd, 0))/NULLIFZERO(cash_net_revenue_mtd) AS net_credit_billings_as_percent_of_cash_net_revenue, --Need to modify
        COALESCE(billed_credit_cash_transaction_count_mtd, 0) - COALESCE(billed_credits_successful_on_retry_mtd, 0) AS billed_credits_successful_on_first_try,
        COALESCE(billed_credits_successful_on_retry_mtd, 0) AS billed_credits_successful_on_retry,
        billed_credit_cash_transaction_count_mtd AS billed_credit_cash_transaction_count,
        membership_fee_cash_transaction_amount_mtd AS membership_fee_cash_transaction_amount,
        gift_card_transaction_amount_mtd AS gift_card_transaction_amount,
        legacy_credit_cash_transaction_amount_mtd AS legacy_credit_cash_transaction_amount,
        membership_fee_cash_transaction_amount_mtd + gift_card_transaction_amount_mtd + legacy_credit_cash_transaction_amount_mtd AS other_billing_cash_transaction_amount,
        membership_fee_cash_refund_chargeback_amount_mtd AS membership_fee_cash_refund_chargeback_amount,
        COALESCE(membership_fee_cash_transaction_amount_mtd, 0) - COALESCE(membership_fee_cash_refund_chargeback_amount_mtd, 0) AS membership_fee_net_cash_transaction_amount,
        COALESCE(product_order_landed_product_cost_amount_mtd, 0) + COALESCE(product_order_exchange_product_cost_amount_mtd, 0) + COALESCE(product_order_reship_product_cost_amount_mtd, 0) AS outbound_product_cost_incl_reship_exch,
        COALESCE(oracle_product_order_landed_product_cost_amount_mtd, 0) + COALESCE(oracle_product_order_exchange_product_cost_amount_mtd, 0) + COALESCE(oracle_product_order_reship_product_cost_amount_mtd, 0) AS oracle_outbound_product_cost_incl_reship_exch,
        COALESCE(product_order_count_mtd, 0) + COALESCE(product_order_reship_order_count_mtd, 0) + COALESCE(product_order_exchange_order_count_mtd, 0) AS product_order_count_incl_reship_exch,
        COALESCE(unit_count_mtd, 0) + COALESCE(product_order_reship_unit_count_mtd, 0) + COALESCE(product_order_exchange_unit_count_mtd, 0) AS unit_count_incl_reship_exch,
        product_order_return_unit_count_mtd AS product_order_return_unit_count,
        (COALESCE(product_order_landed_product_cost_amount_mtd, 0) + COALESCE(product_order_exchange_product_cost_amount_mtd, 0) + COALESCE(product_order_reship_product_cost_amount_mtd, 0))/NULLIFZERO(COALESCE(unit_count_mtd, 0) + COALESCE(product_order_reship_unit_count_mtd, 0) + COALESCE(product_order_exchange_unit_count_mtd, 0)) AS auc_incl_reship_exch,
        product_cost_returned_resaleable_incl_reship_exch_mtd AS product_cost_returned_resaleable_incl_reship_exch,
        product_order_cost_product_returned_damaged_amount_mtd AS product_cost_returned_damaged_incl_reship_exch,
        COALESCE(product_order_landed_product_cost_amount_mtd, 0) + COALESCE(product_order_exchange_product_cost_amount_mtd, 0) + COALESCE(product_order_reship_product_cost_amount_mtd, 0) - COALESCE(product_cost_returned_resaleable_incl_reship_exch_mtd,0) AS product_cost_net_of_returns_incl_reship_exch,
        COALESCE(product_order_shipping_cost_amount_mtd, 0) + COALESCE(product_order_reship_shipping_cost_amount_mtd, 0) + COALESCE(product_order_exchange_shipping_cost_amount_mtd, 0) AS shipped_cost_incl_reship_exch,
        COALESCE(product_order_shipping_supplies_cost_amount_mtd, 0) + COALESCE(product_order_exchange_shipping_supplies_cost_amount_mtd, 0) + COALESCE(product_order_reship_shipping_supplies_cost_amount_mtd, 0) AS shipping_supplies_cost_incl_reship_exch,
        return_shipping_costs_incl_reship_exch_mtd AS return_shipping_costs_incl_reship_exch,
        misc_cogs_amount_mtd AS misc_cogs_amount,
        COALESCE(product_order_landed_product_cost_amount_mtd,0) + COALESCE(product_order_exchange_product_cost_amount_mtd,0) + COALESCE(product_order_reship_product_cost_amount_mtd,0) + COALESCE(product_order_shipping_cost_amount_mtd, 0) + COALESCE(product_order_reship_shipping_cost_amount_mtd, 0) + COALESCE(product_order_exchange_shipping_cost_amount_mtd, 0) + COALESCE(return_shipping_costs_incl_reship_exch_mtd,0) + COALESCE(product_order_shipping_supplies_cost_amount_mtd, 0) + COALESCE(product_order_exchange_shipping_supplies_cost_amount_mtd, 0) + COALESCE(product_order_reship_shipping_supplies_cost_amount_mtd, 0) + COALESCE(misc_cogs_amount_mtd,0) - COALESCE(product_cost_returned_resaleable_incl_reship_exch_mtd,0) AS total_cogs,
        COALESCE(oracle_product_order_landed_product_cost_amount_mtd,0) + COALESCE(oracle_product_order_exchange_product_cost_amount_mtd,0) + COALESCE(oracle_product_order_reship_product_cost_amount_mtd,0) + COALESCE(product_order_shipping_cost_amount_mtd, 0) + COALESCE(product_order_reship_shipping_cost_amount_mtd, 0) + COALESCE(product_order_exchange_shipping_cost_amount_mtd, 0) + COALESCE(return_shipping_costs_incl_reship_exch_mtd,0) + COALESCE(product_order_shipping_supplies_cost_amount_mtd, 0) + COALESCE(product_order_exchange_shipping_supplies_cost_amount_mtd, 0) + COALESCE(product_order_reship_shipping_supplies_cost_amount_mtd, 0) + COALESCE(misc_cogs_amount_mtd,0) - COALESCE(product_cost_returned_resaleable_incl_reship_exch_mtd,0) AS oracle_total_cogs,
        product_order_cash_credit_refund_amount_mtd AS product_order_cash_credit_refund_amount,
        activating_product_order_cash_credit_refund_amount_mtd AS activating_product_order_cash_credit_refund_amount,
        nonactivating_product_order_cash_credit_refund_amount_mtd AS nonactivating_product_order_cash_credit_refund_amount,
        COALESCE(product_order_cash_credit_refund_amount_mtd,0) - COALESCE(refund_cash_credit_redeemed_amount_mtd,0) AS product_order_cash_credit_refund_net_billings,
        COALESCE(membership_fee_cash_transaction_amount_mtd,0)
            + COALESCE(gift_card_transaction_amount_mtd,0)
            + COALESCE(legacy_credit_cash_transaction_amount_mtd,0)
            - COALESCE(other_cash_credit_redeemed_amount_mtd,0)
            - COALESCE(gift_card_cash_refund_chargeback_amount_mtd,0)
            - COALESCE(membership_fee_cash_refund_chargeback_amount_mtd,0)
            - COALESCE(legacy_credit_cash_refund_chargeback_amount_mtd,0) AS gift_card_membership_fee_legacy_credit_net_billings,
        noncash_credit_issued_amount_mtd AS noncash_credit_issued_amount,
        product_order_noncash_credit_redeemed_amount_mtd AS product_order_noncash_credit_redeemed_amount,
        pending_order_count AS pending_product_order_count,
        CASE
            WHEN currency_object = 'usd' THEN pending_order_usd_amount
            WHEN currency_object = 'local' THEN pending_order_local_amount
        END AS pending_order_cash_transaction_amount,
        CASE
            WHEN currency_object = 'usd' THEN pending_order_cash_margin_pre_return_usd_amount
            WHEN currency_object = 'local' THEN pending_order_cash_margin_pre_return_eur_amount
        END AS pending_order_cash_margin_pre_return_amount,
        skip_count AS skip_count,
        skip_count/NULLIFZERO(bom_vips) AS skip_rate,
        merch_purchase_count_mtd/NULLIFZERO(bom_vips) AS merch_purchase_rate,
        merch_purchase_hyperion_count_mtd/NULLIFZERO(bom_vips) AS merch_purchase_rate_hyperion,
        COALESCE(bop_vips, 0) + COALESCE(new_vips, 0) - COALESCE(cancels, 0) AS eop_vips,
        reactivated_vip_product_order_count_mtd as reactivated_vip_product_order_count,
        reactivated_vip_product_order_count_excl_seeding_mtd AS reactivated_vip_product_order_count_excl_seeding,
        reactivated_vip_product_margin_pre_return_excl_seeding_mtd AS reactivated_vip_product_margin_pre_return_excl_seeding,
        nonactivating_product_order_product_discount_amount_mtd
            /NULLIFZERO(COALESCE(nonactivating_product_order_non_token_subtotal_excl_tariff_amount_mtd, 0) - COALESCE(nonactivating_product_order_noncash_credit_redeemed_amount_mtd, 0))
                          AS nonactivating_cash_discount_percent,
        nonactivating_product_order_non_token_unit_count_mtd/NULLIFZERO(nonactivating_unit_count_mtd) AS nonactivating_non_token_unit_percent,
        guest_product_order_count_mtd/NULLIFZERO(nonactivating_product_order_count_mtd) AS nonactivating_guest_product_order_percent
    FROM _daily_cash_final_output dcfo
    JOIN _variables v ON v.report_date = dcfo.date
        AND v.source = 'MTD'
),
_report_months AS (
    SELECT DISTINCT report_mapping,
        LAST_DAY(date) AS last_day_of_month
    FROM _daily_cash_final_output
    WHERE date < (
        SELECT report_date
        FROM _variables
        WHERE source = 'Report Month')
),
_eom AS (
    SELECT DATE_TRUNC('MONTH', date) AS date,
        date_object,
        currency_object,
        currency_type,
        dcfo.business_unit,
        store_brand,
        dcfo.report_mapping,
        is_daily_cash_usd,
        is_daily_cash_eur,
        'EOM' AS data_source,
        cash_gross_revenue_mtd AS cash_gross_revenue,
        cash_net_revenue_mtd AS cash_net_revenue,
        cash_gross_profit_mtd AS cash_gross_profit,
        cash_gross_profit_mtd/NULLIFZERO(cash_net_revenue_mtd) AS cash_gross_profit_percent_of_net_cash,
        cash_net_revenue_budgeted_fx_mtd AS cash_net_revenue_with_budgeted_fx_rates,
        activating_cash_gross_revenue_mtd AS activating_cash_gross_revenue,
        activating_cash_net_revenue_mtd AS activating_cash_net_revenue,
        nonactivating_cash_gross_revenue_mtd AS nonactivating_cash_gross_revenue,
        nonactivating_cash_net_revenue_mtd AS nonactivating_cash_net_revenue,
        COALESCE(product_order_cash_refund_amount_mtd, 0) + COALESCE(billing_cash_refund_amount_mtd, 0) AS product_order_and_billing_cash_refund,
        COALESCE(product_order_cash_chargeback_amount_mtd, 0) + COALESCE(billing_cash_chargeback_amount_mtd, 0) AS product_order_and_billing_chargebacks,
        (COALESCE(product_order_cash_refund_amount_mtd, 0) + COALESCE(billing_cash_refund_amount_mtd, 0) + COALESCE(product_order_cash_chargeback_amount_mtd, 0)
            + COALESCE(billing_cash_chargeback_amount_mtd, 0))/NULLIFZERO(cash_gross_revenue_mtd) AS refund_chargebacks_as_percent_of_cash_gross_rev,
        activating_product_gross_revenue_mtd AS activating_product_gross_revenue,
        activating_product_net_revenue_mtd AS activating_product_net_revenue,
        activating_shipping_revenue_mtd AS activating_shipping_revenue,
        activating_product_order_count_mtd AS activating_product_order_count,
        activating_product_order_count_excl_seeding_mtd AS activating_product_order_count_excl_seeding,
        activating_product_margin_pre_return_excl_seeding_mtd AS activating_product_margin_pre_return_excl_seeding,
        activating_product_gross_revenue_mtd/NULLIFZERO(activating_product_order_count_mtd) AS activating_aov,
        activating_unit_count_mtd AS activating_unit_count,
        activating_unit_count_mtd/NULLIFZERO(activating_product_order_count_mtd) AS activating_upt,
        activating_product_gross_revenue_excl_shipping_mtd/NULLIFZERO(activating_unit_count_mtd) AS activating_aur,
        (COALESCE(activating_product_order_landed_product_cost_amount_mtd, 0) + COALESCE(activating_product_order_reship_product_cost_amount_mtd, 0)
            + COALESCE(activating_product_order_exchange_product_cost_amount_mtd, 0))
            /NULLIFZERO(COALESCE(activating_unit_count_mtd, 0) + COALESCE(activating_product_order_exchange_unit_count_mtd, 0)
            + COALESCE(activating_product_order_reship_unit_count_mtd, 0)) AS activating_auc_incl_reship_exch,
        activating_product_order_product_discount_amount_mtd/NULLIFZERO(COALESCE(activating_product_order_product_subtotal_amount_mtd, 0) - COALESCE(activating_product_order_noncash_credit_redeemed_amount_mtd, 0)) AS activating_discount_percent,
        (COALESCE(activating_product_order_air_vip_price_mtd, 0) - (COALESCE(activating_product_order_subtotal_excl_tariff_amount_mtd, 0) - COALESCE(activating_product_order_product_discount_amount_mtd, 0)))/NULLIFZERO(activating_product_order_air_vip_price_mtd) AS planning_activating_discount_percent,
        activating_product_gross_profit_mtd AS activating_product_gross_profit,
        activating_product_gross_profit_mtd/NULLIFZERO(activating_product_order_count_mtd) AS activating_product_gross_profit_per_order,
        nonactivating_product_gross_profit_mtd/NULLIFZERO(nonactivating_product_order_count_mtd) AS nonactivating_product_gross_profit_per_order,
        activating_product_gross_profit_mtd/NULLIFZERO(activating_product_net_revenue_mtd) AS activating_product_gross_profit_percent,
        activating_product_order_cash_gross_revenue_mtd AS activating_product_order_cash_gross_revenue,
        activating_product_margin_pre_return_mtd AS activating_product_margin_pre_return,
        activating_product_margin_pre_return_mtd/NULLIFZERO(activating_product_order_count_mtd) AS activating_product_margin_pre_return_per_order,
        (COALESCE(product_margin_pre_return_mtd, 0) - (COALESCE(activating_product_margin_pre_return_mtd, 0)))/NULLIFZERO(nonactivating_product_order_count_mtd) AS nonactivating_product_margin_pre_return_per_order,
        activating_product_margin_pre_return_mtd/NULLIFZERO(activating_product_gross_revenue_mtd) AS activating_product_margin_pre_return_percent,
        nonactivating_product_gross_revenue_mtd AS nonactivating_product_gross_revenue,
        nonactivating_product_net_revenue_mtd AS nonactivating_product_net_revenue,
        nonactivating_product_order_cash_gross_revenue_mtd/NULLIFZERO(nonactivating_product_gross_revenue_mtd) AS nonactivating_cash_revenue_as_percent_of_product_gross_revenue,
        nonactivating_shipping_revenue_mtd AS nonactivating_shipping_revenue,
        nonactivating_product_order_count_mtd AS nonactivating_product_order_count,
        nonactivating_product_order_count_excl_seeding_mtd AS nonactivating_product_order_count_excl_seeding,
        nonactivating_product_margin_pre_return_excl_seeding_mtd AS nonactivating_product_margin_pre_return_excl_seeding,
        nonactivating_product_gross_revenue_mtd/NULLIFZERO(nonactivating_product_order_count_mtd) AS nonactivating_aov,
        nonactivating_unit_count_mtd AS nonactivating_unit_count,
        nonactivating_unit_count_mtd/NULLIFZERO(nonactivating_product_order_count_mtd) AS nononactivating_upt,
        nonactivating_product_gross_revenue_excl_shipping_mtd/NULLIFZERO(nonactivating_unit_count_mtd) AS nonactivating_aur,
        (COALESCE(nonactivating_product_order_landed_product_cost_amount_mtd, 0) + COALESCE(nonactivating_product_order_reship_product_cost_amount_mtd, 0)
            + COALESCE(nonactivating_product_order_exchange_product_cost_amount_mtd, 0))
            /NULLIFZERO(COALESCE(nonactivating_unit_count_mtd, 0) + COALESCE(nonactivating_product_order_exchange_unit_count_mtd, 0)
            + COALESCE(nonactivating_product_order_reship_unit_count_mtd, 0)) AS nonactivating_auc_incl_reship_exch,
        nonactivating_product_order_product_discount_amount_mtd/NULLIFZERO(COALESCE(nonactivating_product_order_product_subtotal_amount_mtd, 0) - COALESCE(nonactivating_product_order_noncash_credit_redeemed_amount_mtd, 0)) AS nonactivating_discount_percent,
        (COALESCE(nonactivating_product_order_air_price_mtd, 0) - (COALESCE(nonactivating_product_order_subtotal_excl_tariff_amount_mtd, 0) - COALESCE(nonactivating_product_order_product_discount_amount_mtd, 0)))/NULLIFZERO(nonactivating_product_order_air_price_mtd) AS planning_nonactivating_discount_percent,
        nonactivating_product_gross_profit_mtd AS nonactivating_product_gross_profit,
        nonactivating_product_gross_profit_mtd/NULLIFZERO(nonactivating_product_net_revenue_mtd) AS nonactivating_product_gross_profit_percent,
        nonactivating_product_order_cash_gross_profit_mtd AS nonactivating_product_order_cash_gross_profit,
        nonactivating_product_order_cash_gross_profit_mtd/NULLIFZERO(nonactivating_product_order_count_mtd) AS nonactivating_product_order_cash_gross_profit_per_order,
        nonactivating_product_order_cash_gross_revenue_mtd AS nonactivating_product_order_cash_gross_revenue,
        guest_product_order_count_mtd/NULLIFZERO(nonactivating_product_order_count_mtd) AS guest_orders_as_percent_of_non_activating,
        guest_product_net_revenue_mtd AS guest_product_net_revenue,
        guest_product_order_count_mtd AS guest_product_order_count,
        guest_product_order_count_excl_seeding_mtd AS guest_product_order_count_excl_seeding,
        guest_product_margin_pre_return_excl_seeding_mtd AS guest_product_margin_pre_return_excl_seeding,
        guest_product_gross_revenue_mtd/NULLIFZERO(guest_product_order_count_mtd) AS guest_aov,
        guest_product_gross_profit_mtd AS guest_product_gross_profit,
        guest_product_gross_revenue_mtd AS guest_product_gross_revenue,
        guest_shipping_revenue_mtd AS guest_shipping_revenue,
        guest_unit_count_mtd AS guest_unit_count,
        guest_unit_count_mtd/NULLIFZERO(guest_product_order_count_mtd) AS guest_upt,
        guest_product_gross_revenue_excl_shipping_mtd/NULLIFZERO(guest_unit_count_mtd) AS guest_aur,
        guest_product_order_product_discount_amount_mtd/NULLIFZERO(COALESCE(guest_product_gross_revenue_mtd, 0) + COALESCE(guest_product_order_product_discount_amount_mtd, 0)) AS guest_discount_percent,
        (COALESCE(guest_product_order_retail_unit_price_mtd, 0) - (COALESCE(guest_product_order_subtotal_excl_tariff_amount_mtd, 0) - COALESCE(guest_product_order_product_discount_amount_mtd, 0)))/NULLIFZERO(guest_product_order_retail_unit_price_mtd) AS planning_guest_discount_percent,
        guest_product_gross_profit_mtd/NULLIFZERO(guest_product_net_revenue_mtd) AS guest_product_gross_profit_percent,
        (COALESCE(repeat_vip_product_order_air_vip_price_mtd, 0) - (COALESCE(repeat_vip_product_order_subtotal_excl_tariff_amount_mtd, 0) - COALESCE(repeat_vip_product_order_product_discount_amount_mtd, 0)))/NULLIFZERO(repeat_vip_product_order_air_vip_price_mtd) AS planning_repeat_vip_discount_percent,
        billed_credit_cash_transaction_amount_mtd AS billed_credit_cash_transaction_amount,
        billed_credit_cash_refund_chargeback_amount_mtd AS billed_credit_cash_refund_chargeback_amount,
        billed_cash_credit_redeemed_amount_mtd AS billed_cash_credit_redeemed_amount,
        same_month_billed_credit_redeemed_mtd AS same_month_billed_credit_redeemed,
        COALESCE(billed_credit_cash_transaction_amount_mtd, 0) - COALESCE(billed_credit_cash_refund_chargeback_amount_mtd, 0) - COALESCE(billed_cash_credit_redeemed_amount_mtd, 0) AS billed_credit_net_billings,
        (COALESCE(billed_credit_cash_transaction_amount_mtd, 0) - COALESCE(billed_credit_cash_refund_chargeback_amount_mtd, 0) - COALESCE(billed_cash_credit_redeemed_amount_mtd, 0))/NULLIFZERO(cash_net_revenue_mtd) AS net_credit_billings_as_percent_of_cash_net_revenue,
        COALESCE(billed_credit_cash_transaction_count_mtd, 0) - COALESCE(billed_credits_successful_on_retry_mtd, 0) AS billed_credits_successful_on_first_try,
        COALESCE(billed_credits_successful_on_retry_mtd, 0) AS billed_credits_successful_on_retry,
        billed_credit_cash_transaction_count_mtd AS billed_credit_cash_transaction_count,
        membership_fee_cash_transaction_amount_mtd AS membership_fee_cash_transaction_amount,
        gift_card_transaction_amount_mtd AS gift_card_transaction_amount,
        legacy_credit_cash_transaction_amount_mtd AS legacy_credit_cash_transaction_amount,
        membership_fee_cash_transaction_amount_mtd + gift_card_transaction_amount_mtd + legacy_credit_cash_transaction_amount_mtd AS other_billing_cash_transaction_amount,
        membership_fee_cash_refund_chargeback_amount_mtd AS membership_fee_cash_refund_chargeback_amount,
        COALESCE(membership_fee_cash_transaction_amount_mtd, 0) - COALESCE(membership_fee_cash_refund_chargeback_amount_mtd, 0) AS membership_fee_net_cash_transaction_amount,
        COALESCE(product_order_landed_product_cost_amount_mtd, 0) + COALESCE(product_order_exchange_product_cost_amount_mtd, 0) + COALESCE(product_order_reship_product_cost_amount_mtd, 0) AS outbound_product_cost_incl_reship_exch,
        COALESCE(oracle_product_order_landed_product_cost_amount_mtd, 0) + COALESCE(oracle_product_order_exchange_product_cost_amount_mtd, 0) + COALESCE(oracle_product_order_reship_product_cost_amount_mtd, 0) AS oracle_outbound_product_cost_incl_reship_exch,
        COALESCE(product_order_count_mtd, 0) + COALESCE(product_order_reship_order_count_mtd, 0) + COALESCE(product_order_exchange_order_count_mtd, 0) AS product_order_count_incl_reship_exch,
        COALESCE(unit_count_mtd, 0) + COALESCE(product_order_reship_unit_count_mtd, 0) + COALESCE(product_order_exchange_unit_count_mtd, 0) AS unit_count_incl_reship_exch,
        product_order_return_unit_count_mtd AS product_order_return_unit_count,
        (COALESCE(product_order_landed_product_cost_amount_mtd, 0) + COALESCE(product_order_exchange_product_cost_amount_mtd, 0) + COALESCE(product_order_reship_product_cost_amount_mtd, 0))/NULLIFZERO(COALESCE(unit_count_mtd, 0) + COALESCE(product_order_reship_unit_count_mtd, 0) + COALESCE(product_order_exchange_unit_count_mtd, 0)) AS auc_incl_reship_exch,
        product_cost_returned_resaleable_incl_reship_exch_mtd AS product_cost_returned_resaleable_incl_reship_exch,
        product_order_cost_product_returned_damaged_amount_mtd AS product_cost_returned_damaged_incl_reship_exch,
        COALESCE(product_order_landed_product_cost_amount_mtd, 0) + COALESCE(product_order_exchange_product_cost_amount_mtd, 0) + COALESCE(product_order_reship_product_cost_amount_mtd, 0) - COALESCE(product_cost_returned_resaleable_incl_reship_exch_mtd,0) AS product_cost_net_of_returns_incl_reship_exch,
        COALESCE(product_order_shipping_cost_amount_mtd, 0) + COALESCE(product_order_reship_shipping_cost_amount_mtd, 0) + COALESCE(product_order_exchange_shipping_cost_amount_mtd, 0) AS shipped_cost_incl_reship_exch,
        COALESCE(product_order_shipping_supplies_cost_amount_mtd, 0) + COALESCE(product_order_exchange_shipping_supplies_cost_amount_mtd, 0) + COALESCE(product_order_reship_shipping_supplies_cost_amount_mtd, 0) AS shipping_supplies_cost_incl_reship_exch,
        return_shipping_costs_incl_reship_exch_mtd AS return_shipping_costs_incl_reship_exch,
        misc_cogs_amount_mtd AS misc_cogs_amount,
        COALESCE(product_order_landed_product_cost_amount_mtd,0) + COALESCE(product_order_exchange_product_cost_amount_mtd,0) + COALESCE(product_order_reship_product_cost_amount_mtd,0) + COALESCE(product_order_shipping_cost_amount_mtd, 0) + COALESCE(product_order_reship_shipping_cost_amount_mtd, 0) + COALESCE(product_order_exchange_shipping_cost_amount_mtd, 0) + COALESCE(return_shipping_costs_incl_reship_exch_mtd,0) + COALESCE(product_order_shipping_supplies_cost_amount_mtd, 0) + COALESCE(product_order_exchange_shipping_supplies_cost_amount_mtd, 0) + COALESCE(product_order_reship_shipping_supplies_cost_amount_mtd, 0) + COALESCE(misc_cogs_amount_mtd,0) - COALESCE(product_cost_returned_resaleable_incl_reship_exch_mtd,0) AS total_cogs,
        COALESCE(oracle_product_order_landed_product_cost_amount_mtd,0) + COALESCE(oracle_product_order_exchange_product_cost_amount_mtd,0) + COALESCE(oracle_product_order_reship_product_cost_amount_mtd,0) + COALESCE(product_order_shipping_cost_amount_mtd, 0) + COALESCE(product_order_reship_shipping_cost_amount_mtd, 0) + COALESCE(product_order_exchange_shipping_cost_amount_mtd, 0) + COALESCE(return_shipping_costs_incl_reship_exch_mtd,0) + COALESCE(product_order_shipping_supplies_cost_amount_mtd, 0) + COALESCE(product_order_exchange_shipping_supplies_cost_amount_mtd, 0) + COALESCE(product_order_reship_shipping_supplies_cost_amount_mtd, 0) + COALESCE(misc_cogs_amount_mtd,0) - COALESCE(product_cost_returned_resaleable_incl_reship_exch_mtd,0) AS oracle_total_cogs,
        product_order_cash_credit_refund_amount_mtd AS product_order_cash_credit_refund_amount,
        activating_product_order_cash_credit_refund_amount_mtd AS activating_product_order_cash_credit_refund_amount,
        nonactivating_product_order_cash_credit_refund_amount_mtd AS nonactivating_product_order_cash_credit_refund_amount,
        COALESCE(product_order_cash_credit_refund_amount_mtd,0) - COALESCE(refund_cash_credit_redeemed_amount_mtd,0) AS product_order_cash_credit_refund_net_billings,
        COALESCE(membership_fee_cash_transaction_amount_mtd,0)
            + COALESCE(gift_card_transaction_amount_mtd,0)
            + COALESCE(legacy_credit_cash_transaction_amount_mtd,0)
            - COALESCE(other_cash_credit_redeemed_amount_mtd,0)
            - COALESCE(gift_card_cash_refund_chargeback_amount_mtd,0)
            - COALESCE(membership_fee_cash_refund_chargeback_amount_mtd,0)
            - COALESCE(legacy_credit_cash_refund_chargeback_amount_mtd,0) AS gift_card_membership_fee_legacy_credit_net_billings,
        noncash_credit_issued_amount_mtd AS noncash_credit_issued_amount,
        product_order_noncash_credit_redeemed_amount_mtd AS product_order_noncash_credit_redeemed_amount,
        NULL AS pending_product_order_count,
        NULL AS pending_order_cash_transaction_amount,
        NULL AS pending_order_cash_margin_pre_return_amount,
        skip_count AS skip_count,
        skip_count/NULLIFZERO(bom_vips) AS skip_rate,
        merch_purchase_count_mtd/NULLIFZERO(bom_vips) AS merch_purchase_rate,
        merch_purchase_hyperion_count_mtd/NULLIFZERO(bom_vips) AS merch_purchase_rate_hyperion,
        COALESCE(bop_vips, 0) + COALESCE(new_vips, 0) - COALESCE(cancels, 0) AS eop_vips,
        reactivated_vip_product_order_count_mtd as reactivated_vip_product_order_count,
        reactivated_vip_product_order_count_excl_seeding_mtd AS reactivated_vip_product_order_count_excl_seeding,
        reactivated_vip_product_margin_pre_return_excl_seeding_mtd AS reactivated_vip_product_margin_pre_return_excl_seeding,
         nonactivating_product_order_product_discount_amount_mtd
            /NULLIFZERO(COALESCE(nonactivating_product_order_non_token_subtotal_excl_tariff_amount_mtd, 0) - COALESCE(nonactivating_product_order_noncash_credit_redeemed_amount_mtd, 0))
                          AS nonactivating_cash_discount_percent,
        nonactivating_product_order_non_token_unit_count_mtd/NULLIFZERO(nonactivating_unit_count_mtd) AS nonactivating_non_token_unit_percent,
        guest_product_order_count_mtd/NULLIFZERO(nonactivating_product_order_count_mtd) AS nonactivating_guest_product_order_percent
    FROM _daily_cash_final_output dcfo
    JOIN _report_months rm ON dcfo.date = rm.last_day_of_month
        AND rm.report_mapping = dcfo.report_mapping
),
_run_rate AS (
    SELECT date,
        date_object,
        currency_object,
        currency_type,
        business_unit,
        store_brand,
        report_mapping,
        is_daily_cash_usd,
        is_daily_cash_eur,
        'RR' AS data_source,
        (COALESCE(activating_cash_gross_revenue_mtd, 0) * (COALESCE(activating_cash_gross_revenue_month_tot_ly, 0)
                /NULLIFZERO(activating_cash_gross_revenue_mtd_ly)))
            + (COALESCE(nonactivating_cash_gross_revenue_mtd, 0) * (COALESCE(nonactivating_cash_gross_revenue_month_tot_ly, 0)
            /NULLIFZERO(nonactivating_cash_gross_revenue_mtd_ly))) AS cash_gross_revenue,
        (COALESCE(activating_cash_net_revenue_mtd, 0) * (COALESCE(activating_cash_net_revenue_month_tot_ly, 0)
              /NULLIFZERO(activating_cash_net_revenue_mtd_ly)))
            + (COALESCE(nonactivating_cash_net_revenue_mtd, 0) * (COALESCE(nonactivating_cash_net_revenue_month_tot_ly, 0)
                /NULLIFZERO(nonactivating_cash_net_revenue_mtd_ly))) AS cash_net_revenue,
        COALESCE(cash_gross_profit_mtd, 0) * (COALESCE(cash_gross_profit_month_tot_ly, 0)/NULLIFZERO(cash_gross_profit_mtd_ly)) AS cash_gross_profit,
        (COALESCE(cash_gross_profit_mtd, 0) * (COALESCE(cash_gross_profit_month_tot_ly, 0)/NULLIFZERO(cash_gross_profit_mtd_ly)))
            /NULLIFZERO(cash_net_revenue_mtd * (COALESCE(cash_net_revenue_month_tot_ly, 0)/NULLIFZERO(cash_net_revenue_mtd_ly))) AS cash_gross_profit_percent_of_net_cash,
        COALESCE(cash_net_revenue_budgeted_fx_mtd, 0) * (COALESCE(cash_net_revenue_budgeted_fx_month_tot_ly, 0)
            /NULLIFZERO(cash_net_revenue_budgeted_fx_mtd_ly)) AS cash_net_revenue_with_budgeted_fx_rates,
        COALESCE(activating_cash_gross_revenue_mtd, 0) * (COALESCE(activating_cash_gross_revenue_month_tot_ly, 0)
            /NULLIFZERO(activating_cash_gross_revenue_mtd_ly)) AS activating_cash_gross_revenue,
        COALESCE(activating_cash_net_revenue_mtd, 0) * (COALESCE(activating_cash_net_revenue_month_tot_ly, 0)
            /NULLIFZERO(activating_cash_net_revenue_mtd_ly)) AS activating_cash_net_revenue,
        COALESCE(nonactivating_cash_gross_revenue_mtd, 0) * (COALESCE(nonactivating_cash_gross_revenue_month_tot_ly, 0)
            /NULLIFZERO(nonactivating_cash_gross_revenue_mtd_ly)) AS nonactivating_cash_gross_revenue,
        COALESCE(nonactivating_cash_net_revenue_mtd, 0) * (COALESCE(nonactivating_cash_net_revenue_month_tot_ly, 0)
            /NULLIFZERO(nonactivating_cash_net_revenue_mtd_ly)) AS nonactivating_cash_net_revenue,
        (COALESCE(product_order_cash_refund_amount_mtd, 0) * (COALESCE(product_order_cash_refund_amount_month_tot_ly, 0)
                /NULLIFZERO(product_order_cash_refund_amount_mtd_ly)))
            + (COALESCE(billing_cash_refund_amount_mtd, 0) * (COALESCE(billing_cash_refund_amount_month_tot_ly, 0)
                /NULLIFZERO(billing_cash_refund_amount_mtd_ly))) AS product_order_and_billing_cash_refund,
        (COALESCE(product_order_cash_chargeback_amount_mtd, 0) * (COALESCE(product_order_cash_chargeback_amount_month_tot_ly, 0)
                /NULLIFZERO(product_order_cash_chargeback_amount_mtd_ly)))
            + (COALESCE(billing_cash_chargeback_amount_mtd, 0) * (COALESCE(billing_cash_chargeback_amount_month_tot_ly, 0)
                /NULLIFZERO(billing_cash_chargeback_amount_mtd_ly))) AS product_order_and_billing_chargebacks,
        ((COALESCE(product_order_cash_refund_amount_mtd, 0) * (COALESCE(product_order_cash_refund_amount_month_tot_ly, 0)
                /NULLIFZERO(product_order_cash_refund_amount_mtd_ly)))
            + (COALESCE(billing_cash_refund_amount_mtd, 0) * (COALESCE(billing_cash_refund_amount_month_tot_ly, 0)
                /NULLIFZERO(billing_cash_refund_amount_mtd_ly)))
            + (COALESCE(product_order_cash_chargeback_amount_mtd, 0) * (COALESCE(product_order_cash_chargeback_amount_month_tot_ly, 0)
                /NULLIFZERO(product_order_cash_chargeback_amount_mtd_ly)))
            + (COALESCE(billing_cash_chargeback_amount_mtd, 0) * (COALESCE(billing_cash_chargeback_amount_month_tot_ly, 0)
                /NULLIFZERO(billing_cash_chargeback_amount_mtd_ly))))
            /NULLIFZERO(cash_gross_revenue_mtd * (COALESCE(cash_gross_revenue_month_tot_ly, 0)
                /NULLIFZERO(cash_gross_revenue_mtd_ly))) AS refund_chargebacks_as_percent_of_cash_gross_rev,
        COALESCE(activating_product_gross_revenue_mtd, 0) * (COALESCE(activating_product_gross_revenue_month_tot_ly, 0)
            /NULLIFZERO(activating_product_gross_revenue_mtd_ly)) AS activating_product_gross_revenue,
        COALESCE(activating_product_net_revenue_mtd, 0) * (COALESCE(activating_product_net_revenue_month_tot_ly, 0)
            /NULLIFZERO(activating_product_net_revenue_mtd_ly)) AS activating_product_net_revenue,
        COALESCE(activating_shipping_revenue_mtd, 0) * (COALESCE(activating_shipping_revenue_month_tot_ly, 0)
            /NULLIFZERO(activating_shipping_revenue_mtd_ly)) AS activating_shipping_revenue,
        COALESCE(activating_product_order_count_mtd, 0) * (COALESCE(activating_product_order_count_month_tot_ly, 0)
            /NULLIFZERO(activating_product_order_count_mtd_ly)) AS activating_product_order_count,
        COALESCE(activating_product_order_count_excl_seeding_mtd, 0) * (COALESCE(activating_product_order_count_excl_seeding_month_tot_ly, 0)
            /NULLIFZERO(activating_product_order_count_excl_seeding_mtd_ly)) AS activating_product_order_count_excl_seeding,
        COALESCE(activating_product_margin_pre_return_excl_seeding_mtd, 0) * (COALESCE(activating_product_margin_pre_return_excl_seeding_month_tot_ly, 0)
            /NULLIFZERO(activating_product_margin_pre_return_excl_seeding_mtd_ly)) AS activating_product_margin_pre_return_excl_seeding,
        (COALESCE(activating_product_gross_revenue_mtd, 0) * (COALESCE(activating_product_gross_revenue_month_tot_ly, 0)
                /NULLIFZERO(activating_product_gross_revenue_mtd_ly)))
            /NULLIFZERO(activating_product_order_count_mtd * (COALESCE(activating_product_order_count_month_tot_ly, 0)
                /NULLIFZERO(activating_product_order_count_mtd_ly))) AS activating_aov,
        COALESCE(activating_unit_count_mtd, 0) * (COALESCE(activating_unit_count_month_tot_ly, 0)/NULLIFZERO(activating_unit_count_mtd_ly)) AS activating_unit_count,
        (COALESCE(activating_unit_count_mtd, 0) * (COALESCE(activating_unit_count_month_tot_ly, 0)
                /NULLIFZERO(activating_unit_count_mtd_ly)))
            /NULLIFZERO(activating_product_order_count_mtd * (COALESCE(activating_product_order_count_month_tot_ly, 0)
                /NULLIFZERO(activating_product_order_count_mtd_ly))) AS activating_upt,
        (COALESCE(activating_product_gross_revenue_excl_shipping_mtd, 0) * (COALESCE(activating_product_gross_revenue_excl_shipping_month_tot_ly, 0)
                /NULLIFZERO(activating_product_gross_revenue_excl_shipping_mtd_ly)))
            /NULLIFZERO(activating_unit_count_mtd * (COALESCE(activating_unit_count_month_tot_ly, 0)
                /NULLIFZERO(activating_unit_count_mtd_ly))) AS activating_aur,
        ((COALESCE(activating_product_order_landed_product_cost_amount_mtd, 0) * (COALESCE(activating_product_order_landed_product_cost_amount_month_tot_ly, 0)
                    /NULLIFZERO(activating_product_order_landed_product_cost_amount_mtd_ly)))
                + (COALESCE(activating_product_order_reship_product_cost_amount_mtd, 0) * (COALESCE(activating_product_order_reship_product_cost_amount_month_tot_ly, 0)
                    /NULLIFZERO(activating_product_order_reship_product_cost_amount_mtd_ly)))
                + (COALESCE(activating_product_order_exchange_product_cost_amount_mtd, 0) * (COALESCE(activating_product_order_exchange_product_cost_amount_month_tot_ly, 0)
                    /NULLIFZERO(activating_product_order_exchange_product_cost_amount_mtd_ly))))
            /NULLIFZERO((COALESCE(activating_unit_count_mtd, 0) * (COALESCE(activating_unit_count_month_tot_ly, 0)
                    /NULLIFZERO(activating_unit_count_mtd_ly)))
                + (COALESCE(activating_product_order_exchange_unit_count_mtd, 0) * (COALESCE(activating_product_order_exchange_unit_count_month_tot_ly, 0)
                    /NULLIFZERO(activating_product_order_exchange_unit_count_mtd_ly)))
                + (COALESCE(activating_product_order_reship_unit_count_mtd, 0) * (COALESCE(activating_product_order_reship_unit_count_month_tot_ly, 0)
                    /NULLIFZERO(activating_product_order_reship_unit_count_mtd_ly)))) AS activating_auc_incl_reship_exch,
        (COALESCE(activating_product_order_product_discount_amount_mtd, 0) * (COALESCE(activating_product_order_product_discount_amount_month_tot_ly, 0)
                /NULLIFZERO(activating_product_order_product_discount_amount_mtd_ly)))
            /NULLIFZERO((COALESCE(activating_product_order_product_subtotal_amount_mtd, 0) * (COALESCE(activating_product_order_product_subtotal_amount_month_tot_ly, 0)
                    /NULLIFZERO(activating_product_order_product_subtotal_amount_mtd_ly)))
                - (COALESCE(activating_product_order_noncash_credit_redeemed_amount_mtd, 0) * (COALESCE(activating_product_order_noncash_credit_redeemed_amount_month_tot_ly, 0)
                    /NULLIFZERO(activating_product_order_noncash_credit_redeemed_amount_mtd_ly)))) AS activating_discount_percent,
        ((COALESCE(activating_product_order_air_vip_price_mtd, 0) * (COALESCE(activating_product_order_air_vip_price_month_tot_ly, 0) /NULLIFZERO(activating_product_order_air_vip_price_mtd_ly)))
                - ((COALESCE(activating_product_order_subtotal_excl_tariff_amount_mtd, 0) * (COALESCE(activating_product_order_subtotal_excl_tariff_amount_month_tot_ly, 0)/NULLIFZERO(activating_product_order_subtotal_excl_tariff_amount_mtd_ly)))
                    - (COALESCE(activating_product_order_product_discount_amount_mtd, 0) * (COALESCE(activating_product_order_product_discount_amount_month_tot_ly, 0)/NULLIFZERO(activating_product_order_product_discount_amount_mtd_ly)))))
            /NULLIFZERO(COALESCE(activating_product_order_air_vip_price_mtd, 0) * (COALESCE(activating_product_order_air_vip_price_month_tot_ly, 0)/NULLIFZERO(activating_product_order_air_vip_price_mtd_ly))) AS planning_activating_discount_percent,
        COALESCE(activating_product_gross_profit_mtd, 0) * (COALESCE(activating_product_gross_profit_month_tot_ly, 0)
            /NULLIFZERO(activating_product_gross_profit_mtd_ly)) AS activating_product_gross_profit,
        (COALESCE(activating_product_gross_profit_mtd, 0) * (COALESCE(activating_product_gross_profit_month_tot_ly, 0)
                /NULLIFZERO(activating_product_gross_profit_mtd_ly)))
            /NULLIFZERO(activating_product_order_count_mtd * (COALESCE(activating_product_order_count_month_tot_ly, 0)
                /NULLIFZERO(activating_product_order_count_mtd_ly))) AS activating_product_gross_profit_per_order,
        (COALESCE(nonactivating_product_gross_profit_mtd, 0) * (COALESCE(nonactivating_product_gross_profit_month_tot_ly, 0)
                /NULLIFZERO(nonactivating_product_gross_profit_mtd_ly)))
            /NULLIFZERO(nonactivating_product_order_count_mtd * (COALESCE(nonactivating_product_order_count_month_tot_ly, 0)
                /NULLIFZERO(nonactivating_product_order_count_mtd_ly))) AS nonactivating_product_gross_profit_per_order,
        (COALESCE(activating_product_gross_profit_mtd, 0) * (COALESCE(activating_product_gross_profit_month_tot_ly, 0)
                /NULLIFZERO(activating_product_gross_profit_mtd_ly)))
            /NULLIFZERO(activating_product_net_revenue_mtd * (COALESCE(activating_product_net_revenue_month_tot_ly, 0)
                /NULLIFZERO(activating_product_net_revenue_mtd_ly))) AS activating_product_gross_profit_percent,
        COALESCE(activating_product_order_cash_gross_revenue_mtd, 0) * (COALESCE(activating_product_order_cash_gross_revenue_month_tot_ly, 0)
            /NULLIFZERO(activating_product_order_cash_gross_revenue_mtd_ly)) AS activating_product_order_cash_gross_revenue,
        COALESCE(activating_product_margin_pre_return_mtd, 0) * (COALESCE(activating_product_margin_pre_return_month_tot_ly, 0)
            /NULLIFZERO(activating_product_margin_pre_return_mtd_ly)) AS activating_product_margin_pre_return,
        (COALESCE(activating_product_margin_pre_return_mtd, 0) * (COALESCE(activating_product_margin_pre_return_month_tot_ly, 0)
                /NULLIFZERO(activating_product_margin_pre_return_mtd_ly)))
            /NULLIFZERO(activating_product_order_count_mtd * (COALESCE(activating_product_order_count_month_tot_ly, 0)
                /NULLIFZERO(activating_product_order_count_mtd_ly))) AS activating_product_margin_pre_return_per_order,
        ((COALESCE(product_margin_pre_return_mtd, 0) * (COALESCE(product_margin_pre_return_month_tot_ly, 0)
                /NULLIFZERO(product_margin_pre_return_mtd_ly)))
        -(COALESCE(activating_product_margin_pre_return_mtd, 0) * (COALESCE(activating_product_margin_pre_return_month_tot_ly, 0)
                /NULLIFZERO(activating_product_margin_pre_return_mtd_ly))))
        /NULLIFZERO(nonactivating_product_order_count_mtd * (COALESCE(nonactivating_product_order_count_month_tot_ly, 0)
                /NULLIFZERO(nonactivating_product_order_count_mtd_ly))) AS nonactivating_product_margin_pre_return_per_order,
        (COALESCE(activating_product_margin_pre_return_mtd, 0) * (COALESCE(activating_product_margin_pre_return_month_tot_ly, 0)
                /NULLIFZERO(activating_product_margin_pre_return_mtd_ly)))
            /NULLIFZERO(activating_product_gross_revenue_mtd * (COALESCE(activating_product_gross_revenue_month_tot_ly, 0)
                /NULLIFZERO(activating_product_gross_revenue_mtd_ly))) AS activating_product_margin_pre_return_percent,
        COALESCE(nonactivating_product_gross_revenue_mtd, 0) * (COALESCE(nonactivating_product_gross_revenue_month_tot_ly, 0)
            /NULLIFZERO(nonactivating_product_gross_revenue_mtd_ly)) AS nonactivating_product_gross_revenue,
        COALESCE(nonactivating_product_net_revenue_mtd, 0) * (COALESCE(nonactivating_product_net_revenue_month_tot_ly, 0)
            /NULLIFZERO(nonactivating_product_net_revenue_mtd_ly)) AS nonactivating_product_net_revenue,
        (COALESCE(nonactivating_product_order_cash_gross_revenue_mtd, 0) * (COALESCE(nonactivating_product_order_cash_gross_revenue_month_tot_ly, 0)
                /NULLIFZERO(nonactivating_product_order_cash_gross_revenue_mtd_ly)))
            /NULLIFZERO(COALESCE(nonactivating_product_gross_revenue_mtd, 0) * (COALESCE(nonactivating_product_gross_revenue_month_tot_ly, 0)
                /NULLIFZERO(nonactivating_product_gross_revenue_mtd_ly))) AS nonactivating_cash_revenue_as_percent_of_product_gross_revenue,
        COALESCE(nonactivating_shipping_revenue_mtd, 0) * (COALESCE(nonactivating_shipping_revenue_month_tot_ly, 0)
            /NULLIFZERO(nonactivating_shipping_revenue_mtd_ly)) AS nonactivating_shipping_revenue,
        COALESCE(nonactivating_product_order_count_mtd, 0) * (COALESCE(nonactivating_product_order_count_month_tot_ly, 0)
            /NULLIFZERO(nonactivating_product_order_count_mtd_ly)) AS nonactivating_product_order_count,
        COALESCE(nonactivating_product_order_count_excl_seeding_mtd, 0) * (COALESCE(nonactivating_product_order_count_excl_seeding_month_tot_ly, 0)
            /NULLIFZERO(nonactivating_product_order_count_excl_seeding_mtd_ly)) AS nonactivating_product_order_count_excl_seeding,
        COALESCE(nonactivating_product_margin_pre_return_excl_seeding_mtd, 0) * (COALESCE(nonactivating_product_margin_pre_return_excl_seeding_month_tot_ly, 0)
            /NULLIFZERO(nonactivating_product_margin_pre_return_excl_seeding_mtd_ly)) AS nonactivating_product_margin_pre_return_excl_seeding,
        (COALESCE(nonactivating_product_gross_revenue_mtd, 0) * (COALESCE(nonactivating_product_gross_revenue_month_tot_ly, 0)
                /NULLIFZERO(nonactivating_product_gross_revenue_mtd_ly)))
            /NULLIFZERO(nonactivating_product_order_count_mtd * (COALESCE(nonactivating_product_order_count_month_tot_ly, 0)
                /NULLIFZERO(nonactivating_product_order_count_mtd_ly))) AS nonactivating_aov,
        COALESCE(nonactivating_unit_count_mtd, 0) * (COALESCE(nonactivating_unit_count_month_tot_ly, 0)
            /NULLIFZERO(nonactivating_unit_count_mtd_ly)) AS nonactivating_unit_count,
        (COALESCE(nonactivating_unit_count_mtd, 0) * (COALESCE(nonactivating_unit_count_month_tot_ly, 0)
                /NULLIFZERO(nonactivating_unit_count_mtd_ly)))
            /NULLIFZERO(nonactivating_product_order_count_mtd * (COALESCE(nonactivating_product_order_count_month_tot_ly, 0)
                /NULLIFZERO(nonactivating_product_order_count_mtd_ly))) AS nononactivating_upt,
        (COALESCE(nonactivating_product_gross_revenue_excl_shipping_mtd, 0) * (COALESCE(nonactivating_product_gross_revenue_excl_shipping_month_tot_ly, 0)
                /NULLIFZERO(nonactivating_product_gross_revenue_excl_shipping_mtd_ly)))
            /NULLIFZERO(nonactivating_unit_count_mtd * (COALESCE(nonactivating_unit_count_month_tot_ly, 0)
                /NULLIFZERO(nonactivating_unit_count_mtd_ly))) AS nonactivating_aur,
        ((COALESCE(nonactivating_product_order_landed_product_cost_amount_mtd, 0) * (COALESCE(nonactivating_product_order_landed_product_cost_amount_month_tot_ly, 0)
                    /NULLIFZERO(nonactivating_product_order_landed_product_cost_amount_mtd_ly)))
                + (COALESCE(nonactivating_product_order_reship_product_cost_amount_mtd, 0) * (COALESCE(nonactivating_product_order_reship_product_cost_amount_month_tot_ly, 0)
                    /NULLIFZERO(nonactivating_product_order_reship_product_cost_amount_mtd_ly)))
                + (COALESCE(nonactivating_product_order_exchange_product_cost_amount_mtd, 0) * (COALESCE(nonactivating_product_order_exchange_product_cost_amount_month_tot_ly, 0)
                    /NULLIFZERO(nonactivating_product_order_exchange_product_cost_amount_mtd_ly))))
            /NULLIFZERO((COALESCE(nonactivating_unit_count_mtd, 0) * (COALESCE(nonactivating_unit_count_month_tot_ly, 0)
                    /NULLIFZERO(nonactivating_unit_count_mtd_ly)))
                + (COALESCE(nonactivating_product_order_exchange_unit_count_mtd, 0) * (COALESCE(nonactivating_product_order_exchange_unit_count_month_tot_ly, 0)
                    /NULLIFZERO(nonactivating_product_order_exchange_unit_count_mtd_ly)))
                + (COALESCE(nonactivating_product_order_reship_unit_count_mtd, 0) * (COALESCE(nonactivating_product_order_reship_unit_count_month_tot_ly, 0)
                    /NULLIFZERO(nonactivating_product_order_reship_unit_count_mtd_ly)))) AS nonactivating_auc_incl_reship_exch,
        (COALESCE(nonactivating_product_order_product_discount_amount_mtd, 0) * (COALESCE(nonactivating_product_order_product_discount_amount_month_tot_ly, 0)
                /NULLIFZERO(nonactivating_product_order_product_discount_amount_mtd_ly)))
            /NULLIFZERO((COALESCE(nonactivating_product_order_product_subtotal_amount_mtd, 0) * (COALESCE(nonactivating_product_order_product_subtotal_amount_month_tot_ly, 0)
                    /NULLIFZERO(nonactivating_product_order_product_subtotal_amount_mtd_ly)))
                - (COALESCE(nonactivating_product_order_noncash_credit_redeemed_amount_mtd, 0) * (COALESCE(nonactivating_product_order_noncash_credit_redeemed_amount_month_tot_ly, 0)
                    /NULLIFZERO(nonactivating_product_order_noncash_credit_redeemed_amount_mtd_ly)))) AS nonactivating_discount_percent,
        ((COALESCE(nonactivating_product_order_air_price_mtd, 0) * (COALESCE(nonactivating_product_order_air_price_month_tot_ly, 0) /NULLIFZERO(nonactivating_product_order_air_price_mtd_ly)))
                - ((COALESCE(nonactivating_product_order_subtotal_excl_tariff_amount_mtd, 0) * (COALESCE(nonactivating_product_order_subtotal_excl_tariff_amount_month_tot_ly, 0)/NULLIFZERO(nonactivating_product_order_subtotal_excl_tariff_amount_mtd_ly)))
                    - (COALESCE(nonactivating_product_order_product_discount_amount_mtd, 0) * (COALESCE(nonactivating_product_order_product_discount_amount_month_tot_ly, 0)/NULLIFZERO(nonactivating_product_order_product_discount_amount_mtd_ly)))))
            /NULLIFZERO(COALESCE(nonactivating_product_order_air_price_mtd, 0) * (COALESCE(nonactivating_product_order_air_price_month_tot_ly, 0)/NULLIFZERO(nonactivating_product_order_air_price_mtd_ly))) AS planning_nonactivating_discount_percent,
        COALESCE(nonactivating_product_gross_profit_mtd, 0) * (COALESCE(nonactivating_product_gross_profit_month_tot_ly, 0)
            /NULLIFZERO(nonactivating_product_gross_profit_mtd_ly)) AS nonactivating_product_gross_profit,
        (COALESCE(nonactivating_product_gross_profit_mtd, 0) * (COALESCE(nonactivating_product_gross_profit_month_tot_ly, 0)
                /NULLIFZERO(nonactivating_product_gross_profit_mtd_ly)))
            /NULLIFZERO(nonactivating_product_net_revenue_mtd * (COALESCE(nonactivating_product_net_revenue_month_tot_ly, 0)
                /NULLIFZERO(nonactivating_product_net_revenue_mtd_ly))) AS nonactivating_product_gross_profit_percent,
        COALESCE(nonactivating_product_order_cash_gross_profit_mtd, 0) * (COALESCE(nonactivating_product_order_cash_gross_profit_month_tot_ly, 0)
            /NULLIFZERO(nonactivating_product_order_cash_gross_profit_mtd_ly)) AS nonactivating_product_order_cash_gross_profit,
        (COALESCE(nonactivating_product_order_cash_gross_profit_mtd, 0) * (COALESCE(nonactivating_product_order_cash_gross_profit_month_tot_ly, 0)
                /NULLIFZERO(nonactivating_product_order_cash_gross_profit_mtd_ly)))
            /NULLIFZERO((COALESCE(nonactivating_product_order_count_mtd, 0) * (COALESCE(nonactivating_product_order_count_month_tot_ly, 0)
                /NULLIFZERO(nonactivating_product_order_count_mtd_ly)))) AS nonactivating_product_order_cash_gross_profit_per_order,
        COALESCE(nonactivating_product_order_cash_gross_revenue_mtd, 0) * (COALESCE(nonactivating_product_order_cash_gross_revenue_month_tot_ly, 0)
            /NULLIFZERO(nonactivating_product_order_cash_gross_revenue_mtd_ly)) AS nonactivating_product_order_cash_gross_revenue,
        (COALESCE(guest_product_order_count_mtd, 0) * (COALESCE(guest_product_order_count_month_tot_ly, 0)
                /NULLIFZERO(guest_product_order_count_mtd_ly)))
            /NULLIFZERO(nonactivating_product_order_count_mtd * (COALESCE(nonactivating_product_order_count_month_tot_ly, 0)
                /NULLIFZERO(nonactivating_product_order_count_mtd_ly))) AS guest_orders_as_percent_of_non_activating,
        COALESCE(guest_product_net_revenue_mtd, 0) * (COALESCE(guest_product_net_revenue_month_tot_ly, 0)
            /NULLIFZERO(guest_product_net_revenue_mtd_ly)) AS guest_product_net_revenue,
        COALESCE(guest_product_order_count_mtd, 0) * (COALESCE(guest_product_order_count_month_tot_ly, 0)
            /NULLIFZERO(guest_product_order_count_mtd_ly)) AS guest_product_order_count,
        COALESCE(guest_product_order_count_excl_seeding_mtd, 0) * (COALESCE(guest_product_order_count_excl_seeding_month_tot_ly, 0)
            /NULLIFZERO(guest_product_order_count_excl_seeding_mtd_ly)) AS guest_product_order_count_excl_seeding,
        COALESCE(guest_product_margin_pre_return_excl_seeding_mtd, 0) * (COALESCE(guest_product_margin_pre_return_excl_seeding_month_tot_ly, 0)
            /NULLIFZERO(guest_product_margin_pre_return_excl_seeding_mtd_ly)) AS guest_product_margin_pre_return_excl_seeding,
        (COALESCE(guest_product_gross_revenue_mtd, 0) * (COALESCE(guest_product_gross_revenue_month_tot_ly, 0)
                /NULLIFZERO(guest_product_gross_revenue_mtd_ly)))
            /NULLIFZERO(guest_product_order_count_mtd * (COALESCE(guest_product_order_count_month_tot_ly, 0)
                /NULLIFZERO(guest_product_order_count_mtd_ly))) AS guest_aov,
        COALESCE(guest_product_gross_profit_mtd, 0) * (COALESCE(guest_product_gross_profit_month_tot_ly, 0)
            /NULLIFZERO(guest_product_gross_profit_mtd_ly)) AS guest_product_gross_profit,
        COALESCE(guest_product_gross_revenue_mtd, 0) * (COALESCE(guest_product_gross_revenue_month_tot_ly, 0)
            /NULLIFZERO(guest_product_gross_revenue_mtd_ly)) AS guest_product_gross_revenue,
        COALESCE(guest_shipping_revenue_mtd, 0) * (COALESCE(guest_shipping_revenue_month_tot_ly, 0)
            /NULLIFZERO(guest_shipping_revenue_mtd_ly)) AS guest_shipping_revenue,
        COALESCE(guest_unit_count_mtd, 0) * (COALESCE(guest_unit_count_month_tot_ly, 0)
            /NULLIFZERO(guest_unit_count_mtd_ly)) AS guest_unit_count,
        (COALESCE(guest_unit_count_mtd, 0) * (COALESCE(guest_unit_count_month_tot_ly, 0)
                /NULLIFZERO(guest_unit_count_mtd_ly)))
            /NULLIFZERO(COALESCE(guest_product_order_count_mtd, 0) * (COALESCE(guest_product_order_count_month_tot_ly, 0)
                /NULLIFZERO(guest_product_order_count_mtd_ly))) AS guest_upt,
        (COALESCE(guest_product_gross_revenue_excl_shipping_mtd, 0) * (COALESCE(guest_product_gross_revenue_excl_shipping_month_tot_ly, 0)
                /NULLIFZERO(guest_product_gross_revenue_excl_shipping_mtd_ly)))
            /NULLIFZERO(COALESCE(guest_unit_count_mtd, 0) * (COALESCE(guest_unit_count_month_tot_ly, 0)
                /NULLIFZERO(guest_unit_count_mtd_ly))) AS guest_aur,
        (COALESCE(guest_product_order_product_discount_amount_mtd, 0) * (COALESCE(guest_product_order_product_discount_amount_month_tot_ly, 0)
                /NULLIFZERO(guest_product_order_product_discount_amount_mtd_ly)))
            /NULLIFZERO((COALESCE(guest_product_gross_revenue_mtd, 0) * (COALESCE(guest_product_gross_revenue_month_tot_ly, 0)
                    /NULLIFZERO(guest_product_gross_revenue_mtd_ly)))
                + (COALESCE(guest_product_order_product_discount_amount_mtd, 0) * (COALESCE(guest_product_order_product_discount_amount_month_tot_ly, 0)
                    /NULLIFZERO(guest_product_order_product_discount_amount_mtd_ly)))) AS guest_discount_percent,
        ((COALESCE(guest_product_order_retail_unit_price_mtd, 0) * (COALESCE(guest_product_order_retail_unit_price_month_tot_ly, 0) /NULLIFZERO(guest_product_order_retail_unit_price_mtd_ly)))
                - ((COALESCE(guest_product_order_subtotal_excl_tariff_amount_mtd, 0) * (COALESCE(guest_product_order_subtotal_excl_tariff_amount_month_tot_ly, 0)/NULLIFZERO(guest_product_order_subtotal_excl_tariff_amount_mtd_ly)))
                    - (COALESCE(guest_product_order_product_discount_amount_mtd, 0) * (COALESCE(guest_product_order_product_discount_amount_month_tot_ly, 0)/NULLIFZERO(guest_product_order_product_discount_amount_mtd_ly)))))
            /NULLIFZERO(COALESCE(guest_product_order_retail_unit_price_mtd, 0) * (COALESCE(guest_product_order_retail_unit_price_month_tot_ly, 0)/NULLIFZERO(guest_product_order_retail_unit_price_mtd_ly))) AS planning_guest_discount_percent,
        (COALESCE(guest_product_gross_profit_mtd, 0) * (COALESCE(guest_product_gross_profit_month_tot_ly, 0)
                /NULLIFZERO(guest_product_gross_profit_mtd_ly)))
            /NULLIFZERO(COALESCE(guest_product_net_revenue_mtd, 0) * (COALESCE(guest_product_net_revenue_month_tot_ly, 0)
                /NULLIFZERO(guest_product_net_revenue_mtd_ly))) AS guest_product_gross_profit_percent,
        ((COALESCE(repeat_vip_product_order_air_vip_price_mtd, 0) * (COALESCE(repeat_vip_product_order_air_vip_price_month_tot_ly, 0) /NULLIFZERO(repeat_vip_product_order_air_vip_price_mtd_ly)))
                - ((COALESCE(repeat_vip_product_order_subtotal_excl_tariff_amount_mtd, 0) * (COALESCE(repeat_vip_product_order_subtotal_excl_tariff_amount_month_tot_ly, 0)/NULLIFZERO(repeat_vip_product_order_subtotal_excl_tariff_amount_mtd_ly)))
                    - (COALESCE(repeat_vip_product_order_product_discount_amount_mtd, 0) * (COALESCE(repeat_vip_product_order_product_discount_amount_month_tot_ly, 0)/NULLIFZERO(repeat_vip_product_order_product_discount_amount_mtd_ly)))))
            /NULLIFZERO(COALESCE(repeat_vip_product_order_air_vip_price_mtd, 0) * (COALESCE(repeat_vip_product_order_air_vip_price_month_tot_ly, 0)/NULLIFZERO(repeat_vip_product_order_air_vip_price_mtd_ly))) AS planning_repeat_vip_discount_percent,
        COALESCE(billed_credit_cash_transaction_amount_mtd, 0) * (COALESCE(billed_credit_cash_transaction_amount_month_tot_ly, 0)
            /NULLIFZERO(billed_credit_cash_transaction_amount_mtd_ly)) AS billed_credit_cash_transaction_amount,
        COALESCE(billed_credit_cash_refund_chargeback_amount_mtd, 0) * (COALESCE(billed_credit_cash_refund_chargeback_amount_month_tot_ly, 0)
            /NULLIFZERO(billed_credit_cash_refund_chargeback_amount_mtd_ly)) AS billed_credit_cash_refund_chargeback_amount,
        COALESCE(billed_cash_credit_redeemed_amount_mtd, 0) * (COALESCE(billed_cash_credit_redeemed_amount_month_tot_ly, 0)
            /NULLIFZERO(billed_cash_credit_redeemed_amount_mtd_ly)) AS billed_cash_credit_redeemed_amount,
        COALESCE(same_month_billed_credit_redeemed_mtd, 0) * (COALESCE(same_month_billed_credit_redeemed_month_tot_ly, 0)
            /NULLIFZERO(same_month_billed_credit_redeemed_mtd_ly)) AS same_month_billed_credit_redeemed,
        (COALESCE(billed_credit_cash_transaction_amount_mtd, 0) * (COALESCE(billed_credit_cash_transaction_amount_month_tot_ly, 0)
                /NULLIFZERO(billed_credit_cash_transaction_amount_mtd_ly)))
            + (COALESCE(billed_credit_cash_refund_chargeback_amount_mtd, 0) * (COALESCE(billed_credit_cash_refund_chargeback_amount_month_tot_ly, 0)
                /NULLIFZERO(billed_credit_cash_refund_chargeback_amount_mtd_ly)))
            + (COALESCE(billed_cash_credit_redeemed_amount_mtd, 0) * (COALESCE(billed_cash_credit_redeemed_amount_month_tot_ly, 0)
                /NULLIFZERO(billed_cash_credit_redeemed_amount_mtd_ly))) AS billed_credit_net_billings,
        ((COALESCE(billed_credit_cash_transaction_amount_mtd, 0) * (COALESCE(billed_credit_cash_transaction_amount_month_tot_ly, 0)
                    /NULLIFZERO(billed_credit_cash_transaction_amount_mtd_ly)))
                + (COALESCE(billed_credit_cash_refund_chargeback_amount_mtd, 0) * (COALESCE(billed_credit_cash_refund_chargeback_amount_month_tot_ly, 0)
                    /NULLIFZERO(billed_credit_cash_refund_chargeback_amount_mtd_ly)))
                + (COALESCE(billed_cash_credit_redeemed_amount_mtd, 0) * (COALESCE(billed_cash_credit_redeemed_amount_month_tot_ly, 0)
                    /NULLIFZERO(billed_cash_credit_redeemed_amount_mtd_ly))))
            /NULLIFZERO((COALESCE(cash_net_revenue_mtd, 0) * (COALESCE(cash_net_revenue_month_tot_ly, 0)
                /NULLIFZERO(cash_net_revenue_mtd_ly)))) AS net_credit_billings_as_percent_of_cash_net_revenue,
        (COALESCE(billed_credit_cash_transaction_count_mtd, 0) * (COALESCE(billed_credit_cash_transaction_count_month_tot_ly, 0)
                /NULLIFZERO(billed_credit_cash_transaction_count_mtd_ly)))
            - (COALESCE(billed_credits_successful_on_retry_mtd, 0) * (COALESCE(billed_credits_successful_on_retry_month_tot_ly, 0)
                /NULLIFZERO(billed_credits_successful_on_retry_mtd_ly))) AS billed_credits_successful_on_first_try,
        (COALESCE(billed_credits_successful_on_retry_mtd, 0) * (COALESCE(billed_credits_successful_on_retry_month_tot_ly, 0)
                /NULLIFZERO(billed_credits_successful_on_retry_mtd_ly))) AS billed_credits_successful_on_retry,
        (COALESCE(billed_credit_cash_transaction_count_mtd, 0) * (COALESCE(billed_credit_cash_transaction_count_month_tot_ly, 0)
                /NULLIFZERO(billed_credit_cash_transaction_count_mtd_ly))) AS billed_credit_cash_transaction_count,
        COALESCE(membership_fee_cash_transaction_amount_mtd, 0) * (COALESCE(membership_fee_cash_transaction_amount_month_tot_ly, 0)
                /NULLIFZERO(membership_fee_cash_transaction_amount_mtd_ly)) AS membership_fee_cash_transaction_amount,
        COALESCE(gift_card_transaction_amount_mtd, 0) * (COALESCE(gift_card_transaction_amount_month_tot_ly, 0)
                /NULLIFZERO(gift_card_transaction_amount_mtd_ly)) AS gift_card_transaction_amount,
        COALESCE(legacy_credit_cash_transaction_amount_mtd, 0) * (COALESCE(legacy_credit_cash_transaction_amount_month_tot_ly, 0)
                /NULLIFZERO(legacy_credit_cash_transaction_amount_mtd_ly)) AS legacy_credit_cash_transaction_amount,
        COALESCE((COALESCE(membership_fee_cash_transaction_amount_mtd, 0) * (COALESCE(membership_fee_cash_transaction_amount_month_tot_ly, 0)
                /NULLIFZERO(membership_fee_cash_transaction_amount_mtd_ly))), 0)
            + COALESCE((COALESCE(gift_card_transaction_amount_mtd, 0) * (COALESCE(gift_card_transaction_amount_month_tot_ly, 0)
                /NULLIFZERO(gift_card_transaction_amount_mtd_ly))), 0)
            + COALESCE((COALESCE(legacy_credit_cash_transaction_amount_mtd, 0) * (COALESCE(legacy_credit_cash_transaction_amount_month_tot_ly, 0)
                /NULLIFZERO(legacy_credit_cash_transaction_amount_mtd_ly))), 0) AS other_billing_cash_transaction_amount,
        COALESCE(membership_fee_cash_refund_chargeback_amount_mtd, 0) * (COALESCE(membership_fee_cash_refund_chargeback_amount_month_tot_ly, 0)
                /NULLIFZERO(membership_fee_cash_refund_chargeback_amount_mtd_ly)) AS membership_fee_cash_refund_chargeback_amount,
        (COALESCE(membership_fee_cash_transaction_amount_mtd, 0) * (COALESCE(membership_fee_cash_transaction_amount_month_tot_ly, 0)
                /NULLIFZERO(membership_fee_cash_transaction_amount_mtd_ly)))
            + (COALESCE(membership_fee_cash_refund_chargeback_amount_mtd, 0) * (COALESCE(membership_fee_cash_refund_chargeback_amount_month_tot_ly, 0)
                /NULLIFZERO(membership_fee_cash_refund_chargeback_amount_mtd_ly))) AS membership_fee_net_cash_transaction_amount,
        (COALESCE(product_order_landed_product_cost_amount_mtd, 0) * (COALESCE(product_order_landed_product_cost_amount_month_tot_ly, 0)
                     /NULLIFZERO(product_order_landed_product_cost_amount_mtd_ly)))
                + (COALESCE(product_order_exchange_product_cost_amount_mtd, 0) * (COALESCE(product_order_exchange_product_cost_amount_month_tot_ly, 0)
                    /NULLIFZERO(product_order_exchange_product_cost_amount_mtd_ly)))
                + (COALESCE(product_order_reship_product_cost_amount_mtd, 0) * (COALESCE(product_order_reship_product_cost_amount_month_tot_ly, 0)
                    /NULLIFZERO(product_order_reship_product_cost_amount_mtd_ly))) AS outbound_product_cost_incl_reship_exch,
        (COALESCE(oracle_product_order_landed_product_cost_amount_mtd, 0) * (COALESCE(oracle_product_order_landed_product_cost_amount_month_tot_ly, 0)
                     /NULLIFZERO(oracle_product_order_landed_product_cost_amount_mtd_ly)))
                + (COALESCE(oracle_product_order_exchange_product_cost_amount_mtd, 0) * (COALESCE(oracle_product_order_exchange_product_cost_amount_month_tot_ly, 0)
                    /NULLIFZERO(oracle_product_order_exchange_product_cost_amount_mtd_ly)))
                + (COALESCE(oracle_product_order_reship_product_cost_amount_mtd, 0) * (COALESCE(oracle_product_order_reship_product_cost_amount_month_tot_ly, 0)
                    /NULLIFZERO(oracle_product_order_reship_product_cost_amount_mtd_ly))) AS oracle_outbound_product_cost_incl_reship_exch,
        (COALESCE(product_order_count_mtd, 0) * (COALESCE(product_order_count_month_tot_ly, 0)
                     /NULLIFZERO(product_order_count_mtd_ly)))
                + (COALESCE(product_order_reship_order_count_mtd, 0) * (COALESCE(product_order_reship_order_count_month_tot_ly, 0)
                    /NULLIFZERO(product_order_reship_order_count_mtd_ly)))
                + (COALESCE(product_order_exchange_order_count_mtd, 0) * (COALESCE(product_order_exchange_order_count_month_tot_ly, 0)
                    /NULLIFZERO(product_order_exchange_order_count_mtd_ly))) AS product_order_count_incl_reship_exch,
        (COALESCE(unit_count_mtd, 0) * (COALESCE(unit_count_month_tot_ly, 0)
                     /NULLIFZERO(unit_count_mtd_ly)))
                + (COALESCE(product_order_reship_unit_count_mtd, 0) * (COALESCE(product_order_reship_unit_count_month_tot_ly, 0)
                    /NULLIFZERO(product_order_reship_unit_count_mtd_ly)))
                + (COALESCE(product_order_exchange_unit_count_mtd, 0) * (COALESCE(product_order_exchange_unit_count_month_tot_ly, 0)
                    /NULLIFZERO(product_order_exchange_unit_count_mtd_ly))) AS unit_count_incl_reship_exch,
        COALESCE(product_order_return_unit_count_mtd, 0) * (COALESCE(product_order_return_unit_count_month_tot_ly, 0)
            /NULLIFZERO(product_order_return_unit_count_mtd_ly)) AS product_order_return_unit_count,
        ((COALESCE(product_order_landed_product_cost_amount_mtd, 0) * (COALESCE(product_order_landed_product_cost_amount_month_tot_ly, 0)
                     /NULLIFZERO(product_order_landed_product_cost_amount_mtd_ly)))
                + (COALESCE(product_order_exchange_product_cost_amount_mtd, 0) * (COALESCE(product_order_exchange_product_cost_amount_month_tot_ly, 0)
                    /NULLIFZERO(product_order_exchange_product_cost_amount_mtd_ly)))
                + (COALESCE(product_order_reship_product_cost_amount_mtd, 0) * (COALESCE(product_order_reship_product_cost_amount_month_tot_ly, 0)
                    /NULLIFZERO(product_order_reship_product_cost_amount_mtd_ly))))
            /NULLIFZERO((COALESCE(unit_count_mtd, 0) * (COALESCE(unit_count_month_tot_ly, 0)
                     /NULLIFZERO(unit_count_mtd_ly)))
                + (COALESCE(product_order_reship_unit_count_mtd, 0) * (COALESCE(product_order_reship_unit_count_month_tot_ly, 0)
                    /NULLIFZERO(product_order_reship_unit_count_mtd_ly)))
                + (COALESCE(product_order_exchange_unit_count_mtd, 0) * (COALESCE(product_order_exchange_unit_count_month_tot_ly, 0)
                    /NULLIFZERO(product_order_exchange_unit_count_mtd_ly)))) AS auc_incl_reship_exch,
        COALESCE(product_cost_returned_resaleable_incl_reship_exch_mtd, 0) * (COALESCE(product_cost_returned_resaleable_incl_reship_exch_month_tot_ly, 0)
            /NULLIFZERO(product_cost_returned_resaleable_incl_reship_exch_mtd_ly)) AS product_cost_returned_resaleable_incl_reship_exch,
        COALESCE(product_order_cost_product_returned_damaged_amount_mtd, 0) * (COALESCE(product_order_cost_product_returned_damaged_amount_month_tot_ly, 0)
            /NULLIFZERO(product_order_cost_product_returned_damaged_amount_mtd_ly)) AS product_cost_returned_damaged_incl_reship_exch,
        (COALESCE(product_order_landed_product_cost_amount_mtd, 0) + COALESCE(product_order_exchange_product_cost_amount_mtd, 0) + COALESCE(product_order_reship_product_cost_amount_mtd, 0) - COALESCE(product_cost_returned_resaleable_incl_reship_exch_mtd,0))
        * ((COALESCE(product_order_landed_product_cost_amount_month_tot_ly, 0) + COALESCE(product_order_exchange_product_cost_amount_month_tot_ly, 0) + COALESCE(product_order_reship_product_cost_amount_month_tot_ly, 0) - COALESCE(product_cost_returned_resaleable_incl_reship_exch_month_tot_ly,0))
            /NULLIFZERO(COALESCE(product_order_landed_product_cost_amount_mtd_ly, 0) + COALESCE(product_order_exchange_product_cost_amount_mtd_ly, 0) + COALESCE(product_order_reship_product_cost_amount_mtd_ly, 0) - COALESCE(product_cost_returned_resaleable_incl_reship_exch_mtd_ly,0))) AS product_cost_net_of_returns_incl_reship_exch,
        (COALESCE(product_order_shipping_cost_amount_mtd, 0) * (COALESCE(product_order_shipping_cost_amount_month_tot_ly, 0)
                /NULLIFZERO(product_order_shipping_cost_amount_mtd_ly)))
            + (COALESCE(product_order_reship_shipping_cost_amount_mtd, 0) * (COALESCE(product_order_reship_shipping_cost_amount_month_tot_ly, 0)
                /NULLIFZERO(product_order_reship_shipping_cost_amount_mtd_ly)))
            + (COALESCE(product_order_exchange_shipping_cost_amount_mtd, 0) * (COALESCE(product_order_exchange_shipping_cost_amount_month_tot_ly, 0)
                /NULLIFZERO(product_order_exchange_shipping_cost_amount_mtd_ly))) AS shipped_cost_incl_reship_exch,
        (COALESCE(product_order_shipping_supplies_cost_amount_mtd, 0) * (COALESCE(product_order_shipping_supplies_cost_amount_month_tot_ly, 0)
                /NULLIFZERO(product_order_shipping_supplies_cost_amount_mtd_ly)))
            + (COALESCE(product_order_exchange_shipping_supplies_cost_amount_mtd, 0) * (COALESCE(product_order_exchange_shipping_supplies_cost_amount_month_tot_ly, 0)
                /NULLIFZERO(product_order_exchange_shipping_supplies_cost_amount_mtd_ly)))
            + (COALESCE(product_order_reship_shipping_supplies_cost_amount_mtd, 0) * (COALESCE(product_order_reship_shipping_supplies_cost_amount_month_tot_ly, 0)
                /NULLIFZERO(product_order_reship_shipping_supplies_cost_amount_mtd_ly))) AS shipping_supplies_cost_incl_reship_exch,
        COALESCE(return_shipping_costs_incl_reship_exch_mtd, 0) * (COALESCE(return_shipping_costs_incl_reship_exch_month_tot_ly, 0)
            /NULLIFZERO(return_shipping_costs_incl_reship_exch_mtd_ly)) AS return_shipping_costs_incl_reship_exch,
        COALESCE(misc_cogs_amount_mtd, 0) * (COALESCE(misc_cogs_amount_month_tot_ly, 0)
            /NULLIFZERO(misc_cogs_amount_mtd_ly)) AS misc_cogs_amount,
        (COALESCE(product_order_landed_product_cost_amount_mtd,0) * (COALESCE(product_order_landed_product_cost_amount_month_tot_ly,0) / NULLIFZERO(product_order_landed_product_cost_amount_mtd_ly)))
            + (COALESCE(product_order_exchange_product_cost_amount_mtd,0) * (COALESCE(product_order_exchange_product_cost_amount_month_tot_ly,0) / NULLIFZERO(product_order_exchange_product_cost_amount_mtd_ly)))
            + (COALESCE(product_order_reship_product_cost_amount_mtd,0) * (COALESCE(product_order_reship_product_cost_amount_month_tot_ly,0) / NULLIFZERO(product_order_reship_product_cost_amount_mtd_ly)))
            + (COALESCE(product_order_shipping_cost_amount_mtd,0) * (COALESCE(product_order_shipping_cost_amount_month_tot_ly,0) / NULLIFZERO(product_order_shipping_cost_amount_mtd_ly)))
            + (COALESCE(product_order_reship_shipping_cost_amount_mtd,0) * (COALESCE(product_order_reship_shipping_cost_amount_month_tot_ly,0) / NULLIFZERO(product_order_reship_shipping_cost_amount_mtd_ly)))
            + (COALESCE(product_order_exchange_shipping_cost_amount_mtd,0) * (COALESCE(product_order_exchange_shipping_cost_amount_month_tot_ly,0) / NULLIFZERO(product_order_exchange_shipping_cost_amount_mtd_ly)))
            + (COALESCE(return_shipping_costs_incl_reship_exch_mtd,0) * (COALESCE(return_shipping_costs_incl_reship_exch_month_tot_ly,0) / NULLIFZERO(return_shipping_costs_incl_reship_exch_mtd_ly)))
            + (COALESCE(product_order_shipping_supplies_cost_amount_mtd,0) * (COALESCE(product_order_shipping_supplies_cost_amount_month_tot_ly,0) / NULLIFZERO(product_order_shipping_supplies_cost_amount_mtd_ly)))
            + (COALESCE(product_order_exchange_shipping_supplies_cost_amount_mtd,0) * (COALESCE(product_order_exchange_shipping_supplies_cost_amount_month_tot_ly,0) / NULLIFZERO(product_order_exchange_shipping_supplies_cost_amount_mtd_ly)))
            + (COALESCE(product_order_reship_shipping_supplies_cost_amount_mtd,0) * (COALESCE(product_order_reship_shipping_supplies_cost_amount_month_tot_ly,0) / NULLIFZERO(product_order_reship_shipping_supplies_cost_amount_mtd_ly)))
            + (COALESCE(misc_cogs_amount_mtd,0) * (COALESCE(misc_cogs_amount_month_tot_ly,0) / NULLIFZERO(misc_cogs_amount_mtd_ly)))
            - (COALESCE(product_cost_returned_resaleable_incl_reship_exch_mtd,0) * (COALESCE(product_cost_returned_resaleable_incl_reship_exch_month_tot_ly,0) / NULLIFZERO(product_cost_returned_resaleable_incl_reship_exch_mtd_ly))) AS total_cogs,
        (COALESCE(oracle_product_order_landed_product_cost_amount_mtd,0) * (COALESCE(oracle_product_order_landed_product_cost_amount_month_tot_ly,0) / NULLIFZERO(oracle_product_order_landed_product_cost_amount_mtd_ly)))
            + (COALESCE(oracle_product_order_exchange_product_cost_amount_mtd,0) * (COALESCE(oracle_product_order_exchange_product_cost_amount_month_tot_ly,0) / NULLIFZERO(oracle_product_order_exchange_product_cost_amount_mtd_ly)))
            + (COALESCE(oracle_product_order_reship_product_cost_amount_mtd,0) * (COALESCE(oracle_product_order_reship_product_cost_amount_month_tot_ly,0) / NULLIFZERO(oracle_product_order_reship_product_cost_amount_mtd_ly)))
            + (COALESCE(product_order_shipping_cost_amount_mtd,0) * (COALESCE(product_order_shipping_cost_amount_month_tot_ly,0) / NULLIFZERO(product_order_shipping_cost_amount_mtd_ly)))
            + (COALESCE(product_order_reship_shipping_cost_amount_mtd,0) * (COALESCE(product_order_reship_shipping_cost_amount_month_tot_ly,0) / NULLIFZERO(product_order_reship_shipping_cost_amount_mtd_ly)))
            + (COALESCE(product_order_exchange_shipping_cost_amount_mtd,0) * (COALESCE(product_order_exchange_shipping_cost_amount_month_tot_ly,0) / NULLIFZERO(product_order_exchange_shipping_cost_amount_mtd_ly)))
            + (COALESCE(return_shipping_costs_incl_reship_exch_mtd,0) * (COALESCE(return_shipping_costs_incl_reship_exch_month_tot_ly,0) / NULLIFZERO(return_shipping_costs_incl_reship_exch_mtd_ly)))
            + (COALESCE(product_order_shipping_supplies_cost_amount_mtd,0) * (COALESCE(product_order_shipping_supplies_cost_amount_month_tot_ly,0) / NULLIFZERO(product_order_shipping_supplies_cost_amount_mtd_ly)))
            + (COALESCE(product_order_exchange_shipping_supplies_cost_amount_mtd,0) * (COALESCE(product_order_exchange_shipping_supplies_cost_amount_month_tot_ly,0) / NULLIFZERO(product_order_exchange_shipping_supplies_cost_amount_mtd_ly)))
            + (COALESCE(product_order_reship_shipping_supplies_cost_amount_mtd,0) * (COALESCE(product_order_reship_shipping_supplies_cost_amount_month_tot_ly,0) / NULLIFZERO(product_order_reship_shipping_supplies_cost_amount_mtd_ly)))
            + (COALESCE(misc_cogs_amount_mtd,0) * (COALESCE(misc_cogs_amount_month_tot_ly,0) / NULLIFZERO(misc_cogs_amount_mtd_ly)))
            - (COALESCE(product_cost_returned_resaleable_incl_reship_exch_mtd,0) * (COALESCE(product_cost_returned_resaleable_incl_reship_exch_month_tot_ly,0) / NULLIFZERO(product_cost_returned_resaleable_incl_reship_exch_mtd_ly))) AS oracle_total_cogs,
        COALESCE(product_order_cash_credit_refund_amount_mtd, 0) * (COALESCE(product_order_cash_credit_refund_amount_month_tot_ly, 0)/NULLIFZERO(product_order_cash_credit_refund_amount_mtd_ly)) AS product_order_cash_credit_refund_amount,
        COALESCE(activating_product_order_cash_credit_refund_amount_mtd, 0) * (COALESCE(activating_product_order_cash_credit_refund_amount_month_tot_ly, 0)/NULLIFZERO(activating_product_order_cash_credit_refund_amount_mtd_ly)) AS activating_product_order_cash_credit_refund_amount,
        COALESCE(nonactivating_product_order_cash_credit_refund_amount_mtd, 0) * (COALESCE(nonactivating_product_order_cash_credit_refund_amount_month_tot_ly, 0)/NULLIFZERO(nonactivating_product_order_cash_credit_refund_amount_mtd_ly)) AS nonactivating_product_order_cash_credit_refund_amount,
        (COALESCE(product_order_cash_credit_refund_amount_mtd, 0) * (COALESCE(product_order_cash_credit_refund_amount_month_tot_ly, 0)/NULLIFZERO(product_order_cash_credit_refund_amount_mtd_ly)))
            - (COALESCE(refund_cash_credit_redeemed_amount_mtd,0) * (COALESCE(refund_cash_credit_redeemed_amount_month_tot_ly,0) / NULLIFZERO(refund_cash_credit_redeemed_amount_mtd_ly))) AS product_order_cash_credit_refund_net_billings,
        (COALESCE(membership_fee_cash_transaction_amount_mtd, 0) * (COALESCE(membership_fee_cash_transaction_amount_month_tot_ly, 0)/NULLIFZERO(membership_fee_cash_transaction_amount_mtd_ly)))
            + (COALESCE(gift_card_transaction_amount_mtd,0) * (COALESCE(gift_card_transaction_amount_month_tot_ly,0) / NULLIFZERO(gift_card_transaction_amount_mtd_ly)))
            + (COALESCE(legacy_credit_cash_transaction_amount_mtd,0) * (COALESCE(legacy_credit_cash_transaction_amount_month_tot_ly,0) / NULLIFZERO(legacy_credit_cash_transaction_amount_mtd_ly)))
            - (COALESCE(other_cash_credit_redeemed_amount_mtd,0) * (COALESCE(other_cash_credit_redeemed_amount_month_tot_ly,0) / NULLIFZERO(other_cash_credit_redeemed_amount_mtd_ly)))
            - (COALESCE(gift_card_cash_refund_chargeback_amount,0) * (COALESCE(gift_card_cash_refund_chargeback_amount_month_tot_ly,0) / NULLIFZERO(gift_card_cash_refund_chargeback_amount_mtd_ly)))
            - (COALESCE(membership_fee_cash_refund_chargeback_amount_mtd,0) * (COALESCE(membership_fee_cash_refund_chargeback_amount_month_tot_ly,0) / NULLIFZERO(membership_fee_cash_refund_chargeback_amount_mtd_ly)))
            - (COALESCE(legacy_credit_cash_refund_chargeback_amount,0) * (COALESCE(legacy_credit_cash_refund_chargeback_amount_month_tot_ly,0) / NULLIFZERO(legacy_credit_cash_refund_chargeback_amount_mtd_ly))) AS gift_card_membership_fee_legacy_credit_net_billings,
        COALESCE(noncash_credit_issued_amount_mtd, 0) * (COALESCE(noncash_credit_issued_amount_month_tot_ly, 0)/NULLIFZERO(noncash_credit_issued_amount_mtd_ly)) AS noncash_credit_issued_amount,
        COALESCE(product_order_noncash_credit_redeemed_amount_mtd, 0) * (COALESCE(product_order_noncash_credit_redeemed_amount_month_tot_ly, 0)/NULLIFZERO(product_order_noncash_credit_redeemed_amount_mtd_ly)) AS product_order_noncash_credit_redeemed_amount,
        NULL AS pending_product_order_count,
        NULL AS pending_order_cash_transaction_amount,
        NULL AS pending_order_cash_margin_pre_return_amount,
        NULL AS skip_count,
        NULL AS skip_rate,
        COALESCE(merch_purchase_count_mtd, 0) * (COALESCE(merch_purchase_count_month_tot_ly, 0)/NULLIFZERO(merch_purchase_count_mtd_ly))
            /NULLIFZERO(bom_vips) AS merch_purchase_rate,
        COALESCE(merch_purchase_hyperion_count_mtd, 0) * (COALESCE(merch_purchase_hyperion_count_month_tot_ly, 0)/NULLIFZERO(merch_purchase_hyperion_count_mtd_ly))
            /NULLIFZERO(bom_vips) AS merch_purchase_rate_hyperion,
        bom_vips + (COALESCE(new_vips_mtd, 0) * (DAY(LAST_DAY(date))/DAY(date))) - (COALESCE(cancels_mtd, 0) * (DAY(LAST_DAY(date))/DAY(date))) AS eop_vips,
        COALESCE(reactivated_vip_product_order_count_mtd, 0) * (COALESCE(reactivated_vip_product_order_count_month_tot_ly, 0)
            /NULLIFZERO(reactivated_vip_product_order_count_mtd_ly)) AS reactivated_vip_product_order_count,
        COALESCE(reactivated_vip_product_order_count_excl_seeding_mtd, 0) * (COALESCE(reactivated_vip_product_order_count_excl_seeding_month_tot_ly, 0)
            /NULLIFZERO(reactivated_vip_product_order_count_excl_seeding_mtd_ly)) AS reactivated_vip_product_order_count_excl_seeding,
        COALESCE(reactivated_vip_product_margin_pre_return_excl_seeding_mtd, 0) * (COALESCE(reactivated_vip_product_margin_pre_return_excl_seeding_month_tot_ly, 0)
            /NULLIFZERO(reactivated_vip_product_margin_pre_return_excl_seeding_mtd_ly)) AS reactivated_vip_product_margin_pre_return_excl_seeding,
        (COALESCE(nonactivating_product_order_product_discount_amount_mtd,0) * (COALESCE(nonactivating_product_order_product_discount_amount_month_tot_ly,0) / NULLIFZERO(nonactivating_product_order_product_discount_amount_mtd_ly)))
            /NULLIFZERO((COALESCE(nonactivating_product_order_non_token_subtotal_excl_tariff_amount_mtd,0) * (COALESCE(nonactivating_product_order_non_token_subtotal_excl_tariff_amount_month_tot_ly,0) / NULLIFZERO(nonactivating_product_order_non_token_subtotal_excl_tariff_amount_mtd_ly)))
               - (COALESCE(nonactivating_product_order_noncash_credit_redeemed_amount_mtd,0) * (COALESCE(nonactivating_product_order_noncash_credit_redeemed_amount_month_tot_ly,0) / NULLIFZERO(nonactivating_product_order_noncash_credit_redeemed_amount_mtd_ly))))
                          AS nonactivating_cash_discount_percent,
        (COALESCE(nonactivating_product_order_non_token_unit_count_mtd,0) * (COALESCE(nonactivating_product_order_non_token_unit_count_month_tot_ly,0) / NULLIFZERO(nonactivating_product_order_non_token_unit_count_mtd_ly)))
            /NULLIFZERO((COALESCE(nonactivating_unit_count_mtd,0) * (COALESCE(nonactivating_unit_count_month_tot_ly,0) / NULLIFZERO(nonactivating_unit_count_mtd_ly))))
            AS nonactivating_non_token_unit_percent,
        (COALESCE(guest_product_order_count_mtd,0) * (COALESCE(guest_product_order_count_month_tot_ly,0) / NULLIFZERO(guest_product_order_count_mtd_ly)))
        /NULLIFZERO((COALESCE(nonactivating_product_order_count_mtd,0) * (COALESCE(nonactivating_product_order_count_month_tot_ly,0) / NULLIFZERO(nonactivating_product_order_count_mtd_ly))))
            AS nonactivating_guest_product_order_percent
    FROM _daily_cash_final_output
    WHERE date = (
         SELECT report_date
        FROM _variables
        WHERE source = 'Report Date'
    )
),
_forecast AS (
    SELECT date,
        date_object,
        currency_object,
        currency_type,
        business_unit,
        store_brand,
        report_mapping,
        is_daily_cash_usd,
        is_daily_cash_eur,
        'FC' AS data_source,
        cash_gross_revenue_forecast AS cash_gross_revenue,
        cash_net_revenue_forecast AS cash_net_revenue,
        cash_gross_profit_forecast AS cash_gross_profit,
        cash_gross_profit_forecast/NULLIFZERO(cash_net_revenue_forecast) AS cash_gross_profit_percent_of_net_cash,
        cash_net_revenue_forecast AS cash_net_revenue_with_budgeted_fx_rates,
        NULL AS activating_cash_gross_revenue,
        activating_cash_net_revenue_forecast AS activating_cash_net_revenue,
        NULL AS nonactivating_cash_gross_revenue,
        nonactivating_cash_net_revenue_forecast AS nonactivating_cash_net_revenue,
        product_order_and_billing_cash_refund_forecast AS product_order_and_billing_cash_refund,
        product_order_and_billing_chargebacks_forecast AS product_order_and_billing_chargebacks,
        (COALESCE(product_order_and_billing_cash_refund_forecast, 0) + COALESCE(product_order_and_billing_chargebacks_forecast, 0))
            /NULLIFZERO(cash_gross_revenue_forecast) AS refund_chargebacks_as_percent_of_cash_gross_rev,
        activating_product_gross_revenue_forecast AS activating_product_gross_revenue,
        activating_product_net_revenue_forecast AS activating_product_net_revenue,
        NULL AS activating_shipping_revenue,
        activating_product_order_count_forecast AS activating_product_order_count,
        NULL AS activating_product_order_count_excl_seeding,
        NULL AS activating_product_margin_pre_return_excl_seeding,
        activating_product_gross_revenue_forecast/NULLIFZERO(activating_product_order_count_forecast) AS activating_aov,
        activating_unit_count_forecast AS activating_unit_count,
        activating_unit_count_forecast/NULLIFZERO(activating_product_order_count_forecast) AS activating_upt,
        activating_product_gross_revenue_forecast/NULLIFZERO(activating_unit_count_forecast) AS activating_aur,
        NULL AS activating_auc_incl_reship_exch,
        activating_discount_percent_forecast AS activating_discount_percent,
        NULL AS planning_activating_discount_percent,
        activating_product_gross_profit_forecast AS activating_product_gross_profit,
        activating_product_gross_profit_forecast/NULLIFZERO(activating_product_order_count_forecast) AS activating_product_gross_profit_per_order,
        nonactivating_product_gross_profit_forecast/NULLIFZERO(nonactivating_product_order_count_forecast) AS nonactivating_product_gross_profit_per_order,
        activating_product_gross_profit_forecast/NULLIFZERO(activating_product_net_revenue_forecast) AS activating_product_gross_profit_percent,
        NULL AS activating_product_order_cash_gross_revenue,
        activating_product_margin_pre_return_forecast AS activating_product_margin_pre_return,
        activating_product_margin_pre_return_forecast/NULLIFZERO(activating_product_order_count_forecast) AS activating_product_margin_pre_return_per_order,
        NULL AS nonactivating_product_margin_pre_return_per_order,
        activating_product_margin_pre_return_forecast/NULLIFZERO(activating_product_gross_revenue_forecast) AS activating_product_margin_pre_return_percent,
        nonactivating_product_gross_revenue_forecast AS nonactivating_product_gross_revenue,
        nonactivating_product_net_revenue_forecast AS nonactivating_product_net_revenue,
        nonactivating_product_order_cash_gross_revenue_forecast/NULLIFZERO(nonactivating_product_gross_revenue_forecast) AS nonactivating_cash_revenue_as_percent_of_product_gross_revenue,
        NULL AS nonactivating_shipping_revenue,
        nonactivating_product_order_count_forecast AS nonactivating_product_order_count,
        NULL AS nonactivating_product_order_count_excl_seeding,
        NULL AS nonactivating_product_margin_pre_return_excl_seeding,
        nonactivating_product_gross_revenue_forecast/NULLIFZERO(nonactivating_product_order_count_forecast) AS nonactivating_aov,
        nonactivating_unit_count_forecast AS nonactivating_unit_count,
        nonactivating_unit_count_forecast/NULLIFZERO(nonactivating_product_order_count_forecast) AS nononactivating_upt,
        nonactivating_product_gross_revenue_forecast/NULLIFZERO(nonactivating_unit_count_forecast) AS nonactivating_aur,
        NULL AS nonactivating_auc_incl_reship_exch,

        nonactivating_discount_percent_forecast AS nonactivating_discount_percent, --nonactivating_product_order_product_discount_amount_forecast/NULLIFZERO(COALESCE(nonactivating_product_order_subtotal_excl_tariff_amount_forecast, 0) - COALESCE(nonactivating_product_order_noncash_credit_redeemed_amount_forecast, 0))
        NULL AS planning_nonactivating_discount_percent,
        nonactivating_product_gross_profit_forecast AS nonactivating_product_gross_profit,
        nonactivating_product_gross_profit_forecast/NULLIFZERO(nonactivating_product_net_revenue_forecast) AS nonactivating_product_gross_profit_percent,
        nonactivating_product_order_cash_gross_profit_forecast AS nonactivating_product_order_cash_gross_profit,
        nonactivating_product_order_cash_gross_profit_forecast/NULLIFZERO(nonactivating_product_order_count_forecast) AS nonactivating_product_order_cash_gross_profit_per_order,
        nonactivating_product_order_cash_gross_revenue_forecast AS nonactivating_product_order_cash_gross_revenue,

        NULL AS guest_orders_as_percent_of_non_activating,
        NULL AS guest_product_net_revenue,
        NULL AS guest_product_order_count,
        NULL AS guest_product_order_count_excl_seeding,
        NULL AS guest_product_margin_pre_return_excl_seeding,
        NULL AS guest_aov,
        NULL AS guest_product_gross_profit,
        NULL AS guest_product_gross_revenue,
        NULL As guest_shipping_revenue,
        NULL AS guest_unit_count,
        NULL AS guest_upt,
        NULL AS guest_aur,
        NULL AS guest_discount_percent,
        NULL AS planning_guest_discount_percent,
        NULL AS guest_product_gross_profit_percent,
        NULL AS planning_repeat_vip_discount_percent,
        billed_credit_cash_transaction_amount_forecast AS billed_credit_cash_transaction_amount,
        billed_credit_cash_refund_chargeback_amount_forecast AS billed_credit_cash_refund_chargeback_amount,
        billed_cash_credit_redeemed_amount_forecast AS billed_cash_credit_redeemed_amount,
        NULL AS same_month_billed_credit_redeemed,
        COALESCE(billed_credit_cash_transaction_amount_forecast, 0) - COALESCE(billed_credit_cash_refund_chargeback_amount_forecast, 0) - COALESCE(billed_cash_credit_redeemed_amount_forecast, 0) AS billed_credit_net_billings,
        (COALESCE(billed_credit_cash_transaction_amount_forecast, 0) - COALESCE(billed_credit_cash_refund_chargeback_amount_forecast, 0) - COALESCE(billed_cash_credit_redeemed_amount_forecast, 0))/NULLIFZERO(cash_net_revenue_forecast) AS net_credit_billings_as_percent_of_cash_net_revenue,
        NULL AS billed_credits_successful_on_first_try,
        NULL AS billed_credits_successful_on_retry,
        NULL AS billed_credit_cash_transaction_count,
        NULL AS membership_fee_cash_transaction_amount,
        NULL AS gift_card_transaction_amount,
        NULL AS legacy_credit_cash_transaction_amount,
        NULL AS other_billing_cash_transaction_amount,
        NULL AS membership_fee_cash_refund_chargeback_amount,
        NULL AS membership_fee_net_cash_transaction_amount,
        product_cost_calc_forecast AS outbound_product_cost_incl_reship_exch,
        NULL AS oracle_outbound_product_cost_incl_reship_exch,
        COALESCE(activating_product_order_count_forecast, 0) + COALESCE(nonactivating_product_order_count_forecast, 0) + COALESCE(product_order_reship_exch_order_count_forecast, 0) AS product_order_count_incl_reship_exch,
        unit_count_forecast AS unit_count_incl_reship_exch,
        NULL AS product_order_return_unit_count,
        product_cost_calc_forecast/NULLIFZERO(unit_count_forecast) AS auc_incl_reship_exch,
        NULL AS product_cost_returned_resaleable_incl_reship_exch,
        NULL AS product_cost_returned_damaged_incl_reship_exch,
        NULL AS product_cost_net_of_returns_incl_reship_exch,
        shipping_cost_incl_reship_exch_forecast AS shipped_cost_incl_reship_exch,
        shipping_supplies_cost_incl_reship_exch_forecast AS shipping_supplies_cost_incl_reship_exch,
        return_shipping_costs_incl_reship_exch_forecast AS return_shipping_costs_incl_reship_exch,
        misc_cogs_amount_forecast AS misc_cogs_amount,
        total_cogs_forecast AS total_cogs,
        NULL AS oracle_total_cogs,
        product_order_cash_credit_refund_amount_forecast AS product_order_cash_credit_refund_amount,
        activating_product_order_cash_credit_refund_amount_forecast AS activating_product_order_cash_credit_refund_amount,
        nonactivating_product_order_cash_credit_refund_amount_forecast AS nonactivating_product_order_cash_credit_refund_amount,
        COALESCE(product_order_cash_credit_refund_amount_forecast,0) - COALESCE(refund_cash_credit_redeemed_amount_forecast,0) AS product_order_cash_credit_refund_net_billings,
        NULL AS gift_card_membership_fee_legacy_credit_net_billings,
        NULL AS noncash_credit_issued_amount,
        NULL AS product_order_noncash_credit_redeemed_amount,
        NULL AS pending_product_order_count,
        NULL AS pending_order_cash_transaction_amount,
        NULL AS pending_order_cash_margin_pre_return_amount,
        NULL AS skip_count,
        NULL AS skip_rate,
        NULL AS merch_purchase_rate,
        NULL AS merch_purchase_rate_hyperion,
        COALESCE(bop_vips_forecast, 0) + COALESCE(new_vips_forecast, 0) - COALESCE(cancels_forecast, 0) AS eop_vips,
        NULL AS reactivated_vip_product_order_count,
        NULL AS reactivated_vip_product_order_count_excl_seeding,
        NULL AS reactivated_vip_product_margin_pre_return_excl_seeding,
        NULL AS nonactivating_cash_discount_percent,
        NULL AS nonactivating_non_token_unit_percent,
        NULL AS nonactivating_guest_product_order_percent
    FROM _daily_cash_final_output
    WHERE date = (
         SELECT report_date
        FROM _variables
        WHERE source = 'Report Date'
    )
),
_budget AS (
    SELECT date,
        date_object,
        currency_object,
        currency_type,
        business_unit,
        store_brand,
        report_mapping,
        is_daily_cash_usd,
        is_daily_cash_eur,
        'BT' AS data_source,
        cash_gross_revenue_budget AS cash_gross_revenue,
        cash_net_revenue_budget AS cash_net_revenue,
        cash_gross_profit_budget AS cash_gross_profit,
        cash_gross_profit_budget/NULLIFZERO(cash_net_revenue_budget) AS cash_gross_profit_percent_of_net_cash,
        cash_net_revenue_budget AS cash_net_revenue_with_budgeted_fx_rates,
        NULL AS activating_cash_gross_revenue,
        activating_cash_net_revenue_budget AS activating_cash_net_revenue,
        NULL AS nonactivating_cash_gross_revenue,
        nonactivating_cash_net_revenue_budget AS nonactivating_cash_net_revenue,
        product_order_and_billing_cash_refund_budget AS product_order_and_billing_cash_refund,
        product_order_and_billing_chargebacks_budget AS product_order_and_billing_chargebacks,
        (COALESCE(product_order_and_billing_cash_refund_budget, 0) + COALESCE(product_order_and_billing_chargebacks_budget, 0))
            /NULLIFZERO(cash_gross_revenue_budget) AS refund_chargebacks_as_percent_of_cash_gross_rev,
        activating_product_gross_revenue_budget AS activating_product_gross_revenue,
        activating_product_net_revenue_budget AS activating_product_net_revenue,
        NULL AS activating_shipping_revenue,
        activating_product_order_count_budget AS activating_product_order_count,
        NULL AS activating_product_order_count_excl_seeding,
        NULL AS activating_product_margin_pre_return_excl_seeding,
        activating_product_gross_revenue_budget/NULLIFZERO(activating_product_order_count_budget) AS activating_aov,
        activating_unit_count_budget AS activating_unit_count,
        activating_unit_count_budget/NULLIFZERO(activating_product_order_count_budget) AS activating_upt,
        activating_product_gross_revenue_budget/NULLIFZERO(activating_unit_count_budget) AS activating_aur,
        NULL AS activating_auc_incl_reship_exch,
        activating_discount_percent_budget AS activating_discount_percent,
        NULL AS planning_activating_discount_percent,
        activating_product_gross_profit_budget AS activating_product_gross_profit,
        activating_product_gross_profit_budget/NULLIFZERO(activating_product_order_count_budget) AS activating_product_gross_profit_per_order,
        nonactivating_product_gross_profit_budget/NULLIFZERO(nonactivating_product_order_count_budget) AS activating_product_gross_profit_per_order,
        activating_product_gross_profit_budget/NULLIFZERO(activating_product_net_revenue_budget) AS activating_product_gross_profit_percent,
        NULL AS activating_product_order_cash_gross_revenue,
        activating_product_margin_pre_return_budget AS activating_product_margin_pre_return,
        activating_product_margin_pre_return_budget/NULLIFZERO(activating_product_order_count_budget) AS activating_product_margin_pre_return_per_order,
        NULL AS nonactivating_product_margin_pre_return_per_order,
        activating_product_margin_pre_return_budget/NULLIFZERO(activating_product_gross_revenue_budget) AS activating_product_margin_pre_return_percent,
        nonactivating_product_gross_revenue_budget AS nonactivating_product_gross_revenue,
        nonactivating_product_net_revenue_budget AS nonactivating_product_net_revenue,
        nonactivating_product_order_cash_gross_revenue_budget/NULLIFZERO(nonactivating_product_gross_revenue_budget) AS nonactivating_cash_revenue_as_percent_of_product_gross_revenue,
        NULL AS nonactivating_shipping_revenue,
        nonactivating_product_order_count_budget AS nonactivating_product_order_count,
        NULL AS nonactivating_product_order_count_excl_seeding,
        NULL AS nonactivating_product_margin_pre_return_excl_seeding,
        nonactivating_product_gross_revenue_budget/NULLIFZERO(nonactivating_product_order_count_budget) AS nonactivating_aov,
        nonactivating_unit_count_budget AS nonactivating_unit_count,
        nonactivating_unit_count_budget/NULLIFZERO(nonactivating_product_order_count_budget) AS nononactivating_upt,
        nonactivating_product_gross_revenue_budget/NULLIFZERO(nonactivating_unit_count_budget) AS nonactivating_aur,
        NULL AS nonactivating_auc_incl_reship_exch,
        nonactivating_discount_percent_budget AS nonactivating_discount_percent,
        NULL AS planning_nonactivating_discount_percent,
        nonactivating_product_gross_profit_budget AS nonactivating_product_gross_profit,
        nonactivating_product_gross_profit_budget/NULLIFZERO(nonactivating_product_net_revenue_budget) AS nonactivating_product_gross_profit_percent,
        nonactivating_product_order_cash_gross_profit_budget AS nonactivating_product_order_cash_gross_profit,
        nonactivating_product_order_cash_gross_profit_budget/NULLIFZERO(nonactivating_product_order_count_budget) AS nonactivating_product_order_cash_gross_profit_per_order,
        nonactivating_product_order_cash_gross_revenue_budget AS nonactivating_product_order_cash_gross_revenue,
        NULL AS guest_orders_as_percent_of_non_activating,
        NULL AS guest_product_net_revenue,
        NULL AS guest_product_order_count,
        NULL AS guest_product_order_count_excl_seeding,
        NULL AS guest_product_margin_pre_return_excl_seeding,
        NULL AS guest_aov,
        NULL AS guest_product_gross_profit,
        NULL AS guest_product_gross_revenue,
        NULL AS guest_shipping_revenue,
        NULL AS guest_unit_count,
        NULL AS guest_upt,
        NULL AS guest_aur,
        NULL AS guest_discount_percent,
        NULL AS planning_guest_discount_percent,
        NULL AS guest_product_gross_profit_percent,
        NULL AS planning_repeat_vip_discount_percent,
        billed_credit_cash_transaction_amount_budget AS billed_credit_cash_transaction_amount,
        billed_credit_cash_refund_chargeback_amount_budget AS billed_credit_cash_refund_chargeback_amount,
        billed_cash_credit_redeemed_amount_budget AS billed_cash_credit_redeemed_amount,
        NULL AS same_month_billed_credit_redeemed,
        COALESCE(billed_credit_cash_transaction_amount_budget, 0) - COALESCE(billed_credit_cash_refund_chargeback_amount_budget, 0) - COALESCE(billed_cash_credit_redeemed_amount_budget, 0)  AS billed_credit_net_billings,
        (COALESCE(billed_credit_cash_transaction_amount_budget, 0) - COALESCE(billed_credit_cash_refund_chargeback_amount_budget, 0) - COALESCE(billed_cash_credit_redeemed_amount_budget, 0))/NULLIFZERO(cash_net_revenue_budget) AS net_credit_billings_as_percent_of_cash_net_revenue, ----Need to modify
        NULL AS billed_credits_successful_on_first_try,
        NULL AS billed_credits_successful_on_retry,
        NULL AS billed_credit_cash_transaction_count,
        NULL AS membership_fee_cash_transaction_amount,
        NULL AS gift_card_transaction_amount,
        NULL AS legacy_credit_cash_transaction_amount,
        NULL AS other_billing_cash_transaction_amount,
        NULL AS membership_fee_cash_refund_chargeback_amount,
        NULL AS membership_fee_net_cash_transaction_amount,
        product_cost_calc_budget AS outbound_product_cost_incl_reship_exch,
        NULL AS oracle_outbound_product_cost_incl_reship_exch,
        COALESCE(activating_product_order_count_budget, 0) + COALESCE(nonactivating_product_order_count_budget, 0) + COALESCE(product_order_reship_exch_order_count_budget, 0) AS product_order_count_incl_reship_exch,
        unit_count_budget AS unit_count_incl_reship_exch,
        NULL AS product_order_return_unit_count,
        product_cost_calc_budget/NULLIFZERO(unit_count_budget) AS auc_incl_reship_exch,
        NULL AS product_cost_returned_resaleable_incl_reship_exch,
        NULL AS product_cost_returned_damaged_incl_reship_exch,
        NULL AS product_cost_net_of_returns_incl_reship_exch,
        shipping_cost_incl_reship_exch_budget AS shipped_cost_incl_reship_exch,
        shipping_supplies_cost_incl_reship_exch_budget AS shipping_supplies_cost_incl_reship_exch,
        return_shipping_costs_incl_reship_exch_budget AS return_shipping_costs_incl_reship_exch,
        misc_cogs_amount_budget AS misc_cogs_amount,
        total_cogs_budget AS total_cogs,
        NULL AS oracle_total_cogs,
        product_order_cash_credit_refund_amount_budget AS product_order_cash_credit_refund_amount,
        activating_product_order_cash_credit_refund_amount_budget AS activating_product_order_cash_credit_refund_amount,
        nonactivating_product_order_cash_credit_refund_amount_budget AS nonactivating_product_order_cash_credit_refund_amount,
        COALESCE(product_order_cash_credit_refund_amount_budget,0) - COALESCE(refund_cash_credit_redeemed_amount_budget,0) AS product_order_cash_credit_refund_net_billings,
        NULL AS gift_card_membership_fee_legacy_credit_net_billings,
        NULL AS noncash_credit_issued_amount,
        NULL AS product_order_noncash_credit_redeemed_amount,
        NULL AS pending_product_order_count,
        NULL AS pending_order_cash_transaction_amount,
        NULL AS pending_order_cash_margin_pre_return_amount,
        NULL AS skip_count,
        NULL AS skip_rate,
        NULL AS merch_purchase_rate,
        NULL AS merch_purchase_rate_hyperion,
        COALESCE(bop_vips_budget, 0) + COALESCE(new_vips_budget, 0) - COALESCE(cancels_budget, 0) AS eop_vips,
        NULL AS reactivated_vip_product_order_count,
        NULL AS reactivated_vip_product_order_count_excl_seeding,
        NULL AS reactivated_vip_product_margin_pre_return_excl_seeding,
        NULL AS nonactivating_cash_discount_percent,
        NULL AS nonactivating_non_token_unit_percent,
        NULL AS nonactivating_guest_product_order_percent
    FROM _daily_cash_final_output
    WHERE date = (
         SELECT report_date
        FROM _variables
        WHERE source = 'Report Date'
    )
),
_budget_alt AS (
    SELECT date,
        date_object,
        currency_object,
        currency_type,
        business_unit,
        store_brand,
        report_mapping,
        is_daily_cash_usd,
        is_daily_cash_eur,
        'BT_ALT' AS data_source,
        cash_gross_revenue_budget_alt AS cash_gross_revenue,
        cash_net_revenue_budget_alt AS cash_net_revenue,
        cash_gross_profit_budget_alt AS cash_gross_profit,
        cash_gross_profit_budget_alt/NULLIFZERO(cash_net_revenue_budget_alt) AS cash_gross_profit_percent_of_net_cash,
        cash_net_revenue_budget_alt AS cash_net_revenue_with_budgeted_fx_rates,
        NULL AS activating_cash_gross_revenue,
        activating_cash_net_revenue_budget_alt AS activating_cash_net_revenue,
        NULL AS nonactivating_cash_gross_revenue,
        nonactivating_cash_net_revenue_budget_alt AS nonactivating_cash_net_revenue,
        product_order_and_billing_cash_refund_budget_alt AS product_order_and_billing_cash_refund,
        product_order_and_billing_chargebacks_budget_alt AS product_order_and_billing_chargebacks,
        (COALESCE(product_order_and_billing_cash_refund_budget_alt, 0) + COALESCE(product_order_and_billing_chargebacks_budget_alt, 0))
            /NULLIFZERO(cash_gross_revenue_budget_alt) AS refund_chargebacks_as_percent_of_cash_gross_rev,
        activating_product_gross_revenue_budget_alt AS activating_product_gross_revenue,
        activating_product_net_revenue_budget_alt AS activating_product_net_revenue,
        NULL AS activating_shipping_revenue_alt,
        activating_product_order_count_budget_alt AS activating_product_order_count,
        NULL AS activating_product_order_count_excl_seeding,
        NULL AS activating_product_margin_pre_return_excl_seeding,
        activating_product_gross_revenue_budget_alt/NULLIFZERO(activating_product_order_count_budget_alt) AS activating_aov,
        activating_unit_count_budget_alt AS activating_unit_count,
        activating_unit_count_budget_alt/NULLIFZERO(activating_product_order_count_budget_alt) AS activating_upt,
        activating_product_gross_revenue_budget_alt/NULLIFZERO(activating_unit_count_budget_alt) AS activating_aur,
        NULL AS activating_auc_incl_reship_exch,
        activating_discount_percent_budget_alt AS activating_discount_percent,
        NULL AS planning_activating_discount_percent,
        activating_product_gross_profit_budget_alt AS activating_product_gross_profit,
        activating_product_gross_profit_budget_alt/NULLIFZERO(activating_product_order_count_budget_alt) AS activating_product_gross_profit_per_order,
        nonactivating_product_gross_profit_budget_alt/NULLIFZERO(nonactivating_product_order_count_budget_alt) AS activating_product_gross_profit_per_order,
        activating_product_gross_profit_budget_alt/NULLIFZERO(activating_product_net_revenue_budget_alt) AS activating_product_gross_profit_percent,
        NULL AS activating_product_order_cash_gross_revenue,
        activating_product_margin_pre_return_budget_alt AS activating_product_margin_pre_return,
        activating_product_margin_pre_return_budget_alt/NULLIFZERO(activating_product_order_count_budget) AS activating_product_margin_pre_return_per_order,
        NULL AS nonactivating_product_margin_pre_return_per_order,
        activating_product_margin_pre_return_budget_alt/NULLIFZERO(activating_product_gross_revenue_budget_alt) AS activating_product_margin_pre_return_percent,
        nonactivating_product_gross_revenue_budget_alt AS nonactivating_product_gross_revenue,
        nonactivating_product_net_revenue_budget_alt AS nonactivating_product_net_revenue,
        nonactivating_product_order_cash_gross_revenue_budget_alt/NULLIFZERO(nonactivating_product_gross_revenue_budget_alt) AS nonactivating_cash_revenue_as_percent_of_product_gross_revenue,
        NULL AS nonactivating_shipping_revenue,
        nonactivating_product_order_count_budget_alt AS nonactivating_product_order_count,
        NULL AS nonactivating_product_order_count_excl_seeding,
        NULL AS nonactivating_product_margin_pre_return_excl_seeding,
        nonactivating_product_gross_revenue_budget_alt/NULLIFZERO(nonactivating_product_order_count_budget_alt) AS nonactivating_aov,
        nonactivating_unit_count_budget_alt AS nonactivating_unit_count,
        nonactivating_unit_count_budget_alt/NULLIFZERO(nonactivating_product_order_count_budget_alt) AS nononactivating_upt,
        nonactivating_product_gross_revenue_budget_alt/NULLIFZERO(nonactivating_unit_count_budget_alt) AS nonactivating_aur,
        NULL AS nonactivating_auc_incl_reship_exch,
        nonactivating_discount_percent_budget_alt AS nonactivating_discount_percent,
        NULL AS planning_nonactivating_discount_percent,
        nonactivating_product_gross_profit_budget_alt AS nonactivating_product_gross_profit,
        nonactivating_product_gross_profit_budget_alt/NULLIFZERO(nonactivating_product_net_revenue_budget_alt) AS nonactivating_product_gross_profit_percent,
        nonactivating_product_order_cash_gross_profit_budget_alt AS nonactivating_product_order_cash_gross_profit,
        nonactivating_product_order_cash_gross_profit_budget_alt/NULLIFZERO(nonactivating_product_order_count_budget_alt) AS nonactivating_product_order_cash_gross_profit_per_order,
        nonactivating_product_order_cash_gross_revenue_budget_alt AS nonactivating_product_order_cash_gross_revenue,
        NULL AS guest_orders_as_percent_of_non_activating,
        NULL AS guest_product_net_revenue,
        NULL AS guest_product_order_count,
        NULL AS guest_product_order_count_excl_seeding,
        NULL AS guest_product_margin_pre_return_excl_seeding,
        NULL AS guest_aov,
        NULL AS guest_product_gross_profit,
        NULL AS guest_product_gross_revenue,
        NULL AS guest_shipping_revenue,
        NULL AS guest_unit_count,
        NULL AS guest_upt,
        NULL AS guest_aur,
        NULL AS guest_discount_percent,
        NULL AS planning_guest_discount_percent,
        NULL AS guest_product_gross_profit_percent,
        NULL AS planning_repeat_vip_discount_percent,
        billed_credit_cash_transaction_amount_budget_alt AS billed_credit_cash_transaction_amount,
        billed_credit_cash_refund_chargeback_amount_budget_alt AS billed_credit_cash_refund_chargeback_amount,
        billed_cash_credit_redeemed_amount_budget_alt AS billed_cash_credit_redeemed_amount,
        NULL AS same_month_billed_credit_redeemed,
        COALESCE(billed_credit_cash_transaction_amount_budget_alt, 0) - COALESCE(billed_credit_cash_refund_chargeback_amount_budget_alt, 0) - COALESCE(billed_cash_credit_redeemed_amount_budget_alt, 0)  AS billed_credit_net_billings,
        (COALESCE(billed_credit_cash_transaction_amount_budget_alt, 0) - COALESCE(billed_credit_cash_refund_chargeback_amount_budget_alt, 0) - COALESCE(billed_cash_credit_redeemed_amount_budget_alt, 0))/NULLIFZERO(cash_net_revenue_budget_alt) AS net_credit_billings_as_percent_of_cash_net_revenue, ----Need to modify
        NULL AS billed_credits_successful_on_first_try,
        NULL AS billed_credits_successful_on_retry,
        NULL AS billed_credit_cash_transaction_count,
        NULL AS membership_fee_cash_transaction_amount,
        NULL AS gift_card_transaction_amount,
        NULL AS legacy_credit_cash_transaction_amount,
        NULL AS other_billing_cash_transaction_amount,
        NULL AS membership_fee_cash_refund_chargeback_amount,
        NULL AS membership_fee_net_cash_transaction_amount,
        product_cost_calc_budget_alt AS outbound_product_cost_incl_reship_exch,
        NULL AS oracle_outbound_product_cost_incl_reship_exch,
        COALESCE(activating_product_order_count_budget_alt, 0) + COALESCE(nonactivating_product_order_count_budget_alt, 0) + COALESCE(product_order_reship_exch_order_count_budget_alt, 0) AS product_order_count_incl_reship_exch,
        unit_count_budget_alt AS unit_count_incl_reship_exch,
        NULL AS product_order_return_unit_count,
        product_cost_calc_budget_alt/NULLIFZERO(unit_count_budget_alt) AS auc_incl_reship_exch,
        NULL AS product_cost_returned_resaleable_incl_reship_exch,
        NULL AS product_cost_returned_damaged_incl_reship_exch,
        NULL AS product_cost_net_of_returns_incl_reship_exch,
        shipping_cost_incl_reship_exch_budget_alt AS shipped_cost_incl_reship_exch,
        shipping_supplies_cost_incl_reship_exch_budget_alt AS shipping_supplies_cost_incl_reship_exch,
        return_shipping_costs_incl_reship_exch_budget_alt AS return_shipping_costs_incl_reship_exch,
        misc_cogs_amount_budget_alt AS misc_cogs_amount,
        total_cogs_budget_alt AS total_cogs,
        NULL AS oracle_total_cogs,
        product_order_cash_credit_refund_amount_budget_alt AS product_order_cash_credit_refund_amount,
        activating_product_order_cash_credit_refund_amount_budget_alt AS activating_product_order_cash_credit_refund_amount,
        nonactivating_product_order_cash_credit_refund_amount_budget_alt AS nonactivating_product_order_cash_credit_refund_amount,
        COALESCE(product_order_cash_credit_refund_amount_budget_alt,0) - COALESCE(refund_cash_credit_redeemed_amount_budget_alt,0) AS product_order_cash_credit_refund_net_billings,
        NULL AS gift_card_membership_fee_legacy_credit_net_billings,
        NULL AS noncash_credit_issued_amount,
        NULL AS product_order_noncash_credit_redeemed_amount,
        NULL AS pending_product_order_count,
        NULL AS pending_order_cash_transaction_amount,
        NULL AS pending_order_cash_margin_pre_return_amount,
        NULL AS skip_count,
        NULL AS skip_rate,
        NULL AS merch_purchase_rate,
        NULL AS merch_purchase_rate_hyperion,
        COALESCE(bop_vips_budget_alt, 0) + COALESCE(new_vips_budget_alt, 0) - COALESCE(cancels_budget_alt, 0) AS eop_vips,
        NULL AS reactivated_vip_product_order_count,
        NULL AS reactivated_vip_product_order_count_excl_seeding,
        NULL AS reactivated_vip_product_margin_pre_return_excl_seeding,
        NULL AS nonactivating_cash_discount_percent,
        NULL AS nonactivating_non_token_unit_percent,
        NULL AS nonactivating_guest_product_order_percent
    FROM _daily_cash_final_output
    WHERE date = (
         SELECT report_date
        FROM _variables
        WHERE source = 'Report Date'
    )
),
_lhs AS (
    SELECT *
    FROM _run_rate

    UNION ALL

    SELECT *
    FROM _mtd
),
_rhs AS(
    SELECT *
    FROM _forecast

    UNION ALL

    SELECT *
    FROM _budget

    UNION ALL

    SELECT *
    FROM _budget_alt

    UNION ALL

    SELECT v.full_date AS date,
        date_object,
        currency_object,
        currency_type,
        business_unit,
        store_brand,
        report_mapping,
        is_daily_cash_usd,
        is_daily_cash_eur,
        IFF(v.source = 'LMMD', 'LM', 'LY') as data_source,
        cash_gross_revenue,
        cash_net_revenue,
        cash_gross_profit,
        cash_gross_profit_percent_of_net_cash,
        cash_net_revenue_with_budgeted_fx_rates,
        activating_cash_gross_revenue,
        activating_cash_net_revenue,
        nonactivating_cash_gross_revenue,
        nonactivating_cash_net_revenue,
        product_order_and_billing_cash_refund,
        product_order_and_billing_chargebacks,
        refund_chargebacks_as_percent_of_cash_gross_rev,
        activating_product_gross_revenue,
        activating_product_net_revenue,
        activating_shipping_revenue,
        activating_product_order_count,
        activating_product_order_count_excl_seeding,
        activating_product_margin_pre_return_excl_seeding,
        activating_aov,
        activating_unit_count,
        activating_upt,
        activating_aur,
        activating_auc_incl_reship_exch,
        activating_discount_percent,
        planning_activating_discount_percent,
        activating_product_gross_profit,
        activating_product_gross_profit_per_order,
        nonactivating_product_gross_profit_per_order,
        activating_product_gross_profit_percent,
        activating_product_order_cash_gross_revenue,
        activating_product_margin_pre_return,
        activating_product_margin_pre_return_per_order,
        nonactivating_product_margin_pre_return_per_order,
        activating_product_margin_pre_return_percent,
        nonactivating_product_gross_revenue,
        nonactivating_product_net_revenue,
        nonactivating_cash_revenue_as_percent_of_product_gross_revenue,
        nonactivating_shipping_revenue,
        nonactivating_product_order_count,
        nonactivating_product_order_count_excl_seeding,
        nonactivating_product_margin_pre_return_excl_seeding,
        nonactivating_aov,
        nonactivating_unit_count,
        nononactivating_upt,
        nonactivating_aur,
        nonactivating_auc_incl_reship_exch,
        nonactivating_discount_percent,
        planning_nonactivating_discount_percent,
        nonactivating_product_gross_profit,
        nonactivating_product_gross_profit_percent,
        nonactivating_product_order_cash_gross_profit,
        nonactivating_product_order_cash_gross_profit_per_order,
        nonactivating_product_order_cash_gross_revenue,
        guest_orders_as_percent_of_non_activating,
        guest_product_net_revenue,
        guest_product_order_count,
        guest_product_order_count_excl_seeding,
        guest_product_margin_pre_return_excl_seeding,
        guest_aov,
        guest_product_gross_profit,
        guest_product_gross_revenue,
        guest_shipping_revenue,
        guest_unit_count,
        guest_upt,
        guest_aur,
        guest_discount_percent,
        planning_guest_discount_percent,
        guest_product_gross_profit_percent,
        planning_repeat_vip_discount_percent,
        billed_credit_cash_transaction_amount,
        billed_credit_cash_refund_chargeback_amount,
        billed_cash_credit_redeemed_amount,
        same_month_billed_credit_redeemed,
        billed_credit_net_billings,
        net_credit_billings_as_percent_of_cash_net_revenue,
        billed_credits_successful_on_first_try,
        billed_credits_successful_on_retry,
        billed_credit_cash_transaction_count,
        membership_fee_cash_transaction_amount,
        gift_card_transaction_amount,
        legacy_credit_cash_transaction_amount,
        other_billing_cash_transaction_amount,
        membership_fee_cash_refund_chargeback_amount,
        membership_fee_net_cash_transaction_amount,
        outbound_product_cost_incl_reship_exch,
        oracle_outbound_product_cost_incl_reship_exch,
        product_order_count_incl_reship_exch,
        unit_count_incl_reship_exch,
        product_order_return_unit_count,
        auc_incl_reship_exch,
        product_cost_returned_resaleable_incl_reship_exch,
        product_cost_returned_damaged_incl_reship_exch,
        product_cost_net_of_returns_incl_reship_exch,
        shipped_cost_incl_reship_exch,
        shipping_supplies_cost_incl_reship_exch,
        return_shipping_costs_incl_reship_exch,
        misc_cogs_amount,
        total_cogs,
        oracle_total_cogs,
        product_order_cash_credit_refund_amount,
        activating_product_order_cash_credit_refund_amount,
        nonactivating_product_order_cash_credit_refund_amount,
        product_order_cash_credit_refund_net_billings,
        gift_card_membership_fee_legacy_credit_net_billings,
        noncash_credit_issued_amount,
        product_order_noncash_credit_redeemed_amount,
        pending_product_order_count,
        pending_order_cash_transaction_amount,
        pending_order_cash_margin_pre_return_amount,
        skip_count,
        skip_rate,
        merch_purchase_rate,
        merch_purchase_rate_hyperion,
        eop_vips,
        reactivated_vip_product_order_count,
        reactivated_vip_product_order_count_excl_seeding,
        reactivated_vip_product_margin_pre_return_excl_seeding,
        NULL AS nonactivating_cash_discount_percent,
        NULL AS nonactivating_non_token_unit_percent,
        NULL AS nonactivating_guest_product_order_percent
    FROM _eom e
    JOIN _variables v ON v.report_date = e.date
        AND v.source IN ('LYMD', 'LMMD')
),
_comparison_mapping AS (
    SELECT 'RR' AS lhs, 'LM' AS rhs
    UNION SELECT 'RR' AS lhs, 'LY' AS rhs
    UNION SELECT 'RR' AS lhs, 'BT' AS rhs
    UNION SELECT 'RR' AS lhs, 'BT_ALT' AS rhs
    UNION SELECT 'RR' AS lhs, 'FC' AS rhs
    UNION SELECT 'MTD' AS lhs, 'BT' AS rhs
    UNION SELECT 'MTD' AS lhs, 'BT_ALT' AS rhs
    UNION SELECT 'MTD' AS lhs, 'FC' AS rhs
),
_comparison_rate AS (
    SELECT l.date,
        l.date_object,
        l.currency_object,
        l.currency_type,
        l.business_unit,
        l.store_brand,
        l.report_mapping,
        l.is_daily_cash_usd,
        l.is_daily_cash_eur,
        l.data_source || r.data_source AS data_source,
        l.cash_gross_revenue/NULLIFZERO(r.cash_gross_revenue) AS cash_gross_revenue,
        l.cash_net_revenue/NULLIFZERO(r.cash_net_revenue) AS cash_net_revenue,
        l.cash_gross_profit/NULLIFZERO(r.cash_gross_profit) AS cash_gross_profit,
        l.cash_gross_profit_percent_of_net_cash/NULLIFZERO(r.cash_gross_profit_percent_of_net_cash) AS cash_gross_profit_percent_of_net_cash,
        l.cash_net_revenue_with_budgeted_fx_rates/NULLIFZERO(r.cash_net_revenue_with_budgeted_fx_rates) AS cash_net_revenue_with_budgeted_fx_rates,
        l.activating_cash_gross_revenue/NULLIFZERO(r.activating_cash_gross_revenue) AS activating_cash_gross_revenue,
        l.activating_cash_net_revenue/NULLIFZERO(r.activating_cash_net_revenue) AS activating_cash_net_revenue,
        l.nonactivating_cash_gross_revenue/NULLIFZERO(r.nonactivating_cash_gross_revenue) AS nonactivating_cash_gross_revenue,
        l.nonactivating_cash_net_revenue/NULLIFZERO(r.nonactivating_cash_net_revenue) AS nonactivating_cash_net_revenue,
        l.product_order_and_billing_cash_refund/NULLIFZERO(r.product_order_and_billing_cash_refund) AS product_order_and_billing_cash_refund,
        l.product_order_and_billing_chargebacks/NULLIFZERO(r.product_order_and_billing_chargebacks) AS product_order_and_billing_chargebacks,
        l.refund_chargebacks_as_percent_of_cash_gross_rev/NULLIFZERO(r.refund_chargebacks_as_percent_of_cash_gross_rev) AS refund_chargebacks_as_percent_of_cash_gross_rev,
        l.activating_product_gross_revenue/NULLIFZERO(r.activating_product_gross_revenue) AS activating_product_gross_revenue,
        l.activating_product_net_revenue/NULLIFZERO(r.activating_product_net_revenue) AS activating_product_net_revenue,
        l.activating_shipping_revenue/NULLIFZERO(r.activating_shipping_revenue) AS activating_shipping_revenue,
        l.activating_product_order_count/NULLIFZERO(r.activating_product_order_count) AS activating_product_order_count,
        l.activating_product_order_count_excl_seeding/NULLIFZERO(r.activating_product_order_count_excl_seeding) AS activating_product_order_count_excl_seeding,
        l.activating_product_margin_pre_return_excl_seeding/NULLIFZERO(r.activating_product_margin_pre_return_excl_seeding) AS activating_product_margin_pre_return_excl_seeding,
        l.activating_aov/NULLIFZERO(r.activating_aov) AS activating_aov,
        l.activating_unit_count/NULLIFZERO(r.activating_unit_count) AS activating_unit_count,
        l.activating_upt/NULLIFZERO(r.activating_upt) AS activating_upt,
        l.activating_aur/NULLIFZERO(r.activating_aur) AS activating_aur,
        l.activating_auc_incl_reship_exch/NULLIFZERO(r.activating_auc_incl_reship_exch) AS activating_auc_incl_reship_exch,
        l.activating_discount_percent/NULLIFZERO(r.activating_discount_percent) AS activating_discount_percent,
        l.planning_activating_discount_percent/NULLIFZERO(r.planning_activating_discount_percent) AS planning_activating_discount_percent,
        l.activating_product_gross_profit/NULLIFZERO(r.activating_product_gross_profit) AS activating_product_gross_profit,
        l.activating_product_gross_profit_per_order/NULLIFZERO(r.activating_product_gross_profit_per_order) AS activating_product_gross_profit_per_order,
        l.nonactivating_product_gross_profit_per_order/NULLIFZERO(r.nonactivating_product_gross_profit_per_order) AS nonactivating_product_gross_profit_per_order,
        l.activating_product_gross_profit_percent/NULLIFZERO(r.activating_product_gross_profit_percent) AS activating_product_gross_profit_percent,
        l.activating_product_order_cash_gross_revenue/NULLIFZERO(r.activating_product_order_cash_gross_revenue) AS activating_product_order_cash_gross_revenue,
        l.activating_product_margin_pre_return/NULLIFZERO(r.activating_product_margin_pre_return) AS activating_product_margin_pre_return,
        l.activating_product_margin_pre_return_per_order/NULLIFZERO(r.activating_product_margin_pre_return_per_order) AS activating_product_margin_pre_return_per_order,
        l.nonactivating_product_margin_pre_return_per_order/NULLIFZERO(r.nonactivating_product_margin_pre_return_per_order) AS nonactivating_product_margin_pre_return_per_order,
        l.activating_product_margin_pre_return_percent/NULLIFZERO(r.activating_product_margin_pre_return_percent) AS activating_product_margin_pre_return_percent,
        l.nonactivating_product_gross_revenue/NULLIFZERO(r.nonactivating_product_gross_revenue) AS nonactivating_product_gross_revenue,
        l.nonactivating_product_net_revenue/NULLIFZERO(r.nonactivating_product_net_revenue) AS nonactivating_product_net_revenue,
        l.nonactivating_cash_revenue_as_percent_of_product_gross_revenue/NULLIFZERO(r.nonactivating_cash_revenue_as_percent_of_product_gross_revenue) AS nonactivating_cash_revenue_as_percent_of_product_gross_revenue,
        l.nonactivating_shipping_revenue/NULLIFZERO(r.nonactivating_shipping_revenue) AS nonactivating_shipping_revenue,
        l.nonactivating_product_order_count/NULLIFZERO(r.nonactivating_product_order_count) AS nonactivating_product_order_count,
        l.nonactivating_product_order_count_excl_seeding/NULLIFZERO(r.nonactivating_product_order_count_excl_seeding) AS nonactivating_product_order_count_excl_seeding,
        l.nonactivating_product_margin_pre_return_excl_seeding/NULLIFZERO(r.nonactivating_product_margin_pre_return_excl_seeding) AS nonactivating_product_margin_pre_return_excl_seeding,
        l.nonactivating_aov/NULLIFZERO(r.nonactivating_aov) AS nonactivating_aov,
        l.nonactivating_unit_count/NULLIFZERO(r.nonactivating_unit_count) AS nonactivating_unit_count,
        l.nononactivating_upt/NULLIFZERO(r.nononactivating_upt) AS nononactivating_upt,
        l.nonactivating_aur/NULLIFZERO(r.nonactivating_aur) AS nonactivating_aur,
        l.nonactivating_auc_incl_reship_exch/NULLIFZERO(r.nonactivating_auc_incl_reship_exch) AS nonactivating_auc_incl_reship_exch,
        l.nonactivating_discount_percent/NULLIFZERO(r.nonactivating_discount_percent) AS nonactivating_discount_percent,
        l.planning_nonactivating_discount_percent/NULLIFZERO(r.planning_nonactivating_discount_percent) AS planning_nonactivating_discount_percent,
        l.nonactivating_product_gross_profit/NULLIFZERO(r.nonactivating_product_gross_profit) AS nonactivating_product_gross_profit,
        l.nonactivating_product_gross_profit_percent/NULLIFZERO(r.nonactivating_product_gross_profit_percent) AS nonactivating_product_gross_profit_percent,
        l.nonactivating_product_order_cash_gross_profit/NULLIFZERO(r.nonactivating_product_order_cash_gross_profit) AS nonactivating_product_order_cash_gross_profit,
        l.nonactivating_product_order_cash_gross_profit_per_order/NULLIFZERO(r.nonactivating_product_order_cash_gross_profit_per_order) AS nonactivating_product_order_cash_gross_profit_per_order,
        l.nonactivating_product_order_cash_gross_revenue/NULLIFZERO(r.nonactivating_product_order_cash_gross_revenue) AS nonactivating_product_order_cash_gross_revenue,
        l.guest_orders_as_percent_of_non_activating/NULLIFZERO(r.guest_orders_as_percent_of_non_activating) AS guest_orders_as_percent_of_non_activating,
        l.guest_product_net_revenue/NULLIFZERO(r.guest_product_net_revenue) AS guest_product_net_revenue,
        l.guest_product_order_count/NULLIFZERO(r.guest_product_order_count) AS guest_product_order_count,
        l.guest_product_order_count_excl_seeding/NULLIFZERO(r.guest_product_order_count_excl_seeding) AS guest_product_order_count_excl_seeding,
        l.guest_product_margin_pre_return_excl_seeding/NULLIFZERO(r.guest_product_margin_pre_return_excl_seeding) AS guest_product_margin_pre_return_excl_seeding,
        l.guest_aov/NULLIFZERO(r.guest_aov) AS guest_aov,
        l.guest_product_gross_profit/NULLIFZERO(r.guest_product_gross_profit) AS guest_product_gross_profit,
        l.guest_product_gross_revenue/NULLIFZERO(r.guest_product_gross_revenue) AS guest_product_gross_revenue,
        l.guest_shipping_revenue/NULLIFZERO(r.guest_shipping_revenue) AS guest_shipping_revenue,
        l.guest_unit_count/NULLIFZERO(r.guest_unit_count) AS guest_unit_count,
        l.guest_upt/NULLIFZERO(r.guest_upt) AS guest_upt,
        l.guest_aur/NULLIFZERO(r.guest_aur) AS guest_aur,
        l.guest_discount_percent/NULLIFZERO(r.guest_discount_percent) AS guest_discount_percent,
        l.planning_guest_discount_percent/NULLIFZERO(r.planning_guest_discount_percent) AS planning_guest_discount_percent,
        l.guest_product_gross_profit_percent/NULLIFZERO(r.guest_product_gross_profit_percent) AS guest_product_gross_profit_percent,
        l.planning_repeat_vip_discount_percent/NULLIFZERO(r.planning_repeat_vip_discount_percent) AS planning_repeat_vip_discount_percent,
        l.billed_credit_cash_transaction_amount/NULLIFZERO(r.billed_credit_cash_transaction_amount) AS billed_credit_cash_transaction_amount,
        l.billed_credit_cash_refund_chargeback_amount/NULLIFZERO(r.billed_credit_cash_refund_chargeback_amount) AS billed_credit_cash_refund_chargeback_amount,
        l.billed_cash_credit_redeemed_amount/NULLIFZERO(r.billed_cash_credit_redeemed_amount) AS billed_cash_credit_redeemed_amount,
        l.same_month_billed_credit_redeemed/NULLIFZERO(r.same_month_billed_credit_redeemed) AS same_month_billed_credit_redeemed,
        l.billed_credit_net_billings/NULLIFZERO(r.billed_credit_net_billings) AS billed_credit_net_billings,
        l.net_credit_billings_as_percent_of_cash_net_revenue/NULLIFZERO(r.net_credit_billings_as_percent_of_cash_net_revenue) AS net_credit_billings_as_percent_of_cash_net_revenue,
        l.billed_credits_successful_on_first_try/NULLIFZERO(r.billed_credits_successful_on_first_try) AS billed_credits_successful_on_first_try,
        l.billed_credits_successful_on_retry/NULLIFZERO(r.billed_credits_successful_on_retry) AS billed_credits_successful_on_retry,
        l.billed_credit_cash_transaction_count/NULLIFZERO(r.billed_credit_cash_transaction_count) AS billed_credit_cash_transaction_count,
        l.membership_fee_cash_transaction_amount/NULLIFZERO(r.membership_fee_cash_transaction_amount) AS membership_fee_cash_transaction_amount,
        l.gift_card_transaction_amount/NULLIFZERO(r.gift_card_transaction_amount) AS gift_card_transaction_amount,
        l.legacy_credit_cash_transaction_amount/NULLIFZERO(r.legacy_credit_cash_transaction_amount) AS legacy_credit_cash_transaction_amount,
        l.other_billing_cash_transaction_amount/NULLIFZERO(r.other_billing_cash_transaction_amount) AS other_billing_cash_transaction_amount,
        l.membership_fee_cash_refund_chargeback_amount/NULLIFZERO(r.membership_fee_cash_refund_chargeback_amount) AS membership_fee_cash_refund_chargeback_amount,
        l.membership_fee_net_cash_transaction_amount/NULLIFZERO(r.membership_fee_net_cash_transaction_amount) AS membership_fee_net_cash_transaction_amount,
        l.outbound_product_cost_incl_reship_exch/NULLIFZERO(r.outbound_product_cost_incl_reship_exch) AS outbound_product_cost_incl_reship_exch,
        l.oracle_outbound_product_cost_incl_reship_exch/NULLIFZERO(r.oracle_outbound_product_cost_incl_reship_exch) AS oracle_outbound_product_cost_incl_reship_exch,
        l.product_order_count_incl_reship_exch/NULLIFZERO(r.product_order_count_incl_reship_exch) AS product_order_count_incl_reship_exch,
        l.unit_count_incl_reship_exch/NULLIFZERO(r.unit_count_incl_reship_exch) AS unit_count_incl_reship_exch,
        l.product_order_return_unit_count/NULLIFZERO(r.product_order_return_unit_count) AS product_order_return_unit_count,
        l.auc_incl_reship_exch/NULLIFZERO(r.auc_incl_reship_exch) AS auc_incl_reship_exch,
        l.product_cost_returned_resaleable_incl_reship_exch/NULLIFZERO(r.product_cost_returned_resaleable_incl_reship_exch) AS product_cost_returned_resaleable_incl_reship_exch,
        l.product_cost_returned_damaged_incl_reship_exch/NULLIFZERO(r.product_cost_returned_damaged_incl_reship_exch) AS product_cost_returned_damaged_incl_reship_exch,
        l.product_cost_net_of_returns_incl_reship_exch/NULLIFZERO(r.product_cost_net_of_returns_incl_reship_exch) AS product_cost_net_of_returns_incl_reship_exch,
        l.shipped_cost_incl_reship_exch/NULLIFZERO(r.shipped_cost_incl_reship_exch) AS shipped_cost_incl_reship_exch,
        l.shipping_supplies_cost_incl_reship_exch/NULLIFZERO(r.shipping_supplies_cost_incl_reship_exch) AS shipping_supplies_cost_incl_reship_exch,
        l.return_shipping_costs_incl_reship_exch/NULLIFZERO(r.return_shipping_costs_incl_reship_exch) AS return_shipping_costs_incl_reship_exch,
        l.misc_cogs_amount/NULLIFZERO(r.misc_cogs_amount) AS misc_cogs_amount,
        l.total_cogs/NULLIFZERO(r.total_cogs) AS total_cogs,
        l.oracle_total_cogs/NULLIFZERO(r.oracle_total_cogs) AS oracle_total_cogs,
        l.product_order_cash_credit_refund_amount/NULLIFZERO(r.product_order_cash_credit_refund_amount) AS product_order_cash_credit_refund_amount,
        l.activating_product_order_cash_credit_refund_amount/NULLIFZERO(r.activating_product_order_cash_credit_refund_amount) AS activating_product_order_cash_credit_refund_amount,
        l.nonactivating_product_order_cash_credit_refund_amount/NULLIFZERO(r.nonactivating_product_order_cash_credit_refund_amount) AS nonactivating_product_order_cash_credit_refund_amount,
        l.product_order_cash_credit_refund_net_billings/NULLIFZERO(r.product_order_cash_credit_refund_net_billings) AS product_order_cash_credit_refund_net_billings,
        l.gift_card_membership_fee_legacy_credit_net_billings/NULLIFZERO(r.gift_card_membership_fee_legacy_credit_net_billings) AS gift_card_membership_fee_legacy_credit_net_billings,
        l.noncash_credit_issued_amount/NULLIFZERO(r.noncash_credit_issued_amount) AS noncash_credit_issued_amount,
        l.product_order_noncash_credit_redeemed_amount/NULLIFZERO(r.product_order_noncash_credit_redeemed_amount) AS product_order_noncash_credit_redeemed_amount,
        l.pending_product_order_count/NULLIFZERO(r.pending_product_order_count) AS pending_product_order_count,
        l.pending_order_cash_transaction_amount/NULLIFZERO(r.pending_order_cash_transaction_amount) AS pending_order_cash_transaction_amount,
        l.pending_order_cash_margin_pre_return_amount/NULLIFZERO(r.pending_order_cash_margin_pre_return_amount) AS pending_order_cash_margin_pre_return_amount,
        l.skip_count/NULLIFZERO(r.skip_count) AS skip_count,
        l.skip_rate/NULLIFZERO(r.skip_rate) AS skip_rate,
        l.merch_purchase_rate/NULLIFZERO(r.merch_purchase_rate) AS merch_purchase_rate,
        l.merch_purchase_rate_hyperion/NULLIFZERO(r.merch_purchase_rate_hyperion) AS merch_purchase_rate_hyperion,
        l.eop_vips/NULLIFZERO(r.eop_vips) AS eop_vips,
        l.reactivated_vip_product_order_count/NULLIFZERO(r.reactivated_vip_product_order_count) AS reactivated_vip_product_order_count,
        l.reactivated_vip_product_order_count_excl_seeding/NULLIFZERO(r.reactivated_vip_product_order_count_excl_seeding) AS reactivated_vip_product_order_count_excl_seeding,
        l.reactivated_vip_product_margin_pre_return_excl_seeding/NULLIFZERO(r.reactivated_vip_product_margin_pre_return_excl_seeding) AS reactivated_vip_product_margin_pre_return_excl_seeding,
        l.nonactivating_cash_discount_percent/NULLIFZERO(r.nonactivating_cash_discount_percent) AS nonactivating_cash_discount_percent,
        l.nonactivating_non_token_unit_percent/NULLIFZERO(r.nonactivating_non_token_unit_percent) AS nonactivating_product_order_non_token_unit_count,
        l.nonactivating_guest_product_order_percent/NULLIFZERO(r.nonactivating_guest_product_order_percent) AS nonactivating_guest_product_order_percent
    FROM _lhs l
    JOIN _comparison_mapping cm ON cm.lhs = l.data_source
    JOIN _rhs r ON r.date = l.date
        AND r.date_object = l.date_object
        AND r.currency_object = l.currency_object
        AND r.currency_type = l.currency_type
        AND r.business_unit = l.business_unit
        AND r.store_brand = l.store_brand
        AND r.report_mapping = l.report_mapping
        AND r.is_daily_cash_usd = l.is_daily_cash_usd
        AND r.is_daily_cash_eur = l.is_daily_cash_eur
        AND r.data_source = cm.rhs
),
_output AS (
    SELECT *
    FROM _daily

    UNION ALL

    SELECT * FROM _mtd

    UNION ALL

    SELECT * FROM _eom

    UNION ALL

    SELECT * FROM _run_rate

    UNION ALL

    SELECT * FROM _forecast

    UNION ALL

    SELECT * FROM _budget

    UNION ALL

    SELECT * FROM _budget_alt

    UNION ALL

    SELECT * FROM _comparison_rate
)
SELECT o.*, master_rank, set_rank
FROM _output o
LEFT JOIN reference.finance_segment_mapping_rank fsmr ON fsmr.report_mapping = o.report_mapping
    AND fsmr.report_version = 'Daily Cash';
