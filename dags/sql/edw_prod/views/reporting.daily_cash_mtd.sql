CREATE OR REPLACE VIEW reporting.daily_cash_mtd AS
    SELECT
/*Daily Cash Version*/
        dcbc.date
        ,dcbc.date_object
        ,dcbc.currency_object
        ,dcbc.currency_type
/*Segment*/
        ,dcbc.report_mapping
/*Measure Filter*/
        ,dcbc.order_membership_classification_key
/*Measures*/
        ,SUM(dcbc.product_order_subtotal_excl_tariff_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_subtotal_excl_tariff_amount_mtd
        ,SUM(dcbc.product_order_product_discount_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_product_discount_amount_mtd
        ,SUM(dcbc.product_order_shipping_revenue_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_shipping_revenue_amount_mtd
        ,SUM(dcbc.product_order_noncash_credit_redeemed_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_noncash_credit_redeemed_amount_mtd
        ,SUM(dcbc.product_order_count)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_count_mtd
        ,SUM(dcbc.product_order_count_excl_seeding)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_count_excl_seeding_mtd
        ,SUM(dcbc.product_margin_pre_return_excl_seeding)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_margin_pre_return_excl_seeding_mtd
        ,SUM(dcbc.product_order_unit_count)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_unit_count_mtd
        ,SUM(dcbc.product_order_air_vip_price)
            OVER(PARTITION BY
                date_trunc('month', dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_air_vip_price_mtd
        ,SUM(dcbc.product_order_retail_unit_price)
            OVER(PARTITION BY
                date_trunc('month', dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_retail_unit_price_mtd
        ,SUM(dcbc.product_order_price_offered_amount)
            OVER(PARTITION BY
                date_trunc('month', dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_price_offered_amount_mtd
        ,SUM(dcbc.product_order_landed_product_cost_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_landed_product_cost_amount_mtd
        ,SUM(dcbc.oracle_product_order_landed_product_cost_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS oracle_product_order_landed_product_cost_amount_mtd
        ,SUM(dcbc.product_order_shipping_cost_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_shipping_cost_amount_mtd
        ,SUM(dcbc.product_order_shipping_supplies_cost_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_shipping_supplies_cost_amount_mtd
        ,SUM(dcbc.product_order_direct_cogs_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_direct_cogs_amount_mtd
        ,SUM(dcbc.product_order_cash_refund_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_cash_refund_amount_mtd
        ,SUM(dcbc.product_order_cash_chargeback_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_cash_chargeback_amount_mtd
        ,SUM(dcbc.product_order_cost_product_returned_resaleable_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_cost_product_returned_resaleable_amount_mtd
        ,SUM(dcbc.product_order_cost_product_returned_damaged_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_cost_product_returned_damaged_amount_mtd
        ,SUM(dcbc.product_order_return_shipping_cost_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_return_shipping_cost_amount_mtd
        ,SUM(dcbc.product_order_reship_order_count)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_reship_order_count_mtd
        ,SUM(dcbc.product_order_reship_unit_count)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_reship_unit_count_mtd
        ,SUM(dcbc.product_order_reship_direct_cogs_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_reship_direct_cogs_amount_mtd
        ,SUM(dcbc.product_order_reship_product_cost_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_reship_product_cost_amount_mtd
        ,SUM(dcbc.oracle_product_order_reship_product_cost_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS oracle_product_order_reship_product_cost_amount_mtd
        ,SUM(dcbc.product_order_reship_shipping_cost_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_reship_shipping_cost_amount_mtd
        ,SUM(dcbc.product_order_reship_shipping_supplies_cost_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_reship_shipping_supplies_cost_amount_mtd
        ,SUM(dcbc.product_order_exchange_order_count)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_exchange_order_count_mtd
        ,SUM(dcbc.product_order_exchange_unit_count)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_exchange_unit_count_mtd
        ,SUM(dcbc.product_order_exchange_product_cost_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_exchange_product_cost_amount_mtd
        ,SUM(dcbc.oracle_product_order_exchange_product_cost_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS oracle_product_order_exchange_product_cost_amount_mtd
        ,SUM(dcbc.product_order_exchange_shipping_cost_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_exchange_shipping_cost_amount_mtd
        ,SUM(dcbc.product_order_exchange_shipping_supplies_cost_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_exchange_shipping_supplies_cost_amount_mtd
        ,SUM(dcbc.product_gross_revenue)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_gross_revenue_mtd
        ,SUM(dcbc.product_gross_revenue_excl_shipping)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_gross_revenue_excl_shipping_mtd
        ,SUM(dcbc.product_net_revenue)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_net_revenue_mtd
        ,SUM(dcbc.product_margin_pre_return)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_margin_pre_return_mtd
        ,SUM(dcbc.product_gross_profit)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_gross_profit_mtd
        ,SUM(dcbc.product_order_cash_gross_revenue_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_cash_gross_revenue_amount_mtd
        ,SUM(dcbc.cash_gross_revenue)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS cash_gross_revenue_mtd
        ,SUM(dcbc.cash_net_revenue)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS cash_net_revenue_mtd
        ,SUM(dcbc.cash_net_revenue_budgeted_fx)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS cash_net_revenue_budgeted_fx_mtd
        ,SUM(dcbc.cash_gross_profit)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS cash_gross_profit_mtd
        ,SUM(dcbc.billed_credit_cash_transaction_count)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS billed_credit_cash_transaction_count_mtd
        ,SUM(dcbc.on_retry_billed_credit_cash_transaction_count)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS on_retry_billed_credit_cash_transaction_count_mtd
        ,SUM(dcbc.billing_cash_refund_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS billing_cash_refund_amount_mtd
        ,SUM(dcbc.billing_cash_chargeback_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS billing_cash_chargeback_amount_mtd
        ,SUM(dcbc.billed_credit_cash_transaction_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS billed_credit_cash_transaction_amount_mtd
        ,SUM(dcbc.billed_credit_cash_refund_chargeback_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS billed_credit_cash_refund_chargeback_amount_mtd
        ,SUM(dcbc.billed_cash_credit_redeemed_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS billed_cash_credit_redeemed_amount_mtd
        ,SUM(dcbc.product_order_misc_cogs_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_misc_cogs_amount_mtd
        ,SUM(dcbc.product_order_cash_gross_profit)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_cash_gross_profit_mtd
        ,SUM(dcbc.billed_cash_credit_redeemed_same_month_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS billed_cash_credit_redeemed_same_month_amount_mtd
        ,SUM(dcbc.billing_order_transaction_count)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS billing_order_transaction_count_mtd
        ,SUM(dcbc.membership_fee_cash_transaction_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS membership_fee_cash_transaction_amount_mtd
        ,SUM(dcbc.gift_card_transaction_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS gift_card_transaction_amount_mtd
        ,SUM(dcbc.legacy_credit_cash_transaction_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS legacy_credit_cash_transaction_amount_mtd
        ,SUM(dcbc.membership_fee_cash_refund_chargeback_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS membership_fee_cash_refund_chargeback_amount_mtd
        ,SUM(dcbc.billed_cash_credit_issued_equivalent_count)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS billed_cash_credit_issued_equivalent_count_mtd
        ,SUM(dcbc.refund_cash_credit_issued_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS refund_cash_credit_issued_amount_mtd
        ,SUM(dcbc.refund_cash_credit_redeemed_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS refund_cash_credit_redeemed_amount_mtd
        ,SUM(dcbc.product_order_cash_credit_refund_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_cash_credit_refund_amount_mtd
        ,SUM(dcbc.product_order_noncash_credit_refund_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_noncash_credit_refund_amount_mtd
        ,SUM(dcbc.refund_cash_credit_cancelled_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS refund_cash_credit_cancelled_amount_mtd
        ,SUM(dcbc.billed_credit_cash_refund_count)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS billed_credit_cash_refund_count_mtd
        ,SUM(dcbc.cash_variable_contribution_profit)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS cash_variable_contribution_profit_mtd
        ,SUM(dcbc.billed_cash_credit_redeemed_equivalent_count)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS billed_cash_credit_redeemed_equivalent_count_mtd
        ,SUM(dcbc.billed_cash_credit_cancelled_equivalent_count)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS billed_cash_credit_cancelled_equivalent_count_mtd
        ,SUM(dcbc.product_order_product_subtotal_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_product_subtotal_amount_mtd
        ,SUM(dcbc.leads)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS leads_mtd
        ,SUM(dcbc.primary_leads)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS primary_leads_mtd
        ,SUM(dcbc.reactivated_leads)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS reactivated_leads_mtd
        ,SUM(dcbc.new_vips)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS new_vips_mtd
        ,SUM(dcbc.reactivated_vips)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS reactivated_vips_mtd
        ,SUM(dcbc.vips_from_reactivated_leads_m1)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS vips_from_reactivated_leads_m1_mtd
        ,SUM(dcbc.paid_vips)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS paid_vips_mtd
        ,SUM(dcbc.unpaid_vips)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS unpaid_vips_mtd
        ,SUM(dcbc.new_vips_m1)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS new_vips_m1_mtd
        ,SUM(dcbc.paid_vips_m1)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS paid_vips_m1_mtd
        ,SUM(dcbc.cancels)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS cancels_mtd
        ,SUM(dcbc.m1_cancels)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS m1_cancels_mtd
        ,first_value(dcbc.bop_vips)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS bop_vips_mtd
        ,SUM(dcbc.media_spend)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS media_spend_mtd
        ,SUM(dcbc.product_order_return_unit_count)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_return_unit_count_mtd
        ,SUM(dcbc.merch_purchase_count)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS merch_purchase_count_mtd
        ,SUM(dcbc.merch_purchase_hyperion_count)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS merch_purchase_hyperion_count_mtd
        ,SUM(dcbc.other_cash_credit_redeemed_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS other_cash_credit_redeemed_amount_mtd
        ,SUM(dcbc.gift_card_cash_refund_chargeback_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS gift_card_cash_refund_chargeback_amount_mtd
        ,SUM(dcbc.legacy_credit_cash_refund_chargeback_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS legacy_credit_cash_refund_chargeback_amount_mtd
        ,SUM(dcbc.noncash_credit_issued_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS noncash_credit_issued_amount_mtd
        ,SUM(dcbc.product_order_non_token_subtotal_excl_tariff_amount)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_non_token_subtotal_excl_tariff_amount_mtd
        ,SUM(dcbc.product_order_non_token_unit_count)
            OVER(PARTITION BY
                date_trunc('month',dcbc.date)
                ,dcbc.date_object
                ,dcbc.currency_object
                ,dcbc.currency_type
                ,dcbc.report_mapping
                ,dcbc.order_membership_classification_key
            ORDER BY
                dcbc.date ASC
            ) AS product_order_non_token_unit_count_mtd
        ,dcbc.meta_update_datetime AS meta_update_datetime
        ,dcbc.meta_create_datetime AS meta_create_datetime
    FROM reporting.daily_cash_base_calc AS dcbc
;
