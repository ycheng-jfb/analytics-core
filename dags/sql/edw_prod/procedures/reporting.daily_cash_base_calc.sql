SET execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);
--SET target_table = 'reporting.daily_cash_base_calc';
--/* if we did a full refresh on fso, we want to do a full refresh on daily cash */
--SET fso_full_refresh = (SELECT IFF(high_watermark_datetime::date = '1900-01-01', TRUE, FALSE)
--                        FROM (
--                                    SELECT high_watermark_datetime
--                                    FROM edw_prod.stg.meta_table_dependency_watermark AT(OFFSET => -60*60) -- hardcoding edw_prod so this doesn't fail on test cluster.  No TT on edw_Dev
--                                    WHERE table_name = 'analytics_base.finance_sales_ops_stg'
--                                    AND dependent_table_name IS NULL
--                        ));
--SET is_watermark_full_refresh = (SELECT IFF(edw_prod.stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));
--SET is_full_refresh = CASE
--                        WHEN DAYOFWEEK(CURRENT_DATE) = 0
--                                 OR DAYOFMONTH(CURRENT_DATE) = 1
--                                 OR $fso_full_refresh = TRUE
--                                 OR $is_watermark_full_refresh = TRUE
--                            THEN TRUE
--                        ELSE FALSE
--                      END;
--SET refresh_from_date = (SELECT IFF($is_full_refresh = FALSE, DATE_TRUNC(MONTH, DATEADD(YEAR, -2, CURRENT_DATE)), '1900-01-01'));

CREATE OR REPLACE TEMPORARY TABLE _fso_agg AS
    SELECT
/*Daily Cash Version*/
        fso.date
        ,fso.date_object
        ,fso.currency_object
        ,fso.currency_type
/*Segment*/
        ,fso.store_id AS event_store_id
        ,fso.vip_store_id
        ,fso.is_retail_vip
        ,fso.gender AS customer_gender
        ,fso.is_cross_promo
        ,CASE WHEN fso.finance_specialty_store IS NULL THEN 'None' ELSE fso.finance_specialty_store END AS finance_specialty_store
        ,fso.is_scrubs_customer
/*Measure Filter*/
        ,fso.order_membership_classification_key
/*Measures*/
         ,SUM(fso.product_order_subtotal_excl_tariff_amount) AS product_order_subtotal_excl_tariff_amount
        ,SUM(fso.product_order_product_discount_amount) AS product_order_product_discount_amount
        ,SUM(fso.product_order_shipping_revenue_amount) AS product_order_shipping_revenue_amount
        ,SUM(fso.product_order_noncash_credit_redeemed_amount) AS product_order_noncash_credit_redeemed_amount
        ,SUM(fso.product_order_count) AS product_order_count
        ,SUM(fso.product_order_count_excl_seeding) AS product_order_count_excl_seeding
        ,SUM(fso.product_margin_pre_return_excl_seeding) AS product_margin_pre_return_excl_seeding
        ,SUM(fso.product_order_unit_count) AS product_order_unit_count
        ,SUM(fso.product_order_air_vip_price) AS product_order_air_vip_price
        ,SUM(fso.product_order_retail_unit_price) AS product_order_retail_unit_price
        ,SUM(fso.product_order_price_offered_amount) AS product_order_price_offered_amount
        ,SUM(fso.product_order_landed_product_cost_amount) AS product_order_landed_product_cost_amount
        ,SUM(fso.product_order_landed_product_cost_amount_accounting) AS product_order_landed_product_cost_amount_accounting
        ,SUM(fso.oracle_product_order_landed_product_cost_amount) AS oracle_product_order_landed_product_cost_amount
        ,SUM(fso.product_order_shipping_cost_amount) AS product_order_shipping_cost_amount
        ,SUM(fso.product_order_shipping_supplies_cost_amount) AS product_order_shipping_supplies_cost_amount
        ,SUM(fso.product_order_direct_cogs_amount) AS product_order_direct_cogs_amount
        ,SUM(fso.product_order_cash_refund_amount) AS product_order_cash_refund_amount
        ,SUM(fso.product_order_cash_chargeback_amount) AS product_order_cash_chargeback_amount
        ,SUM(fso.product_order_cost_product_returned_resaleable_amount) AS product_order_cost_product_returned_resaleable_amount
        ,SUM(fso.product_order_cost_product_returned_resaleable_amount_accounting) AS product_order_cost_product_returned_resaleable_amount_accounting
        ,SUM(fso.product_order_cost_product_returned_damaged_amount) AS product_order_cost_product_returned_damaged_amount
        ,SUM(fso.product_order_return_shipping_cost_amount) AS product_order_return_shipping_cost_amount
        ,SUM(fso.product_order_reship_order_count) AS product_order_reship_order_count
        ,SUM(fso.product_order_reship_unit_count) AS product_order_reship_unit_count
        ,SUM(fso.product_order_reship_direct_cogs_amount) AS product_order_reship_direct_cogs_amount
        ,SUM(fso.product_order_reship_product_cost_amount) AS product_order_reship_product_cost_amount
        ,SUM(fso.product_order_reship_product_cost_amount_accounting) AS product_order_reship_product_cost_amount_accounting
        ,SUM(fso.oracle_product_order_reship_product_cost_amount) AS oracle_product_order_reship_product_cost_amount
        ,SUM(fso.product_order_reship_shipping_cost_amount) AS product_order_reship_shipping_cost_amount
        ,SUM(fso.product_order_reship_shipping_supplies_cost_amount) AS product_order_reship_shipping_supplies_cost_amount
        ,SUM(fso.product_order_exchange_order_count) AS product_order_exchange_order_count
        ,SUM(fso.product_order_exchange_unit_count) AS product_order_exchange_unit_count
        ,SUM(fso.product_order_exchange_product_cost_amount) AS product_order_exchange_product_cost_amount
        ,SUM(fso.product_order_exchange_product_cost_amount_accounting) AS product_order_exchange_product_cost_amount_accounting
        ,SUM(fso.oracle_product_order_exchange_product_cost_amount) AS oracle_product_order_exchange_product_cost_amount
        ,SUM(fso.product_order_exchange_shipping_cost_amount) AS product_order_exchange_shipping_cost_amount
        ,SUM(fso.product_order_exchange_shipping_supplies_cost_amount) AS product_order_exchange_shipping_supplies_cost_amount
        ,SUM(fso.product_gross_revenue) AS product_gross_revenue
        ,SUM(fso.product_net_revenue) AS product_net_revenue
        ,SUM(fso.product_margin_pre_return) AS product_margin_pre_return
        ,SUM(fso.product_gross_profit) AS product_gross_profit
        ,SUM(fso.product_order_cash_gross_revenue_amount) AS product_order_cash_gross_revenue_amount
        ,SUM(fso.cash_gross_revenue) AS cash_gross_revenue
        ,SUM(fso.cash_net_revenue) AS cash_net_revenue
        ,SUM(fso.cash_gross_profit) AS cash_gross_profit
        ,SUM(fso.billed_credit_cash_transaction_count) AS billed_credit_cash_transaction_count
        ,SUM(fso.on_retry_billed_credit_cash_transaction_count) AS on_retry_billed_credit_cash_transaction_count
        ,SUM(fso.billing_cash_refund_amount) AS billing_cash_refund_amount
        ,SUM(fso.billing_cash_chargeback_amount) AS billing_cash_chargeback_amount
        ,SUM(fso.billed_credit_cash_transaction_amount) AS billed_credit_cash_transaction_amount
        ,SUM(fso.billed_credit_cash_refund_chargeback_amount) AS billed_credit_cash_refund_chargeback_amount
        ,SUM(fso.billed_cash_credit_redeemed_amount) AS billed_cash_credit_redeemed_amount
        ,SUM(fso.product_order_misc_cogs_amount) AS product_order_misc_cogs_amount
        ,SUM(fso.product_order_cash_gross_profit) AS product_order_cash_gross_profit
        ,SUM(fso.billed_cash_credit_redeemed_same_month_amount) AS billed_cash_credit_redeemed_same_month_amount
        ,SUM(fso.product_order_tariff_amount) AS product_order_tariff_amount
        ,SUM(fso.product_order_product_subtotal_amount) AS product_order_product_subtotal_amount
        ,SUM(fso.product_order_shipping_discount_amount) AS product_order_shipping_discount_amount
        ,SUM(fso.product_order_shipping_revenue_before_discount_amount) AS product_order_shipping_revenue_before_discount_amount
        ,SUM(fso.product_order_tax_amount) AS product_order_tax_amount
        ,SUM(fso.product_order_cash_transaction_amount) AS product_order_cash_transaction_amount
        ,SUM(fso.product_order_cash_credit_redeemed_amount) AS product_order_cash_credit_redeemed_amount
        ,SUM(fso.product_order_loyalty_unit_count) AS product_order_loyalty_unit_count
        ,SUM(fso.product_order_outfit_component_unit_count) AS product_order_outfit_component_unit_count
        ,SUM(fso.product_order_outfit_count) AS product_order_outfit_count
        ,SUM(fso.product_order_discounted_unit_count) AS product_order_discounted_unit_count
        ,SUM(fso.product_order_zero_revenue_unit_count) AS product_order_zero_revenue_unit_count
        ,SUM(fso.product_order_cash_credit_refund_amount) AS product_order_cash_credit_refund_amount
        ,SUM(fso.product_order_noncash_credit_refund_amount) AS product_order_noncash_credit_refund_amount
        ,SUM(fso.product_order_return_unit_count) AS product_order_return_unit_count
        ,SUM(fso.product_order_exchange_direct_cogs_amount) AS product_order_exchange_direct_cogs_amount
        ,SUM(fso.product_order_selling_expenses_amount) AS product_order_selling_expenses_amount
        ,SUM(fso.product_order_payment_processing_cost_amount) AS product_order_payment_processing_cost_amount
        ,SUM(fso.product_order_variable_gms_cost_amount) AS product_order_variable_gms_cost_amount
        ,SUM(fso.product_order_variable_warehouse_cost_amount) AS product_order_variable_warehouse_cost_amount
        ,SUM(fso.billing_selling_expenses_amount) AS billing_selling_expenses_amount
        ,SUM(fso.billing_payment_processing_cost_amount) AS billing_payment_processing_cost_amount
        ,SUM(fso.billing_variable_gms_cost_amount) AS billing_variable_gms_cost_amount
        ,SUM(fso.product_order_amount_to_pay) AS product_order_amount_to_pay
        ,SUM(fso.product_gross_revenue_excl_shipping) AS product_gross_revenue_excl_shipping
        ,SUM(fso.product_margin_pre_return_excl_shipping) AS product_margin_pre_return_excl_shipping
        ,SUM(fso.product_variable_contribution_profit) AS product_variable_contribution_profit
        ,SUM(fso.product_order_cash_net_revenue) AS product_order_cash_net_revenue
        ,SUM(fso.product_order_cash_margin_pre_return) AS product_order_cash_margin_pre_return
        ,SUM(fso.billing_cash_gross_revenue) AS billing_cash_gross_revenue
        ,SUM(fso.billing_cash_net_revenue) AS billing_cash_net_revenue
        ,SUM(fso.cash_variable_contribution_profit) AS cash_variable_contribution_profit
        ,SUM(fso.billing_order_transaction_count) AS billing_order_transaction_count
        ,SUM(fso.membership_fee_cash_transaction_amount) AS membership_fee_cash_transaction_amount
        ,SUM(fso.gift_card_transaction_amount) AS gift_card_transaction_amount
        ,SUM(fso.legacy_credit_cash_transaction_amount) AS legacy_credit_cash_transaction_amount
        ,SUM(fso.membership_fee_cash_refund_chargeback_amount) AS membership_fee_cash_refund_chargeback_amount
        ,SUM(fso.gift_card_cash_refund_chargeback_amount) AS gift_card_cash_refund_chargeback_amount
        ,SUM(fso.legacy_credit_cash_refund_chargeback_amount) AS legacy_credit_cash_refund_chargeback_amount
        ,SUM(fso.billed_cash_credit_issued_amount) AS billed_cash_credit_issued_amount
        ,SUM(fso.billed_cash_credit_cancelled_amount) AS billed_cash_credit_cancelled_amount
        ,SUM(fso.billed_cash_credit_expired_amount) AS billed_cash_credit_expired_amount
        ,SUM(fso.billed_cash_credit_issued_equivalent_count) AS billed_cash_credit_issued_equivalent_count
        ,SUM(fso.billed_cash_credit_redeemed_equivalent_count) AS billed_cash_credit_redeemed_equivalent_count
        ,SUM(fso.billed_cash_credit_cancelled_equivalent_count) AS billed_cash_credit_cancelled_equivalent_count
        ,SUM(fso.billed_cash_credit_expired_equivalent_count) AS billed_cash_credit_expired_equivalent_count
        ,SUM(fso.refund_cash_credit_issued_amount) AS refund_cash_credit_issued_amount
        ,SUM(fso.refund_cash_credit_redeemed_amount) AS refund_cash_credit_redeemed_amount
        ,SUM(fso.refund_cash_credit_cancelled_amount) AS refund_cash_credit_cancelled_amount
        ,SUM(fso.refund_cash_credit_expired_amount) AS refund_cash_credit_expired_amount
        ,SUM(fso.other_cash_credit_issued_amount) AS other_cash_credit_issued_amount
        ,SUM(fso.other_cash_credit_redeemed_amount) AS other_cash_credit_redeemed_amount
        ,SUM(fso.other_cash_credit_cancelled_amount) AS other_cash_credit_cancelled_amount
        ,SUM(fso.other_cash_credit_expired_amount) AS other_cash_credit_expired_amount
        ,SUM(fso.cash_gift_card_redeemed_amount) AS cash_gift_card_redeemed_amount
        ,SUM(fso.noncash_credit_issued_amount) AS noncash_credit_issued_amount
        ,SUM(fso.noncash_credit_cancelled_amount) AS noncash_credit_cancelled_amount
        ,SUM(fso.noncash_credit_expired_amount) AS noncash_credit_expired_amount
        ,SUM(fso.billed_credit_cash_refund_count) AS billed_credit_cash_refund_count
        ,SUM(fso.product_order_non_token_subtotal_excl_tariff_amount) AS product_order_non_token_subtotal_excl_tariff_amount
        ,SUM(fso.product_order_non_token_unit_count) AS product_order_non_token_unit_count
    FROM edw_prod.analytics_base.finance_sales_ops fso
    WHERE fso.date IS NOT NULL
            AND fso.currency_object NOT IN ('hyperion','eur')
--            and fso.date >= $refresh_from_date
    GROUP BY fso.date, fso.date_object, fso.currency_object, fso.currency_type, fso.store_id, fso.vip_store_id, fso.is_retail_vip, fso.gender, fso.is_cross_promo, fso.finance_specialty_store, fso.is_scrubs_customer, fso.order_membership_classification_key;

CREATE OR REPLACE TEMPORARY TABLE _base_calc_scaffold AS
    WITH cte AS (
        SELECT DISTINCT
            DATE_TRUNC('month', fsoa.date) AS month_date
            ,fsoa.date_object
            ,fsoa.currency_object
            ,fsoa.currency_type
            ,fsoa.event_store_id
            ,fsoa.vip_store_id
            ,fsoa.is_retail_vip
            ,fsoa.customer_gender
            ,fsoa.is_cross_promo
            ,fsm.store_brand
            ,fsm.business_unit
            ,fsm.report_mapping
            ,fsm.is_daily_cash_usd
            ,fsm.is_daily_cash_eur
            ,fsoa.finance_specialty_store
            ,fsoa.order_membership_classification_key
            ,fsoa.is_scrubs_customer
        FROM _fso_agg AS fsoa
        LEFT JOIN edw_prod.reference.finance_segment_mapping AS fsm
            ON fsoa.event_store_id = fsm.event_store_id
            AND fsoa.vip_store_id = fsm.vip_store_id
            AND fsoa.is_retail_vip = fsm.is_retail_vip
            AND fsoa.customer_gender = fsm.customer_gender
            AND fsoa.is_cross_promo = fsm.is_cross_promo
            AND fsoa.finance_specialty_store = fsm.finance_specialty_store
            AND fsoa.is_scrubs_customer = fsm.is_scrubs_customer
            AND (
                    fsm.is_daily_cash_usd = TRUE
                    OR fsm.is_daily_cash_eur = TRUE
                    OR fsm.report_mapping IN ('FLNA-OREV')
            )
            and fsm.metric_type = 'Orders'
    )
    SELECT
        dd.full_date AS date
        ,cte.date_object
        ,cte.currency_object
        ,cte.currency_type
        ,cte.event_store_id
        ,cte.vip_store_id
        ,cte.is_retail_vip
        ,cte.customer_gender
        ,cte.is_cross_promo
        ,cte.store_brand
        ,cte.business_unit
        ,cte.report_mapping
        ,cte.is_daily_cash_usd
        ,cte.is_daily_cash_eur
        ,cte.finance_specialty_store
        ,cte.order_membership_classification_key
        ,cte.is_scrubs_customer
    FROM cte AS cte
    LEFT JOIN edw_prod.data_model_jfb.dim_date AS dd
        ON cte.month_date = dd.month_date
;

CREATE OR REPLACE TEMPORARY TABLE _fso_agg_j AS
    SELECT
/*Daily Cash Version*/
        scf.date
        ,scf.date_object
        ,scf.currency_object
        ,scf.currency_type
/*Segment*/
        ,scf.event_store_id
        ,scf.vip_store_id
        ,scf.is_retail_vip
        ,scf.customer_gender
        ,scf.is_cross_promo
        ,scf.store_brand
        ,scf.business_unit
        ,scf.report_mapping
        ,scf.is_daily_cash_usd
        ,scf.is_daily_cash_eur
        ,scf.finance_specialty_store
        ,scf.is_scrubs_customer
/*Measure Filter*/
        ,scf.order_membership_classification_key
/*Measures*/
        ,IFNULL(fsoa.product_order_subtotal_excl_tariff_amount,0) AS product_order_subtotal_excl_tariff_amount
        ,IFNULL(fsoa.product_order_product_discount_amount,0) AS product_order_product_discount_amount
        ,IFNULL(fsoa.product_order_shipping_revenue_amount,0) AS product_order_shipping_revenue_amount
        ,IFNULL(fsoa.product_order_noncash_credit_redeemed_amount,0) AS product_order_noncash_credit_redeemed_amount
        ,IFNULL(fsoa.product_order_count,0) AS product_order_count
        ,IFNULL(fsoa.product_order_count_excl_seeding, 0) AS product_order_count_excl_seeding
        ,IFNULL(fsoa.product_margin_pre_return_excl_seeding, 0) AS product_margin_pre_return_excl_seeding
        ,IFNULL(fsoa.product_order_unit_count,0) AS product_order_unit_count
        ,IFNULL(fsoa.product_order_air_vip_price,0) AS product_order_air_vip_price
        ,IFNULL(fsoa.product_order_retail_unit_price,0) AS product_order_retail_unit_price
        ,IFNULL(fsoa.product_order_price_offered_amount,0) AS product_order_price_offered_amount
        ,IFNULL(fsoa.product_order_landed_product_cost_amount,0) AS product_order_landed_product_cost_amount
        ,IFNULL(fsoa.product_order_landed_product_cost_amount_accounting,0) AS product_order_landed_product_cost_amount_accounting
        ,IFNULL(fsoa.oracle_product_order_landed_product_cost_amount, 0) AS oracle_product_order_landed_product_cost_amount
        ,IFNULL(fsoa.product_order_shipping_cost_amount,0) AS product_order_shipping_cost_amount
        ,IFNULL(fsoa.product_order_shipping_supplies_cost_amount,0) AS product_order_shipping_supplies_cost_amount
        ,IFNULL(fsoa.product_order_direct_cogs_amount,0) AS product_order_direct_cogs_amount
        ,IFNULL(fsoa.product_order_cash_refund_amount,0) AS product_order_cash_refund_amount
        ,IFNULL(fsoa.product_order_cash_chargeback_amount,0) AS product_order_cash_chargeback_amount
        ,IFNULL(fsoa.product_order_cost_product_returned_resaleable_amount,0) AS product_order_cost_product_returned_resaleable_amount
        ,IFNULL(fsoa.product_order_cost_product_returned_resaleable_amount_accounting,0) AS product_order_cost_product_returned_resaleable_amount_accounting
        ,IFNULL(fsoa.product_order_cost_product_returned_damaged_amount,0) AS product_order_cost_product_returned_damaged_amount
        ,IFNULL(fsoa.product_order_return_shipping_cost_amount,0) AS product_order_return_shipping_cost_amount
        ,IFNULL(fsoa.product_order_reship_order_count,0) AS product_order_reship_order_count
        ,IFNULL(fsoa.product_order_reship_unit_count,0) AS product_order_reship_unit_count
        ,IFNULL(fsoa.product_order_reship_direct_cogs_amount,0) AS product_order_reship_direct_cogs_amount
        ,IFNULL(fsoa.product_order_reship_product_cost_amount,0) AS product_order_reship_product_cost_amount
        ,IFNULL(fsoa.product_order_reship_product_cost_amount_accounting,0) AS product_order_reship_product_cost_amount_accounting
        ,IFNULL(fsoa.oracle_product_order_reship_product_cost_amount,0) AS oracle_product_order_reship_product_cost_amount
        ,IFNULL(fsoa.product_order_reship_shipping_cost_amount,0) AS product_order_reship_shipping_cost_amount
        ,IFNULL(fsoa.product_order_reship_shipping_supplies_cost_amount,0) AS product_order_reship_shipping_supplies_cost_amount
        ,IFNULL(fsoa.product_order_exchange_order_count,0) AS product_order_exchange_order_count
        ,IFNULL(fsoa.product_order_exchange_unit_count,0) AS product_order_exchange_unit_count
        ,IFNULL(fsoa.product_order_exchange_product_cost_amount,0) AS product_order_exchange_product_cost_amount
        ,IFNULL(fsoa.product_order_exchange_product_cost_amount_accounting,0) AS product_order_exchange_product_cost_amount_accounting
        ,IFNULL(fsoa.oracle_product_order_exchange_product_cost_amount,0) AS oracle_product_order_exchange_product_cost_amount
        ,IFNULL(fsoa.product_order_exchange_shipping_cost_amount,0) AS product_order_exchange_shipping_cost_amount
        ,IFNULL(fsoa.product_order_exchange_shipping_supplies_cost_amount,0) AS product_order_exchange_shipping_supplies_cost_amount
        ,IFNULL(fsoa.product_gross_revenue,0) AS product_gross_revenue
        ,IFNULL(fsoa.product_net_revenue,0) AS product_net_revenue
        ,IFNULL(fsoa.product_margin_pre_return,0) AS product_margin_pre_return
        ,IFNULL(fsoa.product_gross_profit,0) AS product_gross_profit
        ,IFNULL(fsoa.product_order_cash_gross_revenue_amount,0) AS product_order_cash_gross_revenue_amount
        ,IFNULL(fsoa.cash_gross_revenue,0) AS cash_gross_revenue
        ,IFNULL(fsoa.cash_net_revenue,0) AS cash_net_revenue
        ,IFNULL(fsoa.cash_gross_profit,0) AS cash_gross_profit
        ,IFNULL(fsoa.billed_credit_cash_transaction_count,0) AS billed_credit_cash_transaction_count
        ,IFNULL(fsoa.on_retry_billed_credit_cash_transaction_count,0) AS on_retry_billed_credit_cash_transaction_count
        ,IFNULL(fsoa.billing_cash_refund_amount,0) AS billing_cash_refund_amount
        ,IFNULL(fsoa.billing_cash_chargeback_amount,0) AS billing_cash_chargeback_amount
        ,IFNULL(fsoa.billed_credit_cash_transaction_amount,0) AS billed_credit_cash_transaction_amount
        ,IFNULL(fsoa.billed_credit_cash_refund_chargeback_amount,0) AS billed_credit_cash_refund_chargeback_amount
        ,IFNULL(fsoa.billed_cash_credit_redeemed_amount,0) AS billed_cash_credit_redeemed_amount
        ,IFNULL(fsoa.product_order_misc_cogs_amount,0) AS product_order_misc_cogs_amount
        ,IFNULL(fsoa.product_order_cash_gross_profit,0) AS product_order_cash_gross_profit
        ,IFNULL(fsoa.billed_cash_credit_redeemed_same_month_amount,0) AS billed_cash_credit_redeemed_same_month_amount
        ,IFNULL(fsoa.product_order_tariff_amount,0) AS product_order_tariff_amount
        ,IFNULL(fsoa.product_order_product_subtotal_amount,0) AS product_order_product_subtotal_amount
        ,IFNULL(fsoa.product_order_shipping_discount_amount,0) AS product_order_shipping_discount_amount
        ,IFNULL(fsoa.product_order_shipping_revenue_before_discount_amount,0) AS product_order_shipping_revenue_before_discount_amount
        ,IFNULL(fsoa.product_order_tax_amount,0) AS product_order_tax_amount
        ,IFNULL(fsoa.product_order_cash_transaction_amount,0) AS product_order_cash_transaction_amount
        ,IFNULL(fsoa.product_order_cash_credit_redeemed_amount,0) AS product_order_cash_credit_redeemed_amount
        ,IFNULL(fsoa.product_order_loyalty_unit_count,0) AS product_order_loyalty_unit_count
        ,IFNULL(fsoa.product_order_outfit_component_unit_count,0) AS product_order_outfit_component_unit_count
        ,IFNULL(fsoa.product_order_outfit_count,0) AS product_order_outfit_count
        ,IFNULL(fsoa.product_order_discounted_unit_count,0) AS product_order_discounted_unit_count
        ,IFNULL(fsoa.product_order_zero_revenue_unit_count,0) AS product_order_zero_revenue_unit_count
        ,IFNULL(fsoa.product_order_cash_credit_refund_amount,0) AS product_order_cash_credit_refund_amount
        ,IFNULL(fsoa.product_order_noncash_credit_refund_amount,0) AS product_order_noncash_credit_refund_amount
        ,IFNULL(fsoa.product_order_return_unit_count,0) AS product_order_return_unit_count
        ,IFNULL(fsoa.product_order_exchange_direct_cogs_amount,0) AS product_order_exchange_direct_cogs_amount
        ,IFNULL(fsoa.product_order_selling_expenses_amount,0) AS product_order_selling_expenses_amount
        ,IFNULL(fsoa.product_order_payment_processing_cost_amount,0) AS product_order_payment_processing_cost_amount
        ,IFNULL(fsoa.product_order_variable_gms_cost_amount,0) AS product_order_variable_gms_cost_amount
        ,IFNULL(fsoa.product_order_variable_warehouse_cost_amount,0) AS product_order_variable_warehouse_cost_amount
        ,IFNULL(fsoa.billing_selling_expenses_amount,0) AS billing_selling_expenses_amount
        ,IFNULL(fsoa.billing_payment_processing_cost_amount,0) AS billing_payment_processing_cost_amount
        ,IFNULL(fsoa.billing_variable_gms_cost_amount,0) AS billing_variable_gms_cost_amount
        ,IFNULL(fsoa.product_order_amount_to_pay,0) AS product_order_amount_to_pay
        ,IFNULL(fsoa.product_gross_revenue_excl_shipping,0) AS product_gross_revenue_excl_shipping
        ,IFNULL(fsoa.product_margin_pre_return_excl_shipping,0) AS product_margin_pre_return_excl_shipping
        ,IFNULL(fsoa.product_variable_contribution_profit,0) AS product_variable_contribution_profit
        ,IFNULL(fsoa.product_order_cash_net_revenue,0) AS product_order_cash_net_revenue
        ,IFNULL(fsoa.product_order_cash_margin_pre_return,0) AS product_order_cash_margin_pre_return
        ,IFNULL(fsoa.billing_cash_gross_revenue,0) AS billing_cash_gross_revenue
        ,IFNULL(fsoa.billing_cash_net_revenue,0) AS billing_cash_net_revenue
        ,IFNULL(fsoa.cash_variable_contribution_profit,0) AS cash_variable_contribution_profit
        ,IFNULL(fsoa.billing_order_transaction_count,0) AS billing_order_transaction_count
        ,IFNULL(fsoa.membership_fee_cash_transaction_amount,0) AS membership_fee_cash_transaction_amount
        ,IFNULL(fsoa.gift_card_transaction_amount,0) AS gift_card_transaction_amount
        ,IFNULL(fsoa.legacy_credit_cash_transaction_amount,0) AS legacy_credit_cash_transaction_amount
        ,IFNULL(fsoa.membership_fee_cash_refund_chargeback_amount,0) AS membership_fee_cash_refund_chargeback_amount
        ,IFNULL(fsoa.gift_card_cash_refund_chargeback_amount,0) AS gift_card_cash_refund_chargeback_amount
        ,IFNULL(fsoa.legacy_credit_cash_refund_chargeback_amount,0) AS legacy_credit_cash_refund_chargeback_amount
        ,IFNULL(fsoa.billed_cash_credit_issued_amount,0) AS billed_cash_credit_issued_amount
        ,IFNULL(fsoa.billed_cash_credit_cancelled_amount,0) AS billed_cash_credit_cancelled_amount
        ,IFNULL(fsoa.billed_cash_credit_expired_amount,0) AS billed_cash_credit_expired_amount
        ,IFNULL(fsoa.billed_cash_credit_issued_equivalent_count,0) AS billed_cash_credit_issued_equivalent_count
        ,IFNULL(fsoa.billed_cash_credit_redeemed_equivalent_count,0) AS billed_cash_credit_redeemed_equivalent_count
        ,IFNULL(fsoa.billed_cash_credit_cancelled_equivalent_count,0) AS billed_cash_credit_cancelled_equivalent_count
        ,IFNULL(fsoa.billed_cash_credit_expired_equivalent_count,0) AS billed_cash_credit_expired_equivalent_count
        ,IFNULL(fsoa.refund_cash_credit_issued_amount,0) AS refund_cash_credit_issued_amount
        ,IFNULL(fsoa.refund_cash_credit_redeemed_amount,0) AS refund_cash_credit_redeemed_amount
        ,IFNULL(fsoa.refund_cash_credit_cancelled_amount,0) AS refund_cash_credit_cancelled_amount
        ,IFNULL(fsoa.refund_cash_credit_expired_amount,0) AS refund_cash_credit_expired_amount
        ,IFNULL(fsoa.other_cash_credit_issued_amount,0) AS other_cash_credit_issued_amount
        ,IFNULL(fsoa.other_cash_credit_redeemed_amount,0) AS other_cash_credit_redeemed_amount
        ,IFNULL(fsoa.other_cash_credit_cancelled_amount,0) AS other_cash_credit_cancelled_amount
        ,IFNULL(fsoa.other_cash_credit_expired_amount,0) AS other_cash_credit_expired_amount
        ,IFNULL(fsoa.cash_gift_card_redeemed_amount,0) AS cash_gift_card_redeemed_amount
        ,IFNULL(fsoa.noncash_credit_issued_amount,0) AS noncash_credit_issued_amount
        ,IFNULL(fsoa.noncash_credit_cancelled_amount,0) AS noncash_credit_cancelled_amount
        ,IFNULL(fsoa.noncash_credit_expired_amount,0) AS noncash_credit_expired_amount
        ,IFNULL(fsoa.billed_credit_cash_refund_count,0) AS billed_credit_cash_refund_count
        ,IFNULL(fsoa.product_order_non_token_subtotal_excl_tariff_amount,0) AS product_order_non_token_subtotal_excl_tariff_amount
        ,IFNULL(fsoa.product_order_non_token_unit_count,0) AS product_order_non_token_unit_count
    FROM _base_calc_scaffold AS scf
    LEFT JOIN _fso_agg AS fsoa
        ON scf.date = fsoa.date
        AND scf.date_object = fsoa.date_object
        AND scf.currency_object = fsoa.currency_object
        AND scf.currency_type = fsoa.currency_type
        AND scf.event_store_id = fsoa.event_store_id
        AND scf.vip_store_id = fsoa.vip_store_id
        AND scf.is_retail_vip = fsoa.is_retail_vip
        AND scf.customer_gender = fsoa.customer_gender
        AND scf.is_cross_promo = fsoa.is_cross_promo
        AND scf.finance_specialty_store = fsoa.finance_specialty_store
        AND scf.order_membership_classification_key = fsoa.order_membership_classification_key
        AND scf.is_scrubs_customer = fsoa.is_scrubs_customer
;

CREATE OR REPLACE TEMPORARY TABLE _daily_cash_base_calc_no_acquisition AS
    SELECT
/*Daily Cash Version*/
        fsoaj.date
        ,fsoaj.date_object
        ,fsoaj.currency_object
        ,fsoaj.currency_type
/*Segment*/
        ,fsoaj.store_brand
        ,fsoaj.business_unit
        ,fsoaj.report_mapping
        ,fsoaj.is_daily_cash_usd
        ,fsoaj.is_daily_cash_eur
/*Measure Filter*/
        ,fsoaj.order_membership_classification_key
/*Measures*/
        ,SUM(fsoaj.product_order_subtotal_excl_tariff_amount) AS product_order_subtotal_excl_tariff_amount
        ,SUM(fsoaj.product_order_product_discount_amount) AS product_order_product_discount_amount
        ,SUM(fsoaj.product_order_shipping_revenue_amount) AS product_order_shipping_revenue_amount
        ,SUM(fsoaj.product_order_noncash_credit_redeemed_amount) AS product_order_noncash_credit_redeemed_amount
        ,SUM(fsoaj.product_order_count) AS product_order_count
        ,SUM(fsoaj.product_order_count_excl_seeding) AS product_order_count_excl_seeding
        ,SUM(fsoaj.product_margin_pre_return_excl_seeding) AS product_margin_pre_return_excl_seeding
        ,SUM(fsoaj.product_order_unit_count) AS product_order_unit_count
        ,SUM(fsoaj.product_order_air_vip_price) AS product_order_air_vip_price
        ,SUM(fsoaj.product_order_retail_unit_price) AS product_order_retail_unit_price
        ,SUM(fsoaj.product_order_price_offered_amount) AS product_order_price_offered_amount
        ,SUM(fsoaj.product_order_landed_product_cost_amount) AS product_order_landed_product_cost_amount
        ,SUM(fsoaj.product_order_landed_product_cost_amount_accounting) AS product_order_landed_product_cost_amount_accounting
        ,SUM(fsoaj.oracle_product_order_landed_product_cost_amount) AS oracle_product_order_landed_product_cost_amount
        ,SUM(fsoaj.product_order_shipping_cost_amount) AS product_order_shipping_cost_amount
        ,SUM(fsoaj.product_order_shipping_supplies_cost_amount) AS product_order_shipping_supplies_cost_amount
        ,SUM(fsoaj.product_order_direct_cogs_amount) AS product_order_direct_cogs_amount
        ,SUM(fsoaj.product_order_cash_refund_amount) AS product_order_cash_refund_amount
        ,SUM(fsoaj.product_order_cash_chargeback_amount) AS product_order_cash_chargeback_amount
        ,SUM(fsoaj.product_order_cost_product_returned_resaleable_amount) AS product_order_cost_product_returned_resaleable_amount
        ,SUM(fsoaj.product_order_cost_product_returned_resaleable_amount_accounting) AS product_order_cost_product_returned_resaleable_amount_accounting
        ,SUM(fsoaj.product_order_cost_product_returned_damaged_amount) AS product_order_cost_product_returned_damaged_amount
        ,SUM(fsoaj.product_order_return_shipping_cost_amount) AS product_order_return_shipping_cost_amount
        ,SUM(fsoaj.product_order_reship_order_count) AS product_order_reship_order_count
        ,SUM(fsoaj.product_order_reship_unit_count) AS product_order_reship_unit_count
        ,SUM(fsoaj.product_order_reship_direct_cogs_amount) AS product_order_reship_direct_cogs_amount
        ,SUM(fsoaj.product_order_reship_product_cost_amount) AS product_order_reship_product_cost_amount
        ,SUM(fsoaj.product_order_reship_product_cost_amount_accounting) AS product_order_reship_product_cost_amount_accounting
        ,SUM(fsoaj.oracle_product_order_reship_product_cost_amount) AS oracle_product_order_reship_product_cost_amount
        ,SUM(fsoaj.product_order_reship_shipping_cost_amount) AS product_order_reship_shipping_cost_amount
        ,SUM(fsoaj.product_order_reship_shipping_supplies_cost_amount) AS product_order_reship_shipping_supplies_cost_amount
        ,SUM(fsoaj.product_order_exchange_order_count) AS product_order_exchange_order_count
        ,SUM(fsoaj.product_order_exchange_unit_count) AS product_order_exchange_unit_count
        ,SUM(fsoaj.product_order_exchange_product_cost_amount) AS product_order_exchange_product_cost_amount
        ,SUM(fsoaj.product_order_exchange_product_cost_amount_accounting) AS product_order_exchange_product_cost_amount_accounting
        ,SUM(fsoaj.oracle_product_order_exchange_product_cost_amount) AS oracle_product_order_exchange_product_cost_amount
        ,SUM(fsoaj.product_order_exchange_shipping_cost_amount) AS product_order_exchange_shipping_cost_amount
        ,SUM(fsoaj.product_order_exchange_shipping_supplies_cost_amount) AS product_order_exchange_shipping_supplies_cost_amount
        ,SUM(fsoaj.product_gross_revenue) AS product_gross_revenue
        ,SUM(fsoaj.product_net_revenue) AS product_net_revenue
        ,SUM(fsoaj.product_margin_pre_return) AS product_margin_pre_return
        ,SUM(fsoaj.product_gross_profit) AS product_gross_profit
        ,SUM(fsoaj.product_order_cash_gross_revenue_amount) AS product_order_cash_gross_revenue_amount
        ,SUM(fsoaj.cash_gross_revenue) AS cash_gross_revenue
        ,SUM(fsoaj.cash_net_revenue) AS cash_net_revenue
        ,SUM(fsoaj.cash_net_revenue * fcc.budgeted_fx_rate) AS cash_net_revenue_budgeted_fx
        ,SUM(fsoaj.cash_gross_profit) AS cash_gross_profit
        ,SUM(fsoaj.billed_credit_cash_transaction_count) AS billed_credit_cash_transaction_count
        ,SUM(fsoaj.on_retry_billed_credit_cash_transaction_count) AS on_retry_billed_credit_cash_transaction_count
        ,SUM(fsoaj.billing_cash_refund_amount) AS billing_cash_refund_amount
        ,SUM(fsoaj.billing_cash_chargeback_amount) AS billing_cash_chargeback_amount
        ,SUM(fsoaj.billed_credit_cash_transaction_amount) AS billed_credit_cash_transaction_amount
        ,SUM(fsoaj.billed_credit_cash_refund_chargeback_amount) AS billed_credit_cash_refund_chargeback_amount
        ,SUM(fsoaj.billed_cash_credit_redeemed_amount) AS billed_cash_credit_redeemed_amount
        ,SUM(fsoaj.product_order_misc_cogs_amount) AS product_order_misc_cogs_amount
        ,SUM(fsoaj.product_order_cash_gross_profit) AS product_order_cash_gross_profit
        ,SUM(fsoaj.billed_cash_credit_redeemed_same_month_amount) AS billed_cash_credit_redeemed_same_month_amount
        ,SUM(fsoaj.product_order_tariff_amount) AS product_order_tariff_amount
        ,SUM(fsoaj.product_order_product_subtotal_amount) AS product_order_product_subtotal_amount
        ,SUM(fsoaj.product_order_shipping_discount_amount) AS product_order_shipping_discount_amount
        ,SUM(fsoaj.product_order_shipping_revenue_before_discount_amount) AS product_order_shipping_revenue_before_discount_amount
        ,SUM(fsoaj.product_order_tax_amount) AS product_order_tax_amount
        ,SUM(fsoaj.product_order_cash_transaction_amount) AS product_order_cash_transaction_amount
        ,SUM(fsoaj.product_order_cash_credit_redeemed_amount) AS product_order_cash_credit_redeemed_amount
        ,SUM(fsoaj.product_order_loyalty_unit_count) AS product_order_loyalty_unit_count
        ,SUM(fsoaj.product_order_outfit_component_unit_count) AS product_order_outfit_component_unit_count
        ,SUM(fsoaj.product_order_outfit_count) AS product_order_outfit_count
        ,SUM(fsoaj.product_order_discounted_unit_count) AS product_order_discounted_unit_count
        ,SUM(fsoaj.product_order_zero_revenue_unit_count) AS product_order_zero_revenue_unit_count
        ,SUM(fsoaj.product_order_cash_credit_refund_amount) AS product_order_cash_credit_refund_amount
        ,SUM(fsoaj.product_order_noncash_credit_refund_amount) AS product_order_noncash_credit_refund_amount
        ,SUM(fsoaj.product_order_return_unit_count) AS product_order_return_unit_count
        ,SUM(fsoaj.product_order_exchange_direct_cogs_amount) AS product_order_exchange_direct_cogs_amount
        ,SUM(fsoaj.product_order_selling_expenses_amount) AS product_order_selling_expenses_amount
        ,SUM(fsoaj.product_order_payment_processing_cost_amount) AS product_order_payment_processing_cost_amount
        ,SUM(fsoaj.product_order_variable_gms_cost_amount) AS product_order_variable_gms_cost_amount
        ,SUM(fsoaj.product_order_variable_warehouse_cost_amount) AS product_order_variable_warehouse_cost_amount
        ,SUM(fsoaj.billing_selling_expenses_amount) AS billing_selling_expenses_amount
        ,SUM(fsoaj.billing_payment_processing_cost_amount) AS billing_payment_processing_cost_amount
        ,SUM(fsoaj.billing_variable_gms_cost_amount) AS billing_variable_gms_cost_amount
        ,SUM(fsoaj.product_order_amount_to_pay) AS product_order_amount_to_pay
        ,SUM(fsoaj.product_gross_revenue_excl_shipping) AS product_gross_revenue_excl_shipping
        ,SUM(fsoaj.product_margin_pre_return_excl_shipping) AS product_margin_pre_return_excl_shipping
        ,SUM(fsoaj.product_variable_contribution_profit) AS product_variable_contribution_profit
        ,SUM(fsoaj.product_order_cash_net_revenue) AS product_order_cash_net_revenue
        ,SUM(fsoaj.product_order_cash_margin_pre_return) AS product_order_cash_margin_pre_return
        ,SUM(fsoaj.billing_cash_gross_revenue) AS billing_cash_gross_revenue
        ,SUM(fsoaj.billing_cash_net_revenue) AS billing_cash_net_revenue
        ,SUM(fsoaj.cash_variable_contribution_profit) AS cash_variable_contribution_profit
        ,SUM(fsoaj.billing_order_transaction_count) AS billing_order_transaction_count
        ,SUM(fsoaj.membership_fee_cash_transaction_amount) AS membership_fee_cash_transaction_amount
        ,SUM(fsoaj.gift_card_transaction_amount) AS gift_card_transaction_amount
        ,SUM(fsoaj.legacy_credit_cash_transaction_amount) AS legacy_credit_cash_transaction_amount
        ,SUM(fsoaj.membership_fee_cash_refund_chargeback_amount) AS membership_fee_cash_refund_chargeback_amount
        ,SUM(fsoaj.gift_card_cash_refund_chargeback_amount) AS gift_card_cash_refund_chargeback_amount
        ,SUM(fsoaj.legacy_credit_cash_refund_chargeback_amount) AS legacy_credit_cash_refund_chargeback_amount
        ,SUM(fsoaj.billed_cash_credit_issued_amount) AS billed_cash_credit_issued_amount
        ,SUM(fsoaj.billed_cash_credit_cancelled_amount) AS billed_cash_credit_cancelled_amount
        ,SUM(fsoaj.billed_cash_credit_expired_amount) AS billed_cash_credit_expired_amount
        ,SUM(fsoaj.billed_cash_credit_issued_equivalent_count) AS billed_cash_credit_issued_equivalent_count
        ,SUM(fsoaj.billed_cash_credit_redeemed_equivalent_count) AS billed_cash_credit_redeemed_equivalent_count
        ,SUM(fsoaj.billed_cash_credit_cancelled_equivalent_count) AS billed_cash_credit_cancelled_equivalent_count
        ,SUM(fsoaj.billed_cash_credit_expired_equivalent_count) AS billed_cash_credit_expired_equivalent_count
        ,SUM(fsoaj.refund_cash_credit_issued_amount) AS refund_cash_credit_issued_amount
        ,SUM(fsoaj.refund_cash_credit_redeemed_amount) AS refund_cash_credit_redeemed_amount
        ,SUM(fsoaj.refund_cash_credit_cancelled_amount) AS refund_cash_credit_cancelled_amount
        ,SUM(fsoaj.refund_cash_credit_expired_amount) AS refund_cash_credit_expired_amount
        ,SUM(fsoaj.other_cash_credit_issued_amount) AS other_cash_credit_issued_amount
        ,SUM(fsoaj.other_cash_credit_redeemed_amount) AS other_cash_credit_redeemed_amount
        ,SUM(fsoaj.other_cash_credit_cancelled_amount) AS other_cash_credit_cancelled_amount
        ,SUM(fsoaj.other_cash_credit_expired_amount) AS other_cash_credit_expired_amount
        ,SUM(fsoaj.cash_gift_card_redeemed_amount) AS cash_gift_card_redeemed_amount
        ,SUM(fsoaj.noncash_credit_issued_amount) AS noncash_credit_issued_amount
        ,SUM(fsoaj.noncash_credit_cancelled_amount) AS noncash_credit_cancelled_amount
        ,SUM(fsoaj.noncash_credit_expired_amount) AS noncash_credit_expired_amount
        ,SUM(fsoaj.billed_credit_cash_refund_count) AS billed_credit_cash_refund_count
        ,SUM(fsoaj.product_order_non_token_subtotal_excl_tariff_amount) AS product_order_non_token_subtotal_excl_tariff_amount
        ,SUM(fsoaj.product_order_non_token_unit_count) AS product_order_non_token_unit_count
    FROM _fso_agg_j AS fsoaj
    JOIN edw_prod.data_model_jfb.dim_store ds ON ds.store_id = fsoaj.event_store_id
    LEFT JOIN edw_prod.reference.finance_currency_conversion fcc ON fcc.local_currency = ds.store_currency
        AND fcc.target_currency = 'USD'
        AND fsoaj.date >= fcc.effective_date_from
        AND fsoaj.date < fcc.effective_date_to
    GROUP BY fsoaj.date
        ,fsoaj.date_object
        ,fsoaj.currency_object
        ,fsoaj.currency_type
        ,fsoaj.store_brand
        ,fsoaj.business_unit
        ,fsoaj.report_mapping
        ,fsoaj.is_daily_cash_usd
        ,fsoaj.is_daily_cash_eur
        ,fsoaj.order_membership_classification_key;

    CREATE OR REPLACE TEMPORARY TABLE _acquisition_media_spend AS
        SELECT
            amsda.date
            ,amsda.date_object
            ,amsda.currency_object
            ,amsda.currency_type
            ,fsm.store_brand
            ,fsm.business_unit
            ,fsm.report_mapping
            ,fsm.is_daily_cash_usd
            ,fsm.is_daily_cash_eur
            ,domc.order_membership_classification_key
            ,SUM(amsda.leads) AS leads
            ,SUM(amsda.primary_leads) as primary_leads
            ,SUM(amsda.reactivated_leads) AS reactivated_leads
            ,SUM(amsda.new_vips) AS new_vips
            ,SUM(amsda.reactivated_vips) AS reactivated_vips
            ,SUM(amsda.vips_from_reactivated_leads_m1) AS vips_from_reactivated_leads_m1
            ,SUM(amsda.paid_vips) AS paid_vips
            ,SUM(amsda.unpaid_vips) AS unpaid_vips
            ,SUM(amsda.new_vips_m1) AS new_vips_m1
            ,SUM(amsda.paid_vips_m1) AS paid_vips_m1
            ,SUM(amsda.cancels) AS cancels
            ,SUM(amsda.m1_cancels) AS m1_cancels
            ,SUM(amsda.bop_vips) AS bop_vips
            ,SUM(amsda.media_spend) AS media_spend
        FROM edw_prod.analytics_base.acquisition_media_spend_daily_agg AS amsda
        LEFT JOIN edw_prod.data_model_jfb.dim_order_membership_classification AS domc
            ON amsda.membership_order_type_l1 = domc.membership_order_type_l1
            AND amsda.membership_order_type_l2 = domc.membership_order_type_l2
            AND amsda.membership_order_type_l3 = domc.membership_order_type_l3
            AND domc.order_membership_classification_key in (1,2,3,4,7)
        LEFT JOIN edw_prod.reference.finance_segment_mapping AS fsm
            ON amsda.event_store_id = fsm.event_store_id
            AND amsda.vip_store_id = fsm.vip_store_id
            AND amsda.is_retail_vip = fsm.is_retail_vip
            AND amsda.customer_gender = fsm.customer_gender
            AND amsda.is_cross_promo = fsm.is_cross_promo
            AND amsda.finance_specialty_store = fsm.finance_specialty_store
            AND amsda.is_scrubs_customer = fsm.is_scrubs_customer
            AND fsm.metric_type = 'Acquisition'
            AND (
                fsm.is_daily_cash_usd = TRUE
                OR fsm.is_daily_cash_eur = TRUE
                OR fsm.report_mapping IN ('FL+SC-OREV-NA')
            )
        WHERE
            fsm.report_mapping NOT ILIKE '%RREV'
--            AND amsda.date >= $refresh_from_date
        GROUP BY amsda.date
            ,amsda.date_object
            ,amsda.currency_object
            ,amsda.currency_type
            ,fsm.store_brand
            ,fsm.business_unit
            ,fsm.report_mapping
            ,fsm.is_daily_cash_usd
            ,fsm.is_daily_cash_eur
            ,domc.order_membership_classification_key;

CREATE OR REPLACE TEMPORARY TABLE _fso_merch_base AS
    SELECT
        fso.date,
        fso.customer_id,
        fso.currency_object,
        fso.currency_type,
        fso.store_id,
        fso.vip_store_id,
        fso.is_retail_vip,
        fso.gender,
        fso.is_cross_promo,
        fso.finance_specialty_store,
        fso.is_scrubs_customer,
        fso.order_membership_classification_key,
        domc.membership_order_type_l1,
        fso.is_bop_vip
    FROM edw_prod.analytics_base.finance_sales_ops AS fso
        JOIN edw_prod.data_model_jfb.dim_order_membership_classification AS domc
            ON domc.order_membership_classification_key = fso.order_membership_classification_key
    WHERE
        date_object = 'placed'
        AND date IS NOT NULL
        AND currency_object NOT IN ('hyperion','eur')
        AND product_order_count > 0
--        AND date >= $refresh_from_date
;

CREATE OR REPLACE TEMPORARY TABLE _merch_purchases AS
WITH _daily_merch_purchases AS (
    SELECT date,
        currency_object,
        currency_type,
        customer_id,
        store_brand,
        business_unit,
        report_mapping,
        is_daily_cash_usd,
        is_daily_cash_eur,
        order_membership_classification_key,
        ROW_NUMBER() OVER(
            PARTITION BY currency_object, currency_type, store_brand, business_unit, report_mapping, is_daily_cash_usd,
                is_daily_cash_eur, customer_id, DATE_TRUNC('MONTH', date)
            ORDER BY date ASC
            ) AS index
    FROM _fso_merch_base AS fso
    JOIN edw_prod.reference.finance_segment_mapping fsm ON fsm.event_store_id = fso.store_id
        AND fsm.vip_store_id = fso.vip_store_id
        AND fsm.is_retail_vip = fso.is_retail_vip
        AND fsm.customer_gender = fso.gender
        AND fsm.is_cross_promo = fso.is_cross_promo
        AND fsm.finance_specialty_store = fso.finance_specialty_store
        AND fsm.is_scrubs_customer = fso.is_scrubs_customer
        AND (
            fsm.is_daily_cash_usd = TRUE
            OR fsm.is_daily_cash_eur = TRUE
            OR fsm.report_mapping IN ('FL+SC-OREV-NA')
        )
        AND fsm.metric_type = 'Orders'
    WHERE fso.is_bop_vip = TRUE
)
SELECT date,
    currency_object,
    currency_type,
    is_daily_cash_usd,
    is_daily_cash_eur,
    store_brand,
    business_unit,
    report_mapping,
    order_membership_classification_key,
    COUNT(1) AS purchase_count
FROM _daily_merch_purchases
WHERE index = 1
GROUP BY date,
    currency_object,
    currency_type,
    is_daily_cash_usd,
    is_daily_cash_eur,
    store_brand,
    business_unit,
    report_mapping,
    order_membership_classification_key;

CREATE OR REPLACE TEMPORARY TABLE _merch_purchases_hyp AS
WITH _daily_merch_purchases_hyp AS (
    SELECT date,
        currency_object,
        currency_type,
        customer_id,
        store_brand,
        business_unit,
        report_mapping,
        is_daily_cash_usd,
        is_daily_cash_eur,
        fso.order_membership_classification_key,
        ROW_NUMBER() OVER(
            PARTITION BY currency_object, currency_type, store_brand, business_unit, report_mapping, is_daily_cash_usd,
                is_daily_cash_eur, customer_id, DATE_TRUNC('MONTH', date)
            ORDER BY date ASC
            ) AS index
    FROM _fso_merch_base AS fso
    JOIN edw_prod.reference.finance_segment_mapping fsm ON fsm.event_store_id = fso.store_id
        AND fsm.vip_store_id = fso.vip_store_id
        AND fsm.is_retail_vip = fso.is_retail_vip
        AND fsm.customer_gender = fso.gender
        AND fsm.is_cross_promo = fso.is_cross_promo
        AND fsm.finance_specialty_store = fso.finance_specialty_store
        AND fsm.is_scrubs_customer = fso.is_scrubs_customer
        AND (
            fsm.is_daily_cash_usd = TRUE
            OR fsm.is_daily_cash_eur = TRUE
            OR fsm.report_mapping IN ('FL+SC-OREV-NA')
        )
        AND fsm.metric_type = 'Orders'
    WHERE fso.membership_order_type_l1='NonActivating'
)
SELECT date,
    currency_object,
    currency_type,
    is_daily_cash_usd,
    is_daily_cash_eur,
    store_brand,
    business_unit,
    report_mapping,
    order_membership_classification_key,
    COUNT(1) AS purchase_count_hyp
FROM _daily_merch_purchases_hyp
WHERE index = 1
GROUP BY date,
    currency_object,
    currency_type,
    is_daily_cash_usd,
    is_daily_cash_eur,
    store_brand,
    business_unit,
    report_mapping,
    order_membership_classification_key;

truncate table reporting.daily_cash_base_calc;
--DELETE FROM reporting.daily_cash_base_calc
--WHERE $is_full_refresh = TRUE;
--
--DELETE FROM reporting.daily_cash_base_calc
--WHERE $is_full_refresh = FALSE AND date >= $refresh_from_date;

INSERT INTO reporting.daily_cash_base_calc
(
    date,
    date_object,
    currency_object,
    currency_type,
    store_brand,
    business_unit,
    report_mapping,
    is_daily_cash_usd,
    is_daily_cash_eur,
    order_membership_classification_key,
    product_order_subtotal_excl_tariff_amount,
    product_order_product_discount_amount,
    product_order_shipping_revenue_amount,
    product_order_noncash_credit_redeemed_amount,
    product_order_count,
    product_order_count_excl_seeding,
    product_margin_pre_return_excl_seeding,
    product_order_unit_count,
    product_order_air_vip_price,
    product_order_retail_unit_price,
    product_order_price_offered_amount,
    product_order_landed_product_cost_amount,
    product_order_landed_product_cost_amount_accounting,
    oracle_product_order_landed_product_cost_amount,
    product_order_shipping_cost_amount,
    product_order_shipping_supplies_cost_amount,
    product_order_direct_cogs_amount,
    product_order_cash_refund_amount,
    product_order_cash_chargeback_amount,
    product_order_cost_product_returned_resaleable_amount,
    product_order_cost_product_returned_resaleable_amount_accounting,
    product_order_cost_product_returned_damaged_amount,
    product_order_return_shipping_cost_amount,
    product_order_reship_order_count,
    product_order_reship_unit_count,
    product_order_reship_direct_cogs_amount,
    product_order_reship_product_cost_amount,
    product_order_reship_product_cost_amount_accounting,
    oracle_product_order_reship_product_cost_amount,
    product_order_reship_shipping_cost_amount,
    product_order_reship_shipping_supplies_cost_amount,
    product_order_exchange_order_count,
    product_order_exchange_unit_count,
    product_order_exchange_product_cost_amount,
    product_order_exchange_product_cost_amount_accounting,
    oracle_product_order_exchange_product_cost_amount,
    product_order_exchange_shipping_cost_amount,
    product_order_exchange_shipping_supplies_cost_amount,
    product_gross_revenue,
    product_net_revenue,
    product_margin_pre_return,
    product_gross_profit,
    product_order_cash_gross_revenue_amount,
    cash_gross_revenue,
    cash_net_revenue,
    cash_net_revenue_budgeted_fx,
    cash_gross_profit,
    billed_credit_cash_transaction_count,
    on_retry_billed_credit_cash_transaction_count,
    billing_cash_refund_amount,
    billing_cash_chargeback_amount,
    billed_credit_cash_transaction_amount,
    billed_credit_cash_refund_chargeback_amount,
    billed_cash_credit_redeemed_amount,
    product_order_misc_cogs_amount,
    product_order_cash_gross_profit,
    billed_cash_credit_redeemed_same_month_amount,
    product_order_tariff_amount,
    product_order_product_subtotal_amount,
    product_order_shipping_discount_amount,
    product_order_shipping_revenue_before_discount_amount,
    product_order_tax_amount,
    product_order_cash_transaction_amount,
    product_order_cash_credit_redeemed_amount,
    product_order_loyalty_unit_count,
    product_order_outfit_component_unit_count,
    product_order_outfit_count,
    product_order_discounted_unit_count,
    product_order_zero_revenue_unit_count,
    product_order_cash_credit_refund_amount,
    product_order_noncash_credit_refund_amount,
    product_order_return_unit_count,
    product_order_exchange_direct_cogs_amount,
    product_order_selling_expenses_amount,
    product_order_payment_processing_cost_amount,
    product_order_variable_gms_cost_amount,
    product_order_variable_warehouse_cost_amount,
    billing_selling_expenses_amount,
    billing_payment_processing_cost_amount,
    billing_variable_gms_cost_amount,
    product_order_amount_to_pay,
    product_gross_revenue_excl_shipping,
    product_margin_pre_return_excl_shipping,
    product_variable_contribution_profit,
    product_order_cash_net_revenue,
    product_order_cash_margin_pre_return,
    billing_cash_gross_revenue,
    billing_cash_net_revenue,
    cash_variable_contribution_profit,
    billing_order_transaction_count,
    membership_fee_cash_transaction_amount,
    gift_card_transaction_amount,
    legacy_credit_cash_transaction_amount,
    membership_fee_cash_refund_chargeback_amount,
    gift_card_cash_refund_chargeback_amount,
    legacy_credit_cash_refund_chargeback_amount,
    billed_cash_credit_issued_amount,
    billed_cash_credit_cancelled_amount,
    billed_cash_credit_expired_amount,
    billed_cash_credit_issued_equivalent_count,
    billed_cash_credit_redeemed_equivalent_count,
    billed_cash_credit_cancelled_equivalent_count,
    billed_cash_credit_expired_equivalent_count,
    refund_cash_credit_issued_amount,
    refund_cash_credit_redeemed_amount,
    refund_cash_credit_cancelled_amount,
    refund_cash_credit_expired_amount,
    other_cash_credit_issued_amount,
    other_cash_credit_redeemed_amount,
    other_cash_credit_cancelled_amount,
    other_cash_credit_expired_amount,
    cash_gift_card_redeemed_amount,
    noncash_credit_issued_amount,
    noncash_credit_cancelled_amount,
    noncash_credit_expired_amount,
    billed_credit_cash_refund_count,
    product_order_non_token_subtotal_excl_tariff_amount,
    product_order_non_token_unit_count,
    leads,
    primary_leads,
    reactivated_leads,
    new_vips,
    reactivated_vips,
    vips_from_reactivated_leads_m1,
    paid_vips,
    unpaid_vips,
    new_vips_m1,
    paid_vips_m1,
    cancels,
    m1_cancels,
    bop_vips,
    media_spend,
    merch_purchase_count,
    merch_purchase_hyperion_count,
    meta_update_datetime,
    meta_create_datetime
)
    SELECT
/*Daily Cash Version*/
        COALESCE(dcbcna.date, ams.date) AS date
        ,COALESCE(dcbcna.date_object, ams.date_object) AS date_object
        ,COALESCE(dcbcna.currency_object, ams.currency_object) AS currency_object
        ,COALESCE(dcbcna.currency_type, ams.currency_type) AS currency_type
/*Segment*/
        ,COALESCE(dcbcna.store_brand, ams.store_brand) AS store_brand
        ,COALESCE(dcbcna.business_unit, ams.business_unit) AS business_unit
        ,COALESCE(dcbcna.report_mapping, ams.report_mapping) AS report_mapping
        ,COALESCE(dcbcna.is_daily_cash_usd, ams.is_daily_cash_usd) AS is_daily_cash_usd
        ,COALESCE(dcbcna.is_daily_cash_eur, ams.is_daily_cash_eur) AS is_daily_cash_eur
/*Measure Filter*/
        ,COALESCE(dcbcna.order_membership_classification_key, ams.order_membership_classification_key) AS order_membership_classification_key
/*Measures*/
        ,IFNULL(dcbcna.product_order_subtotal_excl_tariff_amount, 0) AS product_order_subtotal_excl_tariff_amount
        ,IFNULL(dcbcna.product_order_product_discount_amount, 0) AS product_order_product_discount_amount
        ,IFNULL(dcbcna.product_order_shipping_revenue_amount, 0) AS product_order_shipping_revenue_amount
        ,IFNULL(dcbcna.product_order_noncash_credit_redeemed_amount, 0) AS product_order_noncash_credit_redeemed_amount
        ,IFNULL(dcbcna.product_order_count, 0) AS product_order_count
        ,IFNULL(dcbcna.product_order_count_excl_seeding, 0) AS product_order_count_excl_seeding
        ,IFNULL(dcbcna.product_margin_pre_return_excl_seeding, 0) AS product_margin_pre_return_excl_seeding
        ,IFNULL(dcbcna.product_order_unit_count, 0) AS product_order_unit_count
        ,IFNULL(dcbcna.product_order_air_vip_price, 0) AS product_order_air_vip_price
        ,IFNULL(dcbcna.product_order_retail_unit_price, 0) AS product_order_retail_unit_price
        ,IFNULL(dcbcna.product_order_price_offered_amount, 0) AS product_order_price_offered_amount
        ,IFNULL(dcbcna.product_order_landed_product_cost_amount, 0) AS product_order_landed_product_cost_amount
        ,IFNULL(dcbcna.product_order_landed_product_cost_amount_accounting, 0) AS product_order_landed_product_cost_amount_accounting
        ,IFNULL(dcbcna.oracle_product_order_landed_product_cost_amount, 0) AS oracle_product_order_landed_product_cost_amount
        ,IFNULL(dcbcna.product_order_shipping_cost_amount, 0) AS product_order_shipping_cost_amount
        ,IFNULL(dcbcna.product_order_shipping_supplies_cost_amount, 0) AS product_order_shipping_supplies_cost_amount
        ,IFNULL(dcbcna.product_order_direct_cogs_amount, 0) AS product_order_direct_cogs_amount
        ,IFNULL(dcbcna.product_order_cash_refund_amount, 0) AS product_order_cash_refund_amount
        ,IFNULL(dcbcna.product_order_cash_chargeback_amount, 0) AS product_order_cash_chargeback_amount
        ,IFNULL(dcbcna.product_order_cost_product_returned_resaleable_amount, 0) AS product_order_cost_product_returned_resaleable_amount
        ,IFNULL(dcbcna.product_order_cost_product_returned_resaleable_amount_accounting, 0) AS product_order_cost_product_returned_resaleable_amount_accounting
        ,IFNULL(dcbcna.product_order_cost_product_returned_damaged_amount, 0) AS product_order_cost_product_returned_damaged_amount
        ,IFNULL(dcbcna.product_order_return_shipping_cost_amount, 0) AS product_order_return_shipping_cost_amount
        ,IFNULL(dcbcna.product_order_reship_order_count, 0) AS product_order_reship_order_count
        ,IFNULL(dcbcna.product_order_reship_unit_count, 0) AS product_order_reship_unit_count
        ,IFNULL(dcbcna.product_order_reship_direct_cogs_amount, 0) AS product_order_reship_direct_cogs_amount
        ,IFNULL(dcbcna.product_order_reship_product_cost_amount, 0) AS product_order_reship_product_cost_amount
        ,IFNULL(dcbcna.product_order_reship_product_cost_amount_accounting, 0) AS product_order_reship_product_cost_amount_accounting
        ,IFNULL(dcbcna.oracle_product_order_reship_product_cost_amount, 0) AS oracle_product_order_reship_product_cost_amount
        ,IFNULL(dcbcna.product_order_reship_shipping_cost_amount, 0) AS product_order_reship_shipping_cost_amount
        ,IFNULL(dcbcna.product_order_reship_shipping_supplies_cost_amount, 0) AS product_order_reship_shipping_supplies_cost_amount
        ,IFNULL(dcbcna.product_order_exchange_order_count, 0) AS product_order_exchange_order_count
        ,IFNULL(dcbcna.product_order_exchange_unit_count, 0) AS product_order_exchange_unit_count
        ,IFNULL(dcbcna.product_order_exchange_product_cost_amount, 0) AS product_order_exchange_product_cost_amount
        ,IFNULL(dcbcna.product_order_exchange_product_cost_amount_accounting, 0) AS product_order_exchange_product_cost_amount_accounting
        ,IFNULL(dcbcna.oracle_product_order_exchange_product_cost_amount, 0) AS oracle_product_order_exchange_product_cost_amount
        ,IFNULL(dcbcna.product_order_exchange_shipping_cost_amount, 0) AS product_order_exchange_shipping_cost_amount
        ,IFNULL(dcbcna.product_order_exchange_shipping_supplies_cost_amount, 0) AS product_order_exchange_shipping_supplies_cost_amount
        ,IFNULL(dcbcna.product_gross_revenue, 0) AS product_gross_revenue
        ,IFNULL(dcbcna.product_net_revenue, 0) AS product_net_revenue
        ,IFNULL(dcbcna.product_margin_pre_return, 0) AS product_margin_pre_return
        ,IFNULL(dcbcna.product_gross_profit, 0) AS product_gross_profit
        ,IFNULL(dcbcna.product_order_cash_gross_revenue_amount, 0) AS product_order_cash_gross_revenue_amount
        ,IFNULL(dcbcna.cash_gross_revenue, 0) AS cash_gross_revenue
        ,IFNULL(dcbcna.cash_net_revenue, 0) AS cash_net_revenue
        ,IFNULL(dcbcna.cash_net_revenue_budgeted_fx, 0) AS cash_net_revenue_budgeted_fx
        ,IFNULL(dcbcna.cash_gross_profit, 0) AS cash_gross_profit
        ,IFNULL(dcbcna.billed_credit_cash_transaction_count, 0) AS billed_credit_cash_transaction_count
        ,IFNULL(dcbcna.on_retry_billed_credit_cash_transaction_count, 0) AS on_retry_billed_credit_cash_transaction_count
        ,IFNULL(dcbcna.billing_cash_refund_amount, 0) AS billing_cash_refund_amount
        ,IFNULL(dcbcna.billing_cash_chargeback_amount, 0) AS billing_cash_chargeback_amount
        ,IFNULL(dcbcna.billed_credit_cash_transaction_amount, 0) AS billed_credit_cash_transaction_amount
        ,IFNULL(dcbcna.billed_credit_cash_refund_chargeback_amount, 0) AS billed_credit_cash_refund_chargeback_amount
        ,IFNULL(dcbcna.billed_cash_credit_redeemed_amount, 0) AS billed_cash_credit_redeemed_amount
        ,IFNULL(dcbcna.product_order_misc_cogs_amount, 0) AS product_order_misc_cogs_amount
        ,IFNULL(dcbcna.product_order_cash_gross_profit, 0) AS product_order_cash_gross_profit
        ,IFNULL(dcbcna.billed_cash_credit_redeemed_same_month_amount, 0) AS billed_cash_credit_redeemed_same_month_amount
        ,IFNULL(dcbcna.product_order_tariff_amount, 0) AS product_order_tariff_amount
        ,IFNULL(dcbcna.product_order_product_subtotal_amount, 0) AS product_order_product_subtotal_amount
        ,IFNULL(dcbcna.product_order_shipping_discount_amount, 0) AS product_order_shipping_discount_amount
        ,IFNULL(dcbcna.product_order_shipping_revenue_before_discount_amount, 0) AS product_order_shipping_revenue_before_discount_amount
        ,IFNULL(dcbcna.product_order_tax_amount, 0) AS product_order_tax_amount
        ,IFNULL(dcbcna.product_order_cash_transaction_amount, 0) AS product_order_cash_transaction_amount
        ,IFNULL(dcbcna.product_order_cash_credit_redeemed_amount, 0) AS product_order_cash_credit_redeemed_amount
        ,IFNULL(dcbcna.product_order_loyalty_unit_count, 0) AS product_order_loyalty_unit_count
        ,IFNULL(dcbcna.product_order_outfit_component_unit_count, 0) AS product_order_outfit_component_unit_count
        ,IFNULL(dcbcna.product_order_outfit_count, 0) AS product_order_outfit_count
        ,IFNULL(dcbcna.product_order_discounted_unit_count, 0) AS product_order_discounted_unit_count
        ,IFNULL(dcbcna.product_order_zero_revenue_unit_count, 0) AS product_order_zero_revenue_unit_count
        ,IFNULL(dcbcna.product_order_cash_credit_refund_amount, 0) AS product_order_cash_credit_refund_amount
        ,IFNULL(dcbcna.product_order_noncash_credit_refund_amount, 0) AS product_order_noncash_credit_refund_amount
        ,IFNULL(dcbcna.product_order_return_unit_count, 0) AS product_order_return_unit_count
        ,IFNULL(dcbcna.product_order_exchange_direct_cogs_amount, 0) AS product_order_exchange_direct_cogs_amount
        ,IFNULL(dcbcna.product_order_selling_expenses_amount, 0) AS product_order_selling_expenses_amount
        ,IFNULL(dcbcna.product_order_payment_processing_cost_amount, 0) AS product_order_payment_processing_cost_amount
        ,IFNULL(dcbcna.product_order_variable_gms_cost_amount, 0) AS product_order_variable_gms_cost_amount
        ,IFNULL(dcbcna.product_order_variable_warehouse_cost_amount, 0) AS product_order_variable_warehouse_cost_amount
        ,IFNULL(dcbcna.billing_selling_expenses_amount, 0) AS billing_selling_expenses_amount
        ,IFNULL(dcbcna.billing_payment_processing_cost_amount, 0) AS billing_payment_processing_cost_amount
        ,IFNULL(dcbcna.billing_variable_gms_cost_amount, 0) AS billing_variable_gms_cost_amount
        ,IFNULL(dcbcna.product_order_amount_to_pay, 0) AS product_order_amount_to_pay
        ,IFNULL(dcbcna.product_gross_revenue_excl_shipping, 0) AS product_gross_revenue_excl_shipping
        ,IFNULL(dcbcna.product_margin_pre_return_excl_shipping, 0) AS product_margin_pre_return_excl_shipping
        ,IFNULL(dcbcna.product_variable_contribution_profit, 0) AS product_variable_contribution_profit
        ,IFNULL(dcbcna.product_order_cash_net_revenue, 0) AS product_order_cash_net_revenue
        ,IFNULL(dcbcna.product_order_cash_margin_pre_return, 0) AS product_order_cash_margin_pre_return
        ,IFNULL(dcbcna.billing_cash_gross_revenue, 0) AS billing_cash_gross_revenue
        ,IFNULL(dcbcna.billing_cash_net_revenue, 0) AS billing_cash_net_revenue
        ,IFNULL(dcbcna.cash_variable_contribution_profit, 0) AS cash_variable_contribution_profit
        ,IFNULL(dcbcna.billing_order_transaction_count, 0) AS billing_order_transaction_count
        ,IFNULL(dcbcna.membership_fee_cash_transaction_amount, 0) AS membership_fee_cash_transaction_amount
        ,IFNULL(dcbcna.gift_card_transaction_amount, 0) AS gift_card_transaction_amount
        ,IFNULL(dcbcna.legacy_credit_cash_transaction_amount, 0) AS legacy_credit_cash_transaction_amount
        ,IFNULL(dcbcna.membership_fee_cash_refund_chargeback_amount, 0) AS membership_fee_cash_refund_chargeback_amount
        ,IFNULL(dcbcna.gift_card_cash_refund_chargeback_amount, 0) AS gift_card_cash_refund_chargeback_amount
        ,IFNULL(dcbcna.legacy_credit_cash_refund_chargeback_amount, 0) AS legacy_credit_cash_refund_chargeback_amount
        ,IFNULL(dcbcna.billed_cash_credit_issued_amount, 0) AS billed_cash_credit_issued_amount
        ,IFNULL(dcbcna.billed_cash_credit_cancelled_amount, 0) AS billed_cash_credit_cancelled_amount
        ,IFNULL(dcbcna.billed_cash_credit_expired_amount, 0) AS billed_cash_credit_expired_amount
        ,IFNULL(dcbcna.billed_cash_credit_issued_equivalent_count, 0) AS billed_cash_credit_issued_equivalent_count
        ,IFNULL(dcbcna.billed_cash_credit_redeemed_equivalent_count, 0) AS billed_cash_credit_redeemed_equivalent_count
        ,IFNULL(dcbcna.billed_cash_credit_cancelled_equivalent_count, 0) AS billed_cash_credit_cancelled_equivalent_count
        ,IFNULL(dcbcna.billed_cash_credit_expired_equivalent_count, 0) AS billed_cash_credit_expired_equivalent_count
        ,IFNULL(dcbcna.refund_cash_credit_issued_amount, 0) AS refund_cash_credit_issued_amount
        ,IFNULL(dcbcna.refund_cash_credit_redeemed_amount, 0) AS refund_cash_credit_redeemed_amount
        ,IFNULL(dcbcna.refund_cash_credit_cancelled_amount, 0) AS refund_cash_credit_cancelled_amount
        ,IFNULL(dcbcna.refund_cash_credit_expired_amount, 0) AS refund_cash_credit_expired_amount
        ,IFNULL(dcbcna.other_cash_credit_issued_amount, 0) AS other_cash_credit_issued_amount
        ,IFNULL(dcbcna.other_cash_credit_redeemed_amount, 0) AS other_cash_credit_redeemed_amount
        ,IFNULL(dcbcna.other_cash_credit_cancelled_amount, 0) AS other_cash_credit_cancelled_amount
        ,IFNULL(dcbcna.other_cash_credit_expired_amount, 0) AS other_cash_credit_expired_amount
        ,IFNULL(dcbcna.cash_gift_card_redeemed_amount, 0) AS cash_gift_card_redeemed_amount
        ,IFNULL(dcbcna.noncash_credit_issued_amount, 0) AS noncash_credit_issued_amount
        ,IFNULL(dcbcna.noncash_credit_cancelled_amount, 0) AS noncash_credit_cancelled_amount
        ,IFNULL(dcbcna.noncash_credit_expired_amount, 0) AS noncash_credit_expired_amount
        ,IFNULL(dcbcna.billed_credit_cash_refund_count, 0) AS billed_credit_cash_refund_count
        ,IFNULL(dcbcna.product_order_non_token_subtotal_excl_tariff_amount, 0) AS product_order_non_token_subtotal_excl_tariff_amount
        ,IFNULL(dcbcna.product_order_non_token_unit_count, 0) AS product_order_non_token_unit_count
        ,IFNULL(ams.leads,0) AS leads
        ,IFNULL(ams.primary_leads,0) AS primary_leads
        ,IFNULL(ams.reactivated_leads,0) AS reactivated_leads
        ,IFNULL(ams.new_vips,0) AS new_vips
        ,IFNULL(ams.reactivated_vips,0) AS reactivated_vips
        ,IFNULL(ams.vips_from_reactivated_leads_m1,0) AS vips_from_reactivated_leads_m1
        ,IFNULL(ams.paid_vips,0) AS paid_vips
        ,IFNULL(ams.unpaid_vips,0) AS unpaid_vips
        ,IFNULL(ams.new_vips_m1,0) AS new_vips_m1
        ,IFNULL(ams.paid_vips_m1,0) AS paid_vips_m1
        ,IFNULL(ams.cancels,0) AS cancels
        ,IFNULL(ams.m1_cancels,0) AS m1_cancels
        ,IFNULL(ams.bop_vips,0) AS bop_vips
        ,IFNULL(ams.media_spend,0) AS media_spend
        ,IFNULL(mp.purchase_count,0) AS merch_purchase_count
        ,IFNULL(mph.purchase_count_hyp,0) AS merch_purchase_hyperion_count
        ,$execution_start_time AS meta_update_datetime
        ,$execution_start_time AS meta_create_datetime
    FROM _daily_cash_base_calc_no_acquisition AS dcbcna
    FULL JOIN _acquisition_media_spend AS ams
        ON dcbcna.date = ams.date
        AND dcbcna.date_object = ams.date_object
        AND dcbcna.currency_object = ams.currency_object
        AND dcbcna.currency_type = ams.currency_type
        AND dcbcna.report_mapping = ams.report_mapping
        AND dcbcna.is_daily_cash_usd = ams.is_daily_cash_usd
        AND dcbcna.is_daily_cash_eur = ams.is_daily_cash_eur
        AND dcbcna.order_membership_classification_key = ams.order_membership_classification_key
    LEFT JOIN _merch_purchases mp ON mp.date = COALESCE(dcbcna.date, ams.date)
        AND mp.currency_object = COALESCE(dcbcna.currency_object, ams.currency_object)
        AND mp.currency_type = COALESCE(dcbcna.currency_type, ams.currency_type)
        AND mp.report_mapping = COALESCE(dcbcna.report_mapping, ams.report_mapping)
        AND mp.is_daily_cash_usd = COALESCE(dcbcna.is_daily_cash_usd, ams.is_daily_cash_usd)
        AND mp.is_daily_cash_eur = COALESCE(dcbcna.is_daily_cash_eur, ams.is_daily_cash_eur)
        AND mp.order_membership_classification_key = COALESCE(dcbcna.order_membership_classification_key, ams.order_membership_classification_key)
    LEFT JOIN _merch_purchases_hyp mph ON mph.date = COALESCE(dcbcna.date, ams.date)
        AND mph.currency_object = COALESCE(dcbcna.currency_object, ams.currency_object)
        AND mph.currency_type = COALESCE(dcbcna.currency_type, ams.currency_type)
        AND mph.report_mapping = COALESCE(dcbcna.report_mapping, ams.report_mapping)
        AND mph.is_daily_cash_usd = COALESCE(dcbcna.is_daily_cash_usd, ams.is_daily_cash_usd)
        AND mph.is_daily_cash_eur = COALESCE(dcbcna.is_daily_cash_eur, ams.is_daily_cash_eur)
        AND mph.order_membership_classification_key = COALESCE(dcbcna.order_membership_classification_key, ams.order_membership_classification_key);

--UPDATE stg.meta_table_dependency_watermark
--SET
--    high_watermark_datetime = $execution_start_time,
--    new_high_watermark_datetime = $execution_start_time
--WHERE table_name = $target_table;

