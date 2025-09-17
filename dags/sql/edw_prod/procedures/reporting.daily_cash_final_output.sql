SET execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);
--SET target_table = 'reporting.daily_cash_final_output';
--SET is_watermark_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));
/* if reporting.daily_cash_base_calc had a full refresh, we want this script to do full refresh too */
--SET dc_base_calc_full_refresh = ( SELECT IFF(meta_create_datetime_count = 1, TRUE, FALSE)
--                                  FROM (SELECT COUNT(DISTINCT meta_create_datetime) AS meta_create_datetime_count FROM reporting.daily_cash_base_calc));
--SET is_full_refresh = CASE
--                            WHEN DAYOFWEEK(CURRENT_DATE) = 0
--                                     OR DAYOFMONTH(CURRENT_DATE) = 1
--                                     OR $dc_base_calc_full_refresh = TRUE
--                                     OR $is_watermark_full_refresh = TRUE
--                                THEN TRUE
--                            ELSE FALSE END;
--SET refresh_from_date = (SELECT IFF($is_full_refresh = FALSE, DATEADD(YEAR, -2, DATE_TRUNC(MONTH, CURRENT_DATE)), '1900-01-01'));

CREATE OR REPLACE TEMPORARY TABLE _dcbc_j AS
WITH _daily_cash_base_calc AS (
    SELECT *
    FROM reporting.daily_cash_base_calc
--    WHERE $is_full_refresh = TRUE
--    UNION ALL
--    SELECT *
--    FROM reporting.daily_cash_base_calc
--    WHERE
--        $is_full_refresh = FALSE
--        AND date >= $refresh_from_date
    )
SELECT
/*Daily Cash Version*/
    dcbc.date
    ,dcbc.date_object
    ,dcbc.currency_object
    ,dcbc.currency_type
/*Segment*/
    ,dcbc.store_brand
    ,dcbc.business_unit
    ,dcbc.report_mapping
    ,dcbc.is_daily_cash_usd
    ,dcbc.is_daily_cash_eur
/*Measure Filters*/
    ,dcbc.order_membership_classification_key
/*Measures*/
    ,dcbc.product_order_subtotal_excl_tariff_amount AS product_order_subtotal_excl_tariff_amount
    ,dcm.product_order_subtotal_excl_tariff_amount_mtd AS product_order_subtotal_excl_tariff_amount_mtd
    ,dcmly.product_order_subtotal_excl_tariff_amount_mtd AS product_order_subtotal_excl_tariff_amount_mtd_ly
    ,dcmtlm.product_order_subtotal_excl_tariff_amount_month_tot AS product_order_subtotal_excl_tariff_amount_month_tot_lm
    ,dcmtly.product_order_subtotal_excl_tariff_amount_month_tot AS product_order_subtotal_excl_tariff_amount_month_tot_ly
    ,dcbc.product_order_product_discount_amount AS product_order_product_discount_amount
    ,dcm.product_order_product_discount_amount_mtd AS product_order_product_discount_amount_mtd
    ,dcmly.product_order_product_discount_amount_mtd AS product_order_product_discount_amount_mtd_ly
    ,dcmtlm.product_order_product_discount_amount_month_tot AS product_order_product_discount_amount_month_tot_lm
    ,dcmtly.product_order_product_discount_amount_month_tot AS product_order_product_discount_amount_month_tot_ly
    ,dcbc.product_order_shipping_revenue_amount AS product_order_shipping_revenue_amount
    ,dcm.product_order_shipping_revenue_amount_mtd AS product_order_shipping_revenue_amount_mtd
    ,dcmly.product_order_shipping_revenue_amount_mtd AS product_order_shipping_revenue_amount_mtd_ly
    ,dcmtlm.product_order_shipping_revenue_amount_month_tot AS product_order_shipping_revenue_amount_month_tot_lm
    ,dcmtly.product_order_shipping_revenue_amount_month_tot AS product_order_shipping_revenue_amount_month_tot_ly
    ,dcbc.product_order_noncash_credit_redeemed_amount AS product_order_noncash_credit_redeemed_amount
    ,dcm.product_order_noncash_credit_redeemed_amount_mtd AS product_order_noncash_credit_redeemed_amount_mtd
    ,dcmly.product_order_noncash_credit_redeemed_amount_mtd AS product_order_noncash_credit_redeemed_amount_mtd_ly
    ,dcmtlm.product_order_noncash_credit_redeemed_amount_month_tot AS product_order_noncash_credit_redeemed_amount_month_tot_lm
    ,dcmtly.product_order_noncash_credit_redeemed_amount_month_tot AS product_order_noncash_credit_redeemed_amount_month_tot_ly
    ,dcbc.product_order_count AS product_order_count
    ,dcm.product_order_count_mtd AS product_order_count_mtd
    ,dcmly.product_order_count_mtd AS product_order_count_mtd_ly
    ,dcmtlm.product_order_count_month_tot AS product_order_count_month_tot_lm
    ,dcmtly.product_order_count_month_tot AS product_order_count_month_tot_ly
    ,dcbc.product_order_count_excl_seeding AS product_order_count_excl_seeding
    ,dcm.product_order_count_excl_seeding_mtd AS product_order_count_excl_seeding_mtd
    ,dcmly.product_order_count_excl_seeding_mtd AS product_order_count_excl_seeding_mtd_ly
    ,dcmtlm.product_order_count_excl_seeding_month_tot AS product_order_count_excl_seeding_month_tot_lm
    ,dcmtly.product_order_count_excl_seeding_month_tot AS product_order_count_excl_seeding_month_tot_ly
    ,dcbc.product_margin_pre_return_excl_seeding AS product_margin_pre_return_excl_seeding
    ,dcm.product_margin_pre_return_excl_seeding_mtd AS product_margin_pre_return_excl_seeding_mtd
    ,dcmly.product_margin_pre_return_excl_seeding_mtd AS product_margin_pre_return_excl_seeding_mtd_ly
    ,dcmtlm.product_margin_pre_return_excl_seeding_month_tot AS product_margin_pre_return_excl_seeding_month_tot_lm
    ,dcmtly.product_margin_pre_return_excl_seeding_month_tot AS product_margin_pre_return_excl_seeding_month_tot_ly
    ,dcbc.product_order_unit_count AS product_order_unit_count
    ,dcm.product_order_unit_count_mtd AS product_order_unit_count_mtd
    ,dcmly.product_order_unit_count_mtd AS product_order_unit_count_mtd_ly
    ,dcmtlm.product_order_unit_count_month_tot AS product_order_unit_count_month_tot_lm
    ,dcmtly.product_order_unit_count_month_tot AS product_order_unit_count_month_tot_ly
    ,dcbc.product_order_air_vip_price AS product_order_air_vip_price
    ,dcm.product_order_air_vip_price_mtd AS product_order_air_vip_price_mtd
    ,dcmly.product_order_air_vip_price_mtd AS product_order_air_vip_price_mtd_ly
    ,dcmtlm.product_order_air_vip_price_month_tot AS product_order_air_vip_price_month_tot_lm
    ,dcmtly.product_order_air_vip_price_month_tot AS product_order_air_vip_price_month_tot_ly
    ,dcbc.product_order_retail_unit_price AS product_order_retail_unit_price
    ,dcm.product_order_retail_unit_price_mtd AS product_order_retail_unit_price_mtd
    ,dcmly.product_order_retail_unit_price_mtd AS product_order_retail_unit_price_mtd_ly
    ,dcmtlm.product_order_retail_unit_price_month_tot AS product_order_retail_unit_price_month_tot_lm
    ,dcmtly.product_order_retail_unit_price_month_tot AS product_order_retail_unit_price_month_tot_ly
    ,dcbc.product_order_price_offered_amount AS product_order_price_offered_amount
    ,dcm.product_order_price_offered_amount_mtd AS product_order_price_offered_amount_mtd
    ,dcmly.product_order_price_offered_amount_mtd AS product_order_price_offered_amount_mtd_ly
    ,dcmtlm.product_order_price_offered_amount_month_tot AS product_order_price_offered_amount_month_tot_lm
    ,dcmtly.product_order_price_offered_amount_month_tot AS product_order_price_offered_amount_month_tot_ly
    ,dcbc.product_order_landed_product_cost_amount AS product_order_landed_product_cost_amount
    ,dcm.product_order_landed_product_cost_amount_mtd AS product_order_landed_product_cost_amount_mtd
    ,dcmly.product_order_landed_product_cost_amount_mtd AS product_order_landed_product_cost_amount_mtd_ly
    ,dcmtlm.product_order_landed_product_cost_amount_month_tot AS product_order_landed_product_cost_amount_month_tot_lm
    ,dcmtly.product_order_landed_product_cost_amount_month_tot AS product_order_landed_product_cost_amount_month_tot_ly
    ,dcbc.product_order_landed_product_cost_amount_accounting AS product_order_landed_product_cost_amount_accounting
    ,dcm.product_order_landed_product_cost_amount_accounting_mtd AS product_order_landed_product_cost_amount_accounting_mtd
    ,dcmly.product_order_landed_product_cost_amount_accounting_mtd AS product_order_landed_product_cost_amount_accounting_mtd_ly
    ,dcmtlm.product_order_landed_product_cost_amount_accounting_month_tot AS product_order_landed_product_cost_amount_accounting_month_tot_lm
    ,dcmtly.product_order_landed_product_cost_amount_accounting_month_tot AS product_order_landed_product_cost_amount_accounting_month_tot_ly
    ,dcbc.oracle_product_order_landed_product_cost_amount AS oracle_product_order_landed_product_cost_amount
    ,dcm.oracle_product_order_landed_product_cost_amount_mtd AS oracle_product_order_landed_product_cost_amount_mtd
    ,dcmly.oracle_product_order_landed_product_cost_amount_mtd AS oracle_product_order_landed_product_cost_amount_mtd_ly
    ,dcmtlm.oracle_product_order_landed_product_cost_amount_month_tot AS oracle_product_order_landed_product_cost_amount_month_tot_lm
    ,dcmtly.oracle_product_order_landed_product_cost_amount_month_tot AS oracle_product_order_landed_product_cost_amount_month_tot_ly
    ,dcbc.product_order_shipping_cost_amount AS product_order_shipping_cost_amount
    ,dcm.product_order_shipping_cost_amount_mtd AS product_order_shipping_cost_amount_mtd
    ,dcmly.product_order_shipping_cost_amount_mtd AS product_order_shipping_cost_amount_mtd_ly
    ,dcmtlm.product_order_shipping_cost_amount_month_tot AS product_order_shipping_cost_amount_month_tot_lm
    ,dcmtly.product_order_shipping_cost_amount_month_tot AS product_order_shipping_cost_amount_month_tot_ly
    ,dcbc.product_order_shipping_supplies_cost_amount AS product_order_shipping_supplies_cost_amount
    ,dcm.product_order_shipping_supplies_cost_amount_mtd AS product_order_shipping_supplies_cost_amount_mtd
    ,dcmly.product_order_shipping_supplies_cost_amount_mtd AS product_order_shipping_supplies_cost_amount_mtd_ly
    ,dcmtlm.product_order_shipping_supplies_cost_amount_month_tot AS product_order_shipping_supplies_cost_amount_month_tot_lm
    ,dcmtly.product_order_shipping_supplies_cost_amount_month_tot AS product_order_shipping_supplies_cost_amount_month_tot_ly
    ,dcbc.product_order_direct_cogs_amount AS product_order_direct_cogs_amount
    ,dcm.product_order_direct_cogs_amount_mtd AS product_order_direct_cogs_amount_mtd
    ,dcmly.product_order_direct_cogs_amount_mtd AS product_order_direct_cogs_amount_mtd_ly
    ,dcmtlm.product_order_direct_cogs_amount_month_tot AS product_order_direct_cogs_amount_month_tot_lm
    ,dcmtly.product_order_direct_cogs_amount_month_tot AS product_order_direct_cogs_amount_month_tot_ly
    ,dcbc.product_order_cash_refund_amount AS product_order_cash_refund_amount
    ,dcm.product_order_cash_refund_amount_mtd AS product_order_cash_refund_amount_mtd
    ,dcmly.product_order_cash_refund_amount_mtd AS product_order_cash_refund_amount_mtd_ly
    ,dcmtlm.product_order_cash_refund_amount_month_tot AS product_order_cash_refund_amount_month_tot_lm
    ,dcmtly.product_order_cash_refund_amount_month_tot AS product_order_cash_refund_amount_month_tot_ly
    ,dcbc.product_order_cash_chargeback_amount AS product_order_cash_chargeback_amount
    ,dcm.product_order_cash_chargeback_amount_mtd AS product_order_cash_chargeback_amount_mtd
    ,dcmly.product_order_cash_chargeback_amount_mtd AS product_order_cash_chargeback_amount_mtd_ly
    ,dcmtlm.product_order_cash_chargeback_amount_month_tot AS product_order_cash_chargeback_amount_month_tot_lm
    ,dcmtly.product_order_cash_chargeback_amount_month_tot AS product_order_cash_chargeback_amount_month_tot_ly
    ,dcbc.product_order_cost_product_returned_resaleable_amount AS product_order_cost_product_returned_resaleable_amount
    ,dcm.product_order_cost_product_returned_resaleable_amount_mtd AS product_order_cost_product_returned_resaleable_amount_mtd
    ,dcmly.product_order_cost_product_returned_resaleable_amount_mtd AS product_order_cost_product_returned_resaleable_amount_mtd_ly
    ,dcmtlm.product_order_cost_product_returned_resaleable_amount_month_tot AS product_order_cost_product_returned_resaleable_amount_month_tot_lm
    ,dcmtly.product_order_cost_product_returned_resaleable_amount_month_tot AS product_order_cost_product_returned_resaleable_amount_month_tot_ly
    ,dcbc.product_order_cost_product_returned_resaleable_amount_accounting AS product_order_cost_product_returned_resaleable_amount_accounting
    ,dcm.product_order_cost_product_returned_resaleable_amount_accounting_mtd AS product_order_cost_product_returned_resaleable_amount_accounting_mtd
    ,dcmly.product_order_cost_product_returned_resaleable_amount_accounting_mtd AS product_order_cost_product_returned_resaleable_amount_accounting_mtd_ly
    ,dcmtlm.product_order_cost_product_returned_resaleable_amount_accounting_month_tot AS product_order_cost_product_returned_resaleable_amount_accounting_month_tot_lm
    ,dcmtly.product_order_cost_product_returned_resaleable_amount_accounting_month_tot AS product_order_cost_product_returned_resaleable_amount_accounting_month_tot_ly
    ,dcbc.product_order_cost_product_returned_damaged_amount AS product_order_cost_product_returned_damaged_amount
    ,dcm.product_order_cost_product_returned_damaged_amount_mtd AS product_order_cost_product_returned_damaged_amount_mtd
    ,dcmly.product_order_cost_product_returned_damaged_amount_mtd AS product_order_cost_product_returned_damaged_amount_mtd_ly
    ,dcmtlm.product_order_cost_product_returned_damaged_amount_month_tot AS product_order_cost_product_returned_damaged_amount_month_tot_lm
    ,dcmtly.product_order_cost_product_returned_damaged_amount_month_tot AS product_order_cost_product_returned_damaged_amount_month_tot_ly
    ,dcbc.product_order_return_shipping_cost_amount AS product_order_return_shipping_cost_amount
    ,dcm.product_order_return_shipping_cost_amount_mtd AS product_order_return_shipping_cost_amount_mtd
    ,dcmly.product_order_return_shipping_cost_amount_mtd AS product_order_return_shipping_cost_amount_mtd_ly
    ,dcmtlm.product_order_return_shipping_cost_amount_month_tot AS product_order_return_shipping_cost_amount_month_tot_lm
    ,dcmtly.product_order_return_shipping_cost_amount_month_tot AS product_order_return_shipping_cost_amount_month_tot_ly
    ,dcbc.product_order_reship_order_count AS product_order_reship_order_count
    ,dcm.product_order_reship_order_count_mtd AS product_order_reship_order_count_mtd
    ,dcmly.product_order_reship_order_count_mtd AS product_order_reship_order_count_mtd_ly
    ,dcmtlm.product_order_reship_order_count_month_tot AS product_order_reship_order_count_month_tot_lm
    ,dcmtly.product_order_reship_order_count_month_tot AS product_order_reship_order_count_month_tot_ly
    ,dcbc.product_order_reship_unit_count AS product_order_reship_unit_count
    ,dcm.product_order_reship_unit_count_mtd AS product_order_reship_unit_count_mtd
    ,dcmly.product_order_reship_unit_count_mtd AS product_order_reship_unit_count_mtd_ly
    ,dcmtlm.product_order_reship_unit_count_month_tot AS product_order_reship_unit_count_month_tot_lm
    ,dcmtly.product_order_reship_unit_count_month_tot AS product_order_reship_unit_count_month_tot_ly
    ,dcbc.product_order_reship_direct_cogs_amount AS product_order_reship_direct_cogs_amount
    ,dcm.product_order_reship_direct_cogs_amount_mtd AS product_order_reship_direct_cogs_amount_mtd
    ,dcmly.product_order_reship_direct_cogs_amount_mtd AS product_order_reship_direct_cogs_amount_mtd_ly
    ,dcmtlm.product_order_reship_direct_cogs_amount_month_tot AS product_order_reship_direct_cogs_amount_month_tot_lm
    ,dcmtly.product_order_reship_direct_cogs_amount_month_tot AS product_order_reship_direct_cogs_amount_month_tot_ly
    ,dcbc.product_order_reship_product_cost_amount AS product_order_reship_product_cost_amount
    ,dcm.product_order_reship_product_cost_amount_mtd AS product_order_reship_product_cost_amount_mtd
    ,dcmly.product_order_reship_product_cost_amount_mtd AS product_order_reship_product_cost_amount_mtd_ly
    ,dcmtlm.product_order_reship_product_cost_amount_month_tot AS product_order_reship_product_cost_amount_month_tot_lm
    ,dcmtly.product_order_reship_product_cost_amount_month_tot AS product_order_reship_product_cost_amount_month_tot_ly
    ,dcbc.product_order_reship_product_cost_amount_accounting AS product_order_reship_product_cost_amount_accounting
    ,dcm.product_order_reship_product_cost_amount_accounting_mtd AS product_order_reship_product_cost_amount_accounting_mtd
    ,dcmly.product_order_reship_product_cost_amount_accounting_mtd AS product_order_reship_product_cost_amount_accounting_mtd_ly
    ,dcmtlm.product_order_reship_product_cost_amount_accounting_month_tot AS product_order_reship_product_cost_amount_accounting_month_tot_lm
    ,dcmtly.product_order_reship_product_cost_amount_accounting_month_tot AS product_order_reship_product_cost_amount_accounting_month_tot_ly
    ,dcbc.oracle_product_order_reship_product_cost_amount AS oracle_product_order_reship_product_cost_amount
    ,dcm.oracle_product_order_reship_product_cost_amount_mtd AS oracle_product_order_reship_product_cost_amount_mtd
    ,dcmly.oracle_product_order_reship_product_cost_amount_mtd AS oracle_product_order_reship_product_cost_amount_mtd_ly
    ,dcmtlm.oracle_product_order_reship_product_cost_amount_month_tot AS oracle_product_order_reship_product_cost_amount_month_tot_lm
    ,dcmtly.oracle_product_order_reship_product_cost_amount_month_tot AS oracle_product_order_reship_product_cost_amount_month_tot_ly
    ,dcbc.product_order_reship_shipping_cost_amount AS product_order_reship_shipping_cost_amount
    ,dcm.product_order_reship_shipping_cost_amount_mtd AS product_order_reship_shipping_cost_amount_mtd
    ,dcmly.product_order_reship_shipping_cost_amount_mtd AS product_order_reship_shipping_cost_amount_mtd_ly
    ,dcmtlm.product_order_reship_shipping_cost_amount_month_tot AS product_order_reship_shipping_cost_amount_month_tot_lm
    ,dcmtly.product_order_reship_shipping_cost_amount_month_tot AS product_order_reship_shipping_cost_amount_month_tot_ly
    ,dcbc.product_order_reship_shipping_supplies_cost_amount AS product_order_reship_shipping_supplies_cost_amount
    ,dcm.product_order_reship_shipping_supplies_cost_amount_mtd AS product_order_reship_shipping_supplies_cost_amount_mtd
    ,dcmly.product_order_reship_shipping_supplies_cost_amount_mtd AS product_order_reship_shipping_supplies_cost_amount_mtd_ly
    ,dcmtlm.product_order_reship_shipping_supplies_cost_amount_month_tot AS product_order_reship_shipping_supplies_cost_amount_month_tot_lm
    ,dcmtly.product_order_reship_shipping_supplies_cost_amount_month_tot AS product_order_reship_shipping_supplies_cost_amount_month_tot_ly
    ,dcbc.product_order_exchange_order_count AS product_order_exchange_order_count
    ,dcm.product_order_exchange_order_count_mtd AS product_order_exchange_order_count_mtd
    ,dcmly.product_order_exchange_order_count_mtd AS product_order_exchange_order_count_mtd_ly
    ,dcmtlm.product_order_exchange_order_count_month_tot AS product_order_exchange_order_count_month_tot_lm
    ,dcmtly.product_order_exchange_order_count_month_tot AS product_order_exchange_order_count_month_tot_ly
    ,dcbc.product_order_exchange_unit_count AS product_order_exchange_unit_count
    ,dcm.product_order_exchange_unit_count_mtd AS product_order_exchange_unit_count_mtd
    ,dcmly.product_order_exchange_unit_count_mtd AS product_order_exchange_unit_count_mtd_ly
    ,dcmtlm.product_order_exchange_unit_count_month_tot AS product_order_exchange_unit_count_month_tot_lm
    ,dcmtly.product_order_exchange_unit_count_month_tot AS product_order_exchange_unit_count_month_tot_ly
    ,dcbc.product_order_exchange_product_cost_amount AS product_order_exchange_product_cost_amount
    ,dcm.product_order_exchange_product_cost_amount_mtd AS product_order_exchange_product_cost_amount_mtd
    ,dcmly.product_order_exchange_product_cost_amount_mtd AS product_order_exchange_product_cost_amount_mtd_ly
    ,dcmtlm.product_order_exchange_product_cost_amount_month_tot AS product_order_exchange_product_cost_amount_month_tot_lm
    ,dcmtly.product_order_exchange_product_cost_amount_month_tot AS product_order_exchange_product_cost_amount_month_tot_ly
    ,dcbc.product_order_exchange_product_cost_amount_accounting AS product_order_exchange_product_cost_amount_accounting
    ,dcm.product_order_exchange_product_cost_amount_accounting_mtd AS product_order_exchange_product_cost_amount_accounting_mtd
    ,dcmly.product_order_exchange_product_cost_amount_accounting_mtd AS product_order_exchange_product_cost_amount_accounting_mtd_ly
    ,dcmtlm.product_order_exchange_product_cost_amount_accounting_month_tot AS product_order_exchange_product_cost_amount_accounting_month_tot_lm
    ,dcmtly.product_order_exchange_product_cost_amount_accounting_month_tot AS product_order_exchange_product_cost_amount_accounting_month_tot_ly
    ,dcbc.oracle_product_order_exchange_product_cost_amount AS oracle_product_order_exchange_product_cost_amount
    ,dcm.oracle_product_order_exchange_product_cost_amount_mtd AS oracle_product_order_exchange_product_cost_amount_mtd
    ,dcmly.oracle_product_order_exchange_product_cost_amount_mtd AS oracle_product_order_exchange_product_cost_amount_mtd_ly
    ,dcmtlm.oracle_product_order_exchange_product_cost_amount_month_tot AS oracle_product_order_exchange_product_cost_amount_month_tot_lm
    ,dcmtly.oracle_product_order_exchange_product_cost_amount_month_tot AS oracle_product_order_exchange_product_cost_amount_month_tot_ly
    ,dcbc.product_order_exchange_shipping_cost_amount AS product_order_exchange_shipping_cost_amount
    ,dcm.product_order_exchange_shipping_cost_amount_mtd AS product_order_exchange_shipping_cost_amount_mtd
    ,dcmly.product_order_exchange_shipping_cost_amount_mtd AS product_order_exchange_shipping_cost_amount_mtd_ly
    ,dcmtlm.product_order_exchange_shipping_cost_amount_month_tot AS product_order_exchange_shipping_cost_amount_month_tot_lm
    ,dcmtly.product_order_exchange_shipping_cost_amount_month_tot AS product_order_exchange_shipping_cost_amount_month_tot_ly
    ,dcbc.product_order_exchange_shipping_supplies_cost_amount AS product_order_exchange_shipping_supplies_cost_amount
    ,dcm.product_order_exchange_shipping_supplies_cost_amount_mtd AS product_order_exchange_shipping_supplies_cost_amount_mtd
    ,dcmly.product_order_exchange_shipping_supplies_cost_amount_mtd AS product_order_exchange_shipping_supplies_cost_amount_mtd_ly
    ,dcmtlm.product_order_exchange_shipping_supplies_cost_amount_month_tot AS product_order_exchange_shipping_supplies_cost_amount_month_tot_lm
    ,dcmtly.product_order_exchange_shipping_supplies_cost_amount_month_tot AS product_order_exchange_shipping_supplies_cost_amount_month_tot_ly
    ,dcbc.product_gross_revenue AS product_gross_revenue
    ,dcm.product_gross_revenue_mtd AS product_gross_revenue_mtd
    ,dcmly.product_gross_revenue_mtd AS product_gross_revenue_mtd_ly
    ,dcmtlm.product_gross_revenue_month_tot AS product_gross_revenue_month_tot_lm
    ,dcmtly.product_gross_revenue_month_tot AS product_gross_revenue_month_tot_ly
    ,dcbc.product_net_revenue AS product_net_revenue
    ,dcm.product_net_revenue_mtd AS product_net_revenue_mtd
    ,dcmly.product_net_revenue_mtd AS product_net_revenue_mtd_ly
    ,dcmtlm.product_net_revenue_month_tot AS product_net_revenue_month_tot_lm
    ,dcmtly.product_net_revenue_month_tot AS product_net_revenue_month_tot_ly
    ,dcbc.product_margin_pre_return AS product_margin_pre_return
    ,dcm.product_margin_pre_return_mtd AS product_margin_pre_return_mtd
    ,dcmly.product_margin_pre_return_mtd AS product_margin_pre_return_mtd_ly
    ,dcmtlm.product_margin_pre_return_month_tot AS product_margin_pre_return_month_tot_lm
    ,dcmtly.product_margin_pre_return_month_tot AS product_margin_pre_return_month_tot_ly
    ,dcbc.product_gross_profit AS product_gross_profit
    ,dcm.product_gross_profit_mtd AS product_gross_profit_mtd
    ,dcmly.product_gross_profit_mtd AS product_gross_profit_mtd_ly
    ,dcmtlm.product_gross_profit_month_tot AS product_gross_profit_month_tot_lm
    ,dcmtly.product_gross_profit_month_tot AS product_gross_profit_month_tot_ly
    ,dcbc.product_order_cash_gross_revenue_amount AS product_order_cash_gross_revenue_amount
    ,dcm.product_order_cash_gross_revenue_amount_mtd AS product_order_cash_gross_revenue_amount_mtd
    ,dcmly.product_order_cash_gross_revenue_amount_mtd AS product_order_cash_gross_revenue_amount_mtd_ly
    ,dcmtlm.product_order_cash_gross_revenue_amount_month_tot AS product_order_cash_gross_revenue_amount_month_tot_lm
    ,dcmtly.product_order_cash_gross_revenue_amount_month_tot AS product_order_cash_gross_revenue_amount_month_tot_ly
    ,dcbc.cash_gross_revenue AS cash_gross_revenue
    ,dcm.cash_gross_revenue_mtd AS cash_gross_revenue_mtd
    ,dcmly.cash_gross_revenue_mtd AS cash_gross_revenue_mtd_ly
    ,dcmtlm.cash_gross_revenue_month_tot AS cash_gross_revenue_month_tot_lm
    ,dcmtly.cash_gross_revenue_month_tot AS cash_gross_revenue_month_tot_ly
    ,dcbc.cash_net_revenue AS cash_net_revenue
    ,dcm.cash_net_revenue_mtd AS cash_net_revenue_mtd
    ,dcmly.cash_net_revenue_mtd AS cash_net_revenue_mtd_ly
    ,dcmtlm.cash_net_revenue_month_tot AS cash_net_revenue_month_tot_lm
    ,dcmtly.cash_net_revenue_month_tot AS cash_net_revenue_month_tot_ly
    ,dcbc.cash_net_revenue_budgeted_fx AS cash_net_revenue_budgeted_fx
    ,dcm.cash_net_revenue_budgeted_fx_mtd AS cash_net_revenue_budgeted_fx_mtd
    ,dcmly.cash_net_revenue_budgeted_fx_mtd AS cash_net_revenue_budgeted_fx_mtd_ly
    ,dcmtlm.cash_net_revenue_budgeted_fx_month_tot AS cash_net_revenue_budgeted_fx_month_tot_lm
    ,dcmtly.cash_net_revenue_budgeted_fx_month_tot AS cash_net_revenue_budgeted_fx_month_tot_ly
    ,dcbc.cash_gross_profit AS cash_gross_profit
    ,dcm.cash_gross_profit_mtd AS cash_gross_profit_mtd
    ,dcmly.cash_gross_profit_mtd AS cash_gross_profit_mtd_ly
    ,dcmtlm.cash_gross_profit_month_tot AS cash_gross_profit_month_tot_lm
    ,dcmtly.cash_gross_profit_month_tot AS cash_gross_profit_month_tot_ly
    ,dcbc.billed_credit_cash_transaction_count AS billed_credit_cash_transaction_count
    ,dcm.billed_credit_cash_transaction_count_mtd AS billed_credit_cash_transaction_count_mtd
    ,dcmly.billed_credit_cash_transaction_count_mtd AS billed_credit_cash_transaction_count_mtd_ly
    ,dcmtlm.billed_credit_cash_transaction_count_month_tot AS billed_credit_cash_transaction_count_month_tot_lm
    ,dcmtly.billed_credit_cash_transaction_count_month_tot AS billed_credit_cash_transaction_count_month_tot_ly
    ,dcbc.on_retry_billed_credit_cash_transaction_count AS on_retry_billed_credit_cash_transaction_count
    ,dcm.on_retry_billed_credit_cash_transaction_count_mtd AS on_retry_billed_credit_cash_transaction_count_mtd
    ,dcmly.on_retry_billed_credit_cash_transaction_count_mtd AS on_retry_billed_credit_cash_transaction_count_mtd_ly
    ,dcmtlm.on_retry_billed_credit_cash_transaction_count_month_tot AS on_retry_billed_credit_cash_transaction_count_month_tot_lm
    ,dcmtly.on_retry_billed_credit_cash_transaction_count_month_tot AS on_retry_billed_credit_cash_transaction_count_month_tot_ly
    ,dcbc.billing_cash_refund_amount AS billing_cash_refund_amount
    ,dcm.billing_cash_refund_amount_mtd AS billing_cash_refund_amount_mtd
    ,dcmly.billing_cash_refund_amount_mtd AS billing_cash_refund_amount_mtd_ly
    ,dcmtlm.billing_cash_refund_amount_month_tot AS billing_cash_refund_amount_month_tot_lm
    ,dcmtly.billing_cash_refund_amount_month_tot AS billing_cash_refund_amount_month_tot_ly
    ,dcbc.billing_cash_chargeback_amount AS billing_cash_chargeback_amount
    ,dcm.billing_cash_chargeback_amount_mtd AS billing_cash_chargeback_amount_mtd
    ,dcmly.billing_cash_chargeback_amount_mtd AS billing_cash_chargeback_amount_mtd_ly
    ,dcmtlm.billing_cash_chargeback_amount_month_tot AS billing_cash_chargeback_amount_month_tot_lm
    ,dcmtly.billing_cash_chargeback_amount_month_tot AS billing_cash_chargeback_amount_month_tot_ly
    ,dcbc.billed_credit_cash_transaction_amount AS billed_credit_cash_transaction_amount
    ,dcm.billed_credit_cash_transaction_amount_mtd AS billed_credit_cash_transaction_amount_mtd
    ,dcmly.billed_credit_cash_transaction_amount_mtd AS billed_credit_cash_transaction_amount_mtd_ly
    ,dcmtlm.billed_credit_cash_transaction_amount_month_tot AS billed_credit_cash_transaction_amount_month_tot_lm
    ,dcmtly.billed_credit_cash_transaction_amount_month_tot AS billed_credit_cash_transaction_amount_month_tot_ly
    ,dcbc.billed_credit_cash_refund_chargeback_amount AS billed_credit_cash_refund_chargeback_amount
    ,dcm.billed_credit_cash_refund_chargeback_amount_mtd AS billed_credit_cash_refund_chargeback_amount_mtd
    ,dcmly.billed_credit_cash_refund_chargeback_amount_mtd AS billed_credit_cash_refund_chargeback_amount_mtd_ly
    ,dcmtlm.billed_credit_cash_refund_chargeback_amount_month_tot AS billed_credit_cash_refund_chargeback_amount_month_tot_lm
    ,dcmtly.billed_credit_cash_refund_chargeback_amount_month_tot AS billed_credit_cash_refund_chargeback_amount_month_tot_ly
    ,dcbc.billed_cash_credit_redeemed_amount AS billed_cash_credit_redeemed_amount
    ,dcm.billed_cash_credit_redeemed_amount_mtd AS billed_cash_credit_redeemed_amount_mtd
    ,dcmly.billed_cash_credit_redeemed_amount_mtd AS billed_cash_credit_redeemed_amount_mtd_ly
    ,dcmtlm.billed_cash_credit_redeemed_amount_month_tot AS billed_cash_credit_redeemed_amount_month_tot_lm
    ,dcmtly.billed_cash_credit_redeemed_amount_month_tot AS billed_cash_credit_redeemed_amount_month_tot_ly
    ,dcbc.product_order_misc_cogs_amount AS product_order_misc_cogs_amount
    ,dcm.product_order_misc_cogs_amount_mtd AS product_order_misc_cogs_amount_mtd
    ,dcmly.product_order_misc_cogs_amount_mtd AS product_order_misc_cogs_amount_mtd_ly
    ,dcmtlm.product_order_misc_cogs_amount_month_tot AS product_order_misc_cogs_amount_month_tot_lm
    ,dcmtly.product_order_misc_cogs_amount_month_tot AS product_order_misc_cogs_amount_month_tot_ly
    ,dcbc.product_order_cash_gross_profit AS product_order_cash_gross_profit
    ,dcm.product_order_cash_gross_profit_mtd AS product_order_cash_gross_profit_mtd
    ,dcmly.product_order_cash_gross_profit_mtd AS product_order_cash_gross_profit_mtd_ly
    ,dcmtlm.product_order_cash_gross_profit_month_tot AS product_order_cash_gross_profit_month_tot_lm
    ,dcmtly.product_order_cash_gross_profit_month_tot AS product_order_cash_gross_profit_month_tot_ly
    ,dcbc.billed_cash_credit_redeemed_same_month_amount AS billed_cash_credit_redeemed_same_month_amount
    ,dcm.billed_cash_credit_redeemed_same_month_amount_mtd AS billed_cash_credit_redeemed_same_month_amount_mtd
    ,dcmly.billed_cash_credit_redeemed_same_month_amount_mtd AS billed_cash_credit_redeemed_same_month_amount_mtd_ly
    ,dcmtlm.billed_cash_credit_redeemed_same_month_amount_month_tot AS billed_cash_credit_redeemed_same_month_amount_month_tot_lm
    ,dcmtly.billed_cash_credit_redeemed_same_month_amount_month_tot AS billed_cash_credit_redeemed_same_month_amount_month_tot_ly
    ,dcbc.billed_credit_cash_refund_count AS billed_credit_cash_refund_count
    ,dcm.billed_credit_cash_refund_count_mtd AS billed_credit_cash_refund_count_mtd
    ,dcmly.billed_credit_cash_refund_count_mtd AS billed_credit_cash_refund_count_mtd_ly
    ,dcmtlm.billed_credit_cash_refund_count_month_tot AS billed_credit_cash_refund_count_month_tot_lm
    ,dcmtly.billed_credit_cash_refund_count_month_tot AS billed_credit_cash_refund_count_month_tot_ly
    ,dcbc.cash_variable_contribution_profit AS cash_variable_contribution_profit
    ,dcm.cash_variable_contribution_profit_mtd AS cash_variable_contribution_profit_mtd
    ,dcmly.cash_variable_contribution_profit_mtd AS cash_variable_contribution_profit_mtd_ly
    ,dcmtlm.cash_variable_contribution_profit_month_tot AS cash_variable_contribution_profit_month_tot_lm
    ,dcmtly.cash_variable_contribution_profit_month_tot AS cash_variable_contribution_profit_month_tot_ly
    ,dcbc.billed_cash_credit_redeemed_equivalent_count AS billed_cash_credit_redeemed_equivalent_count
    ,dcm.billed_cash_credit_redeemed_equivalent_count_mtd AS billed_cash_credit_redeemed_equivalent_count_mtd
    ,dcmly.billed_cash_credit_redeemed_equivalent_count_mtd AS billed_cash_credit_redeemed_equivalent_count_mtd_ly
    ,dcmtlm.billed_cash_credit_redeemed_equivalent_count_month_tot AS billed_cash_credit_redeemed_equivalent_count_month_tot_lm
    ,dcmtly.billed_cash_credit_redeemed_equivalent_count_month_tot AS billed_cash_credit_redeemed_equivalent_count_month_tot_ly
    ,dcbc.billed_cash_credit_cancelled_equivalent_count AS billed_cash_credit_cancelled_equivalent_count
    ,dcm.billed_cash_credit_cancelled_equivalent_count_mtd AS billed_cash_credit_cancelled_equivalent_count_mtd
    ,dcmly.billed_cash_credit_cancelled_equivalent_count_mtd AS billed_cash_credit_cancelled_equivalent_count_mtd_ly
    ,dcmtlm.billed_cash_credit_cancelled_equivalent_count_month_tot AS billed_cash_credit_cancelled_equivalent_count_month_tot_lm
    ,dcmtly.billed_cash_credit_cancelled_equivalent_count_month_tot AS billed_cash_credit_cancelled_equivalent_count_month_tot_ly
    ,dcbc.product_order_product_subtotal_amount AS product_order_product_subtotal_amount
    ,dcm.product_order_product_subtotal_amount_mtd AS product_order_product_subtotal_amount_mtd
    ,dcmly.product_order_product_subtotal_amount_mtd AS product_order_product_subtotal_amount_mtd_ly
    ,dcmtlm.product_order_product_subtotal_amount_month_tot AS product_order_product_subtotal_amount_month_tot_lm
    ,dcmtly.product_order_product_subtotal_amount_month_tot AS product_order_product_subtotal_amount_month_tot_ly
    ,dcbc.leads AS leads
    ,dcm.leads_mtd AS leads_mtd
    ,dcmly.leads_mtd AS leads_mtd_ly
    ,dcmtlm.leads_month_tot AS leads_month_tot_lm
    ,dcmtly.leads_month_tot AS leads_month_tot_ly
    ,dcbc.primary_leads AS primary_leads
    ,dcm.primary_leads_mtd AS primary_leads_mtd
    ,dcmly.primary_leads_mtd AS primary_leads_mtd_ly
    ,dcmtlm.primary_leads_month_tot AS primary_leads_month_tot_lm
    ,dcmtly.primary_leads_month_tot AS primary_leads_month_tot_ly
    ,dcbc.reactivated_leads AS reactivated_leads
    ,dcm.reactivated_leads_mtd AS reactivated_leads_mtd
    ,dcmly.reactivated_leads_mtd AS reactivated_leads_mtd_ly
    ,dcmtlm.reactivated_leads_month_tot AS reactivated_leads_month_tot_lm
    ,dcmtly.reactivated_leads_month_tot AS reactivated_leads_month_tot_ly
    ,dcbc.new_vips AS new_vips
    ,dcm.new_vips_mtd AS new_vips_mtd
    ,dcmly.new_vips_mtd AS new_vips_mtd_ly
    ,dcmtlm.new_vips_month_tot AS new_vips_month_tot_lm
    ,dcmtly.new_vips_month_tot AS new_vips_month_tot_ly
    ,dcbc.reactivated_vips AS reactivated_vips
    ,dcm.reactivated_vips_mtd AS reactivated_vips_mtd
    ,dcmly.reactivated_vips_mtd AS reactivated_vips_mtd_ly
    ,dcmtlm.reactivated_vips_month_tot AS reactivated_vips_month_tot_lm
    ,dcmtly.reactivated_vips_month_tot AS reactivated_vips_month_tot_ly
    ,dcbc.vips_from_reactivated_leads_m1 AS vips_from_reactivated_leads_m1
    ,dcm.vips_from_reactivated_leads_m1_mtd AS vips_from_reactivated_leads_m1_mtd
    ,dcmly.vips_from_reactivated_leads_m1_mtd AS vips_from_reactivated_leads_m1_mtd_ly
    ,dcmtlm.vips_from_reactivated_leads_m1_month_tot AS vips_from_reactivated_leads_m1_month_tot_lm
    ,dcmtly.vips_from_reactivated_leads_m1_month_tot AS vips_from_reactivated_leads_m1_month_tot_ly
    ,dcbc.paid_vips AS paid_vips
    ,dcm.paid_vips_mtd AS paid_vips_mtd
    ,dcmly.paid_vips_mtd AS paid_vips_mtd_ly
    ,dcmtlm.paid_vips_month_tot AS paid_vips_month_tot_lm
    ,dcmtly.paid_vips_month_tot AS paid_vips_month_tot_ly
    ,dcbc.unpaid_vips AS unpaid_vips
    ,dcm.unpaid_vips_mtd AS unpaid_vips_mtd
    ,dcmly.unpaid_vips_mtd AS unpaid_vips_mtd_ly
    ,dcmtlm.unpaid_vips_month_tot AS unpaid_vips_month_tot_lm
    ,dcmtly.unpaid_vips_month_tot AS unpaid_vips_month_tot_ly
    ,dcbc.new_vips_m1 AS new_vips_m1
    ,dcm.new_vips_m1_mtd AS new_vips_m1_mtd
    ,dcmly.new_vips_m1_mtd AS new_vips_m1_mtd_ly
    ,dcmtlm.new_vips_m1_month_tot AS new_vips_m1_month_tot_lm
    ,dcmtly.new_vips_m1_month_tot AS new_vips_m1_month_tot_ly
    ,dcbc.paid_vips_m1 AS paid_vips_m1
    ,dcm.paid_vips_m1_mtd AS paid_vips_m1_mtd
    ,dcmly.paid_vips_m1_mtd AS paid_vips_m1_mtd_ly
    ,dcmtlm.paid_vips_m1_month_tot AS paid_vips_m1_month_tot_lm
    ,dcmtly.paid_vips_m1_month_tot AS paid_vips_m1_month_tot_ly
    ,dcbc.cancels AS cancels
    ,dcm.cancels_mtd AS cancels_mtd
    ,dcmly.cancels_mtd AS cancels_mtd_ly
    ,dcmtlm.cancels_month_tot AS cancels_month_tot_lm
    ,dcmtly.cancels_month_tot AS cancels_month_tot_ly
    ,dcbc.m1_cancels AS m1_cancels
    ,dcm.m1_cancels_mtd AS m1_cancels_mtd
    ,dcmly.m1_cancels_mtd AS m1_cancels_mtd_ly
    ,dcmtlm.m1_cancels_month_tot AS m1_cancels_month_tot_lm
    ,dcmtly.m1_cancels_month_tot AS m1_cancels_month_tot_ly
    ,dcbc.bop_vips AS bop_vips
    ,dcm.bop_vips_mtd AS bop_vips_mtd
    ,dcmly.bop_vips_mtd AS bop_vips_mtd_ly
    ,dcmtlm.bop_vips_month_tot AS bop_vips_month_tot_lm
    ,dcmtly.bop_vips_month_tot AS bop_vips_month_tot_ly
    ,dcbc.media_spend AS media_spend
    ,dcm.media_spend_mtd AS media_spend_mtd
    ,dcmly.media_spend_mtd AS media_spend_mtd_ly
    ,dcmtlm.media_spend_month_tot AS media_spend_month_tot_lm
    ,dcmtly.media_spend_month_tot AS media_spend_month_tot_ly
    ,dcbc.product_order_return_unit_count AS product_order_return_unit_count
    ,dcm.product_order_return_unit_count_mtd AS product_order_return_unit_count_mtd
    ,dcmly.product_order_return_unit_count_mtd AS product_order_return_unit_count_mtd_ly
    ,dcmtlm.product_order_return_unit_count_month_tot AS product_order_return_unit_count_month_tot_lm
    ,dcmtly.product_order_return_unit_count_month_tot AS product_order_return_unit_count_month_tot_ly
    ,dcbc.product_order_tariff_amount AS product_order_tariff_amount
    ,dcbc.product_order_shipping_discount_amount AS product_order_shipping_discount_amount
    ,dcbc.product_order_shipping_revenue_before_discount_amount AS product_order_shipping_revenue_before_discount_amount
    ,dcbc.product_order_tax_amount AS product_order_tax_amount
    ,dcbc.product_order_cash_transaction_amount AS product_order_cash_transaction_amount
    ,dcbc.product_order_cash_credit_redeemed_amount AS product_order_cash_credit_redeemed_amount
    ,dcbc.product_order_loyalty_unit_count AS product_order_loyalty_unit_count
    ,dcbc.product_order_outfit_component_unit_count AS product_order_outfit_component_unit_count
    ,dcbc.product_order_outfit_count AS product_order_outfit_count
    ,dcbc.product_order_discounted_unit_count AS product_order_discounted_unit_count
    ,dcbc.product_order_zero_revenue_unit_count AS product_order_zero_revenue_unit_count

    ,dcbc.product_order_cash_credit_refund_amount AS product_order_cash_credit_refund_amount
    ,dcm.product_order_cash_credit_refund_amount_mtd AS product_order_cash_credit_refund_amount_mtd
    ,dcmly.product_order_cash_credit_refund_amount_mtd AS product_order_cash_credit_refund_amount_mtd_ly
    ,dcmtlm.product_order_cash_credit_refund_amount_month_tot AS product_order_cash_credit_refund_amount_month_tot_lm
    ,dcmtly.product_order_cash_credit_refund_amount_month_tot AS product_order_cash_credit_refund_amount_month_tot_ly

    ,dcbc.product_order_noncash_credit_refund_amount AS product_order_noncash_credit_refund_amount
    ,dcm.product_order_noncash_credit_refund_amount_mtd AS product_order_noncash_credit_refund_amount_mtd
    ,dcmly.product_order_noncash_credit_refund_amount_mtd AS product_order_noncash_credit_refund_amount_mtd_ly
    ,dcmtlm.product_order_noncash_credit_refund_amount_month_tot AS product_order_noncash_credit_refund_amount_month_tot_lm
    ,dcmtly.product_order_noncash_credit_refund_amount_month_tot AS product_order_noncash_credit_refund_amount_month_tot_ly

    ,dcbc.product_order_exchange_direct_cogs_amount AS product_order_exchange_direct_cogs_amount
    ,dcbc.product_order_selling_expenses_amount AS product_order_selling_expenses_amount
    ,dcbc.product_order_payment_processing_cost_amount AS product_order_payment_processing_cost_amount
    ,dcbc.product_order_variable_gms_cost_amount AS product_order_variable_gms_cost_amount
    ,dcbc.product_order_variable_warehouse_cost_amount AS product_order_variable_warehouse_cost_amount
    ,dcbc.billing_selling_expenses_amount AS billing_selling_expenses_amount
    ,dcbc.billing_payment_processing_cost_amount AS billing_payment_processing_cost_amount
    ,dcbc.billing_variable_gms_cost_amount AS billing_variable_gms_cost_amount
    ,dcbc.product_order_amount_to_pay AS product_order_amount_to_pay
    ,dcbc.product_gross_revenue_excl_shipping AS product_gross_revenue_excl_shipping
    ,dcm.product_gross_revenue_excl_shipping_mtd AS product_gross_revenue_excl_shipping_mtd
    ,dcmly.product_gross_revenue_excl_shipping_mtd AS product_gross_revenue_excl_shipping_mtd_ly
    ,dcmtlm.product_gross_revenue_excl_shipping_month_tot AS product_gross_revenue_excl_shipping_month_tot_lm
    ,dcmtly.product_gross_revenue_excl_shipping_month_tot AS product_gross_revenue_excl_shipping_month_tot_ly
    ,dcbc.product_margin_pre_return_excl_shipping AS product_margin_pre_return_excl_shipping
    ,dcbc.product_variable_contribution_profit AS product_variable_contribution_profit
    ,dcbc.product_order_cash_net_revenue AS product_order_cash_net_revenue
    ,dcbc.product_order_cash_margin_pre_return AS product_order_cash_margin_pre_return
    ,dcbc.billing_cash_gross_revenue AS billing_cash_gross_revenue
    ,dcbc.billing_cash_net_revenue AS billing_cash_net_revenue
    ,dcbc.billing_order_transaction_count AS billing_order_transaction_count
    ,dcm.billing_order_transaction_count_mtd AS billing_order_transaction_count_mtd
    ,dcmly.billing_order_transaction_count_mtd AS billing_order_transaction_count_mtd_ly
    ,dcmtlm.billing_order_transaction_count_month_tot AS billing_order_transaction_count_month_tot_lm
    ,dcmtly.billing_order_transaction_count_month_tot AS billing_order_transaction_count_month_tot_ly
    ,dcbc.membership_fee_cash_transaction_amount AS membership_fee_cash_transaction_amount
    ,dcm.membership_fee_cash_transaction_amount_mtd AS membership_fee_cash_transaction_amount_mtd
    ,dcmly.membership_fee_cash_transaction_amount_mtd AS membership_fee_cash_transaction_amount_mtd_ly
    ,dcmtlm.membership_fee_cash_transaction_amount_month_tot AS membership_fee_cash_transaction_amount_month_tot_lm
    ,dcmtly.membership_fee_cash_transaction_amount_month_tot AS membership_fee_cash_transaction_amount_month_tot_ly
    ,dcbc.gift_card_transaction_amount AS gift_card_transaction_amount
    ,dcm.gift_card_transaction_amount_mtd AS gift_card_transaction_amount_mtd
    ,dcmly.gift_card_transaction_amount_mtd AS gift_card_transaction_amount_mtd_ly
    ,dcmtlm.gift_card_transaction_amount_month_tot AS gift_card_transaction_amount_month_tot_lm
    ,dcmtly.gift_card_transaction_amount_month_tot AS gift_card_transaction_amount_month_tot_ly
    ,dcbc.legacy_credit_cash_transaction_amount AS legacy_credit_cash_transaction_amount
    ,dcm.legacy_credit_cash_transaction_amount_mtd AS legacy_credit_cash_transaction_amount_mtd
    ,dcmly.legacy_credit_cash_transaction_amount_mtd AS legacy_credit_cash_transaction_amount_mtd_ly
    ,dcmtlm.legacy_credit_cash_transaction_amount_month_tot AS legacy_credit_cash_transaction_amount_month_tot_lm
    ,dcmtly.legacy_credit_cash_transaction_amount_month_tot AS legacy_credit_cash_transaction_amount_month_tot_ly
    ,dcbc.membership_fee_cash_refund_chargeback_amount AS membership_fee_cash_refund_chargeback_amount
    ,dcm.membership_fee_cash_refund_chargeback_amount_mtd AS membership_fee_cash_refund_chargeback_amount_mtd
    ,dcmly.membership_fee_cash_refund_chargeback_amount_mtd AS membership_fee_cash_refund_chargeback_amount_mtd_ly
    ,dcmtlm.membership_fee_cash_refund_chargeback_amount_month_tot AS membership_fee_cash_refund_chargeback_amount_month_tot_lm
    ,dcmtly.membership_fee_cash_refund_chargeback_amount_month_tot AS membership_fee_cash_refund_chargeback_amount_month_tot_ly
    ,dcbc.gift_card_cash_refund_chargeback_amount AS gift_card_cash_refund_chargeback_amount
    ,dcm.gift_card_cash_refund_chargeback_amount_mtd AS gift_card_cash_refund_chargeback_amount_mtd
    ,dcmly.gift_card_cash_refund_chargeback_amount_mtd AS gift_card_cash_refund_chargeback_amount_mtd_ly
    ,dcmtlm.gift_card_cash_refund_chargeback_amount_month_tot AS gift_card_cash_refund_chargeback_amount_month_tot_lm
    ,dcmtly.gift_card_cash_refund_chargeback_amount_month_tot AS gift_card_cash_refund_chargeback_amount_month_tot_ly
    ,dcbc.legacy_credit_cash_refund_chargeback_amount AS legacy_credit_cash_refund_chargeback_amount
    ,dcm.legacy_credit_cash_refund_chargeback_amount_mtd AS legacy_credit_cash_refund_chargeback_amount_mtd
    ,dcmly.legacy_credit_cash_refund_chargeback_amount_mtd AS legacy_credit_cash_refund_chargeback_amount_mtd_ly
    ,dcmtlm.legacy_credit_cash_refund_chargeback_amount_month_tot AS legacy_credit_cash_refund_chargeback_amount_month_tot_lm
    ,dcmtly.legacy_credit_cash_refund_chargeback_amount_month_tot AS legacy_credit_cash_refund_chargeback_amount_month_tot_ly
    ,dcbc.billed_cash_credit_issued_amount AS billed_cash_credit_issued_amount
    ,dcbc.billed_cash_credit_cancelled_amount AS billed_cash_credit_cancelled_amount
    ,dcbc.billed_cash_credit_expired_amount AS billed_cash_credit_expired_amount
    ,dcbc.billed_cash_credit_issued_equivalent_count AS billed_cash_credit_issued_equivalent_count
    ,dcm.billed_cash_credit_issued_equivalent_count_mtd AS billed_cash_credit_issued_equivalent_count_mtd
    ,dcmly.billed_cash_credit_issued_equivalent_count_mtd AS billed_cash_credit_issued_equivalent_count_mtd_ly
    ,dcmtlm.billed_cash_credit_issued_equivalent_count_month_tot AS billed_cash_credit_issued_equivalent_count_month_tot_lm
    ,dcmtly.billed_cash_credit_issued_equivalent_count_month_tot AS billed_cash_credit_issued_equivalent_count_month_tot_ly
    ,dcbc.billed_cash_credit_expired_equivalent_count AS billed_cash_credit_expired_equivalent_count
    ,dcbc.refund_cash_credit_issued_amount AS refund_cash_credit_issued_amount
    ,dcm.refund_cash_credit_issued_amount_mtd AS refund_cash_credit_issued_amount_mtd
    ,dcmly.refund_cash_credit_issued_amount_mtd AS refund_cash_credit_issued_amount_mtd_ly
    ,dcmtlm.refund_cash_credit_issued_amount_month_tot AS refund_cash_credit_issued_amount_month_tot_lm
    ,dcmtly.refund_cash_credit_issued_amount_month_tot AS refund_cash_credit_issued_amount_month_tot_ly

    ,dcbc.refund_cash_credit_redeemed_amount AS refund_cash_credit_redeemed_amount
    ,dcm.refund_cash_credit_redeemed_amount_mtd AS refund_cash_credit_redeemed_amount_mtd
    ,dcmly.refund_cash_credit_redeemed_amount_mtd AS refund_cash_credit_redeemed_amount_mtd_ly
    ,dcmtlm.refund_cash_credit_redeemed_amount_month_tot AS refund_cash_credit_redeemed_amount_month_tot_lm
    ,dcmtly.refund_cash_credit_redeemed_amount_month_tot AS refund_cash_credit_redeemed_amount_month_tot_ly

    ,dcbc.refund_cash_credit_cancelled_amount AS refund_cash_credit_cancelled_amount
    ,dcm.refund_cash_credit_cancelled_amount_mtd AS refund_cash_credit_cancelled_amount_mtd
    ,dcmly.refund_cash_credit_cancelled_amount_mtd AS refund_cash_credit_cancelled_amount_mtd_ly
    ,dcmtlm.refund_cash_credit_cancelled_amount_month_tot AS refund_cash_credit_cancelled_amount_month_tot_lm
    ,dcmtly.refund_cash_credit_cancelled_amount_month_tot AS refund_cash_credit_cancelled_amount_month_tot_ly
    ,dcbc.refund_cash_credit_expired_amount AS refund_cash_credit_expired_amount
    ,dcbc.other_cash_credit_issued_amount AS other_cash_credit_issued_amount
    ,dcbc.other_cash_credit_redeemed_amount AS other_cash_credit_redeemed_amount
    ,dcm.other_cash_credit_redeemed_amount_mtd AS other_cash_credit_redeemed_amount_mtd
    ,dcmly.other_cash_credit_redeemed_amount_mtd AS other_cash_credit_redeemed_amount_mtd_ly
    ,dcmtlm.other_cash_credit_redeemed_amount_month_tot AS other_cash_credit_redeemed_amount_month_tot_lm
    ,dcmtly.other_cash_credit_redeemed_amount_month_tot AS other_cash_credit_redeemed_amount_month_tot_ly
    ,dcbc.other_cash_credit_cancelled_amount AS other_cash_credit_cancelled_amount
    ,dcbc.other_cash_credit_expired_amount AS other_cash_credit_expired_amount
    ,dcbc.cash_gift_card_redeemed_amount AS cash_gift_card_redeemed_amount
    ,dcbc.noncash_credit_issued_amount AS noncash_credit_issued_amount
    ,dcm.noncash_credit_issued_amount_mtd AS noncash_credit_issued_amount_mtd
    ,dcmly.noncash_credit_issued_amount_mtd AS noncash_credit_issued_amount_mtd_ly
    ,dcmtlm.noncash_credit_issued_amount_month_tot AS noncash_credit_issued_amount_month_tot_lm
    ,dcmtly.noncash_credit_issued_amount_month_tot AS noncash_credit_issued_amount_month_tot_ly
    ,dcbc.noncash_credit_cancelled_amount AS noncash_credit_cancelled_amount
    ,dcbc.noncash_credit_expired_amount AS noncash_credit_expired_amount
    ,dcbc.merch_purchase_count AS merch_purchase_count
    ,dcm.merch_purchase_count_mtd AS merch_purchase_count_mtd
    ,dcmly.merch_purchase_count_mtd AS merch_purchase_count_mtd_ly
    ,dcmtlm.merch_purchase_count_month_tot AS merch_purchase_count_month_tot_lm
    ,dcmtly.merch_purchase_count_month_tot AS merch_purchase_count_month_tot_ly
    ,dcbc.merch_purchase_hyperion_count AS merch_purchase_hyperion_count
    ,dcm.merch_purchase_hyperion_count_mtd AS merch_purchase_hyperion_count_mtd
    ,dcmly.merch_purchase_hyperion_count_mtd AS merch_purchase_hyperion_count_mtd_ly
    ,dcmtlm.merch_purchase_hyperion_count_month_tot AS merch_purchase_hyperion_count_month_tot_lm
    ,dcmtly.merch_purchase_hyperion_count_month_tot AS merch_purchase_hyperion_count_month_tot_ly
    ,dcbc.product_order_non_token_subtotal_excl_tariff_amount AS product_order_non_token_subtotal_excl_tariff_amount
    ,dcm.product_order_non_token_subtotal_excl_tariff_amount_mtd AS product_order_non_token_subtotal_excl_tariff_amount_mtd
    ,dcmly.product_order_non_token_subtotal_excl_tariff_amount_mtd AS product_order_non_token_subtotal_excl_tariff_amount_mtd_ly
    ,dcmtlm.product_order_non_token_subtotal_excl_tariff_amount_month_tot AS product_order_non_token_subtotal_excl_tariff_amount_month_tot_lm
    ,dcmtly.product_order_non_token_subtotal_excl_tariff_amount_month_tot AS product_order_non_token_subtotal_excl_tariff_amount_month_tot_ly
    ,dcbc.product_order_non_token_unit_count AS product_order_non_token_unit_count
    ,dcm.product_order_non_token_unit_count_mtd AS product_order_non_token_unit_count_mtd
    ,dcmly.product_order_non_token_unit_count_mtd AS product_order_non_token_unit_count_mtd_ly
    ,dcmtlm.product_order_non_token_unit_count_month_tot AS product_order_non_token_unit_count_month_tot_lm
    ,dcmtly.product_order_non_token_unit_count_month_tot AS product_order_non_token_unit_count_month_tot_ly
FROM _daily_cash_base_calc AS dcbc
LEFT JOIN reporting.daily_cash_month_tot AS dcmtlm
    ON dateadd(month,-1,date_trunc('month',dcbc.date)) = dcmtlm.month_date
    AND dcbc.date_object = dcmtlm.date_object
    AND dcbc.currency_object = dcmtlm.currency_object
    AND dcbc.currency_type = dcmtlm.currency_type
    AND dcbc.report_mapping = dcmtlm.report_mapping
    AND dcbc.order_membership_classification_key = dcmtlm.order_membership_classification_key
LEFT JOIN reporting.daily_cash_month_tot AS dcmtly
    ON dateadd(year,-1,date_trunc('month',dcbc.date)) = dcmtly.month_date
    AND dcbc.date_object = dcmtly.date_object
    AND dcbc.currency_object = dcmtly.currency_object
    AND dcbc.currency_type = dcmtly.currency_type
    AND dcbc.report_mapping = dcmtly.report_mapping
    AND dcbc.order_membership_classification_key = dcmtly.order_membership_classification_key
LEFT JOIN reporting.daily_cash_mtd AS dcm
    ON dcbc.date = dcm.date
    AND dcbc.date_object = dcm.date_object
    AND dcbc.currency_object = dcm.currency_object
    AND dcbc.currency_type = dcm.currency_type
    AND dcbc.report_mapping = dcm.report_mapping
    AND dcbc.order_membership_classification_key = dcm.order_membership_classification_key
LEFT JOIN reporting.daily_cash_mtd AS dcmly
    ON dateadd(year,-1,dcbc.date) = dcmly.date
    AND dcbc.date_object = dcmly.date_object
    AND dcbc.currency_object = dcmly.currency_object
    AND dcbc.currency_type = dcmly.currency_type
    AND dcbc.report_mapping = dcmly.report_mapping
    AND dcbc.order_membership_classification_key = dcmly.order_membership_classification_key;

CREATE OR REPLACE TEMPORARY TABLE _dcbc_j_agg AS
SELECT
/*Daily Cash Version*/
    dcbcj.date
    ,dcbcj.date_object
    ,dcbcj.currency_object
    ,dcbcj.currency_type
/*Segment*/
    ,dcbcj.store_brand
    ,dcbcj.business_unit
    ,dcbcj.report_mapping
    ,dcbcj.is_daily_cash_usd
    ,dcbcj.is_daily_cash_eur
/*Measures*/
    ,SUM(dcbcj.product_order_subtotal_excl_tariff_amount) AS product_order_subtotal_excl_tariff_amount
    ,SUM(dcbcj.product_order_subtotal_excl_tariff_amount_mtd) AS product_order_subtotal_excl_tariff_amount_mtd
    ,SUM(dcbcj.product_order_subtotal_excl_tariff_amount_mtd_ly) AS product_order_subtotal_excl_tariff_amount_mtd_ly
    ,SUM(dcbcj.product_order_subtotal_excl_tariff_amount_month_tot_lm) AS product_order_subtotal_excl_tariff_amount_month_tot_lm
    ,SUM(dcbcj.product_order_subtotal_excl_tariff_amount_month_tot_ly) AS product_order_subtotal_excl_tariff_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_subtotal_excl_tariff_amount,0)) AS activating_product_order_subtotal_excl_tariff_amount
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_subtotal_excl_tariff_amount_mtd,0)) AS activating_product_order_subtotal_excl_tariff_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_subtotal_excl_tariff_amount_mtd_ly,0)) AS activating_product_order_subtotal_excl_tariff_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_subtotal_excl_tariff_amount_month_tot_lm,0)) AS activating_product_order_subtotal_excl_tariff_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_subtotal_excl_tariff_amount_month_tot_ly,0)) AS activating_product_order_subtotal_excl_tariff_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_subtotal_excl_tariff_amount,0)) AS nonactivating_product_order_subtotal_excl_tariff_amount
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_subtotal_excl_tariff_amount_mtd,0)) AS nonactivating_product_order_subtotal_excl_tariff_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_subtotal_excl_tariff_amount_mtd_ly,0)) AS nonactivating_product_order_subtotal_excl_tariff_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_subtotal_excl_tariff_amount_month_tot_lm,0)) AS nonactivating_product_order_subtotal_excl_tariff_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_subtotal_excl_tariff_amount_month_tot_ly,0)) AS nonactivating_product_order_subtotal_excl_tariff_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_subtotal_excl_tariff_amount,0)) AS guest_product_order_subtotal_excl_tariff_amount
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_subtotal_excl_tariff_amount_mtd,0)) AS guest_product_order_subtotal_excl_tariff_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_subtotal_excl_tariff_amount_mtd_ly,0)) AS guest_product_order_subtotal_excl_tariff_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_subtotal_excl_tariff_amount_month_tot_lm,0)) AS guest_product_order_subtotal_excl_tariff_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_subtotal_excl_tariff_amount_month_tot_ly,0)) AS guest_product_order_subtotal_excl_tariff_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_subtotal_excl_tariff_amount,0)) AS repeat_vip_product_order_subtotal_excl_tariff_amount
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_subtotal_excl_tariff_amount_mtd,0)) AS repeat_vip_product_order_subtotal_excl_tariff_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_subtotal_excl_tariff_amount_mtd_ly,0)) AS repeat_vip_product_order_subtotal_excl_tariff_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_subtotal_excl_tariff_amount_month_tot_lm,0)) AS repeat_vip_product_order_subtotal_excl_tariff_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_subtotal_excl_tariff_amount_month_tot_ly,0)) AS repeat_vip_product_order_subtotal_excl_tariff_amount_month_tot_ly

    ,SUM(dcbcj.product_order_product_subtotal_amount) AS product_order_product_subtotal_amount
    ,SUM(dcbcj.product_order_product_subtotal_amount_mtd) AS product_order_product_subtotal_amount_mtd
    ,SUM(dcbcj.product_order_product_subtotal_amount_mtd_ly) AS product_order_product_subtotal_amount_mtd_ly
    ,SUM(dcbcj.product_order_product_subtotal_amount_month_tot_lm) AS product_order_product_subtotal_amount_month_tot_lm
    ,SUM(dcbcj.product_order_product_subtotal_amount_month_tot_ly) AS product_order_product_subtotal_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_product_subtotal_amount,0)) AS activating_product_order_product_subtotal_amount
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_product_subtotal_amount_mtd,0)) AS activating_product_order_product_subtotal_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_product_subtotal_amount_mtd_ly,0)) AS activating_product_order_product_subtotal_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_product_subtotal_amount_month_tot_lm,0)) AS activating_product_order_product_subtotal_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_product_subtotal_amount_month_tot_ly,0)) AS activating_product_order_product_subtotal_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_product_subtotal_amount,0)) AS nonactivating_product_order_product_subtotal_amount
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_product_subtotal_amount_mtd,0)) AS nonactivating_product_order_product_subtotal_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_product_subtotal_amount_mtd_ly,0)) AS nonactivating_product_order_product_subtotal_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_product_subtotal_amount_month_tot_lm,0)) AS nonactivating_product_order_product_subtotal_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_product_subtotal_amount_month_tot_ly,0)) AS nonactivating_product_order_product_subtotal_amount_month_tot_ly

    ,SUM(dcbcj.product_order_product_discount_amount) AS product_order_product_discount_amount
    ,SUM(dcbcj.product_order_product_discount_amount_mtd) AS product_order_product_discount_amount_mtd
    ,SUM(dcbcj.product_order_product_discount_amount_mtd_ly) AS product_order_product_discount_amount_mtd_ly
    ,SUM(dcbcj.product_order_product_discount_amount_month_tot_lm) AS product_order_product_discount_amount_month_tot_lm
    ,SUM(dcbcj.product_order_product_discount_amount_month_tot_ly) AS product_order_product_discount_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_product_discount_amount,0)) AS activating_product_order_product_discount_amount
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_product_discount_amount_mtd,0)) AS activating_product_order_product_discount_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_product_discount_amount_mtd_ly,0)) AS activating_product_order_product_discount_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_product_discount_amount_month_tot_lm,0)) AS activating_product_order_product_discount_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_product_discount_amount_month_tot_ly,0)) AS activating_product_order_product_discount_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_product_discount_amount,0)) AS nonactivating_product_order_product_discount_amount
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_product_discount_amount_mtd,0)) AS nonactivating_product_order_product_discount_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_product_discount_amount_mtd_ly,0)) AS nonactivating_product_order_product_discount_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_product_discount_amount_month_tot_lm,0)) AS nonactivating_product_order_product_discount_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_product_discount_amount_month_tot_ly,0)) AS nonactivating_product_order_product_discount_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_product_discount_amount,0)) AS guest_product_order_product_discount_amount
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_product_discount_amount_mtd,0)) AS guest_product_order_product_discount_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_product_discount_amount_mtd_ly,0)) AS guest_product_order_product_discount_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_product_discount_amount_month_tot_lm,0)) AS guest_product_order_product_discount_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_product_discount_amount_month_tot_ly,0)) AS guest_product_order_product_discount_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_product_discount_amount,0)) AS repeat_vip_product_order_product_discount_amount
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_product_discount_amount_mtd,0)) AS repeat_vip_product_order_product_discount_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_product_discount_amount_mtd_ly,0)) AS repeat_vip_product_order_product_discount_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_product_discount_amount_month_tot_lm,0)) AS repeat_vip_product_order_product_discount_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_product_discount_amount_month_tot_ly,0)) AS repeat_vip_product_order_product_discount_amount_month_tot_ly

    ,SUM(dcbcj.product_order_shipping_revenue_amount) AS product_order_shipping_revenue_amount
    ,SUM(dcbcj.product_order_shipping_revenue_amount_mtd) AS product_order_shipping_revenue_amount_mtd
    ,SUM(dcbcj.product_order_shipping_revenue_amount_mtd_ly) AS product_order_shipping_revenue_amount_mtd_ly
    ,SUM(dcbcj.product_order_shipping_revenue_amount_month_tot_lm) AS product_order_shipping_revenue_amount_month_tot_lm
    ,SUM(dcbcj.product_order_shipping_revenue_amount_month_tot_ly) AS product_order_shipping_revenue_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_shipping_revenue_amount,0)) AS activating_product_order_shipping_revenue_amount
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_shipping_revenue_amount_mtd,0)) AS activating_product_order_shipping_revenue_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_shipping_revenue_amount_mtd_ly,0)) AS activating_product_order_shipping_revenue_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_shipping_revenue_amount_month_tot_lm,0)) AS activating_product_order_shipping_revenue_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_shipping_revenue_amount_month_tot_ly,0)) AS activating_product_order_shipping_revenue_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_shipping_revenue_amount,0)) AS nonactivating_product_order_shipping_revenue_amount
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_shipping_revenue_amount_mtd,0)) AS nonactivating_product_order_shipping_revenue_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_shipping_revenue_amount_mtd_ly,0)) AS nonactivating_product_order_shipping_revenue_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_shipping_revenue_amount_month_tot_lm,0)) AS nonactivating_product_order_shipping_revenue_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_shipping_revenue_amount_month_tot_ly,0)) AS nonactivating_product_order_shipping_revenue_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_shipping_revenue_amount,0)) AS guest_product_order_shipping_revenue_amount
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_shipping_revenue_amount_mtd,0)) AS guest_product_order_shipping_revenue_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_shipping_revenue_amount_mtd_ly,0)) AS guest_product_order_shipping_revenue_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_shipping_revenue_amount_month_tot_lm,0)) AS guest_product_order_shipping_revenue_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_shipping_revenue_amount_month_tot_ly,0)) AS guest_product_order_shipping_revenue_amount_month_tot_ly

    ,SUM(dcbcj.product_order_noncash_credit_redeemed_amount) AS product_order_noncash_credit_redeemed_amount
    ,SUM(dcbcj.product_order_noncash_credit_redeemed_amount_mtd) AS product_order_noncash_credit_redeemed_amount_mtd
    ,SUM(dcbcj.product_order_noncash_credit_redeemed_amount_mtd_ly) AS product_order_noncash_credit_redeemed_amount_mtd_ly
    ,SUM(dcbcj.product_order_noncash_credit_redeemed_amount_month_tot_lm) AS product_order_noncash_credit_redeemed_amount_month_tot_lm
    ,SUM(dcbcj.product_order_noncash_credit_redeemed_amount_month_tot_ly) AS product_order_noncash_credit_redeemed_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_noncash_credit_redeemed_amount,0)) AS activating_product_order_noncash_credit_redeemed_amount
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_noncash_credit_redeemed_amount_mtd,0)) AS activating_product_order_noncash_credit_redeemed_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_noncash_credit_redeemed_amount_mtd_ly,0)) AS activating_product_order_noncash_credit_redeemed_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_noncash_credit_redeemed_amount_month_tot_lm,0)) AS activating_product_order_noncash_credit_redeemed_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_noncash_credit_redeemed_amount_month_tot_ly,0)) AS activating_product_order_noncash_credit_redeemed_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_noncash_credit_redeemed_amount,0)) AS nonactivating_product_order_noncash_credit_redeemed_amount
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_noncash_credit_redeemed_amount_mtd,0)) AS nonactivating_product_order_noncash_credit_redeemed_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_noncash_credit_redeemed_amount_mtd_ly,0)) AS nonactivating_product_order_noncash_credit_redeemed_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_noncash_credit_redeemed_amount_month_tot_lm,0)) AS nonactivating_product_order_noncash_credit_redeemed_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_noncash_credit_redeemed_amount_month_tot_ly,0)) AS nonactivating_product_order_noncash_credit_redeemed_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_noncash_credit_redeemed_amount,0)) AS repeat_vip_product_order_noncash_credit_redeemed_amount
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_noncash_credit_redeemed_amount_mtd,0)) AS repeat_vip_product_order_noncash_credit_redeemed_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_noncash_credit_redeemed_amount_mtd_ly,0)) AS repeat_vip_product_order_noncash_credit_redeemed_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_noncash_credit_redeemed_amount_month_tot_lm,0)) AS repeat_vip_product_order_noncash_credit_redeemed_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_noncash_credit_redeemed_amount_month_tot_ly,0)) AS repeat_vip_product_order_noncash_credit_redeemed_amount_month_tot_ly

    ,SUM(dcbcj.product_order_count) AS product_order_count
    ,SUM(dcbcj.product_order_count_mtd) AS product_order_count_mtd
    ,SUM(dcbcj.product_order_count_mtd_ly) AS product_order_count_mtd_ly
    ,SUM(dcbcj.product_order_count_month_tot_lm) AS product_order_count_month_tot_lm
    ,SUM(dcbcj.product_order_count_month_tot_ly) AS product_order_count_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_count,0)) AS activating_product_order_count
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_count_mtd,0)) AS activating_product_order_count_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_count_mtd_ly,0)) AS activating_product_order_count_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_count_month_tot_lm,0)) AS activating_product_order_count_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_count_month_tot_ly,0)) AS activating_product_order_count_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_count,0)) AS nonactivating_product_order_count
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_count_mtd,0)) AS nonactivating_product_order_count_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_count_mtd_ly,0)) AS nonactivating_product_order_count_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_count_month_tot_lm,0)) AS nonactivating_product_order_count_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_count_month_tot_ly,0)) AS nonactivating_product_order_count_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_count,0)) AS guest_product_order_count
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_count_mtd,0)) AS guest_product_order_count_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_count_mtd_ly,0)) AS guest_product_order_count_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_count_month_tot_lm,0)) AS guest_product_order_count_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_count_month_tot_ly,0)) AS guest_product_order_count_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_count,0)) AS repeat_vip_product_order_count
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_count_mtd,0)) AS repeat_vip_product_order_count_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_count_mtd_ly,0)) AS repeat_vip_product_order_count_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_count_month_tot_lm,0)) AS repeat_vip_product_order_count_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_count_month_tot_ly,0)) AS repeat_vip_product_order_count_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l4='Reactivated VIP',dcbcj.product_order_count,0)) AS reactivated_vip_product_order_count
    ,SUM(IFF(domc.membership_order_type_l4='Reactivated VIP',dcbcj.product_order_count_mtd,0)) AS reactivated_vip_product_order_count_mtd
    ,SUM(IFF(domc.membership_order_type_l4='Reactivated VIP',dcbcj.product_order_count_mtd_ly,0)) AS reactivated_vip_product_order_count_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l4='Reactivated VIP',dcbcj.product_order_count_month_tot_lm,0)) AS reactivated_vip_product_order_count_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l4='Reactivated VIP',dcbcj.product_order_count_month_tot_ly,0)) AS reactivated_vip_product_order_count_month_tot_ly

    ,SUM(dcbcj.product_order_count_excl_seeding) AS product_order_count_excl_seeding
    ,SUM(dcbcj.product_order_count_excl_seeding_mtd) AS product_order_count_excl_seeding_mtd
    ,SUM(dcbcj.product_order_count_excl_seeding_mtd_ly) AS product_order_count_excl_seeding_mtd_ly
    ,SUM(dcbcj.product_order_count_excl_seeding_month_tot_lm) AS product_order_count_excl_seeding_month_tot_lm
    ,SUM(dcbcj.product_order_count_excl_seeding_month_tot_ly) AS product_order_count_excl_seeding_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_count_excl_seeding,0)) AS activating_product_order_count_excl_seeding
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_count_excl_seeding_mtd,0)) AS activating_product_order_count_excl_seeding_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_count_excl_seeding_mtd_ly,0)) AS activating_product_order_count_excl_seeding_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_count_excl_seeding_month_tot_lm,0)) AS activating_product_order_count_excl_seeding_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_count_excl_seeding_month_tot_ly,0)) AS activating_product_order_count_excl_seeding_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_count_excl_seeding,0)) AS nonactivating_product_order_count_excl_seeding
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_count_excl_seeding_mtd,0)) AS nonactivating_product_order_count_excl_seeding_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_count_excl_seeding_mtd_ly,0)) AS nonactivating_product_order_count_excl_seeding_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_count_excl_seeding_month_tot_lm,0)) AS nonactivating_product_order_count_excl_seeding_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_count_excl_seeding_month_tot_ly,0)) AS nonactivating_product_order_count_excl_seeding_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_count_excl_seeding,0)) AS guest_product_order_count_excl_seeding
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_count_excl_seeding_mtd,0)) AS guest_product_order_count_excl_seeding_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_count_excl_seeding_mtd_ly,0)) AS guest_product_order_count_excl_seeding_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_count_excl_seeding_month_tot_lm,0)) AS guest_product_order_count_excl_seeding_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_count_excl_seeding_month_tot_ly,0)) AS guest_product_order_count_excl_seeding_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_count_excl_seeding,0)) AS repeat_vip_product_order_count_excl_seeding
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_count_excl_seeding_mtd,0)) AS repeat_vip_product_order_count_excl_seeding_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_count_excl_seeding_mtd_ly,0)) AS repeat_vip_product_order_count_excl_seeding_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_count_excl_seeding_month_tot_lm,0)) AS repeat_vip_product_order_count_excl_seeding_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_count_excl_seeding_month_tot_ly,0)) AS repeat_vip_product_order_count_excl_seeding_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l4='Reactivated VIP',dcbcj.product_order_count_excl_seeding,0)) AS reactivated_vip_product_order_count_excl_seeding
    ,SUM(IFF(domc.membership_order_type_l4='Reactivated VIP',dcbcj.product_order_count_excl_seeding_mtd,0)) AS reactivated_vip_product_order_count_excl_seeding_mtd
    ,SUM(IFF(domc.membership_order_type_l4='Reactivated VIP',dcbcj.product_order_count_excl_seeding_mtd_ly,0)) AS reactivated_vip_product_order_count_excl_seeding_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l4='Reactivated VIP',dcbcj.product_order_count_excl_seeding_month_tot_lm,0)) AS reactivated_vip_product_order_count_excl_seeding_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l4='Reactivated VIP',dcbcj.product_order_count_excl_seeding_month_tot_ly,0)) AS reactivated_vip_product_order_count_excl_seeding_month_tot_ly

    ,SUM(dcbcj.product_margin_pre_return_excl_seeding) AS product_margin_pre_return_excl_seeding
    ,SUM(dcbcj.product_margin_pre_return_excl_seeding_mtd) AS product_margin_pre_return_excl_seeding_mtd
    ,SUM(dcbcj.product_margin_pre_return_excl_seeding_mtd_ly) AS product_margin_pre_return_excl_seeding_mtd_ly
    ,SUM(dcbcj.product_margin_pre_return_excl_seeding_month_tot_lm) AS product_margin_pre_return_excl_seeding_month_tot_lm
    ,SUM(dcbcj.product_margin_pre_return_excl_seeding_month_tot_ly) AS product_margin_pre_return_excl_seeding_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_margin_pre_return_excl_seeding,0)) AS activating_product_margin_pre_return_excl_seeding
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_margin_pre_return_excl_seeding_mtd,0)) AS activating_product_margin_pre_return_excl_seeding_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_margin_pre_return_excl_seeding_mtd_ly,0)) AS activating_product_margin_pre_return_excl_seeding_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_margin_pre_return_excl_seeding_month_tot_lm,0)) AS activating_product_margin_pre_return_excl_seeding_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_margin_pre_return_excl_seeding_month_tot_ly,0)) AS activating_product_margin_pre_return_excl_seeding_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_margin_pre_return_excl_seeding,0)) AS nonactivating_product_margin_pre_return_excl_seeding
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_margin_pre_return_excl_seeding_mtd,0)) AS nonactivating_product_margin_pre_return_excl_seeding_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_margin_pre_return_excl_seeding_mtd_ly,0)) AS nonactivating_product_margin_pre_return_excl_seeding_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_margin_pre_return_excl_seeding_month_tot_lm,0)) AS nonactivating_product_margin_pre_return_excl_seeding_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_margin_pre_return_excl_seeding_month_tot_ly,0)) AS nonactivating_product_margin_pre_return_excl_seeding_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_margin_pre_return_excl_seeding,0)) AS guest_product_margin_pre_return_excl_seeding
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_margin_pre_return_excl_seeding_mtd,0)) AS guest_product_margin_pre_return_excl_seeding_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_margin_pre_return_excl_seeding_mtd_ly,0)) AS guest_product_margin_pre_return_excl_seeding_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_margin_pre_return_excl_seeding_month_tot_lm,0)) AS guest_product_margin_pre_return_excl_seeding_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_margin_pre_return_excl_seeding_month_tot_ly,0)) AS guest_product_margin_pre_return_excl_seeding_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_margin_pre_return_excl_seeding,0)) AS repeat_vip_product_margin_pre_return_excl_seeding
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_margin_pre_return_excl_seeding_mtd,0)) AS repeat_vip_product_margin_pre_return_excl_seeding_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_margin_pre_return_excl_seeding_mtd_ly,0)) AS repeat_vip_product_margin_pre_return_excl_seeding_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_margin_pre_return_excl_seeding_month_tot_lm,0)) AS repeat_vip_product_margin_pre_return_excl_seeding_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_margin_pre_return_excl_seeding_month_tot_ly,0)) AS repeat_vip_product_margin_pre_return_excl_seeding_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l4='Reactivated VIP',dcbcj.product_margin_pre_return_excl_seeding,0)) AS reactivated_vip_product_margin_pre_return_excl_seeding
    ,SUM(IFF(domc.membership_order_type_l4='Reactivated VIP',dcbcj.product_margin_pre_return_excl_seeding_mtd,0)) AS reactivated_vip_product_margin_pre_return_excl_seeding_mtd
    ,SUM(IFF(domc.membership_order_type_l4='Reactivated VIP',dcbcj.product_margin_pre_return_excl_seeding_mtd_ly,0)) AS reactivated_vip_product_margin_pre_return_excl_seeding_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l4='Reactivated VIP',dcbcj.product_margin_pre_return_excl_seeding_month_tot_lm,0)) AS reactivated_vip_product_margin_pre_return_excl_seeding_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l4='Reactivated VIP',dcbcj.product_margin_pre_return_excl_seeding_month_tot_ly,0)) AS reactivated_vip_product_margin_pre_return_excl_seeding_month_tot_ly

    ,SUM(dcbcj.product_order_unit_count) AS product_order_unit_count
    ,SUM(dcbcj.product_order_unit_count_mtd) AS product_order_unit_count_mtd
    ,SUM(dcbcj.product_order_unit_count_mtd_ly) AS product_order_unit_count_mtd_ly
    ,SUM(dcbcj.product_order_unit_count_month_tot_lm) AS product_order_unit_count_month_tot_lm
    ,SUM(dcbcj.product_order_unit_count_month_tot_ly) AS product_order_unit_count_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_unit_count,0)) AS activating_product_order_unit_count
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_unit_count_mtd,0)) AS activating_product_order_unit_count_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_unit_count_mtd_ly,0)) AS activating_product_order_unit_count_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_unit_count_month_tot_lm,0)) AS activating_product_order_unit_count_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_unit_count_month_tot_ly,0)) AS activating_product_order_unit_count_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_unit_count,0)) AS nonactivating_product_order_unit_count
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_unit_count_mtd,0)) AS nonactivating_product_order_unit_count_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_unit_count_mtd_ly,0)) AS nonactivating_product_order_unit_count_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_unit_count_month_tot_lm,0)) AS nonactivating_product_order_unit_count_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_unit_count_month_tot_ly,0)) AS nonactivating_product_order_unit_count_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_unit_count,0)) AS guest_product_order_unit_count
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_unit_count_mtd,0)) AS guest_product_order_unit_count_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_unit_count_mtd_ly,0)) AS guest_product_order_unit_count_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_unit_count_month_tot_lm,0)) AS guest_product_order_unit_count_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_unit_count_month_tot_ly,0)) AS guest_product_order_unit_count_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_unit_count,0)) AS repeat_vip_product_order_unit_count
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_unit_count_mtd,0)) AS repeat_vip_product_order_unit_count_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_unit_count_mtd_ly,0)) AS repeat_vip_product_order_unit_count_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_unit_count_month_tot_lm,0)) AS repeat_vip_product_order_unit_count_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_unit_count_month_tot_ly,0)) AS repeat_vip_product_order_unit_count_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_air_vip_price,0)) AS activating_product_order_air_vip_price
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_air_vip_price_mtd,0)) AS activating_product_order_air_vip_price_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_air_vip_price_mtd_ly,0)) AS activating_product_order_air_vip_price_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_air_vip_price_month_tot_lm,0)) AS activating_product_order_air_vip_price_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_air_vip_price_month_tot_ly,0)) AS activating_product_order_air_vip_price_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating', iff(domc.membership_order_type_l2='Repeat VIP', dcbcj.product_order_air_vip_price, dcbcj.product_order_retail_unit_price),0)) AS nonactivating_product_order_air_price
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating', iff(domc.membership_order_type_l2='Repeat VIP', dcbcj.product_order_air_vip_price_mtd, dcbcj.product_order_retail_unit_price_mtd),0)) AS nonactivating_product_order_air_price_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating', iff(domc.membership_order_type_l2='Repeat VIP', dcbcj.product_order_air_vip_price_mtd_ly, dcbcj.product_order_retail_unit_price_mtd_ly),0)) AS nonactivating_product_order_air_price_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating', iff(domc.membership_order_type_l2='Repeat VIP', dcbcj.product_order_air_vip_price_month_tot_lm, dcbcj.product_order_retail_unit_price_month_tot_lm),0)) AS nonactivating_product_order_air_price_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating', iff(domc.membership_order_type_l2='Repeat VIP', dcbcj.product_order_air_vip_price_month_tot_ly, dcbcj.product_order_retail_unit_price_month_tot_ly),0)) AS nonactivating_product_order_air_price_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_air_vip_price,0)) AS repeat_vip_product_order_air_vip_price
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_air_vip_price_mtd,0)) AS repeat_vip_product_order_air_vip_price_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_air_vip_price_mtd_ly,0)) AS repeat_vip_product_order_air_vip_price_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_air_vip_price_month_tot_lm,0)) AS repeat_vip_product_order_air_vip_price_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_air_vip_price_month_tot_ly,0)) AS repeat_vip_product_order_air_vip_price_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_air_vip_price,0)) AS guest_product_order_air_vip_price
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_air_vip_price_mtd,0)) AS guest_product_order_air_vip_price_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_air_vip_price_mtd_ly,0)) AS guest_product_order_air_vip_price_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_air_vip_price_month_tot_lm,0)) AS guest_product_order_air_vip_price_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_air_vip_price_month_tot_ly,0)) AS guest_product_order_air_vip_price_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_retail_unit_price,0)) AS guest_product_order_retail_unit_price
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_retail_unit_price_mtd,0)) AS guest_product_order_retail_unit_price_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_retail_unit_price_mtd_ly,0)) AS guest_product_order_retail_unit_price_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_retail_unit_price_month_tot_lm,0)) AS guest_product_order_retail_unit_price_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_retail_unit_price_month_tot_ly,0)) AS guest_product_order_retail_unit_price_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_price_offered_amount,0)) AS activating_product_order_price_offered_amount
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_price_offered_amount_mtd,0)) AS activating_product_order_price_offered_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_price_offered_amount_mtd_ly,0)) AS activating_product_order_price_offered_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_price_offered_amount_month_tot_lm,0)) AS activating_product_order_price_offered_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_price_offered_amount_month_tot_ly,0)) AS activating_product_order_price_offered_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_price_offered_amount,0)) AS nonactivating_product_order_price_offered_amount
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_price_offered_amount_mtd,0)) AS nonactivating_product_order_price_offered_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_price_offered_amount_mtd_ly,0)) AS nonactivating_product_order_price_offered_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_price_offered_amount_month_tot_lm,0)) AS nonactivating_product_order_price_offered_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_price_offered_amount_month_tot_ly,0)) AS nonactivating_product_order_price_offered_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_price_offered_amount,0)) AS guest_product_order_price_offered_amount
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_price_offered_amount_mtd,0)) AS guest_product_order_price_offered_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_price_offered_amount_mtd_ly,0)) AS guest_product_order_price_offered_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_price_offered_amount_month_tot_lm,0)) AS guest_product_order_price_offered_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_price_offered_amount_month_tot_ly,0)) AS guest_product_order_price_offered_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_price_offered_amount,0)) AS repeat_vip_product_order_price_offered_amount
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_price_offered_amount_mtd,0)) AS repeat_vip_product_order_price_offered_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_price_offered_amount_mtd_ly,0)) AS repeat_vip_product_order_price_offered_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_price_offered_amount_month_tot_lm,0)) AS repeat_vip_product_order_price_offered_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_price_offered_amount_month_tot_ly,0)) AS repeat_vip_product_order_price_offered_amount_month_tot_ly

    ,SUM(dcbcj.product_order_landed_product_cost_amount) AS product_order_landed_product_cost_amount
    ,SUM(dcbcj.product_order_landed_product_cost_amount_mtd) AS product_order_landed_product_cost_amount_mtd
    ,SUM(dcbcj.product_order_landed_product_cost_amount_mtd_ly) AS product_order_landed_product_cost_amount_mtd_ly
    ,SUM(dcbcj.product_order_landed_product_cost_amount_month_tot_lm) AS product_order_landed_product_cost_amount_month_tot_lm
    ,SUM(dcbcj.product_order_landed_product_cost_amount_month_tot_ly) AS product_order_landed_product_cost_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_landed_product_cost_amount,0)) AS activating_product_order_landed_product_cost_amount
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_landed_product_cost_amount_mtd,0)) AS activating_product_order_landed_product_cost_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_landed_product_cost_amount_mtd_ly,0)) AS activating_product_order_landed_product_cost_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_landed_product_cost_amount_month_tot_lm,0)) AS activating_product_order_landed_product_cost_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_landed_product_cost_amount_month_tot_ly,0)) AS activating_product_order_landed_product_cost_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_landed_product_cost_amount,0)) AS nonactivating_product_order_landed_product_cost_amount
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_landed_product_cost_amount_mtd,0)) AS nonactivating_product_order_landed_product_cost_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_landed_product_cost_amount_mtd_ly,0)) AS nonactivating_product_order_landed_product_cost_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_landed_product_cost_amount_month_tot_lm,0)) AS nonactivating_product_order_landed_product_cost_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_landed_product_cost_amount_month_tot_ly,0)) AS nonactivating_product_order_landed_product_cost_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_landed_product_cost_amount,0)) AS guest_product_order_landed_product_cost_amount
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_landed_product_cost_amount_mtd,0)) AS guest_product_order_landed_product_cost_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_landed_product_cost_amount_mtd_ly,0)) AS guest_product_order_landed_product_cost_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_landed_product_cost_amount_month_tot_lm,0)) AS guest_product_order_landed_product_cost_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_landed_product_cost_amount_month_tot_ly,0)) AS guest_product_order_landed_product_cost_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_landed_product_cost_amount,0)) AS repeat_vip_product_order_landed_product_cost_amount
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_landed_product_cost_amount_mtd,0)) AS repeat_vip_product_order_landed_product_cost_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_landed_product_cost_amount_mtd_ly,0)) AS repeat_vip_product_order_landed_product_cost_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_landed_product_cost_amount_month_tot_lm,0)) AS repeat_vip_product_order_landed_product_cost_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_landed_product_cost_amount_month_tot_ly,0)) AS repeat_vip_product_order_landed_product_cost_amount_month_tot_ly

    ,SUM(dcbcj.product_order_landed_product_cost_amount_accounting) AS product_order_landed_product_cost_amount_accounting
    ,SUM(dcbcj.product_order_landed_product_cost_amount_accounting_mtd) AS product_order_landed_product_cost_amount_accounting_mtd
    ,SUM(dcbcj.product_order_landed_product_cost_amount_accounting_mtd_ly) AS product_order_landed_product_cost_amount_accounting_mtd_ly
    ,SUM(dcbcj.product_order_landed_product_cost_amount_accounting_month_tot_lm) AS product_order_landed_product_cost_amount_accounting_month_tot_lm
    ,SUM(dcbcj.product_order_landed_product_cost_amount_accounting_month_tot_ly) AS product_order_landed_product_cost_amount_accounting_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_landed_product_cost_amount_accounting,0)) AS activating_product_order_landed_product_cost_amount_accounting
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_landed_product_cost_amount_accounting_mtd,0)) AS activating_product_order_landed_product_cost_amount_accounting_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_landed_product_cost_amount_accounting_mtd_ly,0)) AS activating_product_order_landed_product_cost_amount_accounting_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_landed_product_cost_amount_accounting_month_tot_lm,0)) AS activating_product_order_landed_product_cost_amount_accounting_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_landed_product_cost_amount_accounting_month_tot_ly,0)) AS activating_product_order_landed_product_cost_amount_accounting_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_landed_product_cost_amount_accounting,0)) AS nonactivating_product_order_landed_product_cost_amount_accounting
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_landed_product_cost_amount_accounting_mtd,0)) AS nonactivating_product_order_landed_product_cost_amount_accounting_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_landed_product_cost_amount_accounting_mtd_ly,0)) AS nonactivating_product_order_landed_product_cost_amount_accounting_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_landed_product_cost_amount_accounting_month_tot_lm,0)) AS nonactivating_product_order_landed_product_cost_amount_accounting_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_landed_product_cost_amount_accounting_month_tot_ly,0)) AS nonactivating_product_order_landed_product_cost_amount_accounting_month_tot_ly

    ,SUM(dcbcj.oracle_product_order_landed_product_cost_amount) AS oracle_product_order_landed_product_cost_amount
    ,SUM(dcbcj.oracle_product_order_landed_product_cost_amount_mtd) AS oracle_product_order_landed_product_cost_amount_mtd
    ,SUM(dcbcj.oracle_product_order_landed_product_cost_amount_mtd_ly) AS oracle_product_order_landed_product_cost_amount_mtd_ly
    ,SUM(dcbcj.oracle_product_order_landed_product_cost_amount_month_tot_lm) AS oracle_product_order_landed_product_cost_amount_month_tot_lm
    ,SUM(dcbcj.oracle_product_order_landed_product_cost_amount_month_tot_ly) AS oracle_product_order_landed_product_cost_amount_month_tot_ly

    ,SUM(dcbcj.product_order_shipping_cost_amount) AS product_order_shipping_cost_amount
    ,SUM(dcbcj.product_order_shipping_cost_amount_mtd) AS product_order_shipping_cost_amount_mtd
    ,SUM(dcbcj.product_order_shipping_cost_amount_mtd_ly) AS product_order_shipping_cost_amount_mtd_ly
    ,SUM(dcbcj.product_order_shipping_cost_amount_month_tot_lm) AS product_order_shipping_cost_amount_month_tot_lm
    ,SUM(dcbcj.product_order_shipping_cost_amount_month_tot_ly) AS product_order_shipping_cost_amount_month_tot_ly

    ,SUM(dcbcj.product_order_shipping_supplies_cost_amount) AS product_order_shipping_supplies_cost_amount
    ,SUM(dcbcj.product_order_shipping_supplies_cost_amount_mtd) AS product_order_shipping_supplies_cost_amount_mtd
    ,SUM(dcbcj.product_order_shipping_supplies_cost_amount_mtd_ly) AS product_order_shipping_supplies_cost_amount_mtd_ly
    ,SUM(dcbcj.product_order_shipping_supplies_cost_amount_month_tot_lm) AS product_order_shipping_supplies_cost_amount_month_tot_lm
    ,SUM(dcbcj.product_order_shipping_supplies_cost_amount_month_tot_ly) AS product_order_shipping_supplies_cost_amount_month_tot_ly

    ,SUM(dcbcj.product_order_direct_cogs_amount) AS product_order_direct_cogs_amount
    ,SUM(dcbcj.product_order_direct_cogs_amount_mtd) AS product_order_direct_cogs_amount_mtd
    ,SUM(dcbcj.product_order_direct_cogs_amount_mtd_ly) AS product_order_direct_cogs_amount_mtd_ly
    ,SUM(dcbcj.product_order_direct_cogs_amount_month_tot_lm) AS product_order_direct_cogs_amount_month_tot_lm
    ,SUM(dcbcj.product_order_direct_cogs_amount_month_tot_ly) AS product_order_direct_cogs_amount_month_tot_ly

    ,SUM(dcbcj.product_order_cash_refund_amount) AS product_order_cash_refund_amount
    ,SUM(dcbcj.product_order_cash_refund_amount_mtd) AS product_order_cash_refund_amount_mtd
    ,SUM(dcbcj.product_order_cash_refund_amount_mtd_ly) AS product_order_cash_refund_amount_mtd_ly
    ,SUM(dcbcj.product_order_cash_refund_amount_month_tot_lm) AS product_order_cash_refund_amount_month_tot_lm
    ,SUM(dcbcj.product_order_cash_refund_amount_month_tot_ly) AS product_order_cash_refund_amount_month_tot_ly

    ,SUM(dcbcj.product_order_cash_chargeback_amount) AS product_order_cash_chargeback_amount
    ,SUM(dcbcj.product_order_cash_chargeback_amount_mtd) AS product_order_cash_chargeback_amount_mtd
    ,SUM(dcbcj.product_order_cash_chargeback_amount_mtd_ly) AS product_order_cash_chargeback_amount_mtd_ly
    ,SUM(dcbcj.product_order_cash_chargeback_amount_month_tot_lm) AS product_order_cash_chargeback_amount_month_tot_lm
    ,SUM(dcbcj.product_order_cash_chargeback_amount_month_tot_ly) AS product_order_cash_chargeback_amount_month_tot_ly

    ,SUM(dcbcj.product_order_cost_product_returned_resaleable_amount) AS product_order_cost_product_returned_resaleable_amount
    ,SUM(dcbcj.product_order_cost_product_returned_resaleable_amount_mtd) AS product_order_cost_product_returned_resaleable_amount_mtd
    ,SUM(dcbcj.product_order_cost_product_returned_resaleable_amount_mtd_ly) AS product_order_cost_product_returned_resaleable_amount_mtd_ly
    ,SUM(dcbcj.product_order_cost_product_returned_resaleable_amount_month_tot_lm) AS product_order_cost_product_returned_resaleable_amount_month_tot_lm
    ,SUM(dcbcj.product_order_cost_product_returned_resaleable_amount_month_tot_ly) AS product_order_cost_product_returned_resaleable_amount_month_tot_ly

    ,SUM(dcbcj.product_order_cost_product_returned_resaleable_amount_accounting) AS product_order_cost_product_returned_resaleable_amount_accounting
    ,SUM(dcbcj.product_order_cost_product_returned_resaleable_amount_accounting_mtd) AS product_order_cost_product_returned_resaleable_amount_accounting_mtd
    ,SUM(dcbcj.product_order_cost_product_returned_resaleable_amount_accounting_mtd_ly) AS product_order_cost_product_returned_resaleable_amount_accounting_mtd_ly
    ,SUM(dcbcj.product_order_cost_product_returned_resaleable_amount_accounting_month_tot_lm) AS product_order_cost_product_returned_resaleable_amount_accounting_month_tot_lm
    ,SUM(dcbcj.product_order_cost_product_returned_resaleable_amount_accounting_month_tot_ly) AS product_order_cost_product_returned_resaleable_amount_accounting_month_tot_ly

    ,SUM(dcbcj.product_order_cost_product_returned_damaged_amount) AS product_order_cost_product_returned_damaged_amount
    ,SUM(dcbcj.product_order_cost_product_returned_damaged_amount_mtd) AS product_order_cost_product_returned_damaged_amount_mtd
    ,SUM(dcbcj.product_order_cost_product_returned_damaged_amount_mtd_ly) AS product_order_cost_product_returned_damaged_amount_mtd_ly
    ,SUM(dcbcj.product_order_cost_product_returned_damaged_amount_month_tot_lm) AS product_order_cost_product_returned_damaged_amount_month_tot_lm
    ,SUM(dcbcj.product_order_cost_product_returned_damaged_amount_month_tot_ly) AS product_order_cost_product_returned_damaged_amount_month_tot_ly

    ,SUM(dcbcj.product_order_return_shipping_cost_amount) AS product_order_return_shipping_cost_amount
    ,SUM(dcbcj.product_order_return_shipping_cost_amount_mtd) AS product_order_return_shipping_cost_amount_mtd
    ,SUM(dcbcj.product_order_return_shipping_cost_amount_mtd_ly) AS product_order_return_shipping_cost_amount_mtd_ly
    ,SUM(dcbcj.product_order_return_shipping_cost_amount_month_tot_lm) AS product_order_return_shipping_cost_amount_month_tot_lm
    ,SUM(dcbcj.product_order_return_shipping_cost_amount_month_tot_ly) AS product_order_return_shipping_cost_amount_month_tot_ly

    ,SUM(dcbcj.product_order_reship_order_count) AS product_order_reship_order_count
    ,SUM(dcbcj.product_order_reship_order_count_mtd) AS product_order_reship_order_count_mtd
    ,SUM(dcbcj.product_order_reship_order_count_mtd_ly) AS product_order_reship_order_count_mtd_ly
    ,SUM(dcbcj.product_order_reship_order_count_month_tot_lm) AS product_order_reship_order_count_month_tot_lm
    ,SUM(dcbcj.product_order_reship_order_count_month_tot_ly) AS product_order_reship_order_count_month_tot_ly

    ,SUM(dcbcj.product_order_reship_unit_count) AS product_order_reship_unit_count
    ,SUM(dcbcj.product_order_reship_unit_count_mtd) AS product_order_reship_unit_count_mtd
    ,SUM(dcbcj.product_order_reship_unit_count_mtd_ly) AS product_order_reship_unit_count_mtd_ly
    ,SUM(dcbcj.product_order_reship_unit_count_month_tot_lm) AS product_order_reship_unit_count_month_tot_lm
    ,SUM(dcbcj.product_order_reship_unit_count_month_tot_ly) AS product_order_reship_unit_count_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_reship_unit_count,0)) AS activating_product_order_reship_unit_count
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_reship_unit_count_mtd,0)) AS activating_product_order_reship_unit_count_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_reship_unit_count_mtd_ly,0)) AS activating_product_order_reship_unit_count_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_reship_unit_count_month_tot_lm,0)) AS activating_product_order_reship_unit_count_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_reship_unit_count_month_tot_ly,0)) AS activating_product_order_reship_unit_count_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_reship_unit_count,0)) AS nonactivating_product_order_reship_unit_count
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_reship_unit_count_mtd,0)) AS nonactivating_product_order_reship_unit_count_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_reship_unit_count_mtd_ly,0)) AS nonactivating_product_order_reship_unit_count_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_reship_unit_count_month_tot_lm,0)) AS nonactivating_product_order_reship_unit_count_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_reship_unit_count_month_tot_ly,0)) AS nonactivating_product_order_reship_unit_count_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_reship_unit_count,0)) AS guest_product_order_reship_unit_count
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_reship_unit_count_mtd,0)) AS guest_product_order_reship_unit_count_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_reship_unit_count_mtd_ly,0)) AS guest_product_order_reship_unit_count_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_reship_unit_count_month_tot_lm,0)) AS guest_product_order_reship_unit_count_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_reship_unit_count_month_tot_ly,0)) AS guest_product_order_reship_unit_count_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_reship_unit_count,0)) AS repeat_vip_product_order_reship_unit_count
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_reship_unit_count_mtd,0)) AS repeat_vip_product_order_reship_unit_count_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_reship_unit_count_mtd_ly,0)) AS repeat_vip_product_order_reship_unit_count_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_reship_unit_count_month_tot_lm,0)) AS repeat_vip_product_order_reship_unit_count_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_reship_unit_count_month_tot_ly,0)) AS repeat_vip_product_order_reship_unit_count_month_tot_ly

    ,SUM(dcbcj.product_order_reship_direct_cogs_amount) AS product_order_reship_direct_cogs_amount
    ,SUM(dcbcj.product_order_reship_direct_cogs_amount_mtd) AS product_order_reship_direct_cogs_amount_mtd
    ,SUM(dcbcj.product_order_reship_direct_cogs_amount_mtd_ly) AS product_order_reship_direct_cogs_amount_mtd_ly
    ,SUM(dcbcj.product_order_reship_direct_cogs_amount_month_tot_lm) AS product_order_reship_direct_cogs_amount_month_tot_lm
    ,SUM(dcbcj.product_order_reship_direct_cogs_amount_month_tot_ly) AS product_order_reship_direct_cogs_amount_month_tot_ly

    ,SUM(dcbcj.product_order_reship_product_cost_amount) AS product_order_reship_product_cost_amount
    ,SUM(dcbcj.product_order_reship_product_cost_amount_mtd) AS product_order_reship_product_cost_amount_mtd
    ,SUM(dcbcj.product_order_reship_product_cost_amount_mtd_ly) AS product_order_reship_product_cost_amount_mtd_ly
    ,SUM(dcbcj.product_order_reship_product_cost_amount_month_tot_lm) AS product_order_reship_product_cost_amount_month_tot_lm
    ,SUM(dcbcj.product_order_reship_product_cost_amount_month_tot_ly) AS product_order_reship_product_cost_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_reship_product_cost_amount,0)) AS activating_product_order_reship_product_cost_amount
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_reship_product_cost_amount_mtd,0)) AS activating_product_order_reship_product_cost_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_reship_product_cost_amount_mtd_ly,0)) AS activating_product_order_reship_product_cost_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_reship_product_cost_amount_month_tot_lm,0)) AS activating_product_order_reship_product_cost_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_reship_product_cost_amount_month_tot_ly,0)) AS activating_product_order_reship_product_cost_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_reship_product_cost_amount,0)) AS nonactivating_product_order_reship_product_cost_amount
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_reship_product_cost_amount_mtd,0)) AS nonactivating_product_order_reship_product_cost_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_reship_product_cost_amount_mtd_ly,0)) AS nonactivating_product_order_reship_product_cost_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_reship_product_cost_amount_month_tot_lm,0)) AS nonactivating_product_order_reship_product_cost_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_reship_product_cost_amount_month_tot_ly,0)) AS nonactivating_product_order_reship_product_cost_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_reship_product_cost_amount,0)) AS guest_product_order_reship_product_cost_amount
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_reship_product_cost_amount_mtd,0)) AS guest_product_order_reship_product_cost_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_reship_product_cost_amount_mtd_ly,0)) AS guest_product_order_reship_product_cost_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_reship_product_cost_amount_month_tot_lm,0)) AS guest_product_order_reship_product_cost_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_reship_product_cost_amount_month_tot_ly,0)) AS guest_product_order_reship_product_cost_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_reship_product_cost_amount,0)) AS repeat_vip_product_order_reship_product_cost_amount
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_reship_product_cost_amount_mtd,0)) AS repeat_vip_product_order_reship_product_cost_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_reship_product_cost_amount_mtd_ly,0)) AS repeat_vip_product_order_reship_product_cost_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_reship_product_cost_amount_month_tot_lm,0)) AS repeat_vip_product_order_reship_product_cost_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_reship_product_cost_amount_month_tot_ly,0)) AS repeat_vip_product_order_reship_product_cost_amount_month_tot_ly

    ,SUM(dcbcj.product_order_reship_product_cost_amount_accounting) AS product_order_reship_product_cost_amount_accounting
    ,SUM(dcbcj.product_order_reship_product_cost_amount_accounting_mtd) AS product_order_reship_product_cost_amount_accounting_mtd
    ,SUM(dcbcj.product_order_reship_product_cost_amount_accounting_mtd_ly) AS product_order_reship_product_cost_amount_accounting_mtd_ly
    ,SUM(dcbcj.product_order_reship_product_cost_amount_accounting_month_tot_lm) AS product_order_reship_product_cost_amount_accounting_month_tot_lm
    ,SUM(dcbcj.product_order_reship_product_cost_amount_accounting_month_tot_ly) AS product_order_reship_product_cost_amount_accounting_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_reship_product_cost_amount_accounting,0)) AS activating_product_order_reship_product_cost_amount_accounting
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_reship_product_cost_amount_accounting_mtd,0)) AS activating_product_order_reship_product_cost_amount_accounting_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_reship_product_cost_amount_accounting_mtd_ly,0)) AS activating_product_order_reship_product_cost_amount_accounting_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_reship_product_cost_amount_accounting_month_tot_lm,0)) AS activating_product_order_reship_product_cost_amount_accounting_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_reship_product_cost_amount_accounting_month_tot_ly,0)) AS activating_product_order_reship_product_cost_amount_accounting_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_reship_product_cost_amount_accounting,0)) AS nonactivating_product_order_reship_product_cost_amount_accounting
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_reship_product_cost_amount_accounting_mtd,0)) AS nonactivating_product_order_reship_product_cost_amount_accounting_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_reship_product_cost_amount_accounting_mtd_ly,0)) AS nonactivating_product_order_reship_product_cost_amount_accounting_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_reship_product_cost_amount_accounting_month_tot_lm,0)) AS nonactivating_product_order_reship_product_cost_amount_accounting_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_reship_product_cost_amount_accounting_month_tot_ly,0)) AS nonactivating_product_order_reship_product_cost_amount_accounting_month_tot_ly

    ,SUM(dcbcj.oracle_product_order_reship_product_cost_amount) AS oracle_product_order_reship_product_cost_amount
    ,SUM(dcbcj.oracle_product_order_reship_product_cost_amount_mtd) AS oracle_product_order_reship_product_cost_amount_mtd
    ,SUM(dcbcj.oracle_product_order_reship_product_cost_amount_mtd_ly) AS oracle_product_order_reship_product_cost_amount_mtd_ly
    ,SUM(dcbcj.oracle_product_order_reship_product_cost_amount_month_tot_lm) AS oracle_product_order_reship_product_cost_amount_month_tot_lm
    ,SUM(dcbcj.oracle_product_order_reship_product_cost_amount_month_tot_ly) AS oracle_product_order_reship_product_cost_amount_month_tot_ly

    ,SUM(dcbcj.product_order_reship_shipping_cost_amount) AS product_order_reship_shipping_cost_amount
    ,SUM(dcbcj.product_order_reship_shipping_cost_amount_mtd) AS product_order_reship_shipping_cost_amount_mtd
    ,SUM(dcbcj.product_order_reship_shipping_cost_amount_mtd_ly) AS product_order_reship_shipping_cost_amount_mtd_ly
    ,SUM(dcbcj.product_order_reship_shipping_cost_amount_month_tot_lm) AS product_order_reship_shipping_cost_amount_month_tot_lm
    ,SUM(dcbcj.product_order_reship_shipping_cost_amount_month_tot_ly) AS product_order_reship_shipping_cost_amount_month_tot_ly

    ,SUM(dcbcj.product_order_reship_shipping_supplies_cost_amount) AS product_order_reship_shipping_supplies_cost_amount
    ,SUM(dcbcj.product_order_reship_shipping_supplies_cost_amount_mtd) AS product_order_reship_shipping_supplies_cost_amount_mtd
    ,SUM(dcbcj.product_order_reship_shipping_supplies_cost_amount_mtd_ly) AS product_order_reship_shipping_supplies_cost_amount_mtd_ly
    ,SUM(dcbcj.product_order_reship_shipping_supplies_cost_amount_month_tot_lm) AS product_order_reship_shipping_supplies_cost_amount_month_tot_lm
    ,SUM(dcbcj.product_order_reship_shipping_supplies_cost_amount_month_tot_ly) AS product_order_reship_shipping_supplies_cost_amount_month_tot_ly

    ,SUM(dcbcj.product_order_exchange_order_count) AS product_order_exchange_order_count
    ,SUM(dcbcj.product_order_exchange_order_count_mtd) AS product_order_exchange_order_count_mtd
    ,SUM(dcbcj.product_order_exchange_order_count_mtd_ly) AS product_order_exchange_order_count_mtd_ly
    ,SUM(dcbcj.product_order_exchange_order_count_month_tot_lm) AS product_order_exchange_order_count_month_tot_lm
    ,SUM(dcbcj.product_order_exchange_order_count_month_tot_ly) AS product_order_exchange_order_count_month_tot_ly

    ,SUM(dcbcj.product_order_exchange_unit_count) AS product_order_exchange_unit_count
    ,SUM(dcbcj.product_order_exchange_unit_count_mtd) AS product_order_exchange_unit_count_mtd
    ,SUM(dcbcj.product_order_exchange_unit_count_mtd_ly) AS product_order_exchange_unit_count_mtd_ly
    ,SUM(dcbcj.product_order_exchange_unit_count_month_tot_lm) AS product_order_exchange_unit_count_month_tot_lm
    ,SUM(dcbcj.product_order_exchange_unit_count_month_tot_ly) AS product_order_exchange_unit_count_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_exchange_unit_count,0)) AS activating_product_order_exchange_unit_count
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_exchange_unit_count_mtd,0)) AS activating_product_order_exchange_unit_count_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_exchange_unit_count_mtd_ly,0)) AS activating_product_order_exchange_unit_count_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_exchange_unit_count_month_tot_lm,0)) AS activating_product_order_exchange_unit_count_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_exchange_unit_count_month_tot_ly,0)) AS activating_product_order_exchange_unit_count_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_exchange_unit_count,0)) AS nonactivating_product_order_exchange_unit_count
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_exchange_unit_count_mtd,0)) AS nonactivating_product_order_exchange_unit_count_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_exchange_unit_count_mtd_ly,0)) AS nonactivating_product_order_exchange_unit_count_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_exchange_unit_count_month_tot_lm,0)) AS nonactivating_product_order_exchange_unit_count_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_exchange_unit_count_month_tot_ly,0)) AS nonactivating_product_order_exchange_unit_count_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_exchange_unit_count,0)) AS guest_product_order_exchange_unit_count
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_exchange_unit_count_mtd,0)) AS guest_product_order_exchange_unit_count_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_exchange_unit_count_mtd_ly,0)) AS guest_product_order_exchange_unit_count_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_exchange_unit_count_month_tot_lm,0)) AS guest_product_order_exchange_unit_count_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_exchange_unit_count_month_tot_ly,0)) AS guest_product_order_exchange_unit_count_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_exchange_unit_count,0)) AS repeat_vip_product_order_exchange_unit_count
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_exchange_unit_count_mtd,0)) AS repeat_vip_product_order_exchange_unit_count_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_exchange_unit_count_mtd_ly,0)) AS repeat_vip_product_order_exchange_unit_count_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_exchange_unit_count_month_tot_lm,0)) AS repeat_vip_product_order_exchange_unit_count_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_exchange_unit_count_month_tot_ly,0)) AS repeat_vip_product_order_exchange_unit_count_month_tot_ly

    ,SUM(dcbcj.product_order_exchange_product_cost_amount) AS product_order_exchange_product_cost_amount
    ,SUM(dcbcj.product_order_exchange_product_cost_amount_mtd) AS product_order_exchange_product_cost_amount_mtd
    ,SUM(dcbcj.product_order_exchange_product_cost_amount_mtd_ly) AS product_order_exchange_product_cost_amount_mtd_ly
    ,SUM(dcbcj.product_order_exchange_product_cost_amount_month_tot_lm) AS product_order_exchange_product_cost_amount_month_tot_lm
    ,SUM(dcbcj.product_order_exchange_product_cost_amount_month_tot_ly) AS product_order_exchange_product_cost_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_exchange_product_cost_amount,0)) AS activating_product_order_exchange_product_cost_amount
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_exchange_product_cost_amount_mtd,0)) AS activating_product_order_exchange_product_cost_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_exchange_product_cost_amount_mtd_ly,0)) AS activating_product_order_exchange_product_cost_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_exchange_product_cost_amount_month_tot_lm,0)) AS activating_product_order_exchange_product_cost_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_exchange_product_cost_amount_month_tot_ly,0)) AS activating_product_order_exchange_product_cost_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_exchange_product_cost_amount,0)) AS nonactivating_product_order_exchange_product_cost_amount
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_exchange_product_cost_amount_mtd,0)) AS nonactivating_product_order_exchange_product_cost_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_exchange_product_cost_amount_mtd_ly,0)) AS nonactivating_product_order_exchange_product_cost_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_exchange_product_cost_amount_month_tot_lm,0)) AS nonactivating_product_order_exchange_product_cost_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_exchange_product_cost_amount_month_tot_ly,0)) AS nonactivating_product_order_exchange_product_cost_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_exchange_product_cost_amount,0)) AS guest_product_order_exchange_product_cost_amount
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_exchange_product_cost_amount_mtd,0)) AS guest_product_order_exchange_product_cost_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_exchange_product_cost_amount_mtd_ly,0)) AS guest_product_order_exchange_product_cost_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_exchange_product_cost_amount_month_tot_lm,0)) AS guest_product_order_exchange_product_cost_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_order_exchange_product_cost_amount_month_tot_ly,0)) AS guest_product_order_exchange_product_cost_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_exchange_product_cost_amount,0)) AS repeat_vip_product_order_exchange_product_cost_amount
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_exchange_product_cost_amount_mtd,0)) AS repeat_vip_product_order_exchange_product_cost_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_exchange_product_cost_amount_mtd_ly,0)) AS repeat_vip_product_order_exchange_product_cost_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_exchange_product_cost_amount_month_tot_lm,0)) AS repeat_vip_product_order_exchange_product_cost_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_order_exchange_product_cost_amount_month_tot_ly,0)) AS repeat_vip_product_order_exchange_product_cost_amount_month_tot_ly

    ,SUM(dcbcj.product_order_exchange_product_cost_amount_accounting) AS product_order_exchange_product_cost_amount_accounting
    ,SUM(dcbcj.product_order_exchange_product_cost_amount_accounting_mtd) AS product_order_exchange_product_cost_amount_accounting_mtd
    ,SUM(dcbcj.product_order_exchange_product_cost_amount_accounting_mtd_ly) AS product_order_exchange_product_cost_amount_accounting_mtd_ly
    ,SUM(dcbcj.product_order_exchange_product_cost_amount_accounting_month_tot_lm) AS product_order_exchange_product_cost_amount_accounting_month_tot_lm
    ,SUM(dcbcj.product_order_exchange_product_cost_amount_accounting_month_tot_ly) AS product_order_exchange_product_cost_amount_accounting_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_exchange_product_cost_amount_accounting,0)) AS activating_product_order_exchange_product_cost_amount_accounting
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_exchange_product_cost_amount_accounting_mtd,0)) AS activating_product_order_exchange_product_cost_amount_accounting_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_exchange_product_cost_amount_accounting_mtd_ly,0)) AS activating_product_order_exchange_product_cost_amount_accounting_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_exchange_product_cost_amount_accounting_month_tot_lm,0)) AS activating_product_order_exchange_product_cost_amount_accounting_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_exchange_product_cost_amount_accounting_month_tot_ly,0)) AS activating_product_order_exchange_product_cost_amount_accounting_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_exchange_product_cost_amount_accounting,0)) AS nonactivating_product_order_exchange_product_cost_amount_accounting
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_exchange_product_cost_amount_accounting_mtd,0)) AS nonactivating_product_order_exchange_product_cost_amount_accounting_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_exchange_product_cost_amount_accounting_mtd_ly,0)) AS nonactivating_product_order_exchange_product_cost_amount_accounting_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_exchange_product_cost_amount_accounting_month_tot_lm,0)) AS nonactivating_product_order_exchange_product_cost_amount_accounting_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_exchange_product_cost_amount_accounting_month_tot_ly,0)) AS nonactivating_product_order_exchange_product_cost_amount_accounting_month_tot_ly

    ,SUM(dcbcj.oracle_product_order_exchange_product_cost_amount) AS oracle_product_order_exchange_product_cost_amount
    ,SUM(dcbcj.oracle_product_order_exchange_product_cost_amount_mtd) AS oracle_product_order_exchange_product_cost_amount_mtd
    ,SUM(dcbcj.oracle_product_order_exchange_product_cost_amount_mtd_ly) AS oracle_product_order_exchange_product_cost_amount_mtd_ly
    ,SUM(dcbcj.oracle_product_order_exchange_product_cost_amount_month_tot_lm) AS oracle_product_order_exchange_product_cost_amount_month_tot_lm
    ,SUM(dcbcj.oracle_product_order_exchange_product_cost_amount_month_tot_ly) AS oracle_product_order_exchange_product_cost_amount_month_tot_ly

    ,SUM(dcbcj.product_order_exchange_shipping_cost_amount) AS product_order_exchange_shipping_cost_amount
    ,SUM(dcbcj.product_order_exchange_shipping_cost_amount_mtd) AS product_order_exchange_shipping_cost_amount_mtd
    ,SUM(dcbcj.product_order_exchange_shipping_cost_amount_mtd_ly) AS product_order_exchange_shipping_cost_amount_mtd_ly
    ,SUM(dcbcj.product_order_exchange_shipping_cost_amount_month_tot_lm) AS product_order_exchange_shipping_cost_amount_month_tot_lm
    ,SUM(dcbcj.product_order_exchange_shipping_cost_amount_month_tot_ly) AS product_order_exchange_shipping_cost_amount_month_tot_ly

    ,SUM(dcbcj.product_order_exchange_shipping_supplies_cost_amount) AS product_order_exchange_shipping_supplies_cost_amount
    ,SUM(dcbcj.product_order_exchange_shipping_supplies_cost_amount_mtd) AS product_order_exchange_shipping_supplies_cost_amount_mtd
    ,SUM(dcbcj.product_order_exchange_shipping_supplies_cost_amount_mtd_ly) AS product_order_exchange_shipping_supplies_cost_amount_mtd_ly
    ,SUM(dcbcj.product_order_exchange_shipping_supplies_cost_amount_month_tot_lm) AS product_order_exchange_shipping_supplies_cost_amount_month_tot_lm
    ,SUM(dcbcj.product_order_exchange_shipping_supplies_cost_amount_month_tot_ly) AS product_order_exchange_shipping_supplies_cost_amount_month_tot_ly

    ,SUM(dcbcj.product_gross_revenue) AS product_gross_revenue
    ,SUM(dcbcj.product_gross_revenue_mtd) AS product_gross_revenue_mtd
    ,SUM(dcbcj.product_gross_revenue_mtd_ly) AS product_gross_revenue_mtd_ly
    ,SUM(dcbcj.product_gross_revenue_month_tot_lm) AS product_gross_revenue_month_tot_lm
    ,SUM(dcbcj.product_gross_revenue_month_tot_ly) AS product_gross_revenue_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_gross_revenue,0)) AS activating_product_gross_revenue
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_gross_revenue_mtd,0)) AS activating_product_gross_revenue_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_gross_revenue_mtd_ly,0)) AS activating_product_gross_revenue_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_gross_revenue_month_tot_lm,0)) AS activating_product_gross_revenue_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_gross_revenue_month_tot_ly,0)) AS activating_product_gross_revenue_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_gross_revenue,0)) AS nonactivating_product_gross_revenue
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_gross_revenue_mtd,0)) AS nonactivating_product_gross_revenue_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_gross_revenue_mtd_ly,0)) AS nonactivating_product_gross_revenue_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_gross_revenue_month_tot_lm,0)) AS nonactivating_product_gross_revenue_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_gross_revenue_month_tot_ly,0)) AS nonactivating_product_gross_revenue_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_gross_revenue,0)) AS guest_product_gross_revenue
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_gross_revenue_mtd,0)) AS guest_product_gross_revenue_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_gross_revenue_mtd_ly,0)) AS guest_product_gross_revenue_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_gross_revenue_month_tot_lm,0)) AS guest_product_gross_revenue_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_gross_revenue_month_tot_ly,0)) AS guest_product_gross_revenue_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_gross_revenue,0)) AS repeat_vip_product_gross_revenue
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_gross_revenue_mtd,0)) AS repeat_vip_product_gross_revenue_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_gross_revenue_mtd_ly,0)) AS repeat_vip_product_gross_revenue_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_gross_revenue_month_tot_lm,0)) AS repeat_vip_product_gross_revenue_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_gross_revenue_month_tot_ly,0)) AS repeat_vip_product_gross_revenue_month_tot_ly

    ,SUM(dcbcj.product_net_revenue) AS product_net_revenue
    ,SUM(dcbcj.product_net_revenue_mtd) AS product_net_revenue_mtd
    ,SUM(dcbcj.product_net_revenue_mtd_ly) AS product_net_revenue_mtd_ly
    ,SUM(dcbcj.product_net_revenue_month_tot_lm) AS product_net_revenue_month_tot_lm
    ,SUM(dcbcj.product_net_revenue_month_tot_ly) AS product_net_revenue_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_net_revenue,0)) AS activating_product_net_revenue
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_net_revenue_mtd,0)) AS activating_product_net_revenue_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_net_revenue_mtd_ly,0)) AS activating_product_net_revenue_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_net_revenue_month_tot_lm,0)) AS activating_product_net_revenue_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_net_revenue_month_tot_ly,0)) AS activating_product_net_revenue_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_net_revenue,0)) AS nonactivating_product_net_revenue
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_net_revenue_mtd,0)) AS nonactivating_product_net_revenue_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_net_revenue_mtd_ly,0)) AS nonactivating_product_net_revenue_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_net_revenue_month_tot_lm,0)) AS nonactivating_product_net_revenue_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_net_revenue_month_tot_ly,0)) AS nonactivating_product_net_revenue_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_net_revenue,0)) AS guest_product_net_revenue
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_net_revenue_mtd,0)) AS guest_product_net_revenue_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_net_revenue_mtd_ly,0)) AS guest_product_net_revenue_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_net_revenue_month_tot_lm,0)) AS guest_product_net_revenue_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_net_revenue_month_tot_ly,0)) AS guest_product_net_revenue_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_net_revenue,0)) AS repeat_vip_product_net_revenue
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_net_revenue_mtd,0)) AS repeat_vip_product_net_revenue_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_net_revenue_mtd_ly,0)) AS repeat_vip_product_net_revenue_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_net_revenue_month_tot_lm,0)) AS repeat_vip_product_net_revenue_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_net_revenue_month_tot_ly,0)) AS repeat_vip_product_net_revenue_month_tot_ly

    ,SUM(dcbcj.product_margin_pre_return) AS product_margin_pre_return
    ,SUM(dcbcj.product_margin_pre_return_mtd) AS product_margin_pre_return_mtd
    ,SUM(dcbcj.product_margin_pre_return_mtd_ly) AS product_margin_pre_return_mtd_ly
    ,SUM(dcbcj.product_margin_pre_return_month_tot_lm) AS product_margin_pre_return_month_tot_lm
    ,SUM(dcbcj.product_margin_pre_return_month_tot_ly) AS product_margin_pre_return_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_margin_pre_return,0)) AS activating_product_margin_pre_return
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_margin_pre_return_mtd,0)) AS activating_product_margin_pre_return_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_margin_pre_return_mtd_ly,0)) AS activating_product_margin_pre_return_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_margin_pre_return_month_tot_lm,0)) AS activating_product_margin_pre_return_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_margin_pre_return_month_tot_ly,0)) AS activating_product_margin_pre_return_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l3='First Guest',dcbcj.product_margin_pre_return,0)) AS first_guest_product_margin_pre_return
    ,SUM(IFF(domc.membership_order_type_l3='First Guest',dcbcj.product_margin_pre_return_mtd,0)) AS first_guest_product_margin_pre_return_mtd
    ,SUM(IFF(domc.membership_order_type_l3='First Guest',dcbcj.product_margin_pre_return_mtd_ly,0)) AS first_guest_product_margin_pre_return_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l3='First Guest',dcbcj.product_margin_pre_return_month_tot_lm,0)) AS first_guest_product_margin_pre_return_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l3='First Guest',dcbcj.product_margin_pre_return_month_tot_ly,0)) AS first_guest_product_margin_pre_return_month_tot_ly

    ,SUM(dcbcj.product_gross_profit) AS product_gross_profit
    ,SUM(dcbcj.product_gross_profit_mtd) AS product_gross_profit_mtd
    ,SUM(dcbcj.product_gross_profit_mtd_ly) AS product_gross_profit_mtd_ly
    ,SUM(dcbcj.product_gross_profit_month_tot_lm) AS product_gross_profit_month_tot_lm
    ,SUM(dcbcj.product_gross_profit_month_tot_ly) AS product_gross_profit_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_gross_profit,0)) AS activating_product_gross_profit
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_gross_profit_mtd,0)) AS activating_product_gross_profit_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_gross_profit_mtd_ly,0)) AS activating_product_gross_profit_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_gross_profit_month_tot_lm,0)) AS activating_product_gross_profit_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_gross_profit_month_tot_ly,0)) AS activating_product_gross_profit_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_gross_profit,0)) AS nonactivating_product_gross_profit
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_gross_profit_mtd,0)) AS nonactivating_product_gross_profit_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_gross_profit_mtd_ly,0)) AS nonactivating_product_gross_profit_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_gross_profit_month_tot_lm,0)) AS nonactivating_product_gross_profit_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_gross_profit_month_tot_ly,0)) AS nonactivating_product_gross_profit_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_gross_profit,0)) AS guest_product_gross_profit
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_gross_profit_mtd,0)) AS guest_product_gross_profit_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_gross_profit_mtd_ly,0)) AS guest_product_gross_profit_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_gross_profit_month_tot_lm,0)) AS guest_product_gross_profit_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_gross_profit_month_tot_ly,0)) AS guest_product_gross_profit_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_gross_profit,0)) AS repeat_vip_product_gross_profit
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_gross_profit_mtd,0)) AS repeat_vip_product_gross_profit_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_gross_profit_mtd_ly,0)) AS repeat_vip_product_gross_profit_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_gross_profit_month_tot_lm,0)) AS repeat_vip_product_gross_profit_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_gross_profit_month_tot_ly,0)) AS repeat_vip_product_gross_profit_month_tot_ly

    ,SUM(dcbcj.product_order_cash_gross_revenue_amount) AS product_order_cash_gross_revenue_amount
    ,SUM(dcbcj.product_order_cash_gross_revenue_amount_mtd) AS product_order_cash_gross_revenue_amount_mtd
    ,SUM(dcbcj.product_order_cash_gross_revenue_amount_mtd_ly) AS product_order_cash_gross_revenue_amount_mtd_ly
    ,SUM(dcbcj.product_order_cash_gross_revenue_amount_month_tot_lm) AS product_order_cash_gross_revenue_amount_month_tot_lm
    ,SUM(dcbcj.product_order_cash_gross_revenue_amount_month_tot_ly) AS product_order_cash_gross_revenue_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_cash_gross_revenue_amount,0)) AS activating_product_order_cash_gross_revenue_amount
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_cash_gross_revenue_amount_mtd,0)) AS activating_product_order_cash_gross_revenue_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_cash_gross_revenue_amount_mtd_ly,0)) AS activating_product_order_cash_gross_revenue_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_cash_gross_revenue_amount_month_tot_lm,0)) AS activating_product_order_cash_gross_revenue_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_cash_gross_revenue_amount_month_tot_ly,0)) AS activating_product_order_cash_gross_revenue_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_cash_gross_revenue_amount,0)) AS nonactivating_product_order_cash_gross_revenue_amount
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_cash_gross_revenue_amount_mtd,0)) AS nonactivating_product_order_cash_gross_revenue_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_cash_gross_revenue_amount_mtd_ly,0)) AS nonactivating_product_order_cash_gross_revenue_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_cash_gross_revenue_amount_month_tot_lm,0)) AS nonactivating_product_order_cash_gross_revenue_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_cash_gross_revenue_amount_month_tot_ly,0)) AS nonactivating_product_order_cash_gross_revenue_amount_month_tot_ly

    ,SUM(dcbcj.cash_gross_revenue) AS cash_gross_revenue
    ,SUM(dcbcj.cash_gross_revenue_mtd) AS cash_gross_revenue_mtd
    ,SUM(dcbcj.cash_gross_revenue_mtd_ly) AS cash_gross_revenue_mtd_ly
    ,SUM(dcbcj.cash_gross_revenue_month_tot_lm) AS cash_gross_revenue_month_tot_lm
    ,SUM(dcbcj.cash_gross_revenue_month_tot_ly) AS cash_gross_revenue_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.cash_gross_revenue,0)) AS activating_cash_gross_revenue
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.cash_gross_revenue_mtd,0)) AS activating_cash_gross_revenue_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.cash_gross_revenue_mtd_ly,0)) AS activating_cash_gross_revenue_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.cash_gross_revenue_month_tot_lm,0)) AS activating_cash_gross_revenue_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.cash_gross_revenue_month_tot_ly,0)) AS activating_cash_gross_revenue_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.cash_gross_revenue,0)) AS nonactivating_cash_gross_revenue
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.cash_gross_revenue_mtd,0)) AS nonactivating_cash_gross_revenue_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.cash_gross_revenue_mtd_ly,0)) AS nonactivating_cash_gross_revenue_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.cash_gross_revenue_month_tot_lm,0)) AS nonactivating_cash_gross_revenue_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.cash_gross_revenue_month_tot_ly,0)) AS nonactivating_cash_gross_revenue_month_tot_ly

    ,SUM(dcbcj.cash_net_revenue) AS cash_net_revenue
    ,SUM(dcbcj.cash_net_revenue_mtd) AS cash_net_revenue_mtd
    ,SUM(dcbcj.cash_net_revenue_mtd_ly) AS cash_net_revenue_mtd_ly
    ,SUM(dcbcj.cash_net_revenue_month_tot_lm) AS cash_net_revenue_month_tot_lm
    ,SUM(dcbcj.cash_net_revenue_month_tot_ly) AS cash_net_revenue_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.cash_net_revenue,0)) AS activating_cash_net_revenue
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.cash_net_revenue_mtd,0)) AS activating_cash_net_revenue_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.cash_net_revenue_mtd_ly,0)) AS activating_cash_net_revenue_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.cash_net_revenue_month_tot_lm,0)) AS activating_cash_net_revenue_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.cash_net_revenue_month_tot_ly,0)) AS activating_cash_net_revenue_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.cash_net_revenue,0)) AS nonactivating_cash_net_revenue
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.cash_net_revenue_mtd,0)) AS nonactivating_cash_net_revenue_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.cash_net_revenue_mtd_ly,0)) AS nonactivating_cash_net_revenue_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.cash_net_revenue_month_tot_lm,0)) AS nonactivating_cash_net_revenue_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.cash_net_revenue_month_tot_ly,0)) AS nonactivating_cash_net_revenue_month_tot_ly

    ,SUM(dcbcj.cash_net_revenue_budgeted_fx) AS cash_net_revenue_budgeted_fx
    ,SUM(dcbcj.cash_net_revenue_budgeted_fx_mtd) AS cash_net_revenue_budgeted_fx_mtd
    ,SUM(dcbcj.cash_net_revenue_budgeted_fx_mtd_ly) AS cash_net_revenue_budgeted_fx_mtd_ly
    ,SUM(dcbcj.cash_net_revenue_budgeted_fx_month_tot_lm) AS cash_net_revenue_budgeted_fx_month_tot_lm
    ,SUM(dcbcj.cash_net_revenue_budgeted_fx_month_tot_ly) AS cash_net_revenue_budgeted_fx_month_tot_ly

    ,SUM(dcbcj.cash_gross_profit) AS cash_gross_profit
    ,SUM(dcbcj.cash_gross_profit_mtd) AS cash_gross_profit_mtd
    ,SUM(dcbcj.cash_gross_profit_mtd_ly) AS cash_gross_profit_mtd_ly
    ,SUM(dcbcj.cash_gross_profit_month_tot_lm) AS cash_gross_profit_month_tot_lm
    ,SUM(dcbcj.cash_gross_profit_month_tot_ly) AS cash_gross_profit_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.cash_gross_profit,0)) AS nonactivating_cash_gross_profit
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.cash_gross_profit_mtd,0)) AS nonactivating_cash_gross_profit_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.cash_gross_profit_mtd_ly,0)) AS nonactivating_cash_gross_profit_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.cash_gross_profit_month_tot_lm,0)) AS nonactivating_cash_gross_profit_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.cash_gross_profit_month_tot_ly,0)) AS nonactivating_cash_gross_profit_month_tot_ly

    ,SUM(dcbcj.billed_credit_cash_transaction_count) AS billed_credit_cash_transaction_count
    ,SUM(dcbcj.billed_credit_cash_transaction_count_mtd) AS billed_credit_cash_transaction_count_mtd
    ,SUM(dcbcj.billed_credit_cash_transaction_count_mtd_ly) AS billed_credit_cash_transaction_count_mtd_ly
    ,SUM(dcbcj.billed_credit_cash_transaction_count_month_tot_lm) AS billed_credit_cash_transaction_count_month_tot_lm
    ,SUM(dcbcj.billed_credit_cash_transaction_count_month_tot_ly) AS billed_credit_cash_transaction_count_month_tot_ly

    ,SUM(dcbcj.on_retry_billed_credit_cash_transaction_count) AS on_retry_billed_credit_cash_transaction_count
    ,SUM(dcbcj.on_retry_billed_credit_cash_transaction_count_mtd) AS on_retry_billed_credit_cash_transaction_count_mtd
    ,SUM(dcbcj.on_retry_billed_credit_cash_transaction_count_mtd_ly) AS on_retry_billed_credit_cash_transaction_count_mtd_ly
    ,SUM(dcbcj.on_retry_billed_credit_cash_transaction_count_month_tot_lm) AS on_retry_billed_credit_cash_transaction_count_month_tot_lm
    ,SUM(dcbcj.on_retry_billed_credit_cash_transaction_count_month_tot_ly) AS on_retry_billed_credit_cash_transaction_count_month_tot_ly

    ,SUM(dcbcj.billing_cash_refund_amount) AS billing_cash_refund_amount
    ,SUM(dcbcj.billing_cash_refund_amount_mtd) AS billing_cash_refund_amount_mtd
    ,SUM(dcbcj.billing_cash_refund_amount_mtd_ly) AS billing_cash_refund_amount_mtd_ly
    ,SUM(dcbcj.billing_cash_refund_amount_month_tot_lm) AS billing_cash_refund_amount_month_tot_lm
    ,SUM(dcbcj.billing_cash_refund_amount_month_tot_ly) AS billing_cash_refund_amount_month_tot_ly

    ,SUM(dcbcj.billing_cash_chargeback_amount) AS billing_cash_chargeback_amount
    ,SUM(dcbcj.billing_cash_chargeback_amount_mtd) AS billing_cash_chargeback_amount_mtd
    ,SUM(dcbcj.billing_cash_chargeback_amount_mtd_ly) AS billing_cash_chargeback_amount_mtd_ly
    ,SUM(dcbcj.billing_cash_chargeback_amount_month_tot_lm) AS billing_cash_chargeback_amount_month_tot_lm
    ,SUM(dcbcj.billing_cash_chargeback_amount_month_tot_ly) AS billing_cash_chargeback_amount_month_tot_ly

    ,SUM(dcbcj.billed_credit_cash_transaction_amount) AS billed_credit_cash_transaction_amount
    ,SUM(dcbcj.billed_credit_cash_transaction_amount_mtd) AS billed_credit_cash_transaction_amount_mtd
    ,SUM(dcbcj.billed_credit_cash_transaction_amount_mtd_ly) AS billed_credit_cash_transaction_amount_mtd_ly
    ,SUM(dcbcj.billed_credit_cash_transaction_amount_month_tot_lm) AS billed_credit_cash_transaction_amount_month_tot_lm
    ,SUM(dcbcj.billed_credit_cash_transaction_amount_month_tot_ly) AS billed_credit_cash_transaction_amount_month_tot_ly

    ,SUM(dcbcj.billed_credit_cash_refund_chargeback_amount) AS billed_credit_cash_refund_chargeback_amount
    ,SUM(dcbcj.billed_credit_cash_refund_chargeback_amount_mtd) AS billed_credit_cash_refund_chargeback_amount_mtd
    ,SUM(dcbcj.billed_credit_cash_refund_chargeback_amount_mtd_ly) AS billed_credit_cash_refund_chargeback_amount_mtd_ly
    ,SUM(dcbcj.billed_credit_cash_refund_chargeback_amount_month_tot_lm) AS billed_credit_cash_refund_chargeback_amount_month_tot_lm
    ,SUM(dcbcj.billed_credit_cash_refund_chargeback_amount_month_tot_ly) AS billed_credit_cash_refund_chargeback_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.billed_cash_credit_redeemed_amount,0)) AS activating_billed_cash_credit_redeemed_amount

    ,SUM(dcbcj.billed_cash_credit_redeemed_amount) AS billed_cash_credit_redeemed_amount
    ,SUM(dcbcj.billed_cash_credit_redeemed_amount_mtd) AS billed_cash_credit_redeemed_amount_mtd
    ,SUM(dcbcj.billed_cash_credit_redeemed_amount_mtd_ly) AS billed_cash_credit_redeemed_amount_mtd_ly
    ,SUM(dcbcj.billed_cash_credit_redeemed_amount_month_tot_lm) AS billed_cash_credit_redeemed_amount_month_tot_lm
    ,SUM(dcbcj.billed_cash_credit_redeemed_amount_month_tot_ly) AS billed_cash_credit_redeemed_amount_month_tot_ly

    ,SUM(dcbcj.product_order_misc_cogs_amount) AS product_order_misc_cogs_amount
    ,SUM(dcbcj.product_order_misc_cogs_amount_mtd) AS product_order_misc_cogs_amount_mtd
    ,SUM(dcbcj.product_order_misc_cogs_amount_mtd_ly) AS product_order_misc_cogs_amount_mtd_ly
    ,SUM(dcbcj.product_order_misc_cogs_amount_month_tot_lm) AS product_order_misc_cogs_amount_month_tot_lm
    ,SUM(dcbcj.product_order_misc_cogs_amount_month_tot_ly) AS product_order_misc_cogs_amount_month_tot_ly

    ,SUM(dcbcj.product_order_cash_gross_profit) AS product_order_cash_gross_profit
    ,SUM(dcbcj.product_order_cash_gross_profit_mtd) AS product_order_cash_gross_profit_mtd
    ,SUM(dcbcj.product_order_cash_gross_profit_mtd_ly) AS product_order_cash_gross_profit_mtd_ly
    ,SUM(dcbcj.product_order_cash_gross_profit_month_tot_lm) AS product_order_cash_gross_profit_month_tot_lm
    ,SUM(dcbcj.product_order_cash_gross_profit_month_tot_ly) AS product_order_cash_gross_profit_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_cash_gross_profit,0)) AS nonactivating_product_order_cash_gross_profit
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_cash_gross_profit_mtd,0)) AS nonactivating_product_order_cash_gross_profit_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_cash_gross_profit_mtd_ly,0)) AS nonactivating_product_order_cash_gross_profit_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_cash_gross_profit_month_tot_lm,0)) AS nonactivating_product_order_cash_gross_profit_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_cash_gross_profit_month_tot_ly,0)) AS nonactivating_product_order_cash_gross_profit_month_tot_ly

    ,SUM(dcbcj.billed_cash_credit_redeemed_same_month_amount) AS billed_cash_credit_redeemed_same_month_amount
    ,SUM(dcbcj.billed_cash_credit_redeemed_same_month_amount_mtd) AS billed_cash_credit_redeemed_same_month_amount_mtd
    ,SUM(dcbcj.billed_cash_credit_redeemed_same_month_amount_mtd_ly) AS billed_cash_credit_redeemed_same_month_amount_mtd_ly
    ,SUM(dcbcj.billed_cash_credit_redeemed_same_month_amount_month_tot_lm) AS billed_cash_credit_redeemed_same_month_amount_month_tot_lm
    ,SUM(dcbcj.billed_cash_credit_redeemed_same_month_amount_month_tot_ly) AS billed_cash_credit_redeemed_same_month_amount_month_tot_ly

    ,SUM(dcbcj.billed_credit_cash_refund_count) AS billed_credit_cash_refund_count
    ,SUM(dcbcj.billed_credit_cash_refund_count_mtd) AS billed_credit_cash_refund_count_mtd
    ,SUM(dcbcj.billed_credit_cash_refund_count_mtd_ly) AS billed_credit_cash_refund_count_mtd_ly
    ,SUM(dcbcj.billed_credit_cash_refund_count_month_tot_lm) AS billed_credit_cash_refund_count_month_tot_lm
    ,SUM(dcbcj.billed_credit_cash_refund_count_month_tot_ly) AS billed_credit_cash_refund_count_month_tot_ly

    ,SUM(dcbcj.cash_variable_contribution_profit) AS cash_variable_contribution_profit
    ,SUM(dcbcj.cash_variable_contribution_profit_mtd) AS cash_variable_contribution_profit_mtd
    ,SUM(dcbcj.cash_variable_contribution_profit_mtd_ly) AS cash_variable_contribution_profit_mtd_ly
    ,SUM(dcbcj.cash_variable_contribution_profit_month_tot_lm) AS cash_variable_contribution_profit_month_tot_lm
    ,SUM(dcbcj.cash_variable_contribution_profit_month_tot_ly) AS cash_variable_contribution_profit_month_tot_ly

    ,SUM(dcbcj.billed_cash_credit_redeemed_equivalent_count) AS billed_cash_credit_redeemed_equivalent_count
    ,SUM(dcbcj.billed_cash_credit_redeemed_equivalent_count_mtd) AS billed_cash_credit_redeemed_equivalent_count_mtd
    ,SUM(dcbcj.billed_cash_credit_redeemed_equivalent_count_mtd_ly) AS billed_cash_credit_redeemed_equivalent_count_mtd_ly
    ,SUM(dcbcj.billed_cash_credit_redeemed_equivalent_count_month_tot_lm) AS billed_cash_credit_redeemed_equivalent_count_month_tot_lm
    ,SUM(dcbcj.billed_cash_credit_redeemed_equivalent_count_month_tot_ly) AS billed_cash_credit_redeemed_equivalent_count_month_tot_ly

    ,SUM(dcbcj.billed_cash_credit_cancelled_equivalent_count) AS billed_cash_credit_cancelled_equivalent_count
    ,SUM(dcbcj.billed_cash_credit_cancelled_equivalent_count_mtd) AS billed_cash_credit_cancelled_equivalent_count_mtd
    ,SUM(dcbcj.billed_cash_credit_cancelled_equivalent_count_mtd_ly) AS billed_cash_credit_cancelled_equivalent_count_mtd_ly
    ,SUM(dcbcj.billed_cash_credit_cancelled_equivalent_count_month_tot_lm) AS billed_cash_credit_cancelled_equivalent_count_month_tot_lm
    ,SUM(dcbcj.billed_cash_credit_cancelled_equivalent_count_month_tot_ly) AS billed_cash_credit_cancelled_equivalent_count_month_tot_ly

    ,SUM(dcbcj.leads) AS leads
    ,SUM(dcbcj.leads_mtd) AS leads_mtd
    ,SUM(dcbcj.leads_mtd_ly) AS leads_mtd_ly
    ,SUM(dcbcj.leads_month_tot_lm) AS leads_month_tot_lm
    ,SUM(dcbcj.leads_month_tot_ly) AS leads_month_tot_ly

    ,SUM(dcbcj.primary_leads) AS primary_leads
    ,SUM(dcbcj.primary_leads_mtd) AS primary_leads_mtd
    ,SUM(dcbcj.primary_leads_mtd_ly) AS primary_leads_mtd_ly
    ,SUM(dcbcj.primary_leads_month_tot_lm) AS primary_leads_month_tot_lm
    ,SUM(dcbcj.primary_leads_month_tot_ly) AS primary_leads_month_tot_ly

    ,SUM(dcbcj.reactivated_leads) AS reactivated_leads
    ,SUM(dcbcj.reactivated_leads_mtd) AS reactivated_leads_mtd
    ,SUM(dcbcj.reactivated_leads_mtd_ly) AS reactivated_leads_mtd_ly
    ,SUM(dcbcj.reactivated_leads_month_tot_lm) AS reactivated_leads_month_tot_lm
    ,SUM(dcbcj.reactivated_leads_month_tot_ly) AS reactivated_leads_month_tot_ly

    ,SUM(dcbcj.new_vips) AS new_vips
    ,SUM(dcbcj.new_vips_mtd) AS new_vips_mtd
    ,SUM(dcbcj.new_vips_mtd_ly) AS new_vips_mtd_ly
    ,SUM(dcbcj.new_vips_month_tot_lm) AS new_vips_month_tot_lm
    ,SUM(dcbcj.new_vips_month_tot_ly) AS new_vips_month_tot_ly

    ,SUM(dcbcj.reactivated_vips) AS reactivated_vips
    ,SUM(dcbcj.reactivated_vips_mtd) AS reactivated_vips_mtd
    ,SUM(dcbcj.reactivated_vips_mtd_ly) AS reactivated_vips_mtd_ly
    ,SUM(dcbcj.reactivated_vips_month_tot_lm) AS reactivated_vips_month_tot_lm
    ,SUM(dcbcj.reactivated_vips_month_tot_ly) AS reactivated_vips_month_tot_ly

    ,SUM(dcbcj.vips_from_reactivated_leads_m1) AS vips_from_reactivated_leads_m1
    ,SUM(dcbcj.vips_from_reactivated_leads_m1_mtd) AS vips_from_reactivated_leads_m1_mtd
    ,SUM(dcbcj.vips_from_reactivated_leads_m1_mtd_ly) AS vips_from_reactivated_leads_m1_mtd_ly
    ,SUM(dcbcj.vips_from_reactivated_leads_m1_month_tot_lm) AS vips_from_reactivated_leads_m1_month_tot_lm
    ,SUM(dcbcj.vips_from_reactivated_leads_m1_month_tot_ly) AS vips_from_reactivated_leads_m1_month_tot_ly

    ,SUM(dcbcj.paid_vips) AS paid_vips
    ,SUM(dcbcj.paid_vips_mtd) AS paid_vips_mtd
    ,SUM(dcbcj.paid_vips_mtd_ly) AS paid_vips_mtd_ly
    ,SUM(dcbcj.paid_vips_month_tot_lm) AS paid_vips_month_tot_lm
    ,SUM(dcbcj.paid_vips_month_tot_ly) AS paid_vips_month_tot_ly

    ,SUM(dcbcj.unpaid_vips) AS unpaid_vips
    ,SUM(dcbcj.unpaid_vips_mtd) AS unpaid_vips_mtd
    ,SUM(dcbcj.unpaid_vips_mtd_ly) AS unpaid_vips_mtd_ly
    ,SUM(dcbcj.unpaid_vips_month_tot_lm) AS unpaid_vips_month_tot_lm
    ,SUM(dcbcj.unpaid_vips_month_tot_ly) AS unpaid_vips_month_tot_ly

    ,SUM(dcbcj.new_vips_m1) AS new_vips_m1
    ,SUM(dcbcj.new_vips_m1_mtd) AS new_vips_m1_mtd
    ,SUM(dcbcj.new_vips_m1_mtd_ly) AS new_vips_m1_mtd_ly
    ,SUM(dcbcj.new_vips_m1_month_tot_lm) AS new_vips_m1_month_tot_lm
    ,SUM(dcbcj.new_vips_m1_month_tot_ly) AS new_vips_m1_month_tot_ly

    ,SUM(dcbcj.paid_vips_m1) AS paid_vips_m1
    ,SUM(dcbcj.paid_vips_m1_mtd) AS paid_vips_m1_mtd
    ,SUM(dcbcj.paid_vips_m1_mtd_ly) AS paid_vips_m1_mtd_ly
    ,SUM(dcbcj.paid_vips_m1_month_tot_lm) AS paid_vips_m1_month_tot_lm
    ,SUM(dcbcj.paid_vips_m1_month_tot_ly) AS paid_vips_m1_month_tot_ly

    ,SUM(dcbcj.cancels) AS cancels
    ,SUM(dcbcj.cancels_mtd) AS cancels_mtd
    ,SUM(dcbcj.cancels_mtd_ly) AS cancels_mtd_ly
    ,SUM(dcbcj.cancels_month_tot_lm) AS cancels_month_tot_lm
    ,SUM(dcbcj.cancels_month_tot_ly) AS cancels_month_tot_ly

    ,SUM(dcbcj.m1_cancels) AS m1_cancels
    ,SUM(dcbcj.m1_cancels_mtd) AS m1_cancels_mtd
    ,SUM(dcbcj.m1_cancels_mtd_ly) AS m1_cancels_mtd_ly
    ,SUM(dcbcj.m1_cancels_month_tot_lm) AS m1_cancels_month_tot_lm
    ,SUM(dcbcj.m1_cancels_month_tot_ly) AS m1_cancels_month_tot_ly

    ,SUM(dcbcj.bop_vips) AS bop_vips
    ,SUM(dcbcj.bop_vips_mtd) AS bop_vips_mtd
    ,SUM(dcbcj.bop_vips_mtd_ly) AS bop_vips_mtd_ly
    ,SUM(dcbcj.bop_vips_month_tot_lm) AS bop_vips_month_tot_lm
    ,SUM(dcbcj.bop_vips_month_tot_ly) AS bop_vips_month_tot_ly

    ,SUM(dcbcj.media_spend) AS media_spend
    ,SUM(dcbcj.media_spend_mtd) AS media_spend_mtd
    ,SUM(dcbcj.media_spend_mtd_ly) AS media_spend_mtd_ly
    ,SUM(dcbcj.media_spend_month_tot_lm) AS media_spend_month_tot_lm
    ,SUM(dcbcj.media_spend_month_tot_ly) AS media_spend_month_tot_ly

    ,SUM(dcbcj.product_order_return_unit_count) AS product_order_return_unit_count
    ,SUM(dcbcj.product_order_return_unit_count_mtd) AS product_order_return_unit_count_mtd
    ,SUM(dcbcj.product_order_return_unit_count_mtd_ly) AS product_order_return_unit_count_mtd_ly
    ,SUM(dcbcj.product_order_return_unit_count_month_tot_lm) AS product_order_return_unit_count_month_tot_lm
    ,SUM(dcbcj.product_order_return_unit_count_month_tot_ly) AS product_order_return_unit_count_month_tot_ly

    ,SUM(dcbcj.product_order_tariff_amount) AS product_order_tariff_amount

    ,SUM(dcbcj.product_order_shipping_discount_amount) AS product_order_shipping_discount_amount

    ,SUM(dcbcj.product_order_shipping_revenue_before_discount_amount) AS product_order_shipping_revenue_before_discount_amount

    ,SUM(dcbcj.product_order_tax_amount) AS product_order_tax_amount

    ,SUM(dcbcj.product_order_cash_transaction_amount) AS product_order_cash_transaction_amount

    ,SUM(dcbcj.product_order_cash_credit_redeemed_amount) AS product_order_cash_credit_redeemed_amount

    ,SUM(dcbcj.product_order_loyalty_unit_count) AS product_order_loyalty_unit_count

    ,SUM(dcbcj.product_order_outfit_component_unit_count) AS product_order_outfit_component_unit_count

    ,SUM(dcbcj.product_order_outfit_count) AS product_order_outfit_count

    ,SUM(dcbcj.product_order_discounted_unit_count) AS product_order_discounted_unit_count

    ,SUM(dcbcj.product_order_zero_revenue_unit_count) AS product_order_zero_revenue_unit_count

    ,SUM(dcbcj.product_order_cash_credit_refund_amount) AS product_order_cash_credit_refund_amount
    ,SUM(dcbcj.product_order_cash_credit_refund_amount_mtd) AS product_order_cash_credit_refund_amount_mtd
    ,SUM(dcbcj.product_order_cash_credit_refund_amount_mtd_ly) AS product_order_cash_credit_refund_amount_mtd_ly
    ,SUM(dcbcj.product_order_cash_credit_refund_amount_month_tot_lm) AS product_order_cash_credit_refund_amount_month_tot_lm
    ,SUM(dcbcj.product_order_cash_credit_refund_amount_month_tot_ly) AS product_order_cash_credit_refund_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_cash_credit_refund_amount,0)) AS activating_product_order_cash_credit_refund_amount
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_cash_credit_refund_amount_mtd,0)) AS activating_product_order_cash_credit_refund_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_cash_credit_refund_amount_mtd_ly,0)) AS activating_product_order_cash_credit_refund_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_cash_credit_refund_amount_month_tot_lm,0)) AS activating_product_order_cash_credit_refund_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_order_cash_credit_refund_amount_month_tot_ly,0)) AS activating_product_order_cash_credit_refund_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_cash_credit_refund_amount,0)) AS nonactivating_product_order_cash_credit_refund_amount
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_cash_credit_refund_amount_mtd,0)) AS nonactivating_product_order_cash_credit_refund_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_cash_credit_refund_amount_mtd_ly,0)) AS nonactivating_product_order_cash_credit_refund_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_cash_credit_refund_amount_month_tot_lm,0)) AS nonactivating_product_order_cash_credit_refund_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_cash_credit_refund_amount_month_tot_ly,0)) AS nonactivating_product_order_cash_credit_refund_amount_month_tot_ly

    ,SUM(dcbcj.product_order_noncash_credit_refund_amount) AS product_order_noncash_credit_refund_amount
    ,SUM(dcbcj.product_order_noncash_credit_refund_amount_mtd) AS product_order_noncash_credit_refund_amount_mtd
    ,SUM(dcbcj.product_order_noncash_credit_refund_amount_mtd_ly) AS product_order_noncash_credit_refund_amount_mtd_ly
    ,SUM(dcbcj.product_order_noncash_credit_refund_amount_month_tot_lm) AS product_order_noncash_credit_refund_amount_month_tot_lm
    ,SUM(dcbcj.product_order_noncash_credit_refund_amount_month_tot_ly) AS product_order_noncash_credit_refund_amount_month_tot_ly

    ,SUM(dcbcj.product_order_exchange_direct_cogs_amount) AS product_order_exchange_direct_cogs_amount

    ,SUM(dcbcj.product_order_selling_expenses_amount) AS product_order_selling_expenses_amount

    ,SUM(dcbcj.product_order_payment_processing_cost_amount) AS product_order_payment_processing_cost_amount

    ,SUM(dcbcj.product_order_variable_gms_cost_amount) AS product_order_variable_gms_cost_amount

    ,SUM(dcbcj.product_order_variable_warehouse_cost_amount) AS product_order_variable_warehouse_cost_amount

    ,SUM(dcbcj.billing_selling_expenses_amount) AS billing_selling_expenses_amount

    ,SUM(dcbcj.billing_payment_processing_cost_amount) AS billing_payment_processing_cost_amount

    ,SUM(dcbcj.billing_variable_gms_cost_amount) AS billing_variable_gms_cost_amount

    ,SUM(dcbcj.product_order_amount_to_pay) AS product_order_amount_to_pay

    ,SUM(dcbcj.product_gross_revenue_excl_shipping) AS product_gross_revenue_excl_shipping
    ,SUM(dcbcj.product_gross_revenue_excl_shipping_mtd) AS product_gross_revenue_excl_shipping_mtd
    ,SUM(dcbcj.product_gross_revenue_excl_shipping_mtd_ly) AS product_gross_revenue_excl_shipping_mtd_ly
    ,SUM(dcbcj.product_gross_revenue_excl_shipping_month_tot_lm) AS product_gross_revenue_excl_shipping_month_tot_lm
    ,SUM(dcbcj.product_gross_revenue_excl_shipping_month_tot_ly) AS product_gross_revenue_excl_shipping_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_gross_revenue_excl_shipping,0)) AS activating_product_gross_revenue_excl_shipping
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_gross_revenue_excl_shipping_mtd,0)) AS activating_product_gross_revenue_excl_shipping_mtd
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_gross_revenue_excl_shipping_mtd_ly,0)) AS activating_product_gross_revenue_excl_shipping_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_gross_revenue_excl_shipping_month_tot_lm,0)) AS activating_product_gross_revenue_excl_shipping_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_gross_revenue_excl_shipping_month_tot_ly,0)) AS activating_product_gross_revenue_excl_shipping_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_gross_revenue_excl_shipping,0)) AS nonactivating_product_gross_revenue_excl_shipping
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_gross_revenue_excl_shipping_mtd,0)) AS nonactivating_product_gross_revenue_excl_shipping_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_gross_revenue_excl_shipping_mtd_ly,0)) AS nonactivating_product_gross_revenue_excl_shipping_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_gross_revenue_excl_shipping_month_tot_lm,0)) AS nonactivating_product_gross_revenue_excl_shipping_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_gross_revenue_excl_shipping_month_tot_ly,0)) AS nonactivating_product_gross_revenue_excl_shipping_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_gross_revenue_excl_shipping,0)) AS guest_product_gross_revenue_excl_shipping
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_gross_revenue_excl_shipping_mtd,0)) AS guest_product_gross_revenue_excl_shipping_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_gross_revenue_excl_shipping_mtd_ly,0)) AS guest_product_gross_revenue_excl_shipping_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_gross_revenue_excl_shipping_month_tot_lm,0)) AS guest_product_gross_revenue_excl_shipping_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.product_gross_revenue_excl_shipping_month_tot_ly,0)) AS guest_product_gross_revenue_excl_shipping_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_gross_revenue_excl_shipping,0)) AS repeat_vip_product_gross_revenue_excl_shipping
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_gross_revenue_excl_shipping_mtd,0)) AS repeat_vip_product_gross_revenue_excl_shipping_mtd
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_gross_revenue_excl_shipping_mtd_ly,0)) AS repeat_vip_product_gross_revenue_excl_shipping_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_gross_revenue_excl_shipping_month_tot_lm,0)) AS repeat_vip_product_gross_revenue_excl_shipping_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l2='Repeat VIP',dcbcj.product_gross_revenue_excl_shipping_month_tot_ly,0)) AS repeat_vip_product_gross_revenue_excl_shipping_month_tot_ly

    ,SUM(dcbcj.product_margin_pre_return_excl_shipping) AS product_margin_pre_return_excl_shipping
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.product_margin_pre_return_excl_shipping,0)) AS activating_product_margin_pre_return_excl_shipping

    ,SUM(dcbcj.product_variable_contribution_profit) AS product_variable_contribution_profit

    ,SUM(dcbcj.product_order_cash_net_revenue) AS product_order_cash_net_revenue

    ,SUM(dcbcj.product_order_cash_margin_pre_return) AS product_order_cash_margin_pre_return

    ,SUM(dcbcj.billing_cash_gross_revenue) AS billing_cash_gross_revenue

    ,SUM(dcbcj.billing_cash_net_revenue) AS billing_cash_net_revenue

    ,SUM(dcbcj.billing_order_transaction_count) AS billing_order_transaction_count
    ,SUM(dcbcj.billing_order_transaction_count_mtd) AS billing_order_transaction_count_mtd
    ,SUM(dcbcj.billing_order_transaction_count_mtd_ly) AS billing_order_transaction_count_mtd_ly
    ,SUM(dcbcj.billing_order_transaction_count_month_tot_lm) AS billing_order_transaction_count_month_tot_lm
    ,SUM(dcbcj.billing_order_transaction_count_month_tot_ly) AS billing_order_transaction_count_month_tot_ly

    ,SUM(dcbcj.membership_fee_cash_transaction_amount) AS membership_fee_cash_transaction_amount
    ,SUM(dcbcj.membership_fee_cash_transaction_amount_mtd) AS membership_fee_cash_transaction_amount_mtd
    ,SUM(dcbcj.membership_fee_cash_transaction_amount_mtd_ly) AS membership_fee_cash_transaction_amount_mtd_ly
    ,SUM(dcbcj.membership_fee_cash_transaction_amount_month_tot_lm) AS membership_fee_cash_transaction_amount_month_tot_lm
    ,SUM(dcbcj.membership_fee_cash_transaction_amount_month_tot_ly) AS membership_fee_cash_transaction_amount_month_tot_ly

    ,SUM(dcbcj.gift_card_transaction_amount) AS gift_card_transaction_amount
    ,SUM(dcbcj.gift_card_transaction_amount_mtd) AS gift_card_transaction_amount_mtd
    ,SUM(dcbcj.gift_card_transaction_amount_mtd_ly) AS gift_card_transaction_amount_mtd_ly
    ,SUM(dcbcj.gift_card_transaction_amount_month_tot_lm) AS gift_card_transaction_amount_month_tot_lm
    ,SUM(dcbcj.gift_card_transaction_amount_month_tot_ly) AS gift_card_transaction_amount_month_tot_ly

    ,SUM(dcbcj.legacy_credit_cash_transaction_amount) AS legacy_credit_cash_transaction_amount
    ,SUM(dcbcj.legacy_credit_cash_transaction_amount_mtd) AS legacy_credit_cash_transaction_amount_mtd
    ,SUM(dcbcj.legacy_credit_cash_transaction_amount_mtd_ly) AS legacy_credit_cash_transaction_amount_mtd_ly
    ,SUM(dcbcj.legacy_credit_cash_transaction_amount_month_tot_lm) AS legacy_credit_cash_transaction_amount_month_tot_lm
    ,SUM(dcbcj.legacy_credit_cash_transaction_amount_month_tot_ly) AS legacy_credit_cash_transaction_amount_month_tot_ly

    ,SUM(dcbcj.membership_fee_cash_refund_chargeback_amount) AS membership_fee_cash_refund_chargeback_amount
    ,SUM(dcbcj.membership_fee_cash_refund_chargeback_amount_mtd) AS membership_fee_cash_refund_chargeback_amount_mtd
    ,SUM(dcbcj.membership_fee_cash_refund_chargeback_amount_mtd_ly) AS membership_fee_cash_refund_chargeback_amount_mtd_ly
    ,SUM(dcbcj.membership_fee_cash_refund_chargeback_amount_month_tot_lm) AS membership_fee_cash_refund_chargeback_amount_month_tot_lm
    ,SUM(dcbcj.membership_fee_cash_refund_chargeback_amount_month_tot_ly) AS membership_fee_cash_refund_chargeback_amount_month_tot_ly

    ,SUM(dcbcj.gift_card_cash_refund_chargeback_amount) AS gift_card_cash_refund_chargeback_amount
    ,SUM(dcbcj.gift_card_cash_refund_chargeback_amount_mtd) AS gift_card_cash_refund_chargeback_amount_mtd
    ,SUM(dcbcj.gift_card_cash_refund_chargeback_amount_mtd_ly) AS gift_card_cash_refund_chargeback_amount_mtd_ly
    ,SUM(dcbcj.gift_card_cash_refund_chargeback_amount_month_tot_lm) AS gift_card_cash_refund_chargeback_amount_month_tot_lm
    ,SUM(dcbcj.gift_card_cash_refund_chargeback_amount_month_tot_ly) AS gift_card_cash_refund_chargeback_amount_month_tot_ly

    ,SUM(dcbcj.legacy_credit_cash_refund_chargeback_amount) AS legacy_credit_cash_refund_chargeback_amount
    ,SUM(dcbcj.legacy_credit_cash_refund_chargeback_amount_mtd) AS legacy_credit_cash_refund_chargeback_amount_mtd
    ,SUM(dcbcj.legacy_credit_cash_refund_chargeback_amount_mtd_ly) AS legacy_credit_cash_refund_chargeback_amount_mtd_ly
    ,SUM(dcbcj.legacy_credit_cash_refund_chargeback_amount_month_tot_lm) AS legacy_credit_cash_refund_chargeback_amount_month_tot_lm
    ,SUM(dcbcj.legacy_credit_cash_refund_chargeback_amount_month_tot_ly) AS legacy_credit_cash_refund_chargeback_amount_month_tot_ly

    ,SUM(dcbcj.billed_cash_credit_issued_amount) AS billed_cash_credit_issued_amount

    ,SUM(dcbcj.billed_cash_credit_cancelled_amount) AS billed_cash_credit_cancelled_amount

    ,SUM(dcbcj.billed_cash_credit_expired_amount) AS billed_cash_credit_expired_amount

    ,SUM(dcbcj.billed_cash_credit_issued_equivalent_count) AS billed_cash_credit_issued_equivalent_count
    ,SUM(dcbcj.billed_cash_credit_issued_equivalent_count_mtd) AS billed_cash_credit_issued_equivalent_count_mtd
    ,SUM(dcbcj.billed_cash_credit_issued_equivalent_count_mtd_ly) AS billed_cash_credit_issued_equivalent_count_mtd_ly
    ,SUM(dcbcj.billed_cash_credit_issued_equivalent_count_month_tot_lm) AS billed_cash_credit_issued_equivalent_count_month_tot_lm
    ,SUM(dcbcj.billed_cash_credit_issued_equivalent_count_month_tot_ly) AS billed_cash_credit_issued_equivalent_count_month_tot_ly

    ,SUM(dcbcj.billed_cash_credit_expired_equivalent_count) AS billed_cash_credit_expired_equivalent_count

    ,SUM(dcbcj.refund_cash_credit_issued_amount) AS refund_cash_credit_issued_amount
    ,SUM(dcbcj.refund_cash_credit_issued_amount_mtd) AS refund_cash_credit_issued_amount_mtd
    ,SUM(dcbcj.refund_cash_credit_issued_amount_mtd_ly) AS refund_cash_credit_issued_amount_mtd_ly
    ,SUM(dcbcj.refund_cash_credit_issued_amount_month_tot_lm) AS refund_cash_credit_issued_amount_month_tot_lm
    ,SUM(dcbcj.refund_cash_credit_issued_amount_month_tot_ly) AS refund_cash_credit_issued_amount_month_tot_ly

    ,SUM(dcbcj.refund_cash_credit_redeemed_amount) AS refund_cash_credit_redeemed_amount
    ,SUM(dcbcj.refund_cash_credit_redeemed_amount_mtd) AS refund_cash_credit_redeemed_amount_mtd
    ,SUM(dcbcj.refund_cash_credit_redeemed_amount_mtd_ly) AS refund_cash_credit_redeemed_amount_mtd_ly
    ,SUM(dcbcj.refund_cash_credit_redeemed_amount_month_tot_lm) AS refund_cash_credit_redeemed_amount_month_tot_lm
    ,SUM(dcbcj.refund_cash_credit_redeemed_amount_month_tot_ly) AS refund_cash_credit_redeemed_amount_month_tot_ly

    ,SUM(dcbcj.refund_cash_credit_cancelled_amount) AS refund_cash_credit_cancelled_amount
    ,SUM(dcbcj.refund_cash_credit_cancelled_amount_mtd) AS refund_cash_credit_cancelled_amount_mtd
    ,SUM(dcbcj.refund_cash_credit_cancelled_amount_mtd_ly) AS refund_cash_credit_cancelled_amount_mtd_ly
    ,SUM(dcbcj.refund_cash_credit_cancelled_amount_month_tot_lm) AS refund_cash_credit_cancelled_amount_month_tot_lm
    ,SUM(dcbcj.refund_cash_credit_cancelled_amount_month_tot_ly) AS refund_cash_credit_cancelled_amount_month_tot_ly

    ,SUM(dcbcj.refund_cash_credit_expired_amount) AS refund_cash_credit_expired_amount

    ,SUM(dcbcj.other_cash_credit_issued_amount) AS other_cash_credit_issued_amount

    ,SUM(dcbcj.other_cash_credit_redeemed_amount) AS other_cash_credit_redeemed_amount
    ,SUM(dcbcj.other_cash_credit_redeemed_amount_mtd) AS other_cash_credit_redeemed_amount_mtd
    ,SUM(dcbcj.other_cash_credit_redeemed_amount_mtd_ly) AS other_cash_credit_redeemed_amount_mtd_ly
    ,SUM(dcbcj.other_cash_credit_redeemed_amount_month_tot_lm) AS other_cash_credit_redeemed_amount_month_tot_lm
    ,SUM(dcbcj.other_cash_credit_redeemed_amount_month_tot_ly) AS other_cash_credit_redeemed_amount_month_tot_ly

    ,SUM(dcbcj.other_cash_credit_cancelled_amount) AS other_cash_credit_cancelled_amount

    ,SUM(dcbcj.other_cash_credit_expired_amount) AS other_cash_credit_expired_amount

    ,SUM(dcbcj.cash_gift_card_redeemed_amount) AS cash_gift_card_redeemed_amount
    ,SUM(IFF(domc.membership_order_type_l1='Activating VIP',dcbcj.cash_gift_card_redeemed_amount,0)) AS activating_cash_gift_card_redeemed_amount
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.cash_gift_card_redeemed_amount,0)) AS nonactivating_cash_gift_card_redeemed_amount
    ,SUM(IFF(domc.membership_order_type_l2='Guest',dcbcj.cash_gift_card_redeemed_amount,0)) AS guest_cash_gift_card_redeemed_amount

    ,SUM(dcbcj.noncash_credit_issued_amount) AS noncash_credit_issued_amount
    ,SUM(dcbcj.noncash_credit_issued_amount_mtd) AS noncash_credit_issued_amount_mtd
    ,SUM(dcbcj.noncash_credit_issued_amount_mtd_ly) AS noncash_credit_issued_amount_mtd_ly
    ,SUM(dcbcj.noncash_credit_issued_amount_month_tot_lm) AS noncash_credit_issued_amount_month_tot_lm
    ,SUM(dcbcj.noncash_credit_issued_amount_month_tot_ly) AS noncash_credit_issued_amount_month_tot_ly

    ,SUM(dcbcj.noncash_credit_cancelled_amount) AS noncash_credit_cancelled_amount

    ,SUM(dcbcj.noncash_credit_expired_amount) AS noncash_credit_expired_amount

    ,SUM(dcbcj.merch_purchase_count) AS merch_purchase_count
    ,SUM(dcbcj.merch_purchase_count_mtd) AS merch_purchase_count_mtd
    ,SUM(dcbcj.merch_purchase_count_mtd_ly) AS merch_purchase_count_mtd_ly
    ,SUM(dcbcj.merch_purchase_count_month_tot_lm) AS merch_purchase_count_month_tot_lm
    ,SUM(dcbcj.merch_purchase_count_month_tot_ly) AS merch_purchase_count_month_tot_ly

    ,SUM(dcbcj.merch_purchase_hyperion_count) AS merch_purchase_hyperion_count
    ,SUM(dcbcj.merch_purchase_hyperion_count_mtd) AS merch_purchase_hyperion_count_mtd
    ,SUM(dcbcj.merch_purchase_hyperion_count_mtd_ly) AS merch_purchase_hyperion_count_mtd_ly
    ,SUM(dcbcj.merch_purchase_hyperion_count_month_tot_lm) AS merch_purchase_hyperion_count_month_tot_lm
    ,SUM(dcbcj.merch_purchase_hyperion_count_month_tot_ly) AS merch_purchase_hyperion_count_month_tot_ly

    ,SUM(dcbcj.product_order_non_token_subtotal_excl_tariff_amount) AS product_order_non_token_subtotal_excl_tariff_amount
    ,SUM(dcbcj.product_order_non_token_subtotal_excl_tariff_amount_mtd) AS product_order_non_token_subtotal_excl_tariff_amount_mtd
    ,SUM(dcbcj.product_order_non_token_subtotal_excl_tariff_amount_mtd_ly) AS product_order_non_token_subtotal_excl_tariff_amount_mtd_ly
    ,SUM(dcbcj.product_order_non_token_subtotal_excl_tariff_amount_month_tot_lm) AS product_order_non_token_subtotal_excl_tariff_amount_month_tot_lm
    ,SUM(dcbcj.product_order_non_token_subtotal_excl_tariff_amount_month_tot_ly) AS product_order_non_token_subtotal_excl_tariff_amount_month_tot_ly

    ,SUM(dcbcj.product_order_non_token_unit_count) AS product_order_non_token_unit_count
    ,SUM(dcbcj.product_order_non_token_unit_count_mtd) AS product_order_non_token_unit_count_mtd
    ,SUM(dcbcj.product_order_non_token_unit_count_mtd_ly) AS product_order_non_token_unit_count_mtd_ly
    ,SUM(dcbcj.product_order_non_token_unit_count_month_tot_lm) AS product_order_non_token_unit_count_month_tot_lm
    ,SUM(dcbcj.product_order_non_token_unit_count_month_tot_ly) AS product_order_non_token_unit_count_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_non_token_subtotal_excl_tariff_amount,0)) AS nonactivating_product_order_non_token_subtotal_excl_tariff_amount
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_non_token_subtotal_excl_tariff_amount_mtd,0)) AS nonactivating_product_order_non_token_subtotal_excl_tariff_amount_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_non_token_subtotal_excl_tariff_amount_mtd_ly,0)) AS nonactivating_product_order_non_token_subtotal_excl_tariff_amount_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_non_token_subtotal_excl_tariff_amount_month_tot_lm,0)) AS nonactivating_product_order_non_token_subtotal_excl_tariff_amount_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_non_token_subtotal_excl_tariff_amount_month_tot_ly,0)) AS nonactivating_product_order_non_token_subtotal_excl_tariff_amount_month_tot_ly

    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_non_token_unit_count,0)) AS nonactivating_product_order_non_token_unit_count
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_non_token_unit_count_mtd,0)) AS nonactivating_product_order_non_token_unit_count_mtd
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_non_token_unit_count_mtd_ly,0)) AS nonactivating_product_order_non_token_unit_count_mtd_ly
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_non_token_unit_count_month_tot_lm,0)) AS nonactivating_product_order_non_token_unit_count_month_tot_lm
    ,SUM(IFF(domc.membership_order_type_l1='NonActivating',dcbcj.product_order_non_token_unit_count_month_tot_ly,0)) AS nonactivating_product_order_non_token_unit_count_month_tot_ly

FROM _dcbc_j AS dcbcj
LEFT JOIN edw_prod.data_model_jfb.dim_order_membership_classification AS domc
    ON dcbcj.order_membership_classification_key = domc.order_membership_classification_key
GROUP BY dcbcj.date
    ,dcbcj.date_object
    ,dcbcj.currency_object
    ,dcbcj.currency_type
    ,dcbcj.store_brand
    ,dcbcj.business_unit
    ,dcbcj.report_mapping
    ,dcbcj.is_daily_cash_usd
    ,dcbcj.is_daily_cash_eur
;

SET execution_start_date_calc = CASE WHEN hour($execution_start_time) < 15 THEN dateadd(day,-1,$execution_start_time) ELSE $execution_start_time END :: date;

CREATE OR REPLACE TEMPORARY TABLE _pending_order AS
    SELECT
     $execution_start_date_calc AS calc_date
    ,fsm.report_mapping
    ,COUNT(*)                              AS pending_order_count
    ,SUM(subtotal_excl_tariff_local_amount
        + shipping_revenue_local_amount
        - product_discount_local_amount
        - cash_credit_local_amount
        - non_cash_credit_local_amount)  AS pending_order_local_amount
    ,SUM(order_date_usd_conversion_rate * (subtotal_excl_tariff_local_amount
        + shipping_revenue_local_amount
        - product_discount_local_amount
        - cash_credit_local_amount
        - non_cash_credit_local_amount))  AS pending_order_usd_amount
    ,SUM(order_date_eur_conversion_rate * (subtotal_excl_tariff_local_amount
        + shipping_revenue_local_amount
        - product_discount_local_amount
        - cash_credit_local_amount
        - non_cash_credit_local_amount))  AS pending_order_eur_amount
    ,SUM(product_order_cash_margin_pre_return_local_amount) AS pending_order_cash_margin_pre_return_local_amount
    ,SUM(order_date_usd_conversion_rate*product_order_cash_margin_pre_return_local_amount) AS pending_order_cash_margin_pre_return_usd_amount
    ,SUM(order_date_eur_conversion_rate*product_order_cash_margin_pre_return_local_amount) AS pending_order_cash_margin_pre_return_eur_amount
    FROM data_model_jfb.fact_order AS fo
    JOIN data_model_jfb.dim_order_status AS dos
        ON fo.order_status_key = dos.order_status_key
    JOIN data_model_jfb.dim_order_sales_channel AS dosc
        ON fo.order_sales_channel_key = dosc.order_sales_channel_key
    JOIN data_model_jfb.fact_activation AS fa
        ON fo.activation_key = fa.activation_key
    JOIN data_model_jfb.dim_store AS ds2
        ON fa.sub_store_id = ds2.store_id
    JOIN data_model_jfb.dim_customer AS dc
        ON fo.customer_id = dc.customer_id
    JOIN data_model_jfb.dim_order_processing_status AS dops
        ON fo.order_processing_status_key = dops.order_processing_status_key
    JOIN reference.finance_segment_mapping AS fsm
        ON fo.store_id = fsm.event_store_id
        AND IFF((fa.sub_store_id IS NOT NULL AND fa.sub_store_id <> -1), fa.sub_store_id, dc.store_id) = fsm.vip_store_id
        AND IFF(ds2.store_type = 'Retail', 1, 0) = fsm.is_retail_vip
        AND IFF(dc.gender ILIKE 'M' AND dc.registration_local_datetime >= '2020-01-01', 'M', 'F') = fsm.customer_gender
        AND dc.is_cross_promo = fsm.is_cross_promo
        AND dc.finance_specialty_store = fsm.finance_specialty_store
        AND dc.is_scrubs_customer = fsm.is_scrubs_customer
        AND fsm.metric_type = 'Orders'
    WHERE dos.order_status = 'Pending'
      AND dosc.order_classification_l1 = 'Product Order'
      AND DATEADD(MONTH, -1, $execution_start_date_calc) <= fo.order_local_datetime
      AND CASE
            WHEN iff(dc.finance_specialty_store is null, 'None', dc.finance_specialty_store) <> 'None' THEN fsm.report_mapping in  ('JF-TREV-DE-FR','FL-TREV-AT-DE','JF-TREV-BE-NL','JF-TREV-AT-DE','FL-TREV-BE-FR','FL+SC-TREV-AU-US','SX-TREV-AU-US','SX-TREV-BF-US','FK-TREV-CA-US','SD-TREV-CA-US')
            ELSE TRUE
        END
    GROUP BY calc_date, fsm.report_mapping
;

/*SKIP Rate Calculation*/
SET begin_of_month_date = DATE_TRUNC('MONTH', $execution_start_date_calc);

CREATE OR REPLACE TEMPORARY TABLE _skip_purchases AS
SELECT DISTINCT DATE_TRUNC('MONTH', date) AS month_date, customer_id
FROM analytics_base.finance_sales_ops fso
JOIN data_model_jfb.dim_store ds ON ds.store_id = fso.store_id
    AND store_brand_abbr = 'FL'
JOIN data_model_jfb.dim_order_membership_classification domc ON domc.order_membership_classification_key = fso.order_membership_classification_key
    AND domc.membership_order_type_l3 = 'Repeat VIP'
WHERE date_object = 'placed'
    AND is_bop_vip = TRUE
    AND DAY(date) BETWEEN 1 AND 5
    AND (
        (store_country = 'US' AND fso.date < '2021-01-01')
        OR (store_country = 'CA' AND fso.date < '2021-08-01')
        OR (store_region = 'EU' AND fso.date < '2021-11-01')
    );

CREATE OR REPLACE TEMPORARY TABLE _bom_vips_current_month AS
SELECT DISTINCT fa.sub_store_id AS event_store_id,
    fa.sub_store_id             AS vip_store_id,
    fa.is_retail_vip            AS is_retail_vip,
    dc.gender                   AS customer_gender,
    dc.is_cross_promo           AS is_cross_promo,
    dc.finance_specialty_store  AS finance_specialty_store,
    fa.is_scrubs_vip            AS is_scrubs_customer,
    $begin_of_month_date        AS date,
    fa.customer_id              AS customer_id
FROM data_model_jfb.fact_activation fa
JOIN data_model_jfb.dim_customer dc ON dc.customer_id = fa.customer_id
JOIN data_model_jfb.dim_store st ON st.store_id = fa.sub_store_id
WHERE fa.activation_local_datetime::DATE < $begin_of_month_date
    AND fa.cancellation_local_datetime::DATE >= $begin_of_month_date;

/*Customer who skipped the billing*/
CREATE OR REPLACE TEMPORARY TABLE _skip_customers_current_month AS
SELECT bv.customer_id
FROM lake_jfb_view.ultra_merchant.membership_skip ms
JOIN lake_jfb_view.ultra_merchant.period p ON p.period_id = ms.period_id
    AND date_period_start = $begin_of_month_date
JOIN lake_jfb_view.ultra_merchant.membership m ON m.membership_id = ms.membership_id
JOIN (SELECT DISTINCT customer_id FROM _bom_vips_current_month) bv on m.customer_id = bv.customer_id

UNION
/*Purchases between 1st and 5th of the month*/
SELECT edw_prod.stg.udf_unconcat_brand(customer_id) AS customer_id
FROM _skip_purchases
WHERE month_date = $begin_of_month_date;

CREATE OR REPLACE TEMPORARY TABLE _skip_count_current_month AS
SELECT $execution_start_date_calc AS calc_date,
    fsm.report_mapping,
    CAST('usd' AS VARCHAR) AS currency_object,
    CAST('USD' AS VARCHAR) AS currency_type,
    COUNT(1) AS bom_vips,
    COUNT(sc.customer_id) AS skip_count
FROM _bom_vips_current_month bv
JOIN reference.finance_segment_mapping fsm ON bv.event_store_id = fsm.event_store_id
    AND bv.vip_store_id = fsm.vip_store_id
    AND bv.is_retail_vip = fsm.is_retail_vip
    AND bv.customer_gender = fsm.customer_gender
    AND bv.is_cross_promo = fsm.is_cross_promo
    AND bv.finance_specialty_store = fsm.finance_specialty_store
    AND bv.is_scrubs_customer = fsm.is_scrubs_customer
    AND fsm.metric_type = 'Acquisition'
    AND (
            fsm.is_daily_cash_usd = TRUE
            OR fsm.is_daily_cash_eur = TRUE
            OR fsm.report_mapping IN ('FL+SC-OREV-NA')
    )
LEFT JOIN _skip_customers_current_month sc ON sc.customer_id = bv.customer_id
WHERE report_mapping ILIKE '%-TREV-%'
    AND report_mapping NOT ILIKE '%-CA-%'
    AND report_mapping NOT ILIKE '%-AT-%'
    AND report_mapping NOT ILIKE '%-BE-%'
    AND report_mapping NOT ILIKE '%-AU-%'
GROUP BY fsm.report_mapping;

INSERT INTO _skip_count_current_month
SELECT $execution_start_date_calc AS calc_date,
    fsm.report_mapping,
    'local' AS currency_object,
    CASE
       WHEN store_country = 'SE' THEN 'SEK'
       WHEN store_country = 'DK' THEN 'DKK'
       WHEN store_country = 'UK' THEN 'GBP'
       ELSE 'EUR' END AS currency_type,
    COUNT(1) AS bom_vips,
    COUNT(sc.customer_id) AS skip_count
FROM _bom_vips_current_month bv
JOIN data_model_jfb.dim_store ds ON ds.store_id = bv.event_store_id
    AND ds.store_region = 'EU'
JOIN reference.finance_segment_mapping fsm ON bv.event_store_id = fsm.event_store_id
    AND bv.vip_store_id = fsm.vip_store_id
    AND bv.is_retail_vip = fsm.is_retail_vip
    AND bv.customer_gender = fsm.customer_gender
    AND bv.is_cross_promo = fsm.is_cross_promo
    AND bv.finance_specialty_store = fsm.finance_specialty_store
    AND bv.is_scrubs_customer = fsm.is_scrubs_customer
    AND fsm.metric_type = 'Acquisition'
    AND (
            fsm.is_daily_cash_usd = TRUE
            OR fsm.is_daily_cash_eur = TRUE
            OR fsm.report_mapping IN ('FL+SC-OREV-NA')
    )
LEFT JOIN _skip_customers_current_month sc ON sc.customer_id = bv.customer_id
WHERE report_mapping ILIKE '%-TREV-%'
    AND report_mapping NOT ILIKE '%-CA-%'
    AND report_mapping NOT ILIKE '%-AT-%'
    AND report_mapping NOT ILIKE '%-BE-%'
    AND report_mapping NOT ILIKE '%-AU-%'
GROUP BY fsm.report_mapping,
    CASE
       WHEN store_country = 'SE' THEN 'SEK'
       WHEN store_country = 'DK' THEN 'DKK'
       WHEN store_country = 'UK' THEN 'GBP'
       ELSE 'EUR' END;

CREATE OR REPLACE TEMPORARY TABLE _skip_count_historical_base AS
SELECT cltvm.month_date AS date,
    fa.sub_store_id AS event_store_id,
    vip_store_id,
    cltvm.is_retail_vip,
    cltvm.gender AS customer_gender,
    cltvm.is_cross_promo,
    cltvm.finance_specialty_store,
    fa.is_scrubs_vip as is_scrubs_customer,
    COUNT(DISTINCT cltvm.customer_id) AS skip_count
FROM analytics_base.customer_lifetime_value_monthly_cust cltvm
JOIN data_model_jfb.fact_activation fa ON cltvm.activation_key = fa.activation_key
LEFT JOIN _skip_purchases sp ON cltvm.month_date = sp.month_date
    AND cltvm.customer_id = sp.customer_id
WHERE is_bop_vip = TRUE
    AND (
        is_skip = TRUE
        OR sp.customer_id IS NOT NULL
    )
    AND cltvm.month_date < $begin_of_month_date
--    AND cltvm.month_date >= $refresh_from_date
GROUP BY cltvm.month_date,
    fa.sub_store_id,
    vip_store_id,
    cltvm.is_retail_vip,
    cltvm.gender,
    cltvm.is_cross_promo,
    cltvm.finance_specialty_store,
    fa.is_scrubs_vip;

CREATE OR REPLACE TEMPORARY TABLE _skip_count_historical AS
SELECT schb.date,
    fsm.report_mapping,
    CAST('usd' AS VARCHAR) AS currency_object,
    CAST('USD' AS VARCHAR) AS currency_type,
    SUM(schb.skip_count) AS skip_count
FROM _skip_count_historical_base schb
JOIN reference.finance_segment_mapping fsm ON schb.event_store_id = fsm.event_store_id
    AND schb.vip_store_id = fsm.vip_store_id
    AND schb.is_retail_vip = fsm.is_retail_vip
    AND schb.customer_gender = fsm.customer_gender
    AND schb.is_cross_promo = fsm.is_cross_promo
    AND schb.finance_specialty_store = fsm.finance_specialty_store
    AND schb.is_scrubs_customer = fsm.is_scrubs_customer
    AND fsm.metric_type = 'Acquisition'
    AND (
            fsm.is_daily_cash_usd = TRUE
            OR fsm.is_daily_cash_eur = TRUE
            OR fsm.report_mapping IN ('FL+SC-OREV-NA')
    )
WHERE report_mapping ILIKE '%-TREV-%'
    AND report_mapping NOT ILIKE '%-CA-%'
    AND report_mapping NOT ILIKE '%-AT-%'
    AND report_mapping NOT ILIKE '%-BE-%'
    AND report_mapping NOT ILIKE '%-AU-%'
GROUP BY schb.date,
    fsm.report_mapping;

INSERT INTO _skip_count_historical
SELECT schb.date,
    fsm.report_mapping,
    'local' AS currency_object,
    CASE
       WHEN store_country = 'SE' THEN 'SEK'
       WHEN store_country = 'DK' THEN 'DKK'
       WHEN store_country = 'UK' THEN 'GBP'
       ELSE 'EUR' END AS currency_type,
    SUM(schb.skip_count) AS skip_count
FROM _skip_count_historical_base schb
JOIN reference.finance_segment_mapping fsm ON schb.event_store_id = fsm.event_store_id
    AND schb.vip_store_id = fsm.vip_store_id
    AND schb.is_retail_vip = fsm.is_retail_vip
    AND schb.customer_gender = fsm.customer_gender
    AND schb.is_cross_promo = fsm.is_cross_promo
    AND schb.finance_specialty_store = fsm.finance_specialty_store
    AND schb.is_scrubs_customer = fsm.is_scrubs_customer
    AND fsm.metric_type = 'Acquisition'
    AND (
            fsm.is_daily_cash_usd = TRUE
            OR fsm.is_daily_cash_eur = TRUE
            OR fsm.report_mapping IN ('FL+SC-OREV-NA')
    )
JOIN data_model.dim_store ds ON ds.store_id = schb.event_store_id
    AND ds.store_region = 'EU'
WHERE report_mapping ILIKE '%-TREV-%'
    AND report_mapping NOT ILIKE '%-CA-%'
    AND report_mapping NOT ILIKE '%-AT-%'
    AND report_mapping NOT ILIKE '%-BE-%'
    AND report_mapping NOT ILIKE '%-AU-%'
GROUP BY schb.date,
    fsm.report_mapping,
    CASE WHEN ds.store_country = 'SE' THEN 'SEK'
       WHEN ds.store_country = 'DK' THEN 'DKK'
       WHEN ds.store_country = 'UK' THEN 'GBP'
       ELSE 'EUR' END;

CREATE OR REPLACE TEMPORARY TABLE _bom_vips AS
SELECT date,
    report_mapping,
    currency_object,
    currency_type,
    SUM(bop_vips) AS bom_vips
FROM analytics_base.acquisition_media_spend_daily_agg amsda
JOIN reference.finance_segment_mapping AS fsm
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
WHERE amsda.date = DATE_TRUNC('MONTH', amsda.date)
--    AND amsda.date >= $refresh_from_date
    AND date_object = 'placed'
    AND report_mapping ILIKE '%-TREV-%'
    AND report_mapping NOT ILIKE '%-CA-%'
    AND report_mapping NOT ILIKE '%-AT-%'
    AND report_mapping NOT ILIKE '%-BE-%'
    AND report_mapping NOT ILIKE '%-AU-%'
GROUP BY date,
    report_mapping,
    currency_object,
    currency_type;

CREATE OR REPLACE TEMPORARY TABLE _skip_count AS
SELECT LAST_DAY(date, 'MONTH') AS date,
    report_mapping,
    currency_object,
    currency_type,
    skip_count
FROM _skip_count_historical
UNION
SELECT calc_date AS date,
    report_mapping,
    currency_object,
    currency_type,
    skip_count
FROM _skip_count_current_month;


CREATE OR REPLACE TEMPORARY TABLE _future_budget_forecast_scaffold AS
SELECT DISTINCT dcbcja.date_object
              , dcbcja.currency_object
              , dcbcja.currency_type
              , dcbcja.store_brand
              , dcbcja.business_unit
              , dcbcja.report_mapping
              , dcbcja.is_daily_cash_usd
              , dcbcja.is_daily_cash_eur
              , fd.financial_date as date
FROM reporting.daily_cash_base_calc dcbcja
         CROSS JOIN (SELECT DISTINCT financial_date::DATE AS financial_date
                     FROM reference.finance_budget_forecast
                     WHERE version IN ('Budget', 'Forecast')
                       AND financial_date::DATE > $execution_start_date_calc) fd
;

--DELETE FROM reporting.daily_cash_final_output
--WHERE $is_full_refresh = TRUE;

--DELETE FROM reporting.daily_cash_final_output
--WHERE $is_full_refresh = FALSE
--AND date >= $refresh_from_date;

truncate table reporting.daily_cash_final_output;

INSERT INTO reporting.daily_cash_final_output(
    date,
    date_object,
    currency_object,
    currency_type,
    store_brand,
    business_unit,
    report_mapping,
    is_daily_cash_usd,
    is_daily_cash_eur,
    product_order_subtotal_excl_tariff_amount,
    product_order_subtotal_excl_tariff_amount_mtd,
    product_order_subtotal_excl_tariff_amount_mtd_ly,
    product_order_subtotal_excl_tariff_amount_month_tot_lm,
    product_order_subtotal_excl_tariff_amount_month_tot_ly,
    activating_product_order_subtotal_excl_tariff_amount,
    activating_product_order_subtotal_excl_tariff_amount_mtd,
    activating_product_order_subtotal_excl_tariff_amount_mtd_ly,
    activating_product_order_subtotal_excl_tariff_amount_month_tot_lm,
    activating_product_order_subtotal_excl_tariff_amount_month_tot_ly,
    nonactivating_product_order_subtotal_excl_tariff_amount,
    nonactivating_product_order_subtotal_excl_tariff_amount_mtd,
    nonactivating_product_order_subtotal_excl_tariff_amount_mtd_ly,
    nonactivating_product_order_subtotal_excl_tariff_amount_month_tot_lm,
    nonactivating_product_order_subtotal_excl_tariff_amount_month_tot_ly,
    guest_product_order_subtotal_excl_tariff_amount,
    guest_product_order_subtotal_excl_tariff_amount_mtd,
    guest_product_order_subtotal_excl_tariff_amount_mtd_ly,
    guest_product_order_subtotal_excl_tariff_amount_month_tot_lm,
    guest_product_order_subtotal_excl_tariff_amount_month_tot_ly,
    repeat_vip_product_order_subtotal_excl_tariff_amount,
    repeat_vip_product_order_subtotal_excl_tariff_amount_mtd,
    repeat_vip_product_order_subtotal_excl_tariff_amount_mtd_ly,
    repeat_vip_product_order_subtotal_excl_tariff_amount_month_tot_lm,
    repeat_vip_product_order_subtotal_excl_tariff_amount_month_tot_ly,
    product_order_product_subtotal_amount,
    product_order_product_subtotal_amount_mtd,
    product_order_product_subtotal_amount_mtd_ly,
    product_order_product_subtotal_amount_month_tot_lm,
    product_order_product_subtotal_amount_month_tot_ly,
    activating_product_order_product_subtotal_amount,
    activating_product_order_product_subtotal_amount_mtd,
    activating_product_order_product_subtotal_amount_mtd_ly,
    activating_product_order_product_subtotal_amount_month_tot_lm,
    activating_product_order_product_subtotal_amount_month_tot_ly,
    nonactivating_product_order_product_subtotal_amount,
    nonactivating_product_order_product_subtotal_amount_mtd,
    nonactivating_product_order_product_subtotal_amount_mtd_ly,
    nonactivating_product_order_product_subtotal_amount_month_tot_lm,
    nonactivating_product_order_product_subtotal_amount_month_tot_ly,
    product_order_product_discount_amount,
    product_order_product_discount_amount_mtd,
    product_order_product_discount_amount_mtd_ly,
    product_order_product_discount_amount_month_tot_lm,
    product_order_product_discount_amount_month_tot_ly,
    activating_product_order_product_discount_amount,
    activating_product_order_product_discount_amount_mtd,
    activating_product_order_product_discount_amount_mtd_ly,
    activating_product_order_product_discount_amount_month_tot_lm,
    activating_product_order_product_discount_amount_month_tot_ly,
    nonactivating_product_order_product_discount_amount,
    nonactivating_product_order_product_discount_amount_mtd,
    nonactivating_product_order_product_discount_amount_mtd_ly,
    nonactivating_product_order_product_discount_amount_month_tot_lm,
    nonactivating_product_order_product_discount_amount_month_tot_ly,
    guest_product_order_product_discount_amount,
    guest_product_order_product_discount_amount_mtd,
    guest_product_order_product_discount_amount_mtd_ly,
    guest_product_order_product_discount_amount_month_tot_lm,
    guest_product_order_product_discount_amount_month_tot_ly,
    repeat_vip_product_order_product_discount_amount,
    repeat_vip_product_order_product_discount_amount_mtd,
    repeat_vip_product_order_product_discount_amount_mtd_ly,
    repeat_vip_product_order_product_discount_amount_month_tot_lm,
    repeat_vip_product_order_product_discount_amount_month_tot_ly,
    shipping_revenue,
    shipping_revenue_mtd,
    shipping_revenue_mtd_ly,
    shipping_revenue_month_tot_lm,
    shipping_revenue_month_tot_ly,
    activating_shipping_revenue,
    activating_shipping_revenue_mtd,
    activating_shipping_revenue_mtd_ly,
    activating_shipping_revenue_month_tot_lm,
    activating_shipping_revenue_month_tot_ly,
    nonactivating_shipping_revenue,
    nonactivating_shipping_revenue_mtd,
    nonactivating_shipping_revenue_mtd_ly,
    nonactivating_shipping_revenue_month_tot_lm,
    nonactivating_shipping_revenue_month_tot_ly,
    guest_shipping_revenue,
    guest_shipping_revenue_mtd,
    guest_shipping_revenue_mtd_ly,
    guest_shipping_revenue_month_tot_lm,
    guest_shipping_revenue_month_tot_ly,
    product_order_noncash_credit_redeemed_amount,
    product_order_noncash_credit_redeemed_amount_mtd,
    product_order_noncash_credit_redeemed_amount_mtd_ly,
    product_order_noncash_credit_redeemed_amount_month_tot_lm,
    product_order_noncash_credit_redeemed_amount_month_tot_ly,
    activating_product_order_noncash_credit_redeemed_amount,
    activating_product_order_noncash_credit_redeemed_amount_mtd,
    activating_product_order_noncash_credit_redeemed_amount_mtd_ly,
    activating_product_order_noncash_credit_redeemed_amount_month_tot_lm,
    activating_product_order_noncash_credit_redeemed_amount_month_tot_ly,
    nonactivating_product_order_noncash_credit_redeemed_amount,
    nonactivating_product_order_noncash_credit_redeemed_amount_mtd,
    nonactivating_product_order_noncash_credit_redeemed_amount_mtd_ly,
    nonactivating_product_order_noncash_credit_redeemed_amount_month_tot_lm,
    nonactivating_product_order_noncash_credit_redeemed_amount_month_tot_ly,
    repeat_vip_product_order_noncash_credit_redeemed_amount,
    repeat_vip_product_order_noncash_credit_redeemed_amount_mtd,
    repeat_vip_product_order_noncash_credit_redeemed_amount_mtd_ly,
    repeat_vip_product_order_noncash_credit_redeemed_amount_month_tot_lm,
    repeat_vip_product_order_noncash_credit_redeemed_amount_month_tot_ly,
    product_order_count,
    product_order_count_mtd,
    product_order_count_mtd_ly,
    product_order_count_month_tot_lm,
    product_order_count_month_tot_ly,
    activating_product_order_count,
    activating_product_order_count_mtd,
    activating_product_order_count_mtd_ly,
    activating_product_order_count_month_tot_lm,
    activating_product_order_count_month_tot_ly,
    activating_product_order_count_budget,
    activating_product_order_count_budget_alt,
    activating_product_order_count_forecast,
    nonactivating_product_order_count,
    nonactivating_product_order_count_mtd,
    nonactivating_product_order_count_mtd_ly,
    nonactivating_product_order_count_month_tot_lm,
    nonactivating_product_order_count_month_tot_ly,
    nonactivating_product_order_count_budget,
    nonactivating_product_order_count_budget_alt,
    nonactivating_product_order_count_forecast,
    guest_product_order_count,
    guest_product_order_count_mtd,
    guest_product_order_count_mtd_ly,
    guest_product_order_count_month_tot_lm,
    guest_product_order_count_month_tot_ly,
    repeat_vip_product_order_count,
    repeat_vip_product_order_count_mtd,
    repeat_vip_product_order_count_mtd_ly,
    repeat_vip_product_order_count_month_tot_lm,
    repeat_vip_product_order_count_month_tot_ly,
    reactivated_vip_product_order_count,
    reactivated_vip_product_order_count_mtd,
    reactivated_vip_product_order_count_mtd_ly,
    reactivated_vip_product_order_count_month_tot_lm,
    reactivated_vip_product_order_count_month_tot_ly,
    product_order_count_excl_seeding,
    product_order_count_excl_seeding_mtd,
    product_order_count_excl_seeding_mtd_ly,
    product_order_count_excl_seeding_month_tot_lm,
    product_order_count_excl_seeding_month_tot_ly,
    activating_product_order_count_excl_seeding,
    activating_product_order_count_excl_seeding_mtd,
    activating_product_order_count_excl_seeding_mtd_ly,
    activating_product_order_count_excl_seeding_month_tot_lm,
    activating_product_order_count_excl_seeding_month_tot_ly,
    nonactivating_product_order_count_excl_seeding,
    nonactivating_product_order_count_excl_seeding_mtd,
    nonactivating_product_order_count_excl_seeding_mtd_ly,
    nonactivating_product_order_count_excl_seeding_month_tot_lm,
    nonactivating_product_order_count_excl_seeding_month_tot_ly,
    guest_product_order_count_excl_seeding,
    guest_product_order_count_excl_seeding_mtd,
    guest_product_order_count_excl_seeding_mtd_ly,
    guest_product_order_count_excl_seeding_month_tot_lm,
    guest_product_order_count_excl_seeding_month_tot_ly,
    repeat_vip_product_order_count_excl_seeding,
    repeat_vip_product_order_count_excl_seeding_mtd,
    repeat_vip_product_order_count_excl_seeding_mtd_ly,
    repeat_vip_product_order_count_excl_seeding_month_tot_lm,
    repeat_vip_product_order_count_excl_seeding_month_tot_ly,
    reactivated_vip_product_order_count_excl_seeding,
    reactivated_vip_product_order_count_excl_seeding_mtd,
    reactivated_vip_product_order_count_excl_seeding_mtd_ly,
    reactivated_vip_product_order_count_excl_seeding_month_tot_lm,
    reactivated_vip_product_order_count_excl_seeding_month_tot_ly,
    product_margin_pre_return_excl_seeding,
    product_margin_pre_return_excl_seeding_mtd,
    product_margin_pre_return_excl_seeding_mtd_ly,
    product_margin_pre_return_excl_seeding_month_tot_lm,
    product_margin_pre_return_excl_seeding_month_tot_ly,
    activating_product_margin_pre_return_excl_seeding,
    activating_product_margin_pre_return_excl_seeding_mtd,
    activating_product_margin_pre_return_excl_seeding_mtd_ly,
    activating_product_margin_pre_return_excl_seeding_month_tot_lm,
    activating_product_margin_pre_return_excl_seeding_month_tot_ly,
    nonactivating_product_margin_pre_return_excl_seeding,
    nonactivating_product_margin_pre_return_excl_seeding_mtd,
    nonactivating_product_margin_pre_return_excl_seeding_mtd_ly,
    nonactivating_product_margin_pre_return_excl_seeding_month_tot_lm,
    nonactivating_product_margin_pre_return_excl_seeding_month_tot_ly,
    guest_product_margin_pre_return_excl_seeding,
    guest_product_margin_pre_return_excl_seeding_mtd,
    guest_product_margin_pre_return_excl_seeding_mtd_ly,
    guest_product_margin_pre_return_excl_seeding_month_tot_lm,
    guest_product_margin_pre_return_excl_seeding_month_tot_ly,
    repeat_vip_product_margin_pre_return_excl_seeding,
    repeat_vip_product_margin_pre_return_excl_seeding_mtd,
    repeat_vip_product_margin_pre_return_excl_seeding_mtd_ly,
    repeat_vip_product_margin_pre_return_excl_seeding_month_tot_lm,
    repeat_vip_product_margin_pre_return_excl_seeding_month_tot_ly,
    reactivated_vip_product_margin_pre_return_excl_seeding,
    reactivated_vip_product_margin_pre_return_excl_seeding_mtd,
    reactivated_vip_product_margin_pre_return_excl_seeding_mtd_ly,
    reactivated_vip_product_margin_pre_return_excl_seeding_month_tot_lm,
    reactivated_vip_product_margin_pre_return_excl_seeding_month_tot_ly,
    unit_count,
    unit_count_mtd,
    unit_count_mtd_ly,
    unit_count_month_tot_lm,
    unit_count_month_tot_ly,
    unit_count_budget,
    unit_count_budget_alt,
    unit_count_forecast,
    activating_unit_count,
    activating_unit_count_mtd,
    activating_unit_count_mtd_ly,
    activating_unit_count_month_tot_lm,
    activating_unit_count_month_tot_ly,
    activating_unit_count_budget,
    activating_unit_count_budget_alt,
    activating_unit_count_forecast,
    nonactivating_unit_count,
    nonactivating_unit_count_mtd,
    nonactivating_unit_count_mtd_ly,
    nonactivating_unit_count_month_tot_lm,
    nonactivating_unit_count_month_tot_ly,
    nonactivating_unit_count_budget,
    nonactivating_unit_count_budget_alt,
    nonactivating_unit_count_forecast,
    guest_unit_count,
    guest_unit_count_mtd,
    guest_unit_count_mtd_ly,
    guest_unit_count_month_tot_lm,
    guest_unit_count_month_tot_ly,
    repeat_vip_unit_count,
    repeat_vip_unit_count_mtd,
    repeat_vip_unit_count_mtd_ly,
    repeat_vip_unit_count_month_tot_lm,
    repeat_vip_unit_count_month_tot_ly,
    activating_product_order_air_vip_price,
    activating_product_order_air_vip_price_mtd,
    activating_product_order_air_vip_price_mtd_ly,
    activating_product_order_air_vip_price_month_tot_lm,
    activating_product_order_air_vip_price_month_tot_ly,
    nonactivating_product_order_air_price,
    nonactivating_product_order_air_price_mtd,
    nonactivating_product_order_air_price_mtd_ly,
    nonactivating_product_order_air_price_month_tot_lm,
    nonactivating_product_order_air_price_month_tot_ly,
    repeat_vip_product_order_air_vip_price,
    repeat_vip_product_order_air_vip_price_mtd,
    repeat_vip_product_order_air_vip_price_mtd_ly,
    repeat_vip_product_order_air_vip_price_month_tot_lm,
    repeat_vip_product_order_air_vip_price_month_tot_ly,
    guest_product_order_air_vip_price,
    guest_product_order_air_vip_price_mtd,
    guest_product_order_air_vip_price_mtd_ly,
    guest_product_order_air_vip_price_month_tot_lm,
    guest_product_order_air_vip_price_month_tot_ly,
    guest_product_order_retail_unit_price,
    guest_product_order_retail_unit_price_mtd,
    guest_product_order_retail_unit_price_mtd_ly,
    guest_product_order_retail_unit_price_month_tot_lm,
    guest_product_order_retail_unit_price_month_tot_ly,
    activating_product_order_price_offered_amount,
    activating_product_order_price_offered_amount_mtd,
    activating_product_order_price_offered_amount_mtd_ly,
    activating_product_order_price_offered_amount_month_tot_lm,
    activating_product_order_price_offered_amount_month_tot_ly,
    nonactivating_product_order_price_offered_amount,
    nonactivating_product_order_price_offered_amount_mtd,
    nonactivating_product_order_price_offered_amount_mtd_ly,
    nonactivating_product_order_price_offered_amount_month_tot_lm,
    nonactivating_product_order_price_offered_amount_month_tot_ly,
    guest_product_order_price_offered_amount,
    guest_product_order_price_offered_amount_mtd,
    guest_product_order_price_offered_amount_mtd_ly,
    guest_product_order_price_offered_amount_month_tot_lm,
    guest_product_order_price_offered_amount_month_tot_ly,
    repeat_vip_product_order_price_offered_amount,
    repeat_vip_product_order_price_offered_amount_mtd,
    repeat_vip_product_order_price_offered_amount_mtd_ly,
    repeat_vip_product_order_price_offered_amount_month_tot_lm,
    repeat_vip_product_order_price_offered_amount_month_tot_ly,
    product_order_landed_product_cost_amount,
    product_order_landed_product_cost_amount_mtd,
    product_order_landed_product_cost_amount_mtd_ly,
    product_order_landed_product_cost_amount_month_tot_lm,
    product_order_landed_product_cost_amount_month_tot_ly,
    activating_product_order_landed_product_cost_amount,
    activating_product_order_landed_product_cost_amount_mtd,
    activating_product_order_landed_product_cost_amount_mtd_ly,
    activating_product_order_landed_product_cost_amount_month_tot_lm,
    activating_product_order_landed_product_cost_amount_month_tot_ly,
    nonactivating_product_order_landed_product_cost_amount,
    nonactivating_product_order_landed_product_cost_amount_mtd,
    nonactivating_product_order_landed_product_cost_amount_mtd_ly,
    nonactivating_product_order_landed_product_cost_amount_month_tot_lm,
    nonactivating_product_order_landed_product_cost_amount_month_tot_ly,
    guest_product_order_landed_product_cost_amount,
    guest_product_order_landed_product_cost_amount_mtd,
    guest_product_order_landed_product_cost_amount_mtd_ly,
    guest_product_order_landed_product_cost_amount_month_tot_lm,
    guest_product_order_landed_product_cost_amount_month_tot_ly,
    repeat_vip_product_order_landed_product_cost_amount,
    repeat_vip_product_order_landed_product_cost_amount_mtd,
    repeat_vip_product_order_landed_product_cost_amount_mtd_ly,
    repeat_vip_product_order_landed_product_cost_amount_month_tot_lm,
    repeat_vip_product_order_landed_product_cost_amount_month_tot_ly,
    product_order_landed_product_cost_amount_accounting,
    product_order_landed_product_cost_amount_accounting_mtd,
    product_order_landed_product_cost_amount_accounting_mtd_ly,
    product_order_landed_product_cost_amount_accounting_month_tot_lm,
    product_order_landed_product_cost_amount_accounting_month_tot_ly,
    activating_product_order_landed_product_cost_amount_accounting,
    activating_product_order_landed_product_cost_amount_accounting_mtd,
    activating_product_order_landed_product_cost_amount_accounting_mtd_ly,
    activating_product_order_landed_product_cost_amount_accounting_month_tot_lm,
    activating_product_order_landed_product_cost_amount_accounting_month_tot_ly,
    nonactivating_product_order_landed_product_cost_amount_accounting,
    nonactivating_product_order_landed_product_cost_amount_accounting_mtd,
    nonactivating_product_order_landed_product_cost_amount_accounting_mtd_ly,
    nonactivating_product_order_landed_product_cost_amount_accounting_month_tot_lm,
    nonactivating_product_order_landed_product_cost_amount_accounting_month_tot_ly,
    oracle_product_order_landed_product_cost_amount,
    oracle_product_order_landed_product_cost_amount_mtd,
    oracle_product_order_landed_product_cost_amount_mtd_ly,
    oracle_product_order_landed_product_cost_amount_month_tot_lm,
    oracle_product_order_landed_product_cost_amount_month_tot_ly,
    product_order_shipping_cost_amount,
    product_order_shipping_cost_amount_mtd,
    product_order_shipping_cost_amount_mtd_ly,
    product_order_shipping_cost_amount_month_tot_lm,
    product_order_shipping_cost_amount_month_tot_ly,
    product_order_shipping_supplies_cost_amount,
    product_order_shipping_supplies_cost_amount_mtd,
    product_order_shipping_supplies_cost_amount_mtd_ly,
    product_order_shipping_supplies_cost_amount_month_tot_lm,
    product_order_shipping_supplies_cost_amount_month_tot_ly,
    product_order_direct_cogs_amount,
    product_order_direct_cogs_amount_mtd,
    product_order_direct_cogs_amount_mtd_ly,
    product_order_direct_cogs_amount_month_tot_lm,
    product_order_direct_cogs_amount_month_tot_ly,
    product_order_cash_refund_amount,
    product_order_cash_refund_amount_mtd,
    product_order_cash_refund_amount_mtd_ly,
    product_order_cash_refund_amount_month_tot_lm,
    product_order_cash_refund_amount_month_tot_ly,
    product_order_cash_chargeback_amount,
    product_order_cash_chargeback_amount_mtd,
    product_order_cash_chargeback_amount_mtd_ly,
    product_order_cash_chargeback_amount_month_tot_lm,
    product_order_cash_chargeback_amount_month_tot_ly,
    product_cost_returned_resaleable_incl_reship_exch,
    product_cost_returned_resaleable_incl_reship_exch_mtd,
    product_cost_returned_resaleable_incl_reship_exch_mtd_ly,
    product_cost_returned_resaleable_incl_reship_exch_month_tot_lm,
    product_cost_returned_resaleable_incl_reship_exch_month_tot_ly,
    product_cost_returned_resaleable_incl_reship_exch_accounting,
    product_cost_returned_resaleable_incl_reship_exch_accounting_mtd,
    product_cost_returned_resaleable_incl_reship_exch_accounting_mtd_ly,
    product_cost_returned_resaleable_incl_reship_exch_accounting_month_tot_lm,
    product_cost_returned_resaleable_incl_reship_exch_accounting_month_tot_ly,
    product_order_cost_product_returned_damaged_amount,
    product_order_cost_product_returned_damaged_amount_mtd,
    product_order_cost_product_returned_damaged_amount_mtd_ly,
    product_order_cost_product_returned_damaged_amount_month_tot_lm,
    product_order_cost_product_returned_damaged_amount_month_tot_ly,
    return_shipping_costs_incl_reship_exch,
    return_shipping_costs_incl_reship_exch_mtd,
    return_shipping_costs_incl_reship_exch_mtd_ly,
    return_shipping_costs_incl_reship_exch_month_tot_lm,
    return_shipping_costs_incl_reship_exch_month_tot_ly,
    return_shipping_costs_incl_reship_exch_budget,
    return_shipping_costs_incl_reship_exch_budget_alt,
    return_shipping_costs_incl_reship_exch_forecast,
    product_order_reship_order_count,
    product_order_reship_order_count_mtd,
    product_order_reship_order_count_mtd_ly,
    product_order_reship_order_count_month_tot_lm,
    product_order_reship_order_count_month_tot_ly,
    product_order_reship_unit_count,
    product_order_reship_unit_count_mtd,
    product_order_reship_unit_count_mtd_ly,
    product_order_reship_unit_count_month_tot_lm,
    product_order_reship_unit_count_month_tot_ly,
    activating_product_order_reship_unit_count,
    activating_product_order_reship_unit_count_mtd,
    activating_product_order_reship_unit_count_mtd_ly,
    activating_product_order_reship_unit_count_month_tot_lm,
    activating_product_order_reship_unit_count_month_tot_ly,
    nonactivating_product_order_reship_unit_count,
    nonactivating_product_order_reship_unit_count_mtd,
    nonactivating_product_order_reship_unit_count_mtd_ly,
    nonactivating_product_order_reship_unit_count_month_tot_lm,
    nonactivating_product_order_reship_unit_count_month_tot_ly,
    guest_product_order_reship_unit_count,
    guest_product_order_reship_unit_count_mtd,
    guest_product_order_reship_unit_count_mtd_ly,
    guest_product_order_reship_unit_count_month_tot_lm,
    guest_product_order_reship_unit_count_month_tot_ly,
    repeat_vip_product_order_reship_unit_count,
    repeat_vip_product_order_reship_unit_count_mtd,
    repeat_vip_product_order_reship_unit_count_mtd_ly,
    repeat_vip_product_order_reship_unit_count_month_tot_lm,
    repeat_vip_product_order_reship_unit_count_month_tot_ly,
    total_cogs_budget,
    total_cogs_budget_alt,
    total_cogs_forecast,
    product_order_reship_product_cost_amount,
    product_order_reship_product_cost_amount_mtd,
    product_order_reship_product_cost_amount_mtd_ly,
    product_order_reship_product_cost_amount_month_tot_lm,
    product_order_reship_product_cost_amount_month_tot_ly,
    activating_product_order_reship_product_cost_amount,
    activating_product_order_reship_product_cost_amount_mtd,
    activating_product_order_reship_product_cost_amount_mtd_ly,
    activating_product_order_reship_product_cost_amount_month_tot_lm,
    activating_product_order_reship_product_cost_amount_month_tot_ly,
    nonactivating_product_order_reship_product_cost_amount,
    nonactivating_product_order_reship_product_cost_amount_mtd,
    nonactivating_product_order_reship_product_cost_amount_mtd_ly,
    nonactivating_product_order_reship_product_cost_amount_month_tot_lm,
    nonactivating_product_order_reship_product_cost_amount_month_tot_ly,
    guest_product_order_reship_product_cost_amount,
    guest_product_order_reship_product_cost_amount_mtd,
    guest_product_order_reship_product_cost_amount_mtd_ly,
    guest_product_order_reship_product_cost_amount_month_tot_lm,
    guest_product_order_reship_product_cost_amount_month_tot_ly,
    repeat_vip_product_order_reship_product_cost_amount,
    repeat_vip_product_order_reship_product_cost_amount_mtd,
    repeat_vip_product_order_reship_product_cost_amount_mtd_ly,
    repeat_vip_product_order_reship_product_cost_amount_month_tot_lm,
    repeat_vip_product_order_reship_product_cost_amount_month_tot_ly,
    product_order_reship_product_cost_amount_accounting,
    product_order_reship_product_cost_amount_accounting_mtd,
    product_order_reship_product_cost_amount_accounting_mtd_ly,
    product_order_reship_product_cost_amount_accounting_month_tot_lm,
    product_order_reship_product_cost_amount_accounting_month_tot_ly,
    activating_product_order_reship_product_cost_amount_accounting,
    activating_product_order_reship_product_cost_amount_accounting_mtd,
    activating_product_order_reship_product_cost_amount_accounting_mtd_ly,
    activating_product_order_reship_product_cost_amount_accounting_month_tot_lm,
    activating_product_order_reship_product_cost_amount_accounting_month_tot_ly,
    nonactivating_product_order_reship_product_cost_amount_accounting,
    nonactivating_product_order_reship_product_cost_amount_accounting_mtd,
    nonactivating_product_order_reship_product_cost_amount_accounting_mtd_ly,
    nonactivating_product_order_reship_product_cost_amount_accounting_month_tot_lm,
    nonactivating_product_order_reship_product_cost_amount_accounting_month_tot_ly,
    oracle_product_order_reship_product_cost_amount,
    oracle_product_order_reship_product_cost_amount_mtd,
    oracle_product_order_reship_product_cost_amount_mtd_ly,
    oracle_product_order_reship_product_cost_amount_month_tot_lm,
    oracle_product_order_reship_product_cost_amount_month_tot_ly,
    product_order_reship_shipping_cost_amount,
    product_order_reship_shipping_cost_amount_mtd,
    product_order_reship_shipping_cost_amount_mtd_ly,
    product_order_reship_shipping_cost_amount_month_tot_lm,
    product_order_reship_shipping_cost_amount_month_tot_ly,
    product_order_reship_shipping_supplies_cost_amount,
    product_order_reship_shipping_supplies_cost_amount_mtd,
    product_order_reship_shipping_supplies_cost_amount_mtd_ly,
    product_order_reship_shipping_supplies_cost_amount_month_tot_lm,
    product_order_reship_shipping_supplies_cost_amount_month_tot_ly,
    product_order_exchange_order_count,
    product_order_exchange_order_count_mtd,
    product_order_exchange_order_count_mtd_ly,
    product_order_exchange_order_count_month_tot_lm,
    product_order_exchange_order_count_month_tot_ly,
    product_order_reship_exch_order_count_budget,
    product_order_reship_exch_order_count_budget_alt,
    product_order_reship_exch_order_count_forecast,
    product_order_exchange_unit_count,
    product_order_exchange_unit_count_mtd,
    product_order_exchange_unit_count_mtd_ly,
    product_order_exchange_unit_count_month_tot_lm,
    product_order_exchange_unit_count_month_tot_ly,
    activating_product_order_exchange_unit_count,
    activating_product_order_exchange_unit_count_mtd,
    activating_product_order_exchange_unit_count_mtd_ly,
    activating_product_order_exchange_unit_count_month_tot_lm,
    activating_product_order_exchange_unit_count_month_tot_ly,
    nonactivating_product_order_exchange_unit_count,
    nonactivating_product_order_exchange_unit_count_mtd,
    nonactivating_product_order_exchange_unit_count_mtd_ly,
    nonactivating_product_order_exchange_unit_count_month_tot_lm,
    nonactivating_product_order_exchange_unit_count_month_tot_ly,
    guest_product_order_exchange_unit_count,
    guest_product_order_exchange_unit_count_mtd,
    guest_product_order_exchange_unit_count_mtd_ly,
    guest_product_order_exchange_unit_count_month_tot_lm,
    guest_product_order_exchange_unit_count_month_tot_ly,
    repeat_vip_product_order_exchange_unit_count,
    repeat_vip_product_order_exchange_unit_count_mtd,
    repeat_vip_product_order_exchange_unit_count_mtd_ly,
    repeat_vip_product_order_exchange_unit_count_month_tot_lm,
    repeat_vip_product_order_exchange_unit_count_month_tot_ly,
    product_order_exchange_product_cost_amount,
    product_order_exchange_product_cost_amount_mtd,
    product_order_exchange_product_cost_amount_mtd_ly,
    product_order_exchange_product_cost_amount_month_tot_lm,
    product_order_exchange_product_cost_amount_month_tot_ly,
    activating_product_order_exchange_product_cost_amount,
    activating_product_order_exchange_product_cost_amount_mtd,
    activating_product_order_exchange_product_cost_amount_mtd_ly,
    activating_product_order_exchange_product_cost_amount_month_tot_lm,
    activating_product_order_exchange_product_cost_amount_month_tot_ly,
    nonactivating_product_order_exchange_product_cost_amount,
    nonactivating_product_order_exchange_product_cost_amount_mtd,
    nonactivating_product_order_exchange_product_cost_amount_mtd_ly,
    nonactivating_product_order_exchange_product_cost_amount_month_tot_lm,
    nonactivating_product_order_exchange_product_cost_amount_month_tot_ly,
    guest_product_order_exchange_product_cost_amount,
    guest_product_order_exchange_product_cost_amount_mtd,
    guest_product_order_exchange_product_cost_amount_mtd_ly,
    guest_product_order_exchange_product_cost_amount_month_tot_lm,
    guest_product_order_exchange_product_cost_amount_month_tot_ly,
    repeat_vip_product_order_exchange_product_cost_amount,
    repeat_vip_product_order_exchange_product_cost_amount_mtd,
    repeat_vip_product_order_exchange_product_cost_amount_mtd_ly,
    repeat_vip_product_order_exchange_product_cost_amount_month_tot_lm,
    repeat_vip_product_order_exchange_product_cost_amount_month_tot_ly,
    product_order_exchange_product_cost_amount_accounting,
    product_order_exchange_product_cost_amount_accounting_mtd,
    product_order_exchange_product_cost_amount_accounting_mtd_ly,
    product_order_exchange_product_cost_amount_accounting_month_tot_lm,
    product_order_exchange_product_cost_amount_accounting_month_tot_ly,
    activating_product_order_exchange_product_cost_amount_accounting,
    activating_product_order_exchange_product_cost_amount_accounting_mtd,
    activating_product_order_exchange_product_cost_amount_accounting_mtd_ly,
    activating_product_order_exchange_product_cost_amount_accounting_month_tot_lm,
    activating_product_order_exchange_product_cost_amount_accounting_month_tot_ly,
    nonactivating_product_order_exchange_product_cost_amount_accounting,
    nonactivating_product_order_exchange_product_cost_amount_accounting_mtd,
    nonactivating_product_order_exchange_product_cost_amount_accounting_mtd_ly,
    nonactivating_product_order_exchange_product_cost_amount_accounting_month_tot_lm,
    nonactivating_product_order_exchange_product_cost_amount_accounting_month_tot_ly,
    oracle_product_order_exchange_product_cost_amount,
    oracle_product_order_exchange_product_cost_amount_mtd,
    oracle_product_order_exchange_product_cost_amount_mtd_ly,
    oracle_product_order_exchange_product_cost_amount_month_tot_lm,
    oracle_product_order_exchange_product_cost_amount_month_tot_ly,
    product_order_exchange_shipping_cost_amount,
    product_order_exchange_shipping_cost_amount_mtd,
    product_order_exchange_shipping_cost_amount_mtd_ly,
    product_order_exchange_shipping_cost_amount_month_tot_lm,
    product_order_exchange_shipping_cost_amount_month_tot_ly,
    product_order_exchange_shipping_supplies_cost_amount,
    product_order_exchange_shipping_supplies_cost_amount_mtd,
    product_order_exchange_shipping_supplies_cost_amount_mtd_ly,
    product_order_exchange_shipping_supplies_cost_amount_month_tot_lm,
    product_order_exchange_shipping_supplies_cost_amount_month_tot_ly,
    product_gross_revenue,
    product_gross_revenue_mtd,
    product_gross_revenue_mtd_ly,
    product_gross_revenue_month_tot_lm,
    product_gross_revenue_month_tot_ly,
    activating_product_gross_revenue,
    activating_product_gross_revenue_mtd,
    activating_product_gross_revenue_mtd_ly,
    activating_product_gross_revenue_month_tot_lm,
    activating_product_gross_revenue_month_tot_ly,
    activating_product_gross_revenue_budget,
    activating_product_gross_revenue_budget_alt,
    activating_product_gross_revenue_forecast,
    nonactivating_product_gross_revenue,
    nonactivating_product_gross_revenue_mtd,
    nonactivating_product_gross_revenue_mtd_ly,
    nonactivating_product_gross_revenue_month_tot_lm,
    nonactivating_product_gross_revenue_month_tot_ly,
    nonactivating_product_gross_revenue_budget,
    nonactivating_product_gross_revenue_budget_alt,
    nonactivating_product_gross_revenue_forecast,
    guest_product_gross_revenue,
    guest_product_gross_revenue_mtd,
    guest_product_gross_revenue_mtd_ly,
    guest_product_gross_revenue_month_tot_lm,
    guest_product_gross_revenue_month_tot_ly,
    repeat_vip_product_gross_revenue,
    repeat_vip_product_gross_revenue_mtd,
    repeat_vip_product_gross_revenue_mtd_ly,
    repeat_vip_product_gross_revenue_month_tot_lm,
    repeat_vip_product_gross_revenue_month_tot_ly,
    product_net_revenue,
    product_net_revenue_mtd,
    product_net_revenue_mtd_ly,
    product_net_revenue_month_tot_lm,
    product_net_revenue_month_tot_ly,
    activating_product_net_revenue,
    activating_product_net_revenue_mtd,
    activating_product_net_revenue_mtd_ly,
    activating_product_net_revenue_month_tot_lm,
    activating_product_net_revenue_month_tot_ly,
    activating_product_net_revenue_budget,
    activating_product_net_revenue_budget_alt,
    activating_product_net_revenue_forecast,
    nonactivating_product_net_revenue,
    nonactivating_product_net_revenue_mtd,
    nonactivating_product_net_revenue_mtd_ly,
    nonactivating_product_net_revenue_month_tot_lm,
    nonactivating_product_net_revenue_month_tot_ly,
    nonactivating_product_net_revenue_budget,
    nonactivating_product_net_revenue_budget_alt,
    nonactivating_product_net_revenue_forecast,
    guest_product_net_revenue,
    guest_product_net_revenue_mtd,
    guest_product_net_revenue_mtd_ly,
    guest_product_net_revenue_month_tot_lm,
    guest_product_net_revenue_month_tot_ly,
    repeat_vip_product_net_revenue,
    repeat_vip_product_net_revenue_mtd,
    repeat_vip_product_net_revenue_mtd_ly,
    repeat_vip_product_net_revenue_month_tot_lm,
    repeat_vip_product_net_revenue_month_tot_ly,
    product_margin_pre_return,
    product_margin_pre_return_mtd,
    product_margin_pre_return_mtd_ly,
    product_margin_pre_return_month_tot_lm,
    product_margin_pre_return_month_tot_ly,
    activating_product_margin_pre_return,
    activating_product_margin_pre_return_mtd,
    activating_product_margin_pre_return_mtd_ly,
    activating_product_margin_pre_return_month_tot_lm,
    activating_product_margin_pre_return_month_tot_ly,
    activating_product_margin_pre_return_budget,
    activating_product_margin_pre_return_budget_alt,
    activating_product_margin_pre_return_forecast,
    first_guest_product_margin_pre_return,
    first_guest_product_margin_pre_return_mtd,
    first_guest_product_margin_pre_return_mtd_ly,
    first_guest_product_margin_pre_return_month_tot_lm,
    first_guest_product_margin_pre_return_month_tot_ly,
    product_gross_profit,
    product_gross_profit_mtd,
    product_gross_profit_mtd_ly,
    product_gross_profit_month_tot_lm,
    product_gross_profit_month_tot_ly,
    activating_product_gross_profit,
    activating_product_gross_profit_mtd,
    activating_product_gross_profit_mtd_ly,
    activating_product_gross_profit_month_tot_lm,
    activating_product_gross_profit_month_tot_ly,
    activating_product_gross_profit_budget,
    activating_product_gross_profit_budget_alt,
    activating_product_gross_profit_forecast,
    nonactivating_product_gross_profit,
    nonactivating_product_gross_profit_mtd,
    nonactivating_product_gross_profit_mtd_ly,
    nonactivating_product_gross_profit_month_tot_lm,
    nonactivating_product_gross_profit_month_tot_ly,
    nonactivating_product_gross_profit_budget,
    nonactivating_product_gross_profit_budget_alt,
    nonactivating_product_gross_profit_forecast,
    guest_product_gross_profit,
    guest_product_gross_profit_mtd,
    guest_product_gross_profit_mtd_ly,
    guest_product_gross_profit_month_tot_lm,
    guest_product_gross_profit_month_tot_ly,
    repeat_vip_product_gross_profit,
    repeat_vip_product_gross_profit_mtd,
    repeat_vip_product_gross_profit_mtd_ly,
    repeat_vip_product_gross_profit_month_tot_lm,
    repeat_vip_product_gross_profit_month_tot_ly,
    product_order_cash_gross_revenue,
    product_order_cash_gross_revenue_mtd,
    product_order_cash_gross_revenue_mtd_ly,
    product_order_cash_gross_revenue_month_tot_lm,
    product_order_cash_gross_revenue_month_tot_ly,
    activating_product_order_cash_gross_revenue,
    activating_product_order_cash_gross_revenue_mtd,
    activating_product_order_cash_gross_revenue_mtd_ly,
    activating_product_order_cash_gross_revenue_month_tot_lm,
    activating_product_order_cash_gross_revenue_month_tot_ly,
    nonactivating_product_order_cash_gross_revenue,
    nonactivating_product_order_cash_gross_revenue_mtd,
    nonactivating_product_order_cash_gross_revenue_mtd_ly,
    nonactivating_product_order_cash_gross_revenue_month_tot_lm,
    nonactivating_product_order_cash_gross_revenue_month_tot_ly,
    nonactivating_product_order_cash_gross_revenue_budget,
    nonactivating_product_order_cash_gross_revenue_budget_alt,
    nonactivating_product_order_cash_gross_revenue_forecast,
    cash_gross_revenue,
    cash_gross_revenue_mtd,
    cash_gross_revenue_mtd_ly,
    cash_gross_revenue_month_tot_lm,
    cash_gross_revenue_month_tot_ly,
    cash_gross_revenue_budget,
    cash_gross_revenue_budget_alt,
    cash_gross_revenue_forecast,
    activating_cash_gross_revenue,
    activating_cash_gross_revenue_mtd,
    activating_cash_gross_revenue_mtd_ly,
    activating_cash_gross_revenue_month_tot_lm,
    activating_cash_gross_revenue_month_tot_ly,
    nonactivating_cash_gross_revenue,
    nonactivating_cash_gross_revenue_mtd,
    nonactivating_cash_gross_revenue_mtd_ly,
    nonactivating_cash_gross_revenue_month_tot_lm,
    nonactivating_cash_gross_revenue_month_tot_ly,
    cash_net_revenue,
    cash_net_revenue_mtd,
    cash_net_revenue_mtd_ly,
    cash_net_revenue_month_tot_lm,
    cash_net_revenue_month_tot_ly,
    cash_net_revenue_budget,
    cash_net_revenue_budget_alt,
    cash_net_revenue_forecast,
    activating_cash_net_revenue,
    activating_cash_net_revenue_mtd,
    activating_cash_net_revenue_mtd_ly,
    activating_cash_net_revenue_month_tot_lm,
    activating_cash_net_revenue_month_tot_ly,
    activating_cash_net_revenue_budget,
    activating_cash_net_revenue_budget_alt,
    activating_cash_net_revenue_forecast,
    nonactivating_cash_net_revenue,
    nonactivating_cash_net_revenue_mtd,
    nonactivating_cash_net_revenue_mtd_ly,
    nonactivating_cash_net_revenue_month_tot_lm,
    nonactivating_cash_net_revenue_month_tot_ly,
    nonactivating_cash_net_revenue_budget,
    nonactivating_cash_net_revenue_budget_alt,
    nonactivating_cash_net_revenue_forecast,
    cash_net_revenue_budgeted_fx,
    cash_net_revenue_budgeted_fx_mtd,
    cash_net_revenue_budgeted_fx_mtd_ly,
    cash_net_revenue_budgeted_fx_month_tot_lm,
    cash_net_revenue_budgeted_fx_month_tot_ly,
    cash_gross_profit,
    cash_gross_profit_mtd,
    cash_gross_profit_mtd_ly,
    cash_gross_profit_month_tot_lm,
    cash_gross_profit_month_tot_ly,
    cash_gross_profit_budget,
    cash_gross_profit_budget_alt,
    cash_gross_profit_forecast,
    nonactivating_cash_gross_profit,
    nonactivating_cash_gross_profit_mtd,
    nonactivating_cash_gross_profit_mtd_ly,
    nonactivating_cash_gross_profit_month_tot_lm,
    nonactivating_cash_gross_profit_month_tot_ly,
    nonactivating_cash_gross_profit_budget,
    nonactivating_cash_gross_profit_budget_alt,
    nonactivating_cash_gross_profit_forecast,
    billed_credit_cash_transaction_count,
    billed_credit_cash_transaction_count_mtd,
    billed_credit_cash_transaction_count_mtd_ly,
    billed_credit_cash_transaction_count_month_tot_lm,
    billed_credit_cash_transaction_count_month_tot_ly,
    billed_credits_successful_on_retry,
    billed_credits_successful_on_retry_mtd,
    billed_credits_successful_on_retry_mtd_ly,
    billed_credits_successful_on_retry_month_tot_lm,
    billed_credits_successful_on_retry_month_tot_ly,
    billing_cash_refund_amount,
    billing_cash_refund_amount_mtd,
    billing_cash_refund_amount_mtd_ly,
    billing_cash_refund_amount_month_tot_lm,
    billing_cash_refund_amount_month_tot_ly,
    billing_cash_chargeback_amount,
    billing_cash_chargeback_amount_mtd,
    billing_cash_chargeback_amount_mtd_ly,
    billing_cash_chargeback_amount_month_tot_lm,
    billing_cash_chargeback_amount_month_tot_ly,
    billed_credit_cash_transaction_amount,
    billed_credit_cash_transaction_amount_mtd,
    billed_credit_cash_transaction_amount_mtd_ly,
    billed_credit_cash_transaction_amount_month_tot_lm,
    billed_credit_cash_transaction_amount_month_tot_ly,
    billed_credit_cash_transaction_amount_budget,
    billed_credit_cash_transaction_amount_budget_alt,
    billed_credit_cash_transaction_amount_forecast,
    billed_credit_cash_refund_chargeback_amount,
    billed_credit_cash_refund_chargeback_amount_mtd,
    billed_credit_cash_refund_chargeback_amount_mtd_ly,
    billed_credit_cash_refund_chargeback_amount_month_tot_lm,
    billed_credit_cash_refund_chargeback_amount_month_tot_ly,
    billed_credit_cash_refund_chargeback_amount_budget,
    billed_credit_cash_refund_chargeback_amount_budget_alt,
    billed_credit_cash_refund_chargeback_amount_forecast,
    activating_billed_cash_credit_redeemed_amount,
    billed_cash_credit_redeemed_amount,
    billed_cash_credit_redeemed_amount_mtd,
    billed_cash_credit_redeemed_amount_mtd_ly,
    billed_cash_credit_redeemed_amount_month_tot_lm,
    billed_cash_credit_redeemed_amount_month_tot_ly,
    billed_cash_credit_redeemed_amount_budget,
    billed_cash_credit_redeemed_amount_budget_alt,
    billed_cash_credit_redeemed_amount_forecast,
    misc_cogs_amount,
    misc_cogs_amount_mtd,
    misc_cogs_amount_mtd_ly,
    misc_cogs_amount_month_tot_lm,
    misc_cogs_amount_month_tot_ly,
    misc_cogs_amount_budget,
    misc_cogs_amount_budget_alt,
    misc_cogs_amount_forecast,
    product_order_cash_gross_profit,
    product_order_cash_gross_profit_mtd,
    product_order_cash_gross_profit_mtd_ly,
    product_order_cash_gross_profit_month_tot_lm,
    product_order_cash_gross_profit_month_tot_ly,
    nonactivating_product_order_cash_gross_profit,
    nonactivating_product_order_cash_gross_profit_mtd,
    nonactivating_product_order_cash_gross_profit_mtd_ly,
    nonactivating_product_order_cash_gross_profit_month_tot_lm,
    nonactivating_product_order_cash_gross_profit_month_tot_ly,
    nonactivating_product_order_cash_gross_profit_budget,
    nonactivating_product_order_cash_gross_profit_budget_alt,
    nonactivating_product_order_cash_gross_profit_forecast,
    same_month_billed_credit_redeemed,
    same_month_billed_credit_redeemed_mtd,
    same_month_billed_credit_redeemed_mtd_ly,
    same_month_billed_credit_redeemed_month_tot_lm,
    same_month_billed_credit_redeemed_month_tot_ly,
    billed_credit_cash_refund_count,
    billed_credit_cash_refund_count_mtd,
    billed_credit_cash_refund_count_mtd_ly,
    billed_credit_cash_refund_count_month_tot_lm,
    billed_credit_cash_refund_count_month_tot_ly,
    cash_variable_contribution_profit,
    cash_variable_contribution_profit_mtd,
    cash_variable_contribution_profit_mtd_ly,
    cash_variable_contribution_profit_month_tot_lm,
    cash_variable_contribution_profit_month_tot_ly,
    cash_contribution_after_media_budget,
    cash_contribution_after_media_budget_alt,
    cash_contribution_after_media_forecast,
    billed_cash_credit_redeemed_equivalent_count,
    billed_cash_credit_redeemed_equivalent_count_mtd,
    billed_cash_credit_redeemed_equivalent_count_mtd_ly,
    billed_cash_credit_redeemed_equivalent_count_month_tot_lm,
    billed_cash_credit_redeemed_equivalent_count_month_tot_ly,
    membership_credits_redeemed_count_budget,
    membership_credits_redeemed_count_budget_alt,
    membership_credits_redeemed_count_forecast,
    billed_cash_credit_cancelled_equivalent_count,
    billed_cash_credit_cancelled_equivalent_count_mtd,
    billed_cash_credit_cancelled_equivalent_count_mtd_ly,
    billed_cash_credit_cancelled_equivalent_count_month_tot_lm,
    billed_cash_credit_cancelled_equivalent_count_month_tot_ly,
    membership_credits_cancelled_count_budget,
    membership_credits_cancelled_count_budget_alt,
    membership_credits_cancelled_count_forecast,
    cash_gross_profit_percent_of_net_cash_budget,
    cash_gross_profit_percent_of_net_cash_budget_alt,
    cash_gross_profit_percent_of_net_cash_forecast,
    product_order_and_billing_cash_refund_budget,
    product_order_and_billing_cash_refund_budget_alt,
    product_order_and_billing_cash_refund_forecast,
    product_order_and_billing_chargebacks_budget,
    product_order_and_billing_chargebacks_budget_alt,
    product_order_and_billing_chargebacks_forecast,
    refunds_plus_chargebacks_as_percent_of_cash_gross_rev_budget,
    refunds_plus_chargebacks_as_percent_of_cash_gross_rev_budget_alt,
    refunds_plus_chargebacks_as_percent_of_cash_gross_rev_forecast,
    activating_aov_budget,
    activating_aov_budget_alt,
    activating_aov_forecast,
    activating_upt_budget,
    activating_upt_budget_alt,
    activating_upt_forecast,
    activating_discount_percent_budget,
    activating_discount_percent_budget_alt,
    activating_discount_percent_forecast,
    activating_product_gross_profit_percent_budget,
    activating_product_gross_profit_percent_budget_alt,
    activating_product_gross_profit_percent_forecast,
    activating_product_margin_pre_return_per_order_budget,
    activating_product_margin_pre_return_per_order_budget_alt,
    activating_product_margin_pre_return_per_order_forecast,
    activating_product_margin_pre_return_percent_budget,
    activating_product_margin_pre_return_percent_budget_alt,
    activating_product_margin_pre_return_percent_forecast,
    nonactivating_aov_budget,
    nonactivating_aov_budget_alt,
    nonactivating_aov_forecast,
    nonactivating_upt_budget,
    nonactivating_upt_budget_alt,
    nonactivating_upt_forecast,
    nonactivating_discount_percent_budget,
    nonactivating_discount_percent_budget_alt,
    nonactivating_discount_percent_forecast,
    nonactivating_product_gross_profit_percent_budget,
    nonactivating_product_gross_profit_percent_budget_alt,
    nonactivating_product_gross_profit_percent_forecast,
    shipping_cost_incl_reship_exch_budget,
    shipping_cost_incl_reship_exch_budget_alt,
    shipping_cost_incl_reship_exch_forecast,
    shipping_supplies_cost_incl_reship_exch_budget,
    shipping_supplies_cost_incl_reship_exch_budget_alt,
    shipping_supplies_cost_incl_reship_exch_forecast,
    product_cost_calc_budget,
    product_cost_calc_budget_alt,
    product_cost_calc_forecast,
    product_order_tariff_amount,
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
    product_order_cash_credit_refund_amount_mtd,
    product_order_cash_credit_refund_amount_mtd_ly,
    product_order_cash_credit_refund_amount_month_tot_lm,
    product_order_cash_credit_refund_amount_month_tot_ly,
    product_order_cash_credit_refund_amount_budget,
    product_order_cash_credit_refund_amount_budget_alt,
    product_order_cash_credit_refund_amount_forecast,
    activating_product_order_cash_credit_refund_amount,
    activating_product_order_cash_credit_refund_amount_mtd,
    activating_product_order_cash_credit_refund_amount_mtd_ly,
    activating_product_order_cash_credit_refund_amount_month_tot_lm,
    activating_product_order_cash_credit_refund_amount_month_tot_ly,
    activating_product_order_cash_credit_refund_amount_budget,
    activating_product_order_cash_credit_refund_amount_budget_alt,
    activating_product_order_cash_credit_refund_amount_forecast,
    nonactivating_product_order_cash_credit_refund_amount,
    nonactivating_product_order_cash_credit_refund_amount_mtd,
    nonactivating_product_order_cash_credit_refund_amount_mtd_ly,
    nonactivating_product_order_cash_credit_refund_amount_month_tot_lm,
    nonactivating_product_order_cash_credit_refund_amount_month_tot_ly,
    nonactivating_product_order_cash_credit_refund_amount_budget,
    nonactivating_product_order_cash_credit_refund_amount_budget_alt,
    nonactivating_product_order_cash_credit_refund_amount_forecast,
    product_order_noncash_credit_refund_amount,
    product_order_noncash_credit_refund_amount_mtd,
    product_order_noncash_credit_refund_amount_mtd_ly,
    product_order_noncash_credit_refund_amount_month_tot_lm,
    product_order_noncash_credit_refund_amount_month_tot_ly,
    product_order_noncash_credit_refund_amount_budget,
    product_order_noncash_credit_refund_amount_budget_alt,
    product_order_noncash_credit_refund_amount_forecast,
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
    product_gross_revenue_excl_shipping_mtd,
    product_gross_revenue_excl_shipping_mtd_ly,
    product_gross_revenue_excl_shipping_month_tot_lm,
    product_gross_revenue_excl_shipping_month_tot_ly,
    activating_product_gross_revenue_excl_shipping,
    activating_product_gross_revenue_excl_shipping_mtd,
    activating_product_gross_revenue_excl_shipping_mtd_ly,
    activating_product_gross_revenue_excl_shipping_month_tot_lm,
    activating_product_gross_revenue_excl_shipping_month_tot_ly,
    nonactivating_product_gross_revenue_excl_shipping,
    nonactivating_product_gross_revenue_excl_shipping_mtd,
    nonactivating_product_gross_revenue_excl_shipping_mtd_ly,
    nonactivating_product_gross_revenue_excl_shipping_month_tot_lm,
    nonactivating_product_gross_revenue_excl_shipping_month_tot_ly,
    guest_product_gross_revenue_excl_shipping,
    guest_product_gross_revenue_excl_shipping_mtd,
    guest_product_gross_revenue_excl_shipping_mtd_ly,
    guest_product_gross_revenue_excl_shipping_month_tot_lm,
    guest_product_gross_revenue_excl_shipping_month_tot_ly,
    repeat_vip_product_gross_revenue_excl_shipping,
    repeat_vip_product_gross_revenue_excl_shipping_mtd,
    repeat_vip_product_gross_revenue_excl_shipping_mtd_ly,
    repeat_vip_product_gross_revenue_excl_shipping_month_tot_lm,
    repeat_vip_product_gross_revenue_excl_shipping_month_tot_ly,
    product_margin_pre_return_excl_shipping,
    activating_product_margin_pre_return_excl_shipping,
    product_variable_contribution_profit,
    product_order_cash_net_revenue,
    product_order_cash_margin_pre_return,
    billing_cash_gross_revenue,
    billing_cash_net_revenue,
    billing_order_transaction_count,
    billing_order_transaction_count_mtd,
    billing_order_transaction_count_mtd_ly,
    billing_order_transaction_count_month_tot_lm,
    billing_order_transaction_count_month_tot_ly,
    billing_order_transaction_count_budget,
    billing_order_transaction_count_budget_alt,
    billing_order_transaction_count_forecast,
    membership_fee_cash_transaction_amount,
    membership_fee_cash_transaction_amount_mtd,
    membership_fee_cash_transaction_amount_mtd_ly,
    membership_fee_cash_transaction_amount_month_tot_lm,
    membership_fee_cash_transaction_amount_month_tot_ly,
    gift_card_transaction_amount,
    gift_card_transaction_amount_mtd,
    gift_card_transaction_amount_mtd_ly,
    gift_card_transaction_amount_month_tot_lm,
    gift_card_transaction_amount_month_tot_ly,
    legacy_credit_cash_transaction_amount,
    legacy_credit_cash_transaction_amount_mtd,
    legacy_credit_cash_transaction_amount_mtd_ly,
    legacy_credit_cash_transaction_amount_month_tot_lm,
    legacy_credit_cash_transaction_amount_month_tot_ly,
    membership_fee_cash_refund_chargeback_amount,
    membership_fee_cash_refund_chargeback_amount_mtd,
    membership_fee_cash_refund_chargeback_amount_mtd_ly,
    membership_fee_cash_refund_chargeback_amount_month_tot_lm,
    membership_fee_cash_refund_chargeback_amount_month_tot_ly,
    gift_card_cash_refund_chargeback_amount,
    gift_card_cash_refund_chargeback_amount_mtd,
    gift_card_cash_refund_chargeback_amount_mtd_ly,
    gift_card_cash_refund_chargeback_amount_month_tot_lm,
    gift_card_cash_refund_chargeback_amount_month_tot_ly,
    legacy_credit_cash_refund_chargeback_amount,
    legacy_credit_cash_refund_chargeback_amount_mtd,
    legacy_credit_cash_refund_chargeback_amount_mtd_ly,
    legacy_credit_cash_refund_chargeback_amount_month_tot_lm,
    legacy_credit_cash_refund_chargeback_amount_month_tot_ly,
    billed_cash_credit_issued_amount,
    billed_cash_credit_cancelled_amount,
    billed_cash_credit_expired_amount,
    billed_cash_credit_issued_equivalent_count,
    billed_cash_credit_issued_equivalent_count_mtd,
    billed_cash_credit_issued_equivalent_count_mtd_ly,
    billed_cash_credit_issued_equivalent_count_month_tot_lm,
    billed_cash_credit_issued_equivalent_count_month_tot_ly,
    billed_cash_credit_expired_equivalent_count,
    refund_cash_credit_issued_amount,
    refund_cash_credit_issued_amount_mtd,
    refund_cash_credit_issued_amount_mtd_ly,
    refund_cash_credit_issued_amount_month_tot_lm,
    refund_cash_credit_issued_amount_month_tot_ly,
    refund_cash_credit_issued_amount_budget,
    refund_cash_credit_issued_amount_budget_alt,
    refund_cash_credit_issued_amount_forecast,
    refund_cash_credit_redeemed_amount,
    refund_cash_credit_redeemed_amount_mtd,
    refund_cash_credit_redeemed_amount_mtd_ly,
    refund_cash_credit_redeemed_amount_month_tot_lm,
    refund_cash_credit_redeemed_amount_month_tot_ly,
    refund_cash_credit_redeemed_amount_budget,
    refund_cash_credit_redeemed_amount_budget_alt,
    refund_cash_credit_redeemed_amount_forecast,
    refund_cash_credit_cancelled_amount,
    refund_cash_credit_cancelled_amount_mtd,
    refund_cash_credit_cancelled_amount_mtd_ly,
    refund_cash_credit_cancelled_amount_month_tot_lm,
    refund_cash_credit_cancelled_amount_month_tot_ly,
    net_unredeemed_refund_credit_budget,
    net_unredeemed_refund_credit_budget_alt,
    net_unredeemed_refund_credit_forecast,
    refund_cash_credit_expired_amount,
    other_cash_credit_issued_amount,
    other_cash_credit_redeemed_amount,
    other_cash_credit_redeemed_amount_mtd,
    other_cash_credit_redeemed_amount_mtd_ly,
    other_cash_credit_redeemed_amount_month_tot_lm,
    other_cash_credit_redeemed_amount_month_tot_ly,
    other_cash_credit_cancelled_amount,
    other_cash_credit_expired_amount,
    cash_gift_card_redeemed_amount,
    activating_cash_gift_card_redeemed_amount,
    nonactivating_cash_gift_card_redeemed_amount,
    guest_cash_gift_card_redeemed_amount,
    noncash_credit_issued_amount,
    noncash_credit_issued_amount_mtd,
    noncash_credit_issued_amount_mtd_ly,
    noncash_credit_issued_amount_month_tot_lm,
    noncash_credit_issued_amount_month_tot_ly,
    noncash_credit_cancelled_amount,
    noncash_credit_expired_amount,
    net_unredeemed_credit_billings_budget,
    net_unredeemed_credit_billings_budget_alt,
    net_unredeemed_credit_billings_forecast,
    product_order_non_token_subtotal_excl_tariff_amount,
    product_order_non_token_subtotal_excl_tariff_amount_mtd,
    product_order_non_token_subtotal_excl_tariff_amount_mtd_ly,
    product_order_non_token_subtotal_excl_tariff_amount_month_tot_lm,
    product_order_non_token_subtotal_excl_tariff_amount_month_tot_ly,
    product_order_non_token_unit_count,
    product_order_non_token_unit_count_mtd,
    product_order_non_token_unit_count_mtd_ly,
    product_order_non_token_unit_count_month_tot_lm,
    product_order_non_token_unit_count_month_tot_ly,
    nonactivating_product_order_non_token_subtotal_excl_tariff_amount,
    nonactivating_product_order_non_token_subtotal_excl_tariff_amount_mtd,
    nonactivating_product_order_non_token_subtotal_excl_tariff_amount_mtd_ly,
    nonactivating_product_order_non_token_subtotal_excl_tariff_amount_month_tot_lm,
    nonactivating_product_order_non_token_subtotal_excl_tariff_amount_month_tot_ly,
    nonactivating_product_order_non_token_unit_count,
    nonactivating_product_order_non_token_unit_count_mtd,
    nonactivating_product_order_non_token_unit_count_mtd_ly,
    nonactivating_product_order_non_token_unit_count_month_tot_lm,
    nonactivating_product_order_non_token_unit_count_month_tot_ly,
    pending_order_count,
    pending_order_local_amount,
    pending_order_usd_amount,
    pending_order_eur_amount,
    pending_order_cash_margin_pre_return_local_amount,
    pending_order_cash_margin_pre_return_usd_amount,
    pending_order_cash_margin_pre_return_eur_amount,
    leads,
    leads_mtd,
    leads_mtd_ly,
    leads_month_tot_lm,
    leads_month_tot_ly,
    leads_budget,
    leads_budget_alt,
    leads_forecast,
    primary_leads,
    primary_leads_mtd,
    primary_leads_mtd_ly,
    primary_leads_month_tot_lm,
    primary_leads_month_tot_ly,
    reactivated_leads,
    reactivated_leads_mtd,
    reactivated_leads_mtd_ly,
    reactivated_leads_month_tot_lm,
    reactivated_leads_month_tot_ly,
    new_vips,
    new_vips_mtd,
    new_vips_mtd_ly,
    new_vips_month_tot_lm,
    new_vips_month_tot_ly,
    new_vips_budget,
    new_vips_budget_alt,
    new_vips_forecast,
    reactivated_vips,
    reactivated_vips_mtd,
    reactivated_vips_mtd_ly,
    reactivated_vips_month_tot_lm,
    reactivated_vips_month_tot_ly,
    vips_from_reactivated_leads_m1,
    vips_from_reactivated_leads_m1_mtd,
    vips_from_reactivated_leads_m1_mtd_ly,
    vips_from_reactivated_leads_m1_month_tot_lm,
    vips_from_reactivated_leads_m1_month_tot_ly,
    paid_vips,
    paid_vips_mtd,
    paid_vips_mtd_ly,
    paid_vips_month_tot_lm,
    paid_vips_month_tot_ly,
    unpaid_vips,
    unpaid_vips_mtd,
    unpaid_vips_mtd_ly,
    unpaid_vips_month_tot_lm,
    unpaid_vips_month_tot_ly,
    new_vips_m1,
    new_vips_m1_mtd,
    new_vips_m1_mtd_ly,
    new_vips_m1_month_tot_lm,
    new_vips_m1_month_tot_ly,
    new_vips_m1_budget,
    new_vips_m1_budget_alt,
    new_vips_m1_forecast,
    paid_vips_m1,
    paid_vips_m1_mtd,
    paid_vips_m1_mtd_ly,
    paid_vips_m1_month_tot_lm,
    paid_vips_m1_month_tot_ly,
    cancels,
    cancels_mtd,
    cancels_mtd_ly,
    cancels_month_tot_lm,
    cancels_month_tot_ly,
    cancels_budget,
    cancels_budget_alt,
    cancels_forecast,
    m1_cancels,
    m1_cancels_mtd,
    m1_cancels_mtd_ly,
    m1_cancels_month_tot_lm,
    m1_cancels_month_tot_ly,
    bop_vips,
    bop_vips_mtd,
    bop_vips_mtd_ly,
    bop_vips_month_tot_lm,
    bop_vips_month_tot_ly,
    bop_vips_budget,
    bop_vips_budget_alt,
    bop_vips_forecast,
    media_spend,
    media_spend_mtd,
    media_spend_mtd_ly,
    media_spend_month_tot_lm,
    media_spend_month_tot_ly,
    media_spend_budget,
    media_spend_budget_alt,
    media_spend_forecast,
    product_order_return_unit_count,
    product_order_return_unit_count_mtd,
    product_order_return_unit_count_mtd_ly,
    product_order_return_unit_count_month_tot_lm,
    product_order_return_unit_count_month_tot_ly,
    merch_purchase_count,
    merch_purchase_count_mtd,
    merch_purchase_count_mtd_ly,
    merch_purchase_count_month_tot_lm,
    merch_purchase_count_month_tot_ly,
    merch_purchase_hyperion_count,
    merch_purchase_hyperion_count_mtd,
    merch_purchase_hyperion_count_mtd_ly,
    merch_purchase_hyperion_count_month_tot_lm,
    merch_purchase_hyperion_count_month_tot_ly,
    bom_vips,
    skip_count,
    report_date_type,
    snapshot_datetime_budget,
    snapshot_datetime_forecast,
    meta_update_datetime,
    meta_create_datetime
)
SELECT
/*Daily Cash Version*/
    dcbcja.date
    ,dcbcja.date_object
    ,dcbcja.currency_object
    ,dcbcja.currency_type
/*Segment*/
    ,dcbcja.store_brand
    ,dcbcja.business_unit
    ,dcbcja.report_mapping
    ,dcbcja.is_daily_cash_usd
    ,dcbcja.is_daily_cash_eur
/*Measures*/

    ,dcbcja.product_order_subtotal_excl_tariff_amount AS product_order_subtotal_excl_tariff_amount
    ,dcbcja.product_order_subtotal_excl_tariff_amount_mtd AS product_order_subtotal_excl_tariff_amount_mtd
    ,dcbcja.product_order_subtotal_excl_tariff_amount_mtd_ly AS product_order_subtotal_excl_tariff_amount_mtd_ly
    ,dcbcja.product_order_subtotal_excl_tariff_amount_month_tot_lm AS product_order_subtotal_excl_tariff_amount_month_tot_lm
    ,dcbcja.product_order_subtotal_excl_tariff_amount_month_tot_ly AS product_order_subtotal_excl_tariff_amount_month_tot_ly

    ,dcbcja.activating_product_order_subtotal_excl_tariff_amount AS activating_product_order_subtotal_excl_tariff_amount
    ,dcbcja.activating_product_order_subtotal_excl_tariff_amount_mtd AS activating_product_order_subtotal_excl_tariff_amount_mtd
    ,dcbcja.activating_product_order_subtotal_excl_tariff_amount_mtd_ly AS activating_product_order_subtotal_excl_tariff_amount_mtd_ly
    ,dcbcja.activating_product_order_subtotal_excl_tariff_amount_month_tot_lm AS activating_product_order_subtotal_excl_tariff_amount_month_tot_lm
    ,dcbcja.activating_product_order_subtotal_excl_tariff_amount_month_tot_ly AS activating_product_order_subtotal_excl_tariff_amount_month_tot_ly

    ,dcbcja.nonactivating_product_order_subtotal_excl_tariff_amount AS nonactivating_product_order_subtotal_excl_tariff_amount
    ,dcbcja.nonactivating_product_order_subtotal_excl_tariff_amount_mtd AS nonactivating_product_order_subtotal_excl_tariff_amount_mtd
    ,dcbcja.nonactivating_product_order_subtotal_excl_tariff_amount_mtd_ly AS nonactivating_product_order_subtotal_excl_tariff_amount_mtd_ly
    ,dcbcja.nonactivating_product_order_subtotal_excl_tariff_amount_month_tot_lm AS nonactivating_product_order_subtotal_excl_tariff_amount_month_tot_lm
    ,dcbcja.nonactivating_product_order_subtotal_excl_tariff_amount_month_tot_ly AS nonactivating_product_order_subtotal_excl_tariff_amount_month_tot_ly

    ,dcbcja.guest_product_order_subtotal_excl_tariff_amount AS guest_product_order_subtotal_excl_tariff_amount
    ,dcbcja.guest_product_order_subtotal_excl_tariff_amount_mtd AS guest_product_order_subtotal_excl_tariff_amount_mtd
    ,dcbcja.guest_product_order_subtotal_excl_tariff_amount_mtd_ly AS guest_product_order_subtotal_excl_tariff_amount_mtd_ly
    ,dcbcja.guest_product_order_subtotal_excl_tariff_amount_month_tot_lm AS guest_product_order_subtotal_excl_tariff_amount_month_tot_lm
    ,dcbcja.guest_product_order_subtotal_excl_tariff_amount_month_tot_ly AS guest_product_order_subtotal_excl_tariff_amount_month_tot_ly

    ,dcbcja.repeat_vip_product_order_subtotal_excl_tariff_amount AS repeat_vip_product_order_subtotal_excl_tariff_amount
    ,dcbcja.repeat_vip_product_order_subtotal_excl_tariff_amount_mtd AS repeat_vip_product_order_subtotal_excl_tariff_amount_mtd
    ,dcbcja.repeat_vip_product_order_subtotal_excl_tariff_amount_mtd_ly AS repeat_vip_product_order_subtotal_excl_tariff_amount_mtd_ly
    ,dcbcja.repeat_vip_product_order_subtotal_excl_tariff_amount_month_tot_lm AS repeat_vip_product_order_subtotal_excl_tariff_amount_month_tot_lm
    ,dcbcja.repeat_vip_product_order_subtotal_excl_tariff_amount_month_tot_ly AS repeat_vip_product_order_subtotal_excl_tariff_amount_month_tot_ly

    ,dcbcja.product_order_product_subtotal_amount AS product_order_product_subtotal_amount
    ,dcbcja.product_order_product_subtotal_amount_mtd AS product_order_product_subtotal_amount_mtd
    ,dcbcja.product_order_product_subtotal_amount_mtd_ly AS product_order_product_subtotal_amount_mtd_ly
    ,dcbcja.product_order_product_subtotal_amount_month_tot_lm AS product_order_product_subtotal_amount_month_tot_lm
    ,dcbcja.product_order_product_subtotal_amount_month_tot_ly AS product_order_product_subtotal_amount_month_tot_ly

    ,dcbcja.activating_product_order_product_subtotal_amount AS activating_product_order_product_subtotal_amount
    ,dcbcja.activating_product_order_product_subtotal_amount_mtd AS activating_product_order_product_subtotal_amount_mtd
    ,dcbcja.activating_product_order_product_subtotal_amount_mtd_ly AS activating_product_order_product_subtotal_amount_mtd_ly
    ,dcbcja.activating_product_order_product_subtotal_amount_month_tot_lm AS activating_product_order_product_subtotal_amount_month_tot_lm
    ,dcbcja.activating_product_order_product_subtotal_amount_month_tot_ly AS activating_product_order_product_subtotal_amount_month_tot_ly

    ,dcbcja.nonactivating_product_order_product_subtotal_amount AS nonactivating_product_order_product_subtotal_amount
    ,dcbcja.nonactivating_product_order_product_subtotal_amount_mtd AS nonactivating_product_order_product_subtotal_amount_mtd
    ,dcbcja.nonactivating_product_order_product_subtotal_amount_mtd_ly AS nonactivating_product_order_product_subtotal_amount_mtd_ly
    ,dcbcja.nonactivating_product_order_product_subtotal_amount_month_tot_lm AS nonactivating_product_order_product_subtotal_amount_month_tot_lm
    ,dcbcja.nonactivating_product_order_product_subtotal_amount_month_tot_ly AS nonactivating_product_order_product_subtotal_amount_month_tot_ly

    ,dcbcja.product_order_product_discount_amount AS product_order_product_discount_amount
    ,dcbcja.product_order_product_discount_amount_mtd AS product_order_product_discount_amount_mtd
    ,dcbcja.product_order_product_discount_amount_mtd_ly AS product_order_product_discount_amount_mtd_ly
    ,dcbcja.product_order_product_discount_amount_month_tot_lm AS product_order_product_discount_amount_month_tot_lm
    ,dcbcja.product_order_product_discount_amount_month_tot_ly AS product_order_product_discount_amount_month_tot_ly

    ,dcbcja.activating_product_order_product_discount_amount AS activating_product_order_product_discount_amount
    ,dcbcja.activating_product_order_product_discount_amount_mtd AS activating_product_order_product_discount_amount_mtd
    ,dcbcja.activating_product_order_product_discount_amount_mtd_ly AS activating_product_order_product_discount_amount_mtd_ly
    ,dcbcja.activating_product_order_product_discount_amount_month_tot_lm AS activating_product_order_product_discount_amount_month_tot_lm
    ,dcbcja.activating_product_order_product_discount_amount_month_tot_ly AS activating_product_order_product_discount_amount_month_tot_ly

    ,dcbcja.nonactivating_product_order_product_discount_amount AS nonactivating_product_order_product_discount_amount
    ,dcbcja.nonactivating_product_order_product_discount_amount_mtd AS nonactivating_product_order_product_discount_amount_mtd
    ,dcbcja.nonactivating_product_order_product_discount_amount_mtd_ly AS nonactivating_product_order_product_discount_amount_mtd_ly
    ,dcbcja.nonactivating_product_order_product_discount_amount_month_tot_lm AS nonactivating_product_order_product_discount_amount_month_tot_lm
    ,dcbcja.nonactivating_product_order_product_discount_amount_month_tot_ly AS nonactivating_product_order_product_discount_amount_month_tot_ly

    ,dcbcja.guest_product_order_product_discount_amount AS guest_product_order_product_discount_amount
    ,dcbcja.guest_product_order_product_discount_amount_mtd AS guest_product_order_product_discount_amount_mtd
    ,dcbcja.guest_product_order_product_discount_amount_mtd_ly AS guest_product_order_product_discount_amount_mtd_ly
    ,dcbcja.guest_product_order_product_discount_amount_month_tot_lm AS guest_product_order_product_discount_amount_month_tot_lm
    ,dcbcja.guest_product_order_product_discount_amount_month_tot_ly AS guest_product_order_product_discount_amount_month_tot_ly

    ,dcbcja.repeat_vip_product_order_product_discount_amount AS repeat_vip_product_order_product_discount_amount
    ,dcbcja.repeat_vip_product_order_product_discount_amount_mtd AS repeat_vip_product_order_product_discount_amount_mtd
    ,dcbcja.repeat_vip_product_order_product_discount_amount_mtd_ly AS repeat_vip_product_order_product_discount_amount_mtd_ly
    ,dcbcja.repeat_vip_product_order_product_discount_amount_month_tot_lm AS repeat_vip_product_order_product_discount_amount_month_tot_lm
    ,dcbcja.repeat_vip_product_order_product_discount_amount_month_tot_ly AS repeat_vip_product_order_product_discount_amount_month_tot_ly

    ,dcbcja.product_order_shipping_revenue_amount AS shipping_revenue
    ,dcbcja.product_order_shipping_revenue_amount_mtd AS shipping_revenue_mtd
    ,dcbcja.product_order_shipping_revenue_amount_mtd_ly AS shipping_revenue_mtd_ly
    ,dcbcja.product_order_shipping_revenue_amount_month_tot_lm AS shipping_revenue_month_tot_lm
    ,dcbcja.product_order_shipping_revenue_amount_month_tot_ly AS shipping_revenue_month_tot_ly

    ,dcbcja.activating_product_order_shipping_revenue_amount AS activating_shipping_revenue
    ,dcbcja.activating_product_order_shipping_revenue_amount_mtd AS activating_shipping_revenue_mtd
    ,dcbcja.activating_product_order_shipping_revenue_amount_mtd_ly AS activating_shipping_revenue_mtd_ly
    ,dcbcja.activating_product_order_shipping_revenue_amount_month_tot_lm AS activating_shipping_revenue_month_tot_lm
    ,dcbcja.activating_product_order_shipping_revenue_amount_month_tot_ly AS activating_shipping_revenue_month_tot_ly

    ,dcbcja.nonactivating_product_order_shipping_revenue_amount AS nonactivating_shipping_revenue
    ,dcbcja.nonactivating_product_order_shipping_revenue_amount_mtd AS nonactivating_shipping_revenue_mtd
    ,dcbcja.nonactivating_product_order_shipping_revenue_amount_mtd_ly AS nonactivating_shipping_revenue_mtd_ly
    ,dcbcja.nonactivating_product_order_shipping_revenue_amount_month_tot_lm AS nonactivating_shipping_revenue_month_tot_lm
    ,dcbcja.nonactivating_product_order_shipping_revenue_amount_month_tot_ly AS nonactivating_shipping_revenue_month_tot_ly

    ,dcbcja.guest_product_order_shipping_revenue_amount AS guest_shipping_revenue
    ,dcbcja.guest_product_order_shipping_revenue_amount_mtd AS guest_shipping_revenue_mtd
    ,dcbcja.guest_product_order_shipping_revenue_amount_mtd_ly AS guest_shipping_revenue_mtd_ly
    ,dcbcja.guest_product_order_shipping_revenue_amount_month_tot_lm AS guest_shipping_revenue_month_tot_lm
    ,dcbcja.guest_product_order_shipping_revenue_amount_month_tot_ly AS guest_shipping_revenue_month_tot_ly

    ,dcbcja.product_order_noncash_credit_redeemed_amount AS product_order_noncash_credit_redeemed_amount
    ,dcbcja.product_order_noncash_credit_redeemed_amount_mtd AS product_order_noncash_credit_redeemed_amount_mtd
    ,dcbcja.product_order_noncash_credit_redeemed_amount_mtd_ly AS product_order_noncash_credit_redeemed_amount_mtd_ly
    ,dcbcja.product_order_noncash_credit_redeemed_amount_month_tot_lm AS product_order_noncash_credit_redeemed_amount_month_tot_lm
    ,dcbcja.product_order_noncash_credit_redeemed_amount_month_tot_ly AS product_order_noncash_credit_redeemed_amount_month_tot_ly

    ,dcbcja.activating_product_order_noncash_credit_redeemed_amount AS activating_product_order_noncash_credit_redeemed_amount
    ,dcbcja.activating_product_order_noncash_credit_redeemed_amount_mtd AS activating_product_order_noncash_credit_redeemed_amount_mtd
    ,dcbcja.activating_product_order_noncash_credit_redeemed_amount_mtd_ly AS activating_product_order_noncash_credit_redeemed_amount_mtd_ly
    ,dcbcja.activating_product_order_noncash_credit_redeemed_amount_month_tot_lm AS activating_product_order_noncash_credit_redeemed_amount_month_tot_lm
    ,dcbcja.activating_product_order_noncash_credit_redeemed_amount_month_tot_ly AS activating_product_order_noncash_credit_redeemed_amount_month_tot_ly

    ,dcbcja.nonactivating_product_order_noncash_credit_redeemed_amount AS nonactivating_product_order_noncash_credit_redeemed_amount
    ,dcbcja.nonactivating_product_order_noncash_credit_redeemed_amount_mtd AS nonactivating_product_order_noncash_credit_redeemed_amount_mtd
    ,dcbcja.nonactivating_product_order_noncash_credit_redeemed_amount_mtd_ly AS nonactivating_product_order_noncash_credit_redeemed_amount_mtd_ly
    ,dcbcja.nonactivating_product_order_noncash_credit_redeemed_amount_month_tot_lm AS nonactivating_product_order_noncash_credit_redeemed_amount_month_tot_lm
    ,dcbcja.nonactivating_product_order_noncash_credit_redeemed_amount_month_tot_ly AS nonactivating_product_order_noncash_credit_redeemed_amount_month_tot_ly

    ,dcbcja.repeat_vip_product_order_noncash_credit_redeemed_amount AS repeat_vip_product_order_noncash_credit_redeemed_amount
    ,dcbcja.repeat_vip_product_order_noncash_credit_redeemed_amount_mtd AS repeat_vip_product_order_noncash_credit_redeemed_amount_mtd
    ,dcbcja.repeat_vip_product_order_noncash_credit_redeemed_amount_mtd_ly AS repeat_vip_product_order_noncash_credit_redeemed_amount_mtd_ly
    ,dcbcja.repeat_vip_product_order_noncash_credit_redeemed_amount_month_tot_lm AS repeat_vip_product_order_noncash_credit_redeemed_amount_month_tot_lm
    ,dcbcja.repeat_vip_product_order_noncash_credit_redeemed_amount_month_tot_ly AS repeat_vip_product_order_noncash_credit_redeemed_amount_month_tot_ly

    ,dcbcja.product_order_count AS product_order_count
    ,dcbcja.product_order_count_mtd AS product_order_count_mtd
    ,dcbcja.product_order_count_mtd_ly AS product_order_count_mtd_ly
    ,dcbcja.product_order_count_month_tot_lm AS product_order_count_month_tot_lm
    ,dcbcja.product_order_count_month_tot_ly AS product_order_count_month_tot_ly

    ,dcbcja.activating_product_order_count AS activating_product_order_count
    ,dcbcja.activating_product_order_count_mtd AS activating_product_order_count_mtd
    ,dcbcja.activating_product_order_count_mtd_ly AS activating_product_order_count_mtd_ly
    ,dcbcja.activating_product_order_count_month_tot_lm AS activating_product_order_count_month_tot_lm
    ,dcbcja.activating_product_order_count_month_tot_ly AS activating_product_order_count_month_tot_ly
    ,bgt.activating_order_count AS activating_product_order_count_budget
    ,bgt_alt.activating_order_count AS activating_product_order_count_budget_alt
    ,fcst.activating_order_count AS activating_product_order_count_forecast

    ,dcbcja.nonactivating_product_order_count AS nonactivating_product_order_count
    ,dcbcja.nonactivating_product_order_count_mtd AS nonactivating_product_order_count_mtd
    ,dcbcja.nonactivating_product_order_count_mtd_ly AS nonactivating_product_order_count_mtd_ly
    ,dcbcja.nonactivating_product_order_count_month_tot_lm AS nonactivating_product_order_count_month_tot_lm
    ,dcbcja.nonactivating_product_order_count_month_tot_ly AS nonactivating_product_order_count_month_tot_ly
    ,bgt.repeat_order_count AS nonactivating_product_order_count_budget
    ,bgt_alt.repeat_order_count AS nonactivating_product_order_count_budget_alt
    ,fcst.repeat_order_count AS nonactivating_product_order_count_forecast

    ,dcbcja.guest_product_order_count AS guest_product_order_count
    ,dcbcja.guest_product_order_count_mtd AS guest_product_order_count_mtd
    ,dcbcja.guest_product_order_count_mtd_ly AS guest_product_order_count_mtd_ly
    ,dcbcja.guest_product_order_count_month_tot_lm AS guest_product_order_count_month_tot_lm
    ,dcbcja.guest_product_order_count_month_tot_ly AS guest_product_order_count_month_tot_ly

    ,dcbcja.repeat_vip_product_order_count AS repeat_vip_product_order_count
    ,dcbcja.repeat_vip_product_order_count_mtd AS repeat_vip_product_order_count_mtd
    ,dcbcja.repeat_vip_product_order_count_mtd_ly AS repeat_vip_product_order_count_mtd_ly
    ,dcbcja.repeat_vip_product_order_count_month_tot_lm AS repeat_vip_product_order_count_month_tot_lm
    ,dcbcja.repeat_vip_product_order_count_month_tot_ly AS repeat_vip_product_order_count_month_tot_ly

    ,dcbcja.reactivated_vip_product_order_count AS reactivated_vip_product_order_count
    ,dcbcja.reactivated_vip_product_order_count_mtd AS reactivated_vip_product_order_count_mtd
    ,dcbcja.reactivated_vip_product_order_count_mtd_ly AS reactivated_vip_product_order_count_mtd_ly
    ,dcbcja.reactivated_vip_product_order_count_month_tot_lm AS reactivated_vip_product_order_count_month_tot_lm
    ,dcbcja.reactivated_vip_product_order_count_month_tot_ly AS reactivated_vip_product_order_count_month_tot_ly


    ,dcbcja.product_order_count_excl_seeding AS product_order_count_excl_seeding
    ,dcbcja.product_order_count_excl_seeding_mtd AS product_order_count_excl_seeding_mtd
    ,dcbcja.product_order_count_excl_seeding_mtd_ly AS product_order_count_excl_seeding_mtd_ly
    ,dcbcja.product_order_count_excl_seeding_month_tot_lm AS product_order_count_excl_seeding_month_tot_lm
    ,dcbcja.product_order_count_excl_seeding_month_tot_ly AS product_order_count_excl_seeding_month_tot_ly

    ,dcbcja.activating_product_order_count_excl_seeding AS activating_product_order_count_excl_seeding
    ,dcbcja.activating_product_order_count_excl_seeding_mtd AS activating_product_order_count_excl_seeding_mtd
    ,dcbcja.activating_product_order_count_excl_seeding_mtd_ly AS activating_product_order_count_excl_seeding_mtd_ly
    ,dcbcja.activating_product_order_count_excl_seeding_month_tot_lm AS activating_product_order_count_excl_seeding_month_tot_lm
    ,dcbcja.activating_product_order_count_excl_seeding_month_tot_ly AS activating_product_order_count_excl_seeding_month_tot_ly

    ,dcbcja.nonactivating_product_order_count_excl_seeding AS nonactivating_product_order_count_excl_seeding
    ,dcbcja.nonactivating_product_order_count_excl_seeding_mtd AS nonactivating_product_order_count_excl_seeding_mtd
    ,dcbcja.nonactivating_product_order_count_excl_seeding_mtd_ly AS nonactivating_product_order_count_excl_seeding_mtd_ly
    ,dcbcja.nonactivating_product_order_count_excl_seeding_month_tot_lm AS nonactivating_product_order_count_excl_seeding_month_tot_lm
    ,dcbcja.nonactivating_product_order_count_excl_seeding_month_tot_ly AS nonactivating_product_order_count_excl_seeding_month_tot_ly

    ,dcbcja.guest_product_order_count_excl_seeding AS guest_product_order_count_excl_seeding
    ,dcbcja.guest_product_order_count_excl_seeding_mtd AS guest_product_order_count_excl_seeding_mtd
    ,dcbcja.guest_product_order_count_excl_seeding_mtd_ly AS guest_product_order_count_excl_seeding_mtd_ly
    ,dcbcja.guest_product_order_count_excl_seeding_month_tot_lm AS guest_product_order_count_excl_seeding_month_tot_lm
    ,dcbcja.guest_product_order_count_excl_seeding_month_tot_ly AS guest_product_order_count_excl_seeding_month_tot_ly

    ,dcbcja.repeat_vip_product_order_count_excl_seeding AS repeat_vip_product_order_count_excl_seeding
    ,dcbcja.repeat_vip_product_order_count_excl_seeding_mtd AS repeat_vip_product_order_count_excl_seeding_mtd
    ,dcbcja.repeat_vip_product_order_count_excl_seeding_mtd_ly AS repeat_vip_product_order_count_excl_seeding_mtd_ly
    ,dcbcja.repeat_vip_product_order_count_excl_seeding_month_tot_lm AS repeat_vip_product_order_count_excl_seeding_month_tot_lm
    ,dcbcja.repeat_vip_product_order_count_excl_seeding_month_tot_ly AS repeat_vip_product_order_count_excl_seeding_month_tot_ly

    ,dcbcja.reactivated_vip_product_order_count_excl_seeding AS reactivated_vip_product_order_count_excl_seeding
    ,dcbcja.reactivated_vip_product_order_count_excl_seeding_mtd AS reactivated_vip_product_order_count_excl_seeding_mtd
    ,dcbcja.reactivated_vip_product_order_count_excl_seeding_mtd_ly AS reactivated_vip_product_order_count_excl_seeding_mtd_ly
    ,dcbcja.reactivated_vip_product_order_count_excl_seeding_month_tot_lm AS reactivated_vip_product_order_count_excl_seeding_month_tot_lm
    ,dcbcja.reactivated_vip_product_order_count_excl_seeding_month_tot_ly AS reactivated_vip_product_order_count_excl_seeding_month_tot_ly


    ,dcbcja.product_margin_pre_return_excl_seeding AS product_margin_pre_return_excl_seeding
    ,dcbcja.product_margin_pre_return_excl_seeding_mtd AS product_margin_pre_return_excl_seeding_mtd
    ,dcbcja.product_margin_pre_return_excl_seeding_mtd_ly AS product_margin_pre_return_excl_seeding_mtd_ly
    ,dcbcja.product_margin_pre_return_excl_seeding_month_tot_lm AS product_margin_pre_return_excl_seeding_month_tot_lm
    ,dcbcja.product_margin_pre_return_excl_seeding_month_tot_ly AS product_margin_pre_return_excl_seeding_month_tot_ly

    ,dcbcja.activating_product_margin_pre_return_excl_seeding AS activating_product_margin_pre_return_excl_seeding
    ,dcbcja.activating_product_margin_pre_return_excl_seeding_mtd AS activating_product_margin_pre_return_excl_seeding_mtd
    ,dcbcja.activating_product_margin_pre_return_excl_seeding_mtd_ly AS activating_product_margin_pre_return_excl_seeding_mtd_ly
    ,dcbcja.activating_product_margin_pre_return_excl_seeding_month_tot_lm AS activating_product_margin_pre_return_excl_seeding_month_tot_lm
    ,dcbcja.activating_product_margin_pre_return_excl_seeding_month_tot_ly AS activating_product_margin_pre_return_excl_seeding_month_tot_ly

    ,dcbcja.nonactivating_product_margin_pre_return_excl_seeding AS nonactivating_product_margin_pre_return_excl_seeding
    ,dcbcja.nonactivating_product_margin_pre_return_excl_seeding_mtd AS nonactivating_product_margin_pre_return_excl_seeding_mtd
    ,dcbcja.nonactivating_product_margin_pre_return_excl_seeding_mtd_ly AS nonactivating_product_margin_pre_return_excl_seeding_mtd_ly
    ,dcbcja.nonactivating_product_margin_pre_return_excl_seeding_month_tot_lm AS nonactivating_product_margin_pre_return_excl_seeding_month_tot_lm
    ,dcbcja.nonactivating_product_margin_pre_return_excl_seeding_month_tot_ly AS nonactivating_product_margin_pre_return_excl_seeding_month_tot_ly

    ,dcbcja.guest_product_margin_pre_return_excl_seeding AS guest_product_margin_pre_return_excl_seeding
    ,dcbcja.guest_product_margin_pre_return_excl_seeding_mtd AS guest_product_margin_pre_return_excl_seeding_mtd
    ,dcbcja.guest_product_margin_pre_return_excl_seeding_mtd_ly AS guest_product_margin_pre_return_excl_seeding_mtd_ly
    ,dcbcja.guest_product_margin_pre_return_excl_seeding_month_tot_lm AS guest_product_margin_pre_return_excl_seeding_month_tot_lm
    ,dcbcja.guest_product_margin_pre_return_excl_seeding_month_tot_ly AS guest_product_margin_pre_return_excl_seeding_month_tot_ly

    ,dcbcja.repeat_vip_product_margin_pre_return_excl_seeding AS repeat_vip_product_margin_pre_return_excl_seeding
    ,dcbcja.repeat_vip_product_margin_pre_return_excl_seeding_mtd AS repeat_vip_product_margin_pre_return_excl_seeding_mtd
    ,dcbcja.repeat_vip_product_margin_pre_return_excl_seeding_mtd_ly AS repeat_vip_product_margin_pre_return_excl_seeding_mtd_ly
    ,dcbcja.repeat_vip_product_margin_pre_return_excl_seeding_month_tot_lm AS repeat_vip_product_margin_pre_return_excl_seeding_month_tot_lm
    ,dcbcja.repeat_vip_product_margin_pre_return_excl_seeding_month_tot_ly AS repeat_vip_product_margin_pre_return_excl_seeding_month_tot_ly

    ,dcbcja.reactivated_vip_product_margin_pre_return_excl_seeding AS reactivated_vip_product_margin_pre_return_excl_seeding
    ,dcbcja.reactivated_vip_product_margin_pre_return_excl_seeding_mtd AS reactivated_vip_product_margin_pre_return_excl_seeding_mtd
    ,dcbcja.reactivated_vip_product_margin_pre_return_excl_seeding_mtd_ly AS reactivated_vip_product_margin_pre_return_excl_seeding_mtd_ly
    ,dcbcja.reactivated_vip_product_margin_pre_return_excl_seeding_month_tot_lm AS reactivated_vip_product_margin_pre_return_excl_seeding_month_tot_lm
    ,dcbcja.reactivated_vip_product_margin_pre_return_excl_seeding_month_tot_ly AS reactivated_vip_product_margin_pre_return_excl_seeding_month_tot_ly


    ,dcbcja.product_order_unit_count AS unit_count
    ,dcbcja.product_order_unit_count_mtd AS unit_count_mtd
    ,dcbcja.product_order_unit_count_mtd_ly AS unit_count_mtd_ly
    ,dcbcja.product_order_unit_count_month_tot_lm AS unit_count_month_tot_lm
    ,dcbcja.product_order_unit_count_month_tot_ly AS unit_count_month_tot_ly
    ,bgt.total_units_shipped AS unit_count_budget
    ,bgt_alt.total_units_shipped AS unit_count_budget_alt
    ,fcst.total_units_shipped AS unit_count_forecast

    ,dcbcja.activating_product_order_unit_count AS activating_unit_count
    ,dcbcja.activating_product_order_unit_count_mtd AS activating_unit_count_mtd
    ,dcbcja.activating_product_order_unit_count_mtd_ly AS activating_unit_count_mtd_ly
    ,dcbcja.activating_product_order_unit_count_month_tot_lm AS activating_unit_count_month_tot_lm
    ,dcbcja.activating_product_order_unit_count_month_tot_ly AS activating_unit_count_month_tot_ly
    ,bgt.activating_units AS activating_unit_count_budget
    ,bgt_alt.activating_units AS activating_unit_count_budget_alt
    ,fcst.activating_units AS activating_unit_count_forecast

    ,dcbcja.nonactivating_product_order_unit_count AS nonactivating_unit_count
    ,dcbcja.nonactivating_product_order_unit_count_mtd AS nonactivating_unit_count_mtd
    ,dcbcja.nonactivating_product_order_unit_count_mtd_ly AS nonactivating_unit_count_mtd_ly
    ,dcbcja.nonactivating_product_order_unit_count_month_tot_lm AS nonactivating_unit_count_month_tot_lm
    ,dcbcja.nonactivating_product_order_unit_count_month_tot_ly AS nonactivating_unit_count_month_tot_ly
    ,bgt.repeat_units AS nonactivating_unit_count_budget
    ,bgt_alt.repeat_units AS nonactivating_unit_count_budget_alt
    ,fcst.repeat_units AS nonactivating_unit_count_forecast

    ,dcbcja.guest_product_order_unit_count AS guest_unit_count
    ,dcbcja.guest_product_order_unit_count_mtd AS guest_unit_count_mtd
    ,dcbcja.guest_product_order_unit_count_mtd_ly AS guest_unit_count_mtd_ly
    ,dcbcja.guest_product_order_unit_count_month_tot_lm AS guest_unit_count_month_tot_lm
    ,dcbcja.guest_product_order_unit_count_month_tot_ly AS guest_unit_count_month_tot_ly

    ,dcbcja.repeat_vip_product_order_unit_count AS repeat_vip_unit_count
    ,dcbcja.repeat_vip_product_order_unit_count_mtd AS repeat_vip_unit_count_mtd
    ,dcbcja.repeat_vip_product_order_unit_count_mtd_ly AS repeat_vip_unit_count_mtd_ly
    ,dcbcja.repeat_vip_product_order_unit_count_month_tot_lm AS repeat_vip_unit_count_month_tot_lm
    ,dcbcja.repeat_vip_product_order_unit_count_month_tot_ly AS repeat_vip_unit_count_month_tot_ly

    ,dcbcja.activating_product_order_air_vip_price AS activating_product_order_air_vip_price
    ,dcbcja.activating_product_order_air_vip_price_mtd AS activating_product_order_air_vip_price_mtd
    ,dcbcja.activating_product_order_air_vip_price_mtd_ly AS activating_product_order_air_vip_price_mtd_ly
    ,dcbcja.activating_product_order_air_vip_price_month_tot_lm AS activating_product_order_air_vip_price_month_tot_lm
    ,dcbcja.activating_product_order_air_vip_price_month_tot_ly AS activating_product_order_air_vip_price_month_tot_ly

    ,dcbcja.nonactivating_product_order_air_price AS nonactivating_product_order_air_price
    ,dcbcja.nonactivating_product_order_air_price_mtd AS nonactivating_product_order_air_price_mtd
    ,dcbcja.nonactivating_product_order_air_price_mtd_ly AS nonactivating_product_order_air_price_mtd_ly
    ,dcbcja.nonactivating_product_order_air_price_month_tot_lm AS nonactivating_product_order_air_price_month_tot_lm
    ,dcbcja.nonactivating_product_order_air_price_month_tot_ly AS nonactivating_product_order_air_price_month_tot_ly

    ,dcbcja.repeat_vip_product_order_air_vip_price AS repeat_vip_product_order_air_vip_price
    ,dcbcja.repeat_vip_product_order_air_vip_price_mtd AS repeat_vip_product_order_air_vip_price_mtd
    ,dcbcja.repeat_vip_product_order_air_vip_price_mtd_ly AS repeat_vip_product_order_air_vip_price_mtd_ly
    ,dcbcja.repeat_vip_product_order_air_vip_price_month_tot_lm AS repeat_vip_product_order_air_vip_price_month_tot_lm
    ,dcbcja.repeat_vip_product_order_air_vip_price_month_tot_ly AS repeat_vip_product_order_air_vip_price_month_tot_ly

    ,dcbcja.guest_product_order_air_vip_price AS guest_product_order_air_vip_price
    ,dcbcja.guest_product_order_air_vip_price_mtd AS guest_product_order_air_vip_price_mtd
    ,dcbcja.guest_product_order_air_vip_price_mtd_ly AS guest_product_order_air_vip_price_mtd_ly
    ,dcbcja.guest_product_order_air_vip_price_month_tot_lm AS guest_product_order_air_vip_price_month_tot_lm
    ,dcbcja.guest_product_order_air_vip_price_month_tot_ly AS guest_product_order_air_vip_price_month_tot_ly

    ,dcbcja.guest_product_order_retail_unit_price AS guest_product_order_retail_unit_price
    ,dcbcja.guest_product_order_retail_unit_price_mtd AS guest_product_order_retail_unit_price_mtd
    ,dcbcja.guest_product_order_retail_unit_price_mtd_ly AS guest_product_order_retail_unit_price_mtd_ly
    ,dcbcja.guest_product_order_retail_unit_price_month_tot_lm AS guest_product_order_retail_unit_price_month_tot_lm
    ,dcbcja.guest_product_order_retail_unit_price_month_tot_ly AS guest_product_order_retail_unit_price_month_tot_ly

    ,dcbcja.activating_product_order_price_offered_amount AS activating_product_order_price_offered_amount
    ,dcbcja.activating_product_order_price_offered_amount_mtd AS activating_product_order_price_offered_amount_mtd
    ,dcbcja.activating_product_order_price_offered_amount_mtd_ly AS activating_product_order_price_offered_amount_mtd_ly
    ,dcbcja.activating_product_order_price_offered_amount_month_tot_lm AS activating_product_order_price_offered_amount_month_tot_lm
    ,dcbcja.activating_product_order_price_offered_amount_month_tot_ly AS activating_product_order_price_offered_amount_month_tot_ly

    ,dcbcja.nonactivating_product_order_price_offered_amount AS nonactivating_product_order_price_offered_amount
    ,dcbcja.nonactivating_product_order_price_offered_amount_mtd AS nonactivating_product_order_price_offered_amount_mtd
    ,dcbcja.nonactivating_product_order_price_offered_amount_mtd_ly AS nonactivating_product_order_price_offered_amount_mtd_ly
    ,dcbcja.nonactivating_product_order_price_offered_amount_month_tot_lm AS nonactivating_product_order_price_offered_amount_month_tot_lm
    ,dcbcja.nonactivating_product_order_price_offered_amount_month_tot_ly AS nonactivating_product_order_price_offered_amount_month_tot_ly

    ,dcbcja.guest_product_order_price_offered_amount AS guest_product_order_price_offered_amount
    ,dcbcja.guest_product_order_price_offered_amount_mtd AS guest_product_order_price_offered_amount_mtd
    ,dcbcja.guest_product_order_price_offered_amount_mtd_ly AS guest_product_order_price_offered_amount_mtd_ly
    ,dcbcja.guest_product_order_price_offered_amount_month_tot_lm AS guest_product_order_price_offered_amount_month_tot_lm
    ,dcbcja.guest_product_order_price_offered_amount_month_tot_ly AS guest_product_order_price_offered_amount_month_tot_ly

    ,dcbcja.repeat_vip_product_order_price_offered_amount AS repeat_vip_product_order_price_offered_amount
    ,dcbcja.repeat_vip_product_order_price_offered_amount_mtd AS repeat_vip_product_order_price_offered_amount_mtd
    ,dcbcja.repeat_vip_product_order_price_offered_amount_mtd_ly AS repeat_vip_product_order_price_offered_amount_mtd_ly
    ,dcbcja.repeat_vip_product_order_price_offered_amount_month_tot_lm AS repeat_vip_product_order_price_offered_amount_month_tot_lm
    ,dcbcja.repeat_vip_product_order_price_offered_amount_month_tot_ly AS repeat_vip_product_order_price_offered_amount_month_tot_ly

    ,dcbcja.product_order_landed_product_cost_amount AS product_order_landed_product_cost_amount
    ,dcbcja.product_order_landed_product_cost_amount_mtd AS product_order_landed_product_cost_amount_mtd
    ,dcbcja.product_order_landed_product_cost_amount_mtd_ly AS product_order_landed_product_cost_amount_mtd_ly
    ,dcbcja.product_order_landed_product_cost_amount_month_tot_lm AS product_order_landed_product_cost_amount_month_tot_lm
    ,dcbcja.product_order_landed_product_cost_amount_month_tot_ly AS product_order_landed_product_cost_amount_month_tot_ly

    ,dcbcja.activating_product_order_landed_product_cost_amount AS activating_product_order_landed_product_cost_amount
    ,dcbcja.activating_product_order_landed_product_cost_amount_mtd AS activating_product_order_landed_product_cost_amount_mtd
    ,dcbcja.activating_product_order_landed_product_cost_amount_mtd_ly AS activating_product_order_landed_product_cost_amount_mtd_ly
    ,dcbcja.activating_product_order_landed_product_cost_amount_month_tot_lm AS activating_product_order_landed_product_cost_amount_month_tot_lm
    ,dcbcja.activating_product_order_landed_product_cost_amount_month_tot_ly AS activating_product_order_landed_product_cost_amount_month_tot_ly

    ,dcbcja.nonactivating_product_order_landed_product_cost_amount AS nonactivating_product_order_landed_product_cost_amount
    ,dcbcja.nonactivating_product_order_landed_product_cost_amount_mtd AS nonactivating_product_order_landed_product_cost_amount_mtd
    ,dcbcja.nonactivating_product_order_landed_product_cost_amount_mtd_ly AS nonactivating_product_order_landed_product_cost_amount_mtd_ly
    ,dcbcja.nonactivating_product_order_landed_product_cost_amount_month_tot_lm AS nonactivating_product_order_landed_product_cost_amount_month_tot_lm
    ,dcbcja.nonactivating_product_order_landed_product_cost_amount_month_tot_ly AS nonactivating_product_order_landed_product_cost_amount_month_tot_ly

    ,dcbcja.guest_product_order_landed_product_cost_amount AS guest_product_order_landed_product_cost_amount
    ,dcbcja.guest_product_order_landed_product_cost_amount_mtd AS guest_product_order_landed_product_cost_amount_mtd
    ,dcbcja.guest_product_order_landed_product_cost_amount_mtd_ly AS guest_product_order_landed_product_cost_amount_mtd_ly
    ,dcbcja.guest_product_order_landed_product_cost_amount_month_tot_lm AS guest_product_order_landed_product_cost_amount_month_tot_lm
    ,dcbcja.guest_product_order_landed_product_cost_amount_month_tot_ly AS guest_product_order_landed_product_cost_amount_month_tot_ly

    ,dcbcja.repeat_vip_product_order_landed_product_cost_amount AS repeat_vip_product_order_landed_product_cost_amount
    ,dcbcja.repeat_vip_product_order_landed_product_cost_amount_mtd AS repeat_vip_product_order_landed_product_cost_amount_mtd
    ,dcbcja.repeat_vip_product_order_landed_product_cost_amount_mtd_ly AS repeat_vip_product_order_landed_product_cost_amount_mtd_ly
    ,dcbcja.repeat_vip_product_order_landed_product_cost_amount_month_tot_lm AS repeat_vip_product_order_landed_product_cost_amount_month_tot_lm
    ,dcbcja.repeat_vip_product_order_landed_product_cost_amount_month_tot_ly AS repeat_vip_product_order_landed_product_cost_amount_month_tot_ly

    ,dcbcja.product_order_landed_product_cost_amount_accounting AS product_order_landed_product_cost_amount_accounting
    ,dcbcja.product_order_landed_product_cost_amount_accounting_mtd AS product_order_landed_product_cost_amount_accounting_mtd
    ,dcbcja.product_order_landed_product_cost_amount_accounting_mtd_ly AS product_order_landed_product_cost_amount_accounting_mtd_ly
    ,dcbcja.product_order_landed_product_cost_amount_accounting_month_tot_lm AS product_order_landed_product_cost_amount_accounting_month_tot_lm
    ,dcbcja.product_order_landed_product_cost_amount_accounting_month_tot_ly AS product_order_landed_product_cost_amount_accounting_month_tot_ly

    ,dcbcja.activating_product_order_landed_product_cost_amount_accounting AS activating_product_order_landed_product_cost_amount_accounting
    ,dcbcja.activating_product_order_landed_product_cost_amount_accounting_mtd AS activating_product_order_landed_product_cost_amount_accounting_mtd
    ,dcbcja.activating_product_order_landed_product_cost_amount_accounting_mtd_ly AS activating_product_order_landed_product_cost_amount_accounting_mtd_ly
    ,dcbcja.activating_product_order_landed_product_cost_amount_accounting_month_tot_lm AS activating_product_order_landed_product_cost_amount_accounting_month_tot_lm
    ,dcbcja.activating_product_order_landed_product_cost_amount_accounting_month_tot_ly AS activating_product_order_landed_product_cost_amount_accounting_month_tot_ly

    ,dcbcja.nonactivating_product_order_landed_product_cost_amount_accounting AS nonactivating_product_order_landed_product_cost_amount_accounting
    ,dcbcja.nonactivating_product_order_landed_product_cost_amount_accounting_mtd AS nonactivating_product_order_landed_product_cost_amount_accounting_mtd
    ,dcbcja.nonactivating_product_order_landed_product_cost_amount_accounting_mtd_ly AS nonactivating_product_order_landed_product_cost_amount_accounting_mtd_ly
    ,dcbcja.nonactivating_product_order_landed_product_cost_amount_accounting_month_tot_lm AS nonactivating_product_order_landed_product_cost_amount_accounting_month_tot_lm
    ,dcbcja.nonactivating_product_order_landed_product_cost_amount_accounting_month_tot_ly AS nonactivating_product_order_landed_product_cost_amount_accounting_month_tot_ly

    ,dcbcja.oracle_product_order_landed_product_cost_amount AS oracle_product_order_landed_product_cost_amount
    ,dcbcja.oracle_product_order_landed_product_cost_amount_mtd AS oracle_product_order_landed_product_cost_amount_mtd
    ,dcbcja.oracle_product_order_landed_product_cost_amount_mtd_ly AS oracle_product_order_landed_product_cost_amount_mtd_ly
    ,dcbcja.oracle_product_order_landed_product_cost_amount_month_tot_lm AS oracle_product_order_landed_product_cost_amount_month_tot_lm
    ,dcbcja.oracle_product_order_landed_product_cost_amount_month_tot_ly AS oracle_product_order_landed_product_cost_amount_month_tot_ly

    ,dcbcja.product_order_shipping_cost_amount AS product_order_shipping_cost_amount
    ,dcbcja.product_order_shipping_cost_amount_mtd AS product_order_shipping_cost_amount_mtd
    ,dcbcja.product_order_shipping_cost_amount_mtd_ly AS product_order_shipping_cost_amount_mtd_ly
    ,dcbcja.product_order_shipping_cost_amount_month_tot_lm AS product_order_shipping_cost_amount_month_tot_lm
    ,dcbcja.product_order_shipping_cost_amount_month_tot_ly AS product_order_shipping_cost_amount_month_tot_ly

    ,dcbcja.product_order_shipping_supplies_cost_amount AS product_order_shipping_supplies_cost_amount
    ,dcbcja.product_order_shipping_supplies_cost_amount_mtd AS product_order_shipping_supplies_cost_amount_mtd
    ,dcbcja.product_order_shipping_supplies_cost_amount_mtd_ly AS product_order_shipping_supplies_cost_amount_mtd_ly
    ,dcbcja.product_order_shipping_supplies_cost_amount_month_tot_lm AS product_order_shipping_supplies_cost_amount_month_tot_lm
    ,dcbcja.product_order_shipping_supplies_cost_amount_month_tot_ly AS product_order_shipping_supplies_cost_amount_month_tot_ly

    ,dcbcja.product_order_direct_cogs_amount AS product_order_direct_cogs_amount
    ,dcbcja.product_order_direct_cogs_amount_mtd AS product_order_direct_cogs_amount_mtd
    ,dcbcja.product_order_direct_cogs_amount_mtd_ly AS product_order_direct_cogs_amount_mtd_ly
    ,dcbcja.product_order_direct_cogs_amount_month_tot_lm AS product_order_direct_cogs_amount_month_tot_lm
    ,dcbcja.product_order_direct_cogs_amount_month_tot_ly AS product_order_direct_cogs_amount_month_tot_ly

    ,dcbcja.product_order_cash_refund_amount AS product_order_cash_refund_amount
    ,dcbcja.product_order_cash_refund_amount_mtd AS product_order_cash_refund_amount_mtd
    ,dcbcja.product_order_cash_refund_amount_mtd_ly AS product_order_cash_refund_amount_mtd_ly
    ,dcbcja.product_order_cash_refund_amount_month_tot_lm AS product_order_cash_refund_amount_month_tot_lm
    ,dcbcja.product_order_cash_refund_amount_month_tot_ly AS product_order_cash_refund_amount_month_tot_ly

    ,dcbcja.product_order_cash_chargeback_amount AS product_order_cash_chargeback_amount
    ,dcbcja.product_order_cash_chargeback_amount_mtd AS product_order_cash_chargeback_amount_mtd
    ,dcbcja.product_order_cash_chargeback_amount_mtd_ly AS product_order_cash_chargeback_amount_mtd_ly
    ,dcbcja.product_order_cash_chargeback_amount_month_tot_lm AS product_order_cash_chargeback_amount_month_tot_lm
    ,dcbcja.product_order_cash_chargeback_amount_month_tot_ly AS product_order_cash_chargeback_amount_month_tot_ly

    ,dcbcja.product_order_cost_product_returned_resaleable_amount AS product_cost_returned_resaleable_incl_reship_exch
    ,dcbcja.product_order_cost_product_returned_resaleable_amount_mtd AS product_cost_returned_resaleable_incl_reship_exch_mtd
    ,dcbcja.product_order_cost_product_returned_resaleable_amount_mtd_ly AS product_cost_returned_resaleable_incl_reship_exch_mtd_ly
    ,dcbcja.product_order_cost_product_returned_resaleable_amount_month_tot_lm AS product_cost_returned_resaleable_incl_reship_exch_month_tot_lm
    ,dcbcja.product_order_cost_product_returned_resaleable_amount_month_tot_ly AS product_cost_returned_resaleable_incl_reship_exch_month_tot_ly

    ,dcbcja.product_order_cost_product_returned_resaleable_amount_accounting AS product_cost_returned_resaleable_incl_reship_exch_accounting
    ,dcbcja.product_order_cost_product_returned_resaleable_amount_accounting_mtd AS product_cost_returned_resaleable_incl_reship_exch_accounting_mtd
    ,dcbcja.product_order_cost_product_returned_resaleable_amount_accounting_mtd_ly AS product_cost_returned_resaleable_incl_reship_exch_accounting_mtd_ly
    ,dcbcja.product_order_cost_product_returned_resaleable_amount_accounting_month_tot_lm AS product_cost_returned_resaleable_incl_reship_exch_accounting_month_tot_lm
    ,dcbcja.product_order_cost_product_returned_resaleable_amount_accounting_month_tot_ly AS product_cost_returned_resaleable_incl_reship_exch_accounting_month_tot_ly

    ,dcbcja.product_order_cost_product_returned_damaged_amount AS product_order_cost_product_returned_damaged_amount
    ,dcbcja.product_order_cost_product_returned_damaged_amount_mtd AS product_order_cost_product_returned_damaged_amount_mtd
    ,dcbcja.product_order_cost_product_returned_damaged_amount_mtd_ly AS product_order_cost_product_returned_damaged_amount_mtd_ly
    ,dcbcja.product_order_cost_product_returned_damaged_amount_month_tot_lm AS product_order_cost_product_returned_damaged_amount_month_tot_lm
    ,dcbcja.product_order_cost_product_returned_damaged_amount_month_tot_ly AS product_order_cost_product_returned_damaged_amount_month_tot_ly

    ,dcbcja.product_order_return_shipping_cost_amount AS return_shipping_costs_incl_reship_exch
    ,dcbcja.product_order_return_shipping_cost_amount_mtd AS return_shipping_costs_incl_reship_exch_mtd
    ,dcbcja.product_order_return_shipping_cost_amount_mtd_ly AS return_shipping_costs_incl_reship_exch_mtd_ly
    ,dcbcja.product_order_return_shipping_cost_amount_month_tot_lm AS return_shipping_costs_incl_reship_exch_month_tot_lm
    ,dcbcja.product_order_return_shipping_cost_amount_month_tot_ly AS return_shipping_costs_incl_reship_exch_month_tot_ly
    ,bgt.returns_shipping_cost AS return_shipping_costs_incl_reship_exch_budget
    ,bgt_alt.returns_shipping_cost AS return_shipping_costs_incl_reship_exch_budget_alt
    ,fcst.returns_shipping_cost AS return_shipping_costs_incl_reship_exch_forecast

    ,dcbcja.product_order_reship_order_count AS product_order_reship_order_count
    ,dcbcja.product_order_reship_order_count_mtd AS product_order_reship_order_count_mtd
    ,dcbcja.product_order_reship_order_count_mtd_ly AS product_order_reship_order_count_mtd_ly
    ,dcbcja.product_order_reship_order_count_month_tot_lm AS product_order_reship_order_count_month_tot_lm
    ,dcbcja.product_order_reship_order_count_month_tot_ly AS product_order_reship_order_count_month_tot_ly

    ,dcbcja.product_order_reship_unit_count AS product_order_reship_unit_count
    ,dcbcja.product_order_reship_unit_count_mtd AS product_order_reship_unit_count_mtd
    ,dcbcja.product_order_reship_unit_count_mtd_ly AS product_order_reship_unit_count_mtd_ly
    ,dcbcja.product_order_reship_unit_count_month_tot_lm AS product_order_reship_unit_count_month_tot_lm
    ,dcbcja.product_order_reship_unit_count_month_tot_ly AS product_order_reship_unit_count_month_tot_ly

    ,dcbcja.activating_product_order_reship_unit_count AS activating_product_order_reship_unit_count
    ,dcbcja.activating_product_order_reship_unit_count_mtd AS activating_product_order_reship_unit_count_mtd
    ,dcbcja.activating_product_order_reship_unit_count_mtd_ly AS activating_product_order_reship_unit_count_mtd_ly
    ,dcbcja.activating_product_order_reship_unit_count_month_tot_lm AS activating_product_order_reship_unit_count_month_tot_lm
    ,dcbcja.activating_product_order_reship_unit_count_month_tot_ly AS activating_product_order_reship_unit_count_month_tot_ly

    ,dcbcja.nonactivating_product_order_reship_unit_count AS nonactivating_product_order_reship_unit_count
    ,dcbcja.nonactivating_product_order_reship_unit_count_mtd AS nonactivating_product_order_reship_unit_count_mtd
    ,dcbcja.nonactivating_product_order_reship_unit_count_mtd_ly AS nonactivating_product_order_reship_unit_count_mtd_ly
    ,dcbcja.nonactivating_product_order_reship_unit_count_month_tot_lm AS nonactivating_product_order_reship_unit_count_month_tot_lm
    ,dcbcja.nonactivating_product_order_reship_unit_count_month_tot_ly AS nonactivating_product_order_reship_unit_count_month_tot_ly

    ,dcbcja.guest_product_order_reship_unit_count AS guest_product_order_reship_unit_count
    ,dcbcja.guest_product_order_reship_unit_count_mtd AS guest_product_order_reship_unit_count_mtd
    ,dcbcja.guest_product_order_reship_unit_count_mtd_ly AS guest_product_order_reship_unit_count_mtd_ly
    ,dcbcja.guest_product_order_reship_unit_count_month_tot_lm AS guest_product_order_reship_unit_count_month_tot_lm
    ,dcbcja.guest_product_order_reship_unit_count_month_tot_ly AS guest_product_order_reship_unit_count_month_tot_ly

    ,dcbcja.repeat_vip_product_order_reship_unit_count AS repeat_vip_product_order_reship_unit_count
    ,dcbcja.repeat_vip_product_order_reship_unit_count_mtd AS repeat_vip_product_order_reship_unit_count_mtd
    ,dcbcja.repeat_vip_product_order_reship_unit_count_mtd_ly AS repeat_vip_product_order_reship_unit_count_mtd_ly
    ,dcbcja.repeat_vip_product_order_reship_unit_count_month_tot_lm AS repeat_vip_product_order_reship_unit_count_month_tot_lm
    ,dcbcja.repeat_vip_product_order_reship_unit_count_month_tot_ly AS repeat_vip_product_order_reship_unit_count_month_tot_ly

    ,bgt.total_cogs_minus_cash AS total_cogs_budget
    ,bgt_alt.total_cogs_minus_cash AS total_cogs_budget_alt
    ,fcst.total_cogs_minus_cash AS total_cogs_forecast

    ,dcbcja.product_order_reship_product_cost_amount AS product_order_reship_product_cost_amount
    ,dcbcja.product_order_reship_product_cost_amount_mtd AS product_order_reship_product_cost_amount_mtd
    ,dcbcja.product_order_reship_product_cost_amount_mtd_ly AS product_order_reship_product_cost_amount_mtd_ly
    ,dcbcja.product_order_reship_product_cost_amount_month_tot_lm AS product_order_reship_product_cost_amount_month_tot_lm
    ,dcbcja.product_order_reship_product_cost_amount_month_tot_ly AS product_order_reship_product_cost_amount_month_tot_ly

    ,dcbcja.activating_product_order_reship_product_cost_amount AS activating_product_order_reship_product_cost_amount
    ,dcbcja.activating_product_order_reship_product_cost_amount_mtd AS activating_product_order_reship_product_cost_amount_mtd
    ,dcbcja.activating_product_order_reship_product_cost_amount_mtd_ly AS activating_product_order_reship_product_cost_amount_mtd_ly
    ,dcbcja.activating_product_order_reship_product_cost_amount_month_tot_lm AS activating_product_order_reship_product_cost_amount_month_tot_lm
    ,dcbcja.activating_product_order_reship_product_cost_amount_month_tot_ly AS activating_product_order_reship_product_cost_amount_month_tot_ly

    ,dcbcja.nonactivating_product_order_reship_product_cost_amount AS nonactivating_product_order_reship_product_cost_amount
    ,dcbcja.nonactivating_product_order_reship_product_cost_amount_mtd AS nonactivating_product_order_reship_product_cost_amount_mtd
    ,dcbcja.nonactivating_product_order_reship_product_cost_amount_mtd_ly AS nonactivating_product_order_reship_product_cost_amount_mtd_ly
    ,dcbcja.nonactivating_product_order_reship_product_cost_amount_month_tot_lm AS nonactivating_product_order_reship_product_cost_amount_month_tot_lm
    ,dcbcja.nonactivating_product_order_reship_product_cost_amount_month_tot_ly AS nonactivating_product_order_reship_product_cost_amount_month_tot_ly

    ,dcbcja.guest_product_order_reship_product_cost_amount AS guest_product_order_reship_product_cost_amount
    ,dcbcja.guest_product_order_reship_product_cost_amount_mtd AS guest_product_order_reship_product_cost_amount_mtd
    ,dcbcja.guest_product_order_reship_product_cost_amount_mtd_ly AS guest_product_order_reship_product_cost_amount_mtd_ly
    ,dcbcja.guest_product_order_reship_product_cost_amount_month_tot_lm AS guest_product_order_reship_product_cost_amount_month_tot_lm
    ,dcbcja.guest_product_order_reship_product_cost_amount_month_tot_ly AS guest_product_order_reship_product_cost_amount_month_tot_ly

    ,dcbcja.repeat_vip_product_order_reship_product_cost_amount AS repeat_vip_product_order_reship_product_cost_amount
    ,dcbcja.repeat_vip_product_order_reship_product_cost_amount_mtd AS repeat_vip_product_order_reship_product_cost_amount_mtd
    ,dcbcja.repeat_vip_product_order_reship_product_cost_amount_mtd_ly AS repeat_vip_product_order_reship_product_cost_amount_mtd_ly
    ,dcbcja.repeat_vip_product_order_reship_product_cost_amount_month_tot_lm AS repeat_vip_product_order_reship_product_cost_amount_month_tot_lm
    ,dcbcja.repeat_vip_product_order_reship_product_cost_amount_month_tot_ly AS repeat_vip_product_order_reship_product_cost_amount_month_tot_ly

    ,dcbcja.product_order_reship_product_cost_amount_accounting AS product_order_reship_product_cost_amount_accounting
    ,dcbcja.product_order_reship_product_cost_amount_accounting_mtd AS product_order_reship_product_cost_amount_accounting_mtd
    ,dcbcja.product_order_reship_product_cost_amount_accounting_mtd_ly AS product_order_reship_product_cost_amount_accounting_mtd_ly
    ,dcbcja.product_order_reship_product_cost_amount_accounting_month_tot_lm AS product_order_reship_product_cost_amount_accounting_month_tot_lm
    ,dcbcja.product_order_reship_product_cost_amount_accounting_month_tot_ly AS product_order_reship_product_cost_amount_accounting_month_tot_ly

    ,dcbcja.activating_product_order_reship_product_cost_amount_accounting AS activating_product_order_reship_product_cost_amount_accounting
    ,dcbcja.activating_product_order_reship_product_cost_amount_accounting_mtd AS activating_product_order_reship_product_cost_amount_accounting_mtd
    ,dcbcja.activating_product_order_reship_product_cost_amount_accounting_mtd_ly AS activating_product_order_reship_product_cost_amount_accounting_mtd_ly
    ,dcbcja.activating_product_order_reship_product_cost_amount_accounting_month_tot_lm AS activating_product_order_reship_product_cost_amount_accounting_month_tot_lm
    ,dcbcja.activating_product_order_reship_product_cost_amount_accounting_month_tot_ly AS activating_product_order_reship_product_cost_amount_accounting_month_tot_ly

    ,dcbcja.nonactivating_product_order_reship_product_cost_amount_accounting AS nonactivating_product_order_reship_product_cost_amount_accounting
    ,dcbcja.nonactivating_product_order_reship_product_cost_amount_accounting_mtd AS nonactivating_product_order_reship_product_cost_amount_accounting_mtd
    ,dcbcja.nonactivating_product_order_reship_product_cost_amount_accounting_mtd_ly AS nonactivating_product_order_reship_product_cost_amount_accounting_mtd_ly
    ,dcbcja.nonactivating_product_order_reship_product_cost_amount_accounting_month_tot_lm AS nonactivating_product_order_reship_product_cost_amount_accounting_month_tot_lm
    ,dcbcja.nonactivating_product_order_reship_product_cost_amount_accounting_month_tot_ly AS nonactivating_product_order_reship_product_cost_amount_accounting_month_tot_ly

    ,dcbcja.oracle_product_order_reship_product_cost_amount AS oracle_product_order_reship_product_cost_amount
    ,dcbcja.oracle_product_order_reship_product_cost_amount_mtd AS oracle_product_order_reship_product_cost_amount_mtd
    ,dcbcja.oracle_product_order_reship_product_cost_amount_mtd_ly AS oracle_product_order_reship_product_cost_amount_mtd_ly
    ,dcbcja.oracle_product_order_reship_product_cost_amount_month_tot_lm AS oracle_product_order_reship_product_cost_amount_month_tot_lm
    ,dcbcja.oracle_product_order_reship_product_cost_amount_month_tot_ly AS oracle_product_order_reship_product_cost_amount_month_tot_ly

    ,dcbcja.product_order_reship_shipping_cost_amount AS product_order_reship_shipping_cost_amount
    ,dcbcja.product_order_reship_shipping_cost_amount_mtd AS product_order_reship_shipping_cost_amount_mtd
    ,dcbcja.product_order_reship_shipping_cost_amount_mtd_ly AS product_order_reship_shipping_cost_amount_mtd_ly
    ,dcbcja.product_order_reship_shipping_cost_amount_month_tot_lm AS product_order_reship_shipping_cost_amount_month_tot_lm
    ,dcbcja.product_order_reship_shipping_cost_amount_month_tot_ly AS product_order_reship_shipping_cost_amount_month_tot_ly

    ,dcbcja.product_order_reship_shipping_supplies_cost_amount AS product_order_reship_shipping_supplies_cost_amount
    ,dcbcja.product_order_reship_shipping_supplies_cost_amount_mtd AS product_order_reship_shipping_supplies_cost_amount_mtd
    ,dcbcja.product_order_reship_shipping_supplies_cost_amount_mtd_ly AS product_order_reship_shipping_supplies_cost_amount_mtd_ly
    ,dcbcja.product_order_reship_shipping_supplies_cost_amount_month_tot_lm AS product_order_reship_shipping_supplies_cost_amount_month_tot_lm
    ,dcbcja.product_order_reship_shipping_supplies_cost_amount_month_tot_ly AS product_order_reship_shipping_supplies_cost_amount_month_tot_ly

    ,dcbcja.product_order_exchange_order_count AS product_order_exchange_order_count
    ,dcbcja.product_order_exchange_order_count_mtd AS product_order_exchange_order_count_mtd
    ,dcbcja.product_order_exchange_order_count_mtd_ly AS product_order_exchange_order_count_mtd_ly
    ,dcbcja.product_order_exchange_order_count_month_tot_lm AS product_order_exchange_order_count_month_tot_lm
    ,dcbcja.product_order_exchange_order_count_month_tot_ly AS product_order_exchange_order_count_month_tot_ly

    ,bgt.reship_exch_orders_shipped AS product_order_reship_exch_order_count_budget
    ,bgt_alt.reship_exch_orders_shipped AS product_order_reship_exch_order_count_budget_alt
    ,fcst.reship_exch_orders_shipped AS product_order_reship_exch_order_count_forecast

    ,dcbcja.product_order_exchange_unit_count AS product_order_exchange_unit_count
    ,dcbcja.product_order_exchange_unit_count_mtd AS product_order_exchange_unit_count_mtd
    ,dcbcja.product_order_exchange_unit_count_mtd_ly AS product_order_exchange_unit_count_mtd_ly
    ,dcbcja.product_order_exchange_unit_count_month_tot_lm AS product_order_exchange_unit_count_month_tot_lm
    ,dcbcja.product_order_exchange_unit_count_month_tot_ly AS product_order_exchange_unit_count_month_tot_ly

    ,dcbcja.activating_product_order_exchange_unit_count AS activating_product_order_exchange_unit_count
    ,dcbcja.activating_product_order_exchange_unit_count_mtd AS activating_product_order_exchange_unit_count_mtd
    ,dcbcja.activating_product_order_exchange_unit_count_mtd_ly AS activating_product_order_exchange_unit_count_mtd_ly
    ,dcbcja.activating_product_order_exchange_unit_count_month_tot_lm AS activating_product_order_exchange_unit_count_month_tot_lm
    ,dcbcja.activating_product_order_exchange_unit_count_month_tot_ly AS activating_product_order_exchange_unit_count_month_tot_ly

    ,dcbcja.nonactivating_product_order_exchange_unit_count AS nonactivating_product_order_exchange_unit_count
    ,dcbcja.nonactivating_product_order_exchange_unit_count_mtd AS nonactivating_product_order_exchange_unit_count_mtd
    ,dcbcja.nonactivating_product_order_exchange_unit_count_mtd_ly AS nonactivating_product_order_exchange_unit_count_mtd_ly
    ,dcbcja.nonactivating_product_order_exchange_unit_count_month_tot_lm AS nonactivating_product_order_exchange_unit_count_month_tot_lm
    ,dcbcja.nonactivating_product_order_exchange_unit_count_month_tot_ly AS nonactivating_product_order_exchange_unit_count_month_tot_ly

    ,dcbcja.guest_product_order_exchange_unit_count AS guest_product_order_exchange_unit_count
    ,dcbcja.guest_product_order_exchange_unit_count_mtd AS guest_product_order_exchange_unit_count_mtd
    ,dcbcja.guest_product_order_exchange_unit_count_mtd_ly AS guest_product_order_exchange_unit_count_mtd_ly
    ,dcbcja.guest_product_order_exchange_unit_count_month_tot_lm AS guest_product_order_exchange_unit_count_month_tot_lm
    ,dcbcja.guest_product_order_exchange_unit_count_month_tot_ly AS guest_product_order_exchange_unit_count_month_tot_ly

    ,dcbcja.repeat_vip_product_order_exchange_unit_count AS repeat_vip_product_order_exchange_unit_count
    ,dcbcja.repeat_vip_product_order_exchange_unit_count_mtd AS repeat_vip_product_order_exchange_unit_count_mtd
    ,dcbcja.repeat_vip_product_order_exchange_unit_count_mtd_ly AS repeat_vip_product_order_exchange_unit_count_mtd_ly
    ,dcbcja.repeat_vip_product_order_exchange_unit_count_month_tot_lm AS repeat_vip_product_order_exchange_unit_count_month_tot_lm
    ,dcbcja.repeat_vip_product_order_exchange_unit_count_month_tot_ly AS repeat_vip_product_order_exchange_unit_count_month_tot_ly

    ,dcbcja.product_order_exchange_product_cost_amount AS product_order_exchange_product_cost_amount
    ,dcbcja.product_order_exchange_product_cost_amount_mtd AS product_order_exchange_product_cost_amount_mtd
    ,dcbcja.product_order_exchange_product_cost_amount_mtd_ly AS product_order_exchange_product_cost_amount_mtd_ly
    ,dcbcja.product_order_exchange_product_cost_amount_month_tot_lm AS product_order_exchange_product_cost_amount_month_tot_lm
    ,dcbcja.product_order_exchange_product_cost_amount_month_tot_ly AS product_order_exchange_product_cost_amount_month_tot_ly

    ,dcbcja.activating_product_order_exchange_product_cost_amount AS activating_product_order_exchange_product_cost_amount
    ,dcbcja.activating_product_order_exchange_product_cost_amount_mtd AS activating_product_order_exchange_product_cost_amount_mtd
    ,dcbcja.activating_product_order_exchange_product_cost_amount_mtd_ly AS activating_product_order_exchange_product_cost_amount_mtd_ly
    ,dcbcja.activating_product_order_exchange_product_cost_amount_month_tot_lm AS activating_product_order_exchange_product_cost_amount_month_tot_lm
    ,dcbcja.activating_product_order_exchange_product_cost_amount_month_tot_ly AS activating_product_order_exchange_product_cost_amount_month_tot_ly

    ,dcbcja.nonactivating_product_order_exchange_product_cost_amount AS nonactivating_product_order_exchange_product_cost_amount
    ,dcbcja.nonactivating_product_order_exchange_product_cost_amount_mtd AS nonactivating_product_order_exchange_product_cost_amount_mtd
    ,dcbcja.nonactivating_product_order_exchange_product_cost_amount_mtd_ly AS nonactivating_product_order_exchange_product_cost_amount_mtd_ly
    ,dcbcja.nonactivating_product_order_exchange_product_cost_amount_month_tot_lm AS nonactivating_product_order_exchange_product_cost_amount_month_tot_lm
    ,dcbcja.nonactivating_product_order_exchange_product_cost_amount_month_tot_ly AS nonactivating_product_order_exchange_product_cost_amount_month_tot_ly

    ,dcbcja.guest_product_order_exchange_product_cost_amount AS guest_product_order_exchange_product_cost_amount
    ,dcbcja.guest_product_order_exchange_product_cost_amount_mtd AS guest_product_order_exchange_product_cost_amount_mtd
    ,dcbcja.guest_product_order_exchange_product_cost_amount_mtd_ly AS guest_product_order_exchange_product_cost_amount_mtd_ly
    ,dcbcja.guest_product_order_exchange_product_cost_amount_month_tot_lm AS guest_product_order_exchange_product_cost_amount_month_tot_lm
    ,dcbcja.guest_product_order_exchange_product_cost_amount_month_tot_ly AS guest_product_order_exchange_product_cost_amount_month_tot_ly

    ,dcbcja.repeat_vip_product_order_exchange_product_cost_amount AS repeat_vip_product_order_exchange_product_cost_amount
    ,dcbcja.repeat_vip_product_order_exchange_product_cost_amount_mtd AS repeat_vip_product_order_exchange_product_cost_amount_mtd
    ,dcbcja.repeat_vip_product_order_exchange_product_cost_amount_mtd_ly AS repeat_vip_product_order_exchange_product_cost_amount_mtd_ly
    ,dcbcja.repeat_vip_product_order_exchange_product_cost_amount_month_tot_lm AS repeat_vip_product_order_exchange_product_cost_amount_month_tot_lm
    ,dcbcja.repeat_vip_product_order_exchange_product_cost_amount_month_tot_ly AS repeat_vip_product_order_exchange_product_cost_amount_month_tot_ly

    ,dcbcja.product_order_exchange_product_cost_amount_accounting AS product_order_exchange_product_cost_amount_accounting
    ,dcbcja.product_order_exchange_product_cost_amount_accounting_mtd AS product_order_exchange_product_cost_amount_accounting_mtd
    ,dcbcja.product_order_exchange_product_cost_amount_accounting_mtd_ly AS product_order_exchange_product_cost_amount_accounting_mtd_ly
    ,dcbcja.product_order_exchange_product_cost_amount_accounting_month_tot_lm AS product_order_exchange_product_cost_amount_accounting_month_tot_lm
    ,dcbcja.product_order_exchange_product_cost_amount_accounting_month_tot_ly AS product_order_exchange_product_cost_amount_accounting_month_tot_ly

    ,dcbcja.activating_product_order_exchange_product_cost_amount_accounting AS activating_product_order_exchange_product_cost_amount_accounting
    ,dcbcja.activating_product_order_exchange_product_cost_amount_accounting_mtd AS activating_product_order_exchange_product_cost_amount_accounting_mtd
    ,dcbcja.activating_product_order_exchange_product_cost_amount_accounting_mtd_ly AS activating_product_order_exchange_product_cost_amount_accounting_mtd_ly
    ,dcbcja.activating_product_order_exchange_product_cost_amount_accounting_month_tot_lm AS activating_product_order_exchange_product_cost_amount_accounting_month_tot_lm
    ,dcbcja.activating_product_order_exchange_product_cost_amount_accounting_month_tot_ly AS activating_product_order_exchange_product_cost_amount_accounting_month_tot_ly

    ,dcbcja.nonactivating_product_order_exchange_product_cost_amount_accounting AS nonactivating_product_order_exchange_product_cost_amount_accounting
    ,dcbcja.nonactivating_product_order_exchange_product_cost_amount_accounting_mtd AS nonactivating_product_order_exchange_product_cost_amount_accounting_mtd
    ,dcbcja.nonactivating_product_order_exchange_product_cost_amount_accounting_mtd_ly AS nonactivating_product_order_exchange_product_cost_amount_accounting_mtd_ly
    ,dcbcja.nonactivating_product_order_exchange_product_cost_amount_accounting_month_tot_lm AS nonactivating_product_order_exchange_product_cost_amount_accounting_month_tot_lm
    ,dcbcja.nonactivating_product_order_exchange_product_cost_amount_accounting_month_tot_ly AS nonactivating_product_order_exchange_product_cost_amount_accounting_month_tot_ly

    ,dcbcja.oracle_product_order_exchange_product_cost_amount AS oracle_product_order_exchange_product_cost_amount
    ,dcbcja.oracle_product_order_exchange_product_cost_amount_mtd AS oracle_product_order_exchange_product_cost_amount_mtd
    ,dcbcja.oracle_product_order_exchange_product_cost_amount_mtd_ly AS oracle_product_order_exchange_product_cost_amount_mtd_ly
    ,dcbcja.oracle_product_order_exchange_product_cost_amount_month_tot_lm AS oracle_product_order_exchange_product_cost_amount_month_tot_lm
    ,dcbcja.oracle_product_order_exchange_product_cost_amount_month_tot_ly AS oracle_product_order_exchange_product_cost_amount_month_tot_ly

    ,dcbcja.product_order_exchange_shipping_cost_amount AS product_order_exchange_shipping_cost_amount
    ,dcbcja.product_order_exchange_shipping_cost_amount_mtd AS product_order_exchange_shipping_cost_amount_mtd
    ,dcbcja.product_order_exchange_shipping_cost_amount_mtd_ly AS product_order_exchange_shipping_cost_amount_mtd_ly
    ,dcbcja.product_order_exchange_shipping_cost_amount_month_tot_lm AS product_order_exchange_shipping_cost_amount_month_tot_lm
    ,dcbcja.product_order_exchange_shipping_cost_amount_month_tot_ly AS product_order_exchange_shipping_cost_amount_month_tot_ly

    ,dcbcja.product_order_exchange_shipping_supplies_cost_amount AS product_order_exchange_shipping_supplies_cost_amount
    ,dcbcja.product_order_exchange_shipping_supplies_cost_amount_mtd AS product_order_exchange_shipping_supplies_cost_amount_mtd
    ,dcbcja.product_order_exchange_shipping_supplies_cost_amount_mtd_ly AS product_order_exchange_shipping_supplies_cost_amount_mtd_ly
    ,dcbcja.product_order_exchange_shipping_supplies_cost_amount_month_tot_lm AS product_order_exchange_shipping_supplies_cost_amount_month_tot_lm
    ,dcbcja.product_order_exchange_shipping_supplies_cost_amount_month_tot_ly AS product_order_exchange_shipping_supplies_cost_amount_month_tot_ly

    ,dcbcja.product_gross_revenue AS product_gross_revenue
    ,dcbcja.product_gross_revenue_mtd AS product_gross_revenue_mtd
    ,dcbcja.product_gross_revenue_mtd_ly AS product_gross_revenue_mtd_ly
    ,dcbcja.product_gross_revenue_month_tot_lm AS product_gross_revenue_month_tot_lm
    ,dcbcja.product_gross_revenue_month_tot_ly AS product_gross_revenue_month_tot_ly

    ,dcbcja.activating_product_gross_revenue AS activating_product_gross_revenue
    ,dcbcja.activating_product_gross_revenue_mtd AS activating_product_gross_revenue_mtd
    ,dcbcja.activating_product_gross_revenue_mtd_ly AS activating_product_gross_revenue_mtd_ly
    ,dcbcja.activating_product_gross_revenue_month_tot_lm AS activating_product_gross_revenue_month_tot_lm
    ,dcbcja.activating_product_gross_revenue_month_tot_ly AS activating_product_gross_revenue_month_tot_ly
    ,bgt.activating_gaap_gross_revenue AS activating_product_gross_revenue_budget
    ,bgt_alt.activating_gaap_gross_revenue AS activating_product_gross_revenue_budget_alt
    ,fcst.activating_gaap_gross_revenue AS activating_product_gross_revenue_forecast

    ,dcbcja.nonactivating_product_gross_revenue AS nonactivating_product_gross_revenue
    ,dcbcja.nonactivating_product_gross_revenue_mtd AS nonactivating_product_gross_revenue_mtd
    ,dcbcja.nonactivating_product_gross_revenue_mtd_ly AS nonactivating_product_gross_revenue_mtd_ly
    ,dcbcja.nonactivating_product_gross_revenue_month_tot_lm AS nonactivating_product_gross_revenue_month_tot_lm
    ,dcbcja.nonactivating_product_gross_revenue_month_tot_ly AS nonactivating_product_gross_revenue_month_tot_ly
    ,bgt.repeat_gaap_gross_revenue AS nonactivating_product_gross_revenue_budget
    ,bgt_alt.repeat_gaap_gross_revenue AS nonactivating_product_gross_revenue_budget_alt
    ,fcst.repeat_gaap_gross_revenue AS nonactivating_product_gross_revenue_forecast

    ,dcbcja.guest_product_gross_revenue AS guest_product_gross_revenue
    ,dcbcja.guest_product_gross_revenue_mtd AS guest_product_gross_revenue_mtd
    ,dcbcja.guest_product_gross_revenue_mtd_ly AS guest_product_gross_revenue_mtd_ly
    ,dcbcja.guest_product_gross_revenue_month_tot_lm AS guest_product_gross_revenue_month_tot_lm
    ,dcbcja.guest_product_gross_revenue_month_tot_ly AS guest_product_gross_revenue_month_tot_ly

    ,dcbcja.repeat_vip_product_gross_revenue AS repeat_vip_product_gross_revenue
    ,dcbcja.repeat_vip_product_gross_revenue_mtd AS repeat_vip_product_gross_revenue_mtd
    ,dcbcja.repeat_vip_product_gross_revenue_mtd_ly AS repeat_vip_product_gross_revenue_mtd_ly
    ,dcbcja.repeat_vip_product_gross_revenue_month_tot_lm AS repeat_vip_product_gross_revenue_month_tot_lm
    ,dcbcja.repeat_vip_product_gross_revenue_month_tot_ly AS repeat_vip_product_gross_revenue_month_tot_ly

    ,dcbcja.product_net_revenue AS product_net_revenue
    ,dcbcja.product_net_revenue_mtd AS product_net_revenue_mtd
    ,dcbcja.product_net_revenue_mtd_ly AS product_net_revenue_mtd_ly
    ,dcbcja.product_net_revenue_month_tot_lm AS product_net_revenue_month_tot_lm
    ,dcbcja.product_net_revenue_month_tot_ly AS product_net_revenue_month_tot_ly

    ,dcbcja.activating_product_net_revenue AS activating_product_net_revenue
    ,dcbcja.activating_product_net_revenue_mtd AS activating_product_net_revenue_mtd
    ,dcbcja.activating_product_net_revenue_mtd_ly AS activating_product_net_revenue_mtd_ly
    ,dcbcja.activating_product_net_revenue_month_tot_lm AS activating_product_net_revenue_month_tot_lm
    ,dcbcja.activating_product_net_revenue_month_tot_ly AS activating_product_net_revenue_month_tot_ly
    ,bgt.activating_gaap_net_revenue AS activating_product_net_revenue_budget
    ,bgt_alt.activating_gaap_net_revenue AS activating_product_net_revenue_budget_alt
    ,fcst.activating_gaap_net_revenue AS activating_product_net_revenue_forecast

    ,dcbcja.nonactivating_product_net_revenue AS nonactivating_product_net_revenue
    ,dcbcja.nonactivating_product_net_revenue_mtd AS nonactivating_product_net_revenue_mtd
    ,dcbcja.nonactivating_product_net_revenue_mtd_ly AS nonactivating_product_net_revenue_mtd_ly
    ,dcbcja.nonactivating_product_net_revenue_month_tot_lm AS nonactivating_product_net_revenue_month_tot_lm
    ,dcbcja.nonactivating_product_net_revenue_month_tot_ly AS nonactivating_product_net_revenue_month_tot_ly
    ,bgt.repeat_gaap_net_revenue AS nonactivating_product_net_revenue_budget
    ,bgt_alt.repeat_gaap_net_revenue AS nonactivating_product_net_revenue_budget_alt
    ,fcst.repeat_gaap_net_revenue AS nonactivating_product_net_revenue_forecast

    ,dcbcja.guest_product_net_revenue AS guest_product_net_revenue
    ,dcbcja.guest_product_net_revenue_mtd AS guest_product_net_revenue_mtd
    ,dcbcja.guest_product_net_revenue_mtd_ly AS guest_product_net_revenue_mtd_ly
    ,dcbcja.guest_product_net_revenue_month_tot_lm AS guest_product_net_revenue_month_tot_lm
    ,dcbcja.guest_product_net_revenue_month_tot_ly AS guest_product_net_revenue_month_tot_ly

    ,dcbcja.repeat_vip_product_net_revenue AS repeat_vip_product_net_revenue
    ,dcbcja.repeat_vip_product_net_revenue_mtd AS repeat_vip_product_net_revenue_mtd
    ,dcbcja.repeat_vip_product_net_revenue_mtd_ly AS repeat_vip_product_net_revenue_mtd_ly
    ,dcbcja.repeat_vip_product_net_revenue_month_tot_lm AS repeat_vip_product_net_revenue_month_tot_lm
    ,dcbcja.repeat_vip_product_net_revenue_month_tot_ly AS repeat_vip_product_net_revenue_month_tot_ly

    ,dcbcja.product_margin_pre_return AS product_margin_pre_return
    ,dcbcja.product_margin_pre_return_mtd AS product_margin_pre_return_mtd
    ,dcbcja.product_margin_pre_return_mtd_ly AS product_margin_pre_return_mtd_ly
    ,dcbcja.product_margin_pre_return_month_tot_lm AS product_margin_pre_return_month_tot_lm
    ,dcbcja.product_margin_pre_return_month_tot_ly AS product_margin_pre_return_month_tot_ly

    ,dcbcja.activating_product_margin_pre_return AS activating_product_margin_pre_return
    ,dcbcja.activating_product_margin_pre_return_mtd AS activating_product_margin_pre_return_mtd
    ,dcbcja.activating_product_margin_pre_return_mtd_ly AS activating_product_margin_pre_return_mtd_ly
    ,dcbcja.activating_product_margin_pre_return_month_tot_lm AS activating_product_margin_pre_return_month_tot_lm
    ,dcbcja.activating_product_margin_pre_return_month_tot_ly AS activating_product_margin_pre_return_month_tot_ly
    ,bgt.acquisition_margin_$ AS activating_product_margin_pre_return_budget
    ,bgt_alt.acquisition_margin_$ AS activating_product_margin_pre_return_budget_alt
    ,fcst.acquisition_margin_$ AS activating_product_margin_pre_return_forecast

    ,dcbcja.first_guest_product_margin_pre_return AS first_guest_product_margin_pre_return
    ,dcbcja.first_guest_product_margin_pre_return_mtd AS first_guest_product_margin_pre_return_mtd
    ,dcbcja.first_guest_product_margin_pre_return_mtd_ly AS first_guest_product_margin_pre_return_mtd_ly
    ,dcbcja.first_guest_product_margin_pre_return_month_tot_lm AS first_guest_product_margin_pre_return_month_tot_lm
    ,dcbcja.first_guest_product_margin_pre_return_month_tot_ly AS first_guest_product_margin_pre_return_month_tot_ly

    ,dcbcja.product_gross_profit AS product_gross_profit
    ,dcbcja.product_gross_profit_mtd AS product_gross_profit_mtd
    ,dcbcja.product_gross_profit_mtd_ly AS product_gross_profit_mtd_ly
    ,dcbcja.product_gross_profit_month_tot_lm AS product_gross_profit_month_tot_lm
    ,dcbcja.product_gross_profit_month_tot_ly AS product_gross_profit_month_tot_ly

    ,dcbcja.activating_product_gross_profit AS activating_product_gross_profit
    ,dcbcja.activating_product_gross_profit_mtd AS activating_product_gross_profit_mtd
    ,dcbcja.activating_product_gross_profit_mtd_ly AS activating_product_gross_profit_mtd_ly
    ,dcbcja.activating_product_gross_profit_month_tot_lm AS activating_product_gross_profit_month_tot_lm
    ,dcbcja.activating_product_gross_profit_month_tot_ly AS activating_product_gross_profit_month_tot_ly
    ,bgt.activating_gross_margin_$ AS activating_product_gross_profit_budget
    ,bgt_alt.activating_gross_margin_$ AS activating_product_gross_profit_budget_alt
    ,fcst.activating_gross_margin_$ AS activating_product_gross_profit_forecast

    ,dcbcja.nonactivating_product_gross_profit AS nonactivating_product_gross_profit
    ,dcbcja.nonactivating_product_gross_profit_mtd AS nonactivating_product_gross_profit_mtd
    ,dcbcja.nonactivating_product_gross_profit_mtd_ly AS nonactivating_product_gross_profit_mtd_ly
    ,dcbcja.nonactivating_product_gross_profit_month_tot_lm AS nonactivating_product_gross_profit_month_tot_lm
    ,dcbcja.nonactivating_product_gross_profit_month_tot_ly AS nonactivating_product_gross_profit_month_tot_ly
    ,bgt.repeat_gaap_gross_margin_$ AS nonactivating_product_gross_profit_budget
    ,bgt_alt.repeat_gaap_gross_margin_$ AS nonactivating_product_gross_profit_budget_alt
    ,fcst.repeat_gaap_gross_margin_$ AS nonactivating_product_gross_profit_forecast

    ,dcbcja.guest_product_gross_profit AS guest_product_gross_profit
    ,dcbcja.guest_product_gross_profit_mtd AS guest_product_gross_profit_mtd
    ,dcbcja.guest_product_gross_profit_mtd_ly AS guest_product_gross_profit_mtd_ly
    ,dcbcja.guest_product_gross_profit_month_tot_lm AS guest_product_gross_profit_month_tot_lm
    ,dcbcja.guest_product_gross_profit_month_tot_ly AS guest_product_gross_profit_month_tot_ly

    ,dcbcja.repeat_vip_product_gross_profit AS repeat_vip_product_gross_profit
    ,dcbcja.repeat_vip_product_gross_profit_mtd AS repeat_vip_product_gross_profit_mtd
    ,dcbcja.repeat_vip_product_gross_profit_mtd_ly AS repeat_vip_product_gross_profit_mtd_ly
    ,dcbcja.repeat_vip_product_gross_profit_month_tot_lm AS repeat_vip_product_gross_profit_month_tot_lm
    ,dcbcja.repeat_vip_product_gross_profit_month_tot_ly AS repeat_vip_product_gross_profit_month_tot_ly

    ,dcbcja.product_order_cash_gross_revenue_amount AS product_order_cash_gross_revenue
    ,dcbcja.product_order_cash_gross_revenue_amount_mtd AS product_order_cash_gross_revenue_mtd
    ,dcbcja.product_order_cash_gross_revenue_amount_mtd_ly AS product_order_cash_gross_revenue_mtd_ly
    ,dcbcja.product_order_cash_gross_revenue_amount_month_tot_lm AS product_order_cash_gross_revenue_month_tot_lm
    ,dcbcja.product_order_cash_gross_revenue_amount_month_tot_ly AS product_order_cash_gross_revenue_month_tot_ly

    ,dcbcja.activating_product_order_cash_gross_revenue_amount AS activating_product_order_cash_gross_revenue
    ,dcbcja.activating_product_order_cash_gross_revenue_amount_mtd AS activating_product_order_cash_gross_revenue_mtd
    ,dcbcja.activating_product_order_cash_gross_revenue_amount_mtd_ly AS activating_product_order_cash_gross_revenue_mtd_ly
    ,dcbcja.activating_product_order_cash_gross_revenue_amount_month_tot_lm AS activating_product_order_cash_gross_revenue_month_tot_lm
    ,dcbcja.activating_product_order_cash_gross_revenue_amount_month_tot_ly AS activating_product_order_cash_gross_revenue_month_tot_ly

    ,dcbcja.nonactivating_product_order_cash_gross_revenue_amount AS nonactivating_product_order_cash_gross_revenue
    ,dcbcja.nonactivating_product_order_cash_gross_revenue_amount_mtd AS nonactivating_product_order_cash_gross_revenue_mtd
    ,dcbcja.nonactivating_product_order_cash_gross_revenue_amount_mtd_ly AS nonactivating_product_order_cash_gross_revenue_mtd_ly
    ,dcbcja.nonactivating_product_order_cash_gross_revenue_amount_month_tot_lm AS nonactivating_product_order_cash_gross_revenue_month_tot_lm
    ,dcbcja.nonactivating_product_order_cash_gross_revenue_amount_month_tot_ly AS nonactivating_product_order_cash_gross_revenue_month_tot_ly
    ,bgt.repeat_shipped_order_cash_collected AS nonactivating_product_order_cash_gross_revenue_budget
    ,bgt_alt.repeat_shipped_order_cash_collected AS nonactivating_product_order_cash_gross_revenue_budget_alt
    ,fcst.repeat_shipped_order_cash_collected AS nonactivating_product_order_cash_gross_revenue_forecast

    ,dcbcja.cash_gross_revenue AS cash_gross_revenue
    ,dcbcja.cash_gross_revenue_mtd AS cash_gross_revenue_mtd
    ,dcbcja.cash_gross_revenue_mtd_ly AS cash_gross_revenue_mtd_ly
    ,dcbcja.cash_gross_revenue_month_tot_lm AS cash_gross_revenue_month_tot_lm
    ,dcbcja.cash_gross_revenue_month_tot_ly AS cash_gross_revenue_month_tot_ly
    ,bgt.gross_cash_revenue AS cash_gross_revenue_budget
    ,bgt_alt.gross_cash_revenue AS cash_gross_revenue_budget_alt
    ,fcst.gross_cash_revenue AS cash_gross_revenue_forecast

    ,dcbcja.activating_cash_gross_revenue AS activating_cash_gross_revenue
    ,dcbcja.activating_cash_gross_revenue_mtd AS activating_cash_gross_revenue_mtd
    ,dcbcja.activating_cash_gross_revenue_mtd_ly AS activating_cash_gross_revenue_mtd_ly
    ,dcbcja.activating_cash_gross_revenue_month_tot_lm AS activating_cash_gross_revenue_month_tot_lm
    ,dcbcja.activating_cash_gross_revenue_month_tot_ly AS activating_cash_gross_revenue_month_tot_ly

    ,dcbcja.nonactivating_cash_gross_revenue AS nonactivating_cash_gross_revenue
    ,dcbcja.nonactivating_cash_gross_revenue_mtd AS nonactivating_cash_gross_revenue_mtd
    ,dcbcja.nonactivating_cash_gross_revenue_mtd_ly AS nonactivating_cash_gross_revenue_mtd_ly
    ,dcbcja.nonactivating_cash_gross_revenue_month_tot_lm AS nonactivating_cash_gross_revenue_month_tot_lm
    ,dcbcja.nonactivating_cash_gross_revenue_month_tot_ly AS nonactivating_cash_gross_revenue_month_tot_ly

    ,dcbcja.cash_net_revenue AS cash_net_revenue
    ,dcbcja.cash_net_revenue_mtd AS cash_net_revenue_mtd
    ,dcbcja.cash_net_revenue_mtd_ly AS cash_net_revenue_mtd_ly
    ,dcbcja.cash_net_revenue_month_tot_lm AS cash_net_revenue_month_tot_lm
    ,dcbcja.cash_net_revenue_month_tot_ly AS cash_net_revenue_month_tot_ly
    ,bgt.net_cash_revenue_total AS cash_net_revenue_budget
    ,bgt_alt.net_cash_revenue_total AS cash_net_revenue_budget_alt
    ,fcst.net_cash_revenue_total AS cash_net_revenue_forecast

    ,dcbcja.activating_cash_net_revenue AS activating_cash_net_revenue
    ,dcbcja.activating_cash_net_revenue_mtd AS activating_cash_net_revenue_mtd
    ,dcbcja.activating_cash_net_revenue_mtd_ly AS activating_cash_net_revenue_mtd_ly
    ,dcbcja.activating_cash_net_revenue_month_tot_lm AS activating_cash_net_revenue_month_tot_lm
    ,dcbcja.activating_cash_net_revenue_month_tot_ly AS activating_cash_net_revenue_month_tot_ly
    ,bgt.activating_gaap_gross_revenue AS activating_cash_net_revenue_budget
    ,bgt_alt.activating_gaap_gross_revenue AS activating_cash_net_revenue_budget_alt
    ,fcst.activating_gaap_gross_revenue AS activating_cash_net_revenue_forecast

    ,dcbcja.nonactivating_cash_net_revenue AS nonactivating_cash_net_revenue
    ,dcbcja.nonactivating_cash_net_revenue_mtd AS nonactivating_cash_net_revenue_mtd
    ,dcbcja.nonactivating_cash_net_revenue_mtd_ly AS nonactivating_cash_net_revenue_mtd_ly
    ,dcbcja.nonactivating_cash_net_revenue_month_tot_lm AS nonactivating_cash_net_revenue_month_tot_lm
    ,dcbcja.nonactivating_cash_net_revenue_month_tot_ly AS nonactivating_cash_net_revenue_month_tot_ly
    ,bgt.repeat_net_cash_revenue AS nonactivating_cash_net_revenue_budget
    ,bgt_alt.repeat_net_cash_revenue AS nonactivating_cash_net_revenue_budget_alt
    ,fcst.repeat_net_cash_revenue AS nonactivating_cash_net_revenue_forecast

    ,dcbcja.cash_net_revenue_budgeted_fx AS cash_net_revenue_budgeted_fx
    ,dcbcja.cash_net_revenue_budgeted_fx_mtd AS cash_net_revenue_budgeted_fx_mtd
    ,dcbcja.cash_net_revenue_budgeted_fx_mtd_ly AS cash_net_revenue_budgeted_fx_mtd_ly
    ,dcbcja.cash_net_revenue_budgeted_fx_month_tot_lm AS cash_net_revenue_budgeted_fx_month_tot_lm
    ,dcbcja.cash_net_revenue_budgeted_fx_month_tot_ly AS cash_net_revenue_budgeted_fx_month_tot_ly

    ,dcbcja.cash_gross_profit AS cash_gross_profit
    ,dcbcja.cash_gross_profit_mtd AS cash_gross_profit_mtd
    ,dcbcja.cash_gross_profit_mtd_ly AS cash_gross_profit_mtd_ly
    ,dcbcja.cash_gross_profit_month_tot_lm AS cash_gross_profit_month_tot_lm
    ,dcbcja.cash_gross_profit_month_tot_ly AS cash_gross_profit_month_tot_ly
    ,bgt.cash_gross_margin AS cash_gross_profit_budget
    ,bgt_alt.cash_gross_margin AS cash_gross_profit_budget_alt
    ,fcst.cash_gross_margin AS cash_gross_profit_forecast

    ,dcbcja.nonactivating_cash_gross_profit AS nonactivating_cash_gross_profit
    ,dcbcja.nonactivating_cash_gross_profit_mtd AS nonactivating_cash_gross_profit_mtd
    ,dcbcja.nonactivating_cash_gross_profit_mtd_ly AS nonactivating_cash_gross_profit_mtd_ly
    ,dcbcja.nonactivating_cash_gross_profit_month_tot_lm AS nonactivating_cash_gross_profit_month_tot_lm
    ,dcbcja.nonactivating_cash_gross_profit_month_tot_ly AS nonactivating_cash_gross_profit_month_tot_ly
    ,bgt.repeat_cash_gross_margin AS nonactivating_cash_gross_profit_budget
    ,bgt_alt.repeat_cash_gross_margin AS nonactivating_cash_gross_profit_budget_alt
    ,fcst.repeat_cash_gross_margin AS nonactivating_cash_gross_profit_forecast

    ,dcbcja.billed_credit_cash_transaction_count AS billed_credit_cash_transaction_count
    ,dcbcja.billed_credit_cash_transaction_count_mtd AS billed_credit_cash_transaction_count_mtd
    ,dcbcja.billed_credit_cash_transaction_count_mtd_ly AS billed_credit_cash_transaction_count_mtd_ly
    ,dcbcja.billed_credit_cash_transaction_count_month_tot_lm AS billed_credit_cash_transaction_count_month_tot_lm
    ,dcbcja.billed_credit_cash_transaction_count_month_tot_ly AS billed_credit_cash_transaction_count_month_tot_ly

    ,dcbcja.on_retry_billed_credit_cash_transaction_count AS billed_credits_successful_on_retry
    ,dcbcja.on_retry_billed_credit_cash_transaction_count_mtd AS billed_credits_successful_on_retry_mtd
    ,dcbcja.on_retry_billed_credit_cash_transaction_count_mtd_ly AS billed_credits_successful_on_retry_mtd_ly
    ,dcbcja.on_retry_billed_credit_cash_transaction_count_month_tot_lm AS billed_credits_successful_on_retry_month_tot_lm
    ,dcbcja.on_retry_billed_credit_cash_transaction_count_month_tot_ly AS billed_credits_successful_on_retry_month_tot_ly

    ,dcbcja.billing_cash_refund_amount AS billing_cash_refund_amount
    ,dcbcja.billing_cash_refund_amount_mtd AS billing_cash_refund_amount_mtd
    ,dcbcja.billing_cash_refund_amount_mtd_ly AS billing_cash_refund_amount_mtd_ly
    ,dcbcja.billing_cash_refund_amount_month_tot_lm AS billing_cash_refund_amount_month_tot_lm
    ,dcbcja.billing_cash_refund_amount_month_tot_ly AS billing_cash_refund_amount_month_tot_ly

    ,dcbcja.billing_cash_chargeback_amount AS billing_cash_chargeback_amount
    ,dcbcja.billing_cash_chargeback_amount_mtd AS billing_cash_chargeback_amount_mtd
    ,dcbcja.billing_cash_chargeback_amount_mtd_ly AS billing_cash_chargeback_amount_mtd_ly
    ,dcbcja.billing_cash_chargeback_amount_month_tot_lm AS billing_cash_chargeback_amount_month_tot_lm
    ,dcbcja.billing_cash_chargeback_amount_month_tot_ly AS billing_cash_chargeback_amount_month_tot_ly

    ,dcbcja.billed_credit_cash_transaction_amount AS billed_credit_cash_transaction_amount
    ,dcbcja.billed_credit_cash_transaction_amount_mtd AS billed_credit_cash_transaction_amount_mtd
    ,dcbcja.billed_credit_cash_transaction_amount_mtd_ly AS billed_credit_cash_transaction_amount_mtd_ly
    ,dcbcja.billed_credit_cash_transaction_amount_month_tot_lm AS billed_credit_cash_transaction_amount_month_tot_lm
    ,dcbcja.billed_credit_cash_transaction_amount_month_tot_ly AS billed_credit_cash_transaction_amount_month_tot_ly
    ,bgt.membership_credits_charged AS billed_credit_cash_transaction_amount_budget
    ,bgt_alt.membership_credits_charged AS billed_credit_cash_transaction_amount_budget_alt
    ,fcst.membership_credits_charged AS billed_credit_cash_transaction_amount_forecast

    ,dcbcja.billed_credit_cash_refund_chargeback_amount AS billed_credit_cash_refund_chargeback_amount
    ,dcbcja.billed_credit_cash_refund_chargeback_amount_mtd AS billed_credit_cash_refund_chargeback_amount_mtd
    ,dcbcja.billed_credit_cash_refund_chargeback_amount_mtd_ly AS billed_credit_cash_refund_chargeback_amount_mtd_ly
    ,dcbcja.billed_credit_cash_refund_chargeback_amount_month_tot_lm AS billed_credit_cash_refund_chargeback_amount_month_tot_lm
    ,dcbcja.billed_credit_cash_refund_chargeback_amount_month_tot_ly AS billed_credit_cash_refund_chargeback_amount_month_tot_ly
    ,bgt.membership_credits_refunded_plus_chargebacks AS billed_credit_cash_refund_chargeback_amount_budget
    ,bgt_alt.membership_credits_refunded_plus_chargebacks AS billed_credit_cash_refund_chargeback_amount_budget_alt
    ,fcst.membership_credits_refunded_plus_chargebacks AS billed_credit_cash_refund_chargeback_amount_forecast

    ,dcbcja.activating_billed_cash_credit_redeemed_amount AS activating_billed_cash_credit_redeemed_amount

    ,dcbcja.billed_cash_credit_redeemed_amount AS billed_cash_credit_redeemed_amount
    ,dcbcja.billed_cash_credit_redeemed_amount_mtd AS billed_cash_credit_redeemed_amount_mtd
    ,dcbcja.billed_cash_credit_redeemed_amount_mtd_ly AS billed_cash_credit_redeemed_amount_mtd_ly
    ,dcbcja.billed_cash_credit_redeemed_amount_month_tot_lm AS billed_cash_credit_redeemed_amount_month_tot_lm
    ,dcbcja.billed_cash_credit_redeemed_amount_month_tot_ly AS billed_cash_credit_redeemed_amount_month_tot_ly
    ,bgt.membership_credits_redeemed AS billed_cash_credit_redeemed_amount_budget
    ,bgt_alt.membership_credits_redeemed AS billed_cash_credit_redeemed_amount_budget_alt
    ,fcst.membership_credits_redeemed AS billed_cash_credit_redeemed_amount_forecast

    ,dcbcja.product_order_misc_cogs_amount AS misc_cogs_amount
    ,dcbcja.product_order_misc_cogs_amount_mtd AS misc_cogs_amount_mtd
    ,dcbcja.product_order_misc_cogs_amount_mtd_ly AS misc_cogs_amount_mtd_ly
    ,dcbcja.product_order_misc_cogs_amount_month_tot_lm AS misc_cogs_amount_month_tot_lm
    ,dcbcja.product_order_misc_cogs_amount_month_tot_ly AS misc_cogs_amount_month_tot_ly
    ,bgt.product_cost_markdown AS misc_cogs_amount_budget
    ,bgt_alt.product_cost_markdown AS misc_cogs_amount_budget_alt
    ,fcst.product_cost_markdown AS misc_cogs_amount_forecast

    ,dcbcja.product_order_cash_gross_profit AS product_order_cash_gross_profit
    ,dcbcja.product_order_cash_gross_profit_mtd AS product_order_cash_gross_profit_mtd
    ,dcbcja.product_order_cash_gross_profit_mtd_ly AS product_order_cash_gross_profit_mtd_ly
    ,dcbcja.product_order_cash_gross_profit_month_tot_lm AS product_order_cash_gross_profit_month_tot_lm
    ,dcbcja.product_order_cash_gross_profit_month_tot_ly AS product_order_cash_gross_profit_month_tot_ly

    ,dcbcja.nonactivating_product_order_cash_gross_profit AS nonactivating_product_order_cash_gross_profit
    ,dcbcja.nonactivating_product_order_cash_gross_profit_mtd AS nonactivating_product_order_cash_gross_profit_mtd
    ,dcbcja.nonactivating_product_order_cash_gross_profit_mtd_ly AS nonactivating_product_order_cash_gross_profit_mtd_ly
    ,dcbcja.nonactivating_product_order_cash_gross_profit_month_tot_lm AS nonactivating_product_order_cash_gross_profit_month_tot_lm
    ,dcbcja.nonactivating_product_order_cash_gross_profit_month_tot_ly AS nonactivating_product_order_cash_gross_profit_month_tot_ly
    ,NULL AS nonactivating_product_order_cash_gross_profit_budget
    ,NULL AS nonactivating_product_order_cash_gross_profit_budget_alt
    ,NULL AS nonactivating_product_order_cash_gross_profit_forecast

    ,dcbcja.billed_cash_credit_redeemed_same_month_amount AS same_month_billed_credit_redeemed
    ,dcbcja.billed_cash_credit_redeemed_same_month_amount_mtd AS same_month_billed_credit_redeemed_mtd
    ,dcbcja.billed_cash_credit_redeemed_same_month_amount_mtd_ly AS same_month_billed_credit_redeemed_mtd_ly
    ,dcbcja.billed_cash_credit_redeemed_same_month_amount_month_tot_lm AS same_month_billed_credit_redeemed_month_tot_lm
    ,dcbcja.billed_cash_credit_redeemed_same_month_amount_month_tot_ly AS same_month_billed_credit_redeemed_month_tot_ly

    ,dcbcja.billed_credit_cash_refund_count AS billed_credit_cash_refund_count
    ,dcbcja.billed_credit_cash_refund_count_mtd AS billed_credit_cash_refund_count_mtd
    ,dcbcja.billed_credit_cash_refund_count_mtd_ly AS billed_credit_cash_refund_count_mtd_ly
    ,dcbcja.billed_credit_cash_refund_count_month_tot_lm AS billed_credit_cash_refund_count_month_tot_lm
    ,dcbcja.billed_credit_cash_refund_count_month_tot_ly AS billed_credit_cash_refund_count_month_tot_ly

    ,dcbcja.cash_variable_contribution_profit AS cash_variable_contribution_profit
    ,dcbcja.cash_variable_contribution_profit_mtd AS cash_variable_contribution_profit_mtd
    ,dcbcja.cash_variable_contribution_profit_mtd_ly AS cash_variable_contribution_profit_mtd_ly
    ,dcbcja.cash_variable_contribution_profit_month_tot_lm AS cash_variable_contribution_profit_month_tot_lm
    ,dcbcja.cash_variable_contribution_profit_month_tot_ly AS cash_variable_contribution_profit_month_tot_ly

    ,bgt.cash_contribution_after_media AS cash_contribution_after_media_budget
    ,bgt_alt.cash_contribution_after_media AS cash_contribution_after_media_budget_alt
    ,fcst.cash_contribution_after_media AS cash_contribution_after_media_forecast

    ,dcbcja.billed_cash_credit_redeemed_equivalent_count AS billed_cash_credit_redeemed_equivalent_count
    ,dcbcja.billed_cash_credit_redeemed_equivalent_count_mtd AS billed_cash_credit_redeemed_equivalent_count_mtd
    ,dcbcja.billed_cash_credit_redeemed_equivalent_count_mtd_ly AS billed_cash_credit_redeemed_equivalent_count_mtd_ly
    ,dcbcja.billed_cash_credit_redeemed_equivalent_count_month_tot_lm AS billed_cash_credit_redeemed_equivalent_count_month_tot_lm
    ,dcbcja.billed_cash_credit_redeemed_equivalent_count_month_tot_ly AS billed_cash_credit_redeemed_equivalent_count_month_tot_ly

    ,bgt.membership_credits_redeemed_count AS membership_credits_redeemed_count_budget
    ,bgt_alt.membership_credits_redeemed_count AS membership_credits_redeemed_count_budget_alt
    ,fcst.membership_credits_redeemed_count AS membership_credits_redeemed_count_forecast

    ,dcbcja.billed_cash_credit_cancelled_equivalent_count AS billed_cash_credit_cancelled_equivalent_count
    ,dcbcja.billed_cash_credit_cancelled_equivalent_count_mtd AS billed_cash_credit_cancelled_equivalent_count_mtd
    ,dcbcja.billed_cash_credit_cancelled_equivalent_count_mtd_ly AS billed_cash_credit_cancelled_equivalent_count_mtd_ly
    ,dcbcja.billed_cash_credit_cancelled_equivalent_count_month_tot_lm AS billed_cash_credit_cancelled_equivalent_count_month_tot_lm
    ,dcbcja.billed_cash_credit_cancelled_equivalent_count_month_tot_ly AS billed_cash_credit_cancelled_equivalent_count_month_tot_ly

    ,bgt.membership_credits_cancelled_count AS membership_credits_cancelled_count_budget
    ,bgt_alt.membership_credits_cancelled_count AS membership_credits_cancelled_count_budget_alt
    ,fcst.membership_credits_cancelled_count AS membership_credits_cancelled_count_forecast

    ,bgt.cash_gross_margin_percent AS cash_gross_profit_percent_of_net_cash_budget
    ,bgt.cash_gross_margin_percent AS cash_gross_profit_percent_of_net_cash_budget_alt
    ,fcst.cash_gross_margin_percent AS cash_gross_profit_percent_of_net_cash_forecast

    ,bgt.cash_refunds * -1 AS product_order_and_billing_cash_refund_budget
    ,bgt_alt.cash_refunds * -1 AS product_order_and_billing_cash_refund_budget_alt
    ,fcst.cash_refunds * -1 AS product_order_and_billing_cash_refund_forecast

    ,bgt.chargebacks * -1 AS product_order_and_billing_chargebacks_budget
    ,bgt_alt.chargebacks * -1 AS product_order_and_billing_chargebacks_budget_alt
    ,fcst.chargebacks * -1 AS product_order_and_billing_chargebacks_forecast

    ,bgt.refunds_and_chargebacks_as_percent_of_gross_cash AS refunds_plus_chargebacks_as_percent_of_cash_gross_rev_budget
    ,bgt_alt.refunds_and_chargebacks_as_percent_of_gross_cash AS refunds_plus_chargebacks_as_percent_of_cash_gross_rev_budget_alt
    ,fcst.refunds_and_chargebacks_as_percent_of_gross_cash AS refunds_plus_chargebacks_as_percent_of_cash_gross_rev_forecast

    ,bgt.activating_gaap_gross_revenue/NULLIFZERO(bgt.activating_order_count) AS activating_aov_budget
    ,bgt_alt.activating_aov_incl_shipping_rev AS activating_aov_budget_alt
    ,fcst.activating_gaap_gross_revenue/NULLIFZERO(fcst.activating_order_count) AS activating_aov_forecast

    ,bgt.activating_units_per_transaction AS activating_upt_budget
    ,bgt.activating_units_per_transaction AS activating_upt_budget_alt
    ,fcst.activating_units_per_transaction AS activating_upt_forecast

    ,bgt.activating_discount_percent AS activating_discount_percent_budget
    ,bgt_alt.activating_discount_percent AS activating_discount_percent_budget_alt
    ,fcst.activating_discount_percent AS activating_discount_percent_forecast

    ,bgt.activating_gross_margin_percent AS activating_product_gross_profit_percent_budget
    ,bgt_alt.activating_gross_margin_percent AS activating_product_gross_profit_percent_budget_alt
    ,fcst.activating_gross_margin_percent AS activating_product_gross_profit_percent_forecast

    ,bgt.acquisition_margin_$__div__order AS activating_product_margin_pre_return_per_order_budget
    ,bgt_alt.acquisition_margin_$__div__order AS activating_product_margin_pre_return_per_order_budget_alt
    ,fcst.acquisition_margin_$__div__order AS activating_product_margin_pre_return_per_order_forecast

    ,bgt.acquisition_margin_percent AS activating_product_margin_pre_return_percent_budget
    ,bgt_alt.acquisition_margin_percent AS activating_product_margin_pre_return_percent_budget_alt
    ,fcst.acquisition_margin_percent AS activating_product_margin_pre_return_percent_forecast

    ,bgt.repeat_gaap_gross_revenue/NULLIFZERO(bgt.repeat_order_count) AS nonactivating_aov_budget
    ,bgt_alt.repeat_aov_incl_shipping_rev AS nonactivating_aov_budget_alt
    ,fcst.repeat_gaap_gross_revenue/NULLIFZERO(fcst.repeat_order_count) AS nonactivating_aov_forecast

    ,bgt.repeat_units_per_transaction AS nonactivating_upt_budget
    ,bgt.repeat_units_per_transaction AS nonactivating_upt_budget_alt
    ,fcst.repeat_units_per_transaction AS nonactivating_upt_forecast

    ,bgt.repeat_discount_percent AS nonactivating_discount_percent_budget
    ,bgt.repeat_discount_percent AS nonactivating_discount_percent_budget_alt
    ,fcst.repeat_discount_percent AS nonactivating_discount_percent_forecast

    ,bgt.repeat_gaap_gross_margin_percent AS nonactivating_product_gross_profit_percent_budget
    ,bgt_alt.repeat_gaap_gross_margin_percent AS nonactivating_product_gross_profit_percent_budget_alt
    ,fcst.repeat_gaap_gross_margin_percent AS nonactivating_product_gross_profit_percent_forecast

    ,bgt.freight_out_cost AS shipping_cost_incl_reship_exch_budget
    ,bgt_alt.freight_out_cost AS shipping_cost_incl_reship_exch_budget_alt
    ,fcst.freight_out_cost AS shipping_cost_incl_reship_exch_forecast

    ,bgt.outbound_shipping_supplies AS shipping_supplies_cost_incl_reship_exch_budget
    ,bgt_alt.outbound_shipping_supplies AS shipping_supplies_cost_incl_reship_exch_budget_alt
    ,fcst.outbound_shipping_supplies AS shipping_supplies_cost_incl_reship_exch_forecast

    ,bgt.product_cost_calc AS product_cost_calc_budget
    ,bgt_alt.product_cost_calc AS product_cost_calc_budget_alt
    ,fcst.product_cost_calc AS product_cost_calc_forecast

    ,dcbcja.product_order_tariff_amount AS product_order_tariff_amount

    ,dcbcja.product_order_shipping_discount_amount AS product_order_shipping_discount_amount

    ,dcbcja.product_order_shipping_revenue_before_discount_amount AS product_order_shipping_revenue_before_discount_amount

    ,dcbcja.product_order_tax_amount AS product_order_tax_amount

    ,dcbcja.product_order_cash_transaction_amount AS product_order_cash_transaction_amount

    ,dcbcja.product_order_cash_credit_redeemed_amount AS product_order_cash_credit_redeemed_amount

    ,dcbcja.product_order_loyalty_unit_count AS product_order_loyalty_unit_count

    ,dcbcja.product_order_outfit_component_unit_count AS product_order_outfit_component_unit_count

    ,dcbcja.product_order_outfit_count AS product_order_outfit_count

    ,dcbcja.product_order_discounted_unit_count AS product_order_discounted_unit_count

    ,dcbcja.product_order_zero_revenue_unit_count AS product_order_zero_revenue_unit_count

    ,dcbcja.product_order_cash_credit_refund_amount AS product_order_cash_credit_refund_amount
    ,product_order_cash_credit_refund_amount_mtd AS product_order_cash_credit_refund_amount_mtd
    ,product_order_cash_credit_refund_amount_mtd_ly AS product_order_cash_credit_refund_amount_mtd_ly
    ,product_order_cash_credit_refund_amount_month_tot_lm AS product_order_cash_credit_refund_amount_month_tot_lm
    ,product_order_cash_credit_refund_amount_month_tot_ly AS product_order_cash_credit_refund_amount_month_tot_ly
    ,NULL AS product_order_cash_credit_refund_amount_budget
    ,NULL AS product_order_cash_credit_refund_amount_budget_alt
    ,NULL AS product_order_cash_credit_refund_amount_forecast

    ,dcbcja.activating_product_order_cash_credit_refund_amount AS activating_product_order_cash_credit_refund_amount
    ,activating_product_order_cash_credit_refund_amount_mtd AS activating_product_order_cash_credit_refund_amount_mtd
    ,activating_product_order_cash_credit_refund_amount_mtd_ly AS activating_product_order_cash_credit_refund_amount_mtd_ly
    ,activating_product_order_cash_credit_refund_amount_month_tot_lm AS activating_product_order_cash_credit_refund_amount_month_tot_lm
    ,activating_product_order_cash_credit_refund_amount_month_tot_ly AS activating_product_order_cash_credit_refund_amount_month_tot_ly
    ,NULL AS activating_product_order_cash_credit_refund_amount_budget
    ,NULL AS activating_product_order_cash_credit_refund_amount_budget_alt
    ,NULL AS activating_product_order_cash_credit_refund_amount_forecast

    ,dcbcja.nonactivating_product_order_cash_credit_refund_amount AS nonactivating_product_order_cash_credit_refund_amount
    ,nonactivating_product_order_cash_credit_refund_amount_mtd AS nonactivating_product_order_cash_credit_refund_amount_mtd
    ,nonactivating_product_order_cash_credit_refund_amount_mtd_ly AS nonactivating_product_order_cash_credit_refund_amount_mtd_ly
    ,nonactivating_product_order_cash_credit_refund_amount_month_tot_lm AS nonactivating_product_order_cash_credit_refund_amount_month_tot_lm
    ,nonactivating_product_order_cash_credit_refund_amount_month_tot_ly AS nonactivating_product_order_cash_credit_refund_amount_month_tot_ly
    ,NULL AS nonactivating_product_order_cash_credit_refund_amount_budget
    ,NULL AS nonactivating_product_order_cash_credit_refund_amount_budget_alt
    ,NULL AS nonactivating_product_order_cash_credit_refund_amount_forecast

    ,dcbcja.product_order_noncash_credit_refund_amount AS product_order_noncash_credit_refund_amount
    ,product_order_noncash_credit_refund_amount_mtd AS product_order_noncash_credit_refund_amount_mtd
    ,product_order_noncash_credit_refund_amount_mtd_ly AS product_order_noncash_credit_refund_amount_mtd_ly
    ,product_order_noncash_credit_refund_amount_month_tot_lm AS product_order_noncash_credit_refund_amount_month_tot_lm
    ,product_order_noncash_credit_refund_amount_month_tot_ly AS product_order_noncash_credit_refund_amount_month_tot_ly
    ,NULL AS product_order_noncash_credit_refund_amount_budget
    ,NULL AS product_order_noncash_credit_refund_amount_budget_alt
    ,NULL AS product_order_noncash_credit_refund_amount_forecast

    ,dcbcja.product_order_exchange_direct_cogs_amount AS product_order_exchange_direct_cogs_amount

    ,dcbcja.product_order_selling_expenses_amount AS product_order_selling_expenses_amount

    ,dcbcja.product_order_payment_processing_cost_amount AS product_order_payment_processing_cost_amount

    ,dcbcja.product_order_variable_gms_cost_amount AS product_order_variable_gms_cost_amount

    ,dcbcja.product_order_variable_warehouse_cost_amount AS product_order_variable_warehouse_cost_amount

    ,dcbcja.billing_selling_expenses_amount AS billing_selling_expenses_amount

    ,dcbcja.billing_payment_processing_cost_amount AS billing_payment_processing_cost_amount

    ,dcbcja.billing_variable_gms_cost_amount AS billing_variable_gms_cost_amount

    ,dcbcja.product_order_amount_to_pay AS product_order_amount_to_pay

    ,dcbcja.product_gross_revenue_excl_shipping AS product_gross_revenue_excl_shipping
    ,dcbcja.product_gross_revenue_excl_shipping_mtd AS product_gross_revenue_excl_shipping_mtd
    ,dcbcja.product_gross_revenue_excl_shipping_mtd_ly AS product_gross_revenue_excl_shipping_mtd_ly
    ,dcbcja.product_gross_revenue_excl_shipping_month_tot_lm AS product_gross_revenue_excl_shipping_month_tot_lm
    ,dcbcja.product_gross_revenue_excl_shipping_month_tot_ly AS product_gross_revenue_excl_shipping_month_tot_ly

    ,dcbcja.activating_product_gross_revenue_excl_shipping AS activating_product_gross_revenue_excl_shipping
    ,dcbcja.activating_product_gross_revenue_excl_shipping_mtd AS activating_product_gross_revenue_excl_shipping_mtd
    ,dcbcja.activating_product_gross_revenue_excl_shipping_mtd_ly AS activating_product_gross_revenue_excl_shipping_mtd_ly
    ,dcbcja.activating_product_gross_revenue_excl_shipping_month_tot_lm AS activating_product_gross_revenue_excl_shipping_month_tot_lm
    ,dcbcja.activating_product_gross_revenue_excl_shipping_month_tot_ly AS activating_product_gross_revenue_excl_shipping_month_tot_ly

    ,dcbcja.nonactivating_product_gross_revenue_excl_shipping AS nonactivating_product_gross_revenue_excl_shipping
    ,dcbcja.nonactivating_product_gross_revenue_excl_shipping_mtd AS nonactivating_product_gross_revenue_excl_shipping_mtd
    ,dcbcja.nonactivating_product_gross_revenue_excl_shipping_mtd_ly AS nonactivating_product_gross_revenue_excl_shipping_mtd_ly
    ,dcbcja.nonactivating_product_gross_revenue_excl_shipping_month_tot_lm AS nonactivating_product_gross_revenue_excl_shipping_month_tot_lm
    ,dcbcja.nonactivating_product_gross_revenue_excl_shipping_month_tot_ly AS nonactivating_product_gross_revenue_excl_shipping_month_tot_ly

    ,dcbcja.guest_product_gross_revenue_excl_shipping AS guest_product_gross_revenue_excl_shipping
    ,dcbcja.guest_product_gross_revenue_excl_shipping_mtd AS guest_product_gross_revenue_excl_shipping_mtd
    ,dcbcja.guest_product_gross_revenue_excl_shipping_mtd_ly AS guest_product_gross_revenue_excl_shipping_mtd_ly
    ,dcbcja.guest_product_gross_revenue_excl_shipping_month_tot_lm AS guest_product_gross_revenue_excl_shipping_month_tot_lm
    ,dcbcja.guest_product_gross_revenue_excl_shipping_month_tot_ly AS guest_product_gross_revenue_excl_shipping_month_tot_ly

    ,dcbcja.repeat_vip_product_gross_revenue_excl_shipping AS repeat_vip_product_gross_revenue_excl_shipping
    ,dcbcja.repeat_vip_product_gross_revenue_excl_shipping_mtd AS repeat_vip_product_gross_revenue_excl_shipping_mtd
    ,dcbcja.repeat_vip_product_gross_revenue_excl_shipping_mtd_ly AS repeat_vip_product_gross_revenue_excl_shipping_mtd_ly
    ,dcbcja.repeat_vip_product_gross_revenue_excl_shipping_month_tot_lm AS repeat_vip_product_gross_revenue_excl_shipping_month_tot_lm
    ,dcbcja.repeat_vip_product_gross_revenue_excl_shipping_month_tot_ly AS repeat_vip_product_gross_revenue_excl_shipping_month_tot_ly

    ,dcbcja.product_margin_pre_return_excl_shipping AS product_margin_pre_return_excl_shipping
    ,dcbcja.activating_product_margin_pre_return_excl_shipping AS activating_product_margin_pre_return_excl_shipping

    ,dcbcja.product_variable_contribution_profit AS product_variable_contribution_profit

    ,dcbcja.product_order_cash_net_revenue AS product_order_cash_net_revenue

    ,dcbcja.product_order_cash_margin_pre_return AS product_order_cash_margin_pre_return

    ,dcbcja.billing_cash_gross_revenue AS billing_cash_gross_revenue

    ,dcbcja.billing_cash_net_revenue AS billing_cash_net_revenue

    ,dcbcja.billing_order_transaction_count AS billing_order_transaction_count
    ,dcbcja.billing_order_transaction_count_mtd AS billing_order_transaction_count_mtd
    ,dcbcja.billing_order_transaction_count_mtd_ly AS billing_order_transaction_count_mtd_ly
    ,dcbcja.billing_order_transaction_count_month_tot_lm AS billing_order_transaction_count_month_tot_lm
    ,dcbcja.billing_order_transaction_count_month_tot_ly AS billing_order_transaction_count_month_tot_ly
    ,bgt.membership_credits_charged_count AS billing_order_transaction_count_budget
    ,bgt_alt.membership_credits_charged_count AS billing_order_transaction_count_budget_alt
    ,fcst.membership_credits_charged_count AS billing_order_transaction_count_forecast

    ,dcbcja.membership_fee_cash_transaction_amount AS membership_fee_cash_transaction_amount
    ,dcbcja.membership_fee_cash_transaction_amount_mtd AS membership_fee_cash_transaction_amount_mtd
    ,dcbcja.membership_fee_cash_transaction_amount_mtd_ly AS membership_fee_cash_transaction_amount_mtd_ly
    ,dcbcja.membership_fee_cash_transaction_amount_month_tot_lm AS membership_fee_cash_transaction_amount_month_tot_lm
    ,dcbcja.membership_fee_cash_transaction_amount_month_tot_ly AS membership_fee_cash_transaction_amount_month_tot_ly

    ,dcbcja.gift_card_transaction_amount AS gift_card_transaction_amount
    ,dcbcja.gift_card_transaction_amount_mtd AS gift_card_transaction_amount_mtd
    ,dcbcja.gift_card_transaction_amount_mtd_ly AS gift_card_transaction_amount_mtd_ly
    ,dcbcja.gift_card_transaction_amount_month_tot_lm AS gift_card_transaction_amount_month_tot_lm
    ,dcbcja.gift_card_transaction_amount_month_tot_ly AS gift_card_transaction_amount_month_tot_ly

    ,dcbcja.legacy_credit_cash_transaction_amount AS legacy_credit_cash_transaction_amount
    ,dcbcja.legacy_credit_cash_transaction_amount_mtd AS legacy_credit_cash_transaction_amount_mtd
    ,dcbcja.legacy_credit_cash_transaction_amount_mtd_ly AS legacy_credit_cash_transaction_amount_mtd_ly
    ,dcbcja.legacy_credit_cash_transaction_amount_month_tot_lm AS legacy_credit_cash_transaction_amount_month_tot_lm
    ,dcbcja.legacy_credit_cash_transaction_amount_month_tot_ly AS legacy_credit_cash_transaction_amount_month_tot_ly

    ,dcbcja.membership_fee_cash_refund_chargeback_amount AS membership_fee_cash_refund_chargeback_amount
    ,dcbcja.membership_fee_cash_refund_chargeback_amount_mtd AS membership_fee_cash_refund_chargeback_amount_mtd
    ,dcbcja.membership_fee_cash_refund_chargeback_amount_mtd_ly AS membership_fee_cash_refund_chargeback_amount_mtd_ly
    ,dcbcja.membership_fee_cash_refund_chargeback_amount_month_tot_lm AS membership_fee_cash_refund_chargeback_amount_month_tot_lm
    ,dcbcja.membership_fee_cash_refund_chargeback_amount_month_tot_ly AS membership_fee_cash_refund_chargeback_amount_month_tot_ly

    ,dcbcja.gift_card_cash_refund_chargeback_amount AS gift_card_cash_refund_chargeback_amount
    ,dcbcja.gift_card_cash_refund_chargeback_amount_mtd AS gift_card_cash_refund_chargeback_amount_mtd
    ,dcbcja.gift_card_cash_refund_chargeback_amount_mtd_ly AS gift_card_cash_refund_chargeback_amount_mtd_ly
    ,dcbcja.gift_card_cash_refund_chargeback_amount_month_tot_lm AS gift_card_cash_refund_chargeback_amount_month_tot_lm
    ,dcbcja.gift_card_cash_refund_chargeback_amount_month_tot_ly AS gift_card_cash_refund_chargeback_amount_month_tot_ly

    ,dcbcja.legacy_credit_cash_refund_chargeback_amount AS legacy_credit_cash_refund_chargeback_amount
    ,dcbcja.legacy_credit_cash_refund_chargeback_amount_mtd AS legacy_credit_cash_refund_chargeback_amount_mtd
    ,dcbcja.legacy_credit_cash_refund_chargeback_amount_mtd_ly AS legacy_credit_cash_refund_chargeback_amount_mtd_ly
    ,dcbcja.legacy_credit_cash_refund_chargeback_amount_month_tot_lm AS legacy_credit_cash_refund_chargeback_amount_month_tot_lm
    ,dcbcja.legacy_credit_cash_refund_chargeback_amount_month_tot_ly AS legacy_credit_cash_refund_chargeback_amount_month_tot_ly

    ,dcbcja.billed_cash_credit_issued_amount AS billed_cash_credit_issued_amount

    ,dcbcja.billed_cash_credit_cancelled_amount AS billed_cash_credit_cancelled_amount

    ,dcbcja.billed_cash_credit_expired_amount AS billed_cash_credit_expired_amount

    ,dcbcja.billed_cash_credit_issued_equivalent_count AS billed_cash_credit_issued_equivalent_count
    ,dcbcja.billed_cash_credit_issued_equivalent_count_mtd AS billed_cash_credit_issued_equivalent_count_mtd
    ,dcbcja.billed_cash_credit_issued_equivalent_count_mtd_ly AS billed_cash_credit_issued_equivalent_count_mtd_ly
    ,dcbcja.billed_cash_credit_issued_equivalent_count_month_tot_lm AS billed_cash_credit_issued_equivalent_count_month_tot_lm
    ,dcbcja.billed_cash_credit_issued_equivalent_count_month_tot_ly AS billed_cash_credit_issued_equivalent_count_month_tot_ly

    ,dcbcja.billed_cash_credit_expired_equivalent_count AS billed_cash_credit_expired_equivalent_count

    ,dcbcja.refund_cash_credit_issued_amount AS refund_cash_credit_issued_amount
    ,dcbcja.refund_cash_credit_issued_amount_mtd AS refund_cash_credit_issued_amount_mtd
    ,dcbcja.refund_cash_credit_issued_amount_mtd_ly AS refund_cash_credit_issued_amount_mtd_ly
    ,dcbcja.refund_cash_credit_issued_amount_month_tot_lm AS refund_cash_credit_issued_amount_month_tot_lm
    ,dcbcja.refund_cash_credit_issued_amount_month_tot_ly AS refund_cash_credit_issued_amount_month_tot_ly
    ,bgt.refunded_as_credit AS refund_cash_credit_issued_amount_budget
    ,bgt_alt.refunded_as_credit AS refund_cash_credit_issued_amount_budget_alt
    ,fcst.refunded_as_credit AS refund_cash_credit_issued_amount_forecast

    ,dcbcja.refund_cash_credit_redeemed_amount AS refund_cash_credit_redeemed_amount
    ,dcbcja.refund_cash_credit_redeemed_amount_mtd AS refund_cash_credit_redeemed_amount_mtd
    ,dcbcja.refund_cash_credit_redeemed_amount_mtd_ly AS refund_cash_credit_redeemed_amount_mtd_ly
    ,dcbcja.refund_cash_credit_redeemed_amount_month_tot_lm AS refund_cash_credit_redeemed_amount_month_tot_lm
    ,dcbcja.refund_cash_credit_redeemed_amount_month_tot_ly AS refund_cash_credit_redeemed_amount_month_tot_ly
    ,bgt.refund_credit_redeemed AS refund_cash_credit_redeemed_amount_budget
    ,bgt_alt.refund_credit_redeemed AS refund_cash_credit_redeemed_amount_budget_alt
    ,fcst.refund_credit_redeemed AS refund_cash_credit_redeemed_amount_forecast

    ,dcbcja.refund_cash_credit_cancelled_amount AS refund_cash_credit_cancelled_amount
    ,dcbcja.refund_cash_credit_cancelled_amount_mtd AS refund_cash_credit_cancelled_amount_mtd
    ,dcbcja.refund_cash_credit_cancelled_amount_mtd_ly AS refund_cash_credit_cancelled_amount_mtd_ly
    ,dcbcja.refund_cash_credit_cancelled_amount_month_tot_lm AS refund_cash_credit_cancelled_amount_month_tot_lm
    ,dcbcja.refund_cash_credit_cancelled_amount_month_tot_ly AS refund_cash_credit_cancelled_amount_month_tot_ly

    ,bgt.net_unredeemed_refund_credit AS net_unredeemed_refund_credit_budget
    ,bgt_alt.net_unredeemed_refund_credit AS net_unredeemed_refund_credit_budget_alt
    ,fcst.net_unredeemed_refund_credit AS net_unredeemed_refund_credit_forecast

    ,dcbcja.refund_cash_credit_expired_amount AS refund_cash_credit_expired_amount

    ,dcbcja.other_cash_credit_issued_amount AS other_cash_credit_issued_amount

    ,dcbcja.other_cash_credit_redeemed_amount AS other_cash_credit_redeemed_amount
    ,dcbcja.other_cash_credit_redeemed_amount_mtd AS other_cash_credit_redeemed_amount_mtd
    ,dcbcja.other_cash_credit_redeemed_amount_mtd_ly AS other_cash_credit_redeemed_amount_mtd_ly
    ,dcbcja.other_cash_credit_redeemed_amount_month_tot_lm AS other_cash_credit_redeemed_amount_month_tot_lm
    ,dcbcja.other_cash_credit_redeemed_amount_month_tot_ly AS other_cash_credit_redeemed_amount_month_tot_ly

    ,dcbcja.other_cash_credit_cancelled_amount AS other_cash_credit_cancelled_amount

    ,dcbcja.other_cash_credit_expired_amount AS other_cash_credit_expired_amount

    ,dcbcja.cash_gift_card_redeemed_amount AS cash_gift_card_redeemed_amount
    ,dcbcja.activating_cash_gift_card_redeemed_amount AS activating_cash_gift_card_redeemed_amount
    ,dcbcja.nonactivating_cash_gift_card_redeemed_amount AS nonactivating_cash_gift_card_redeemed_amount
    ,dcbcja.guest_cash_gift_card_redeemed_amount AS guest_cash_gift_card_redeemed_amount

    ,dcbcja.noncash_credit_issued_amount AS noncash_credit_issued_amount
    ,dcbcja.noncash_credit_issued_amount_mtd AS noncash_credit_issued_amount_mtd
    ,dcbcja.noncash_credit_issued_amount_mtd_ly AS noncash_credit_issued_amount_mtd_ly
    ,dcbcja.noncash_credit_issued_amount_month_tot_lm AS noncash_credit_issued_amount_month_tot_lm
    ,dcbcja.noncash_credit_issued_amount_month_tot_ly AS noncash_credit_issued_amount_month_tot_ly

    ,dcbcja.noncash_credit_cancelled_amount AS noncash_credit_cancelled_amount

    ,dcbcja.noncash_credit_expired_amount AS noncash_credit_expired_amount

    ,bgt.net_unredeemed_credit_billings AS net_unredeemed_credit_billings_budget
    ,bgt_alt.net_unredeemed_credit_billings AS net_unredeemed_credit_billings_budget_alt
    ,fcst.net_unredeemed_credit_billings AS net_unredeemed_credit_billings_forecast

    ,dcbcja.product_order_non_token_subtotal_excl_tariff_amount AS product_order_non_token_subtotal_excl_tariff_amount
    ,dcbcja.product_order_non_token_subtotal_excl_tariff_amount_mtd AS product_order_non_token_subtotal_excl_tariff_amount_mtd
    ,dcbcja.product_order_non_token_subtotal_excl_tariff_amount_mtd_ly AS product_order_non_token_subtotal_excl_tariff_amount_mtd_ly
    ,dcbcja.product_order_non_token_subtotal_excl_tariff_amount_month_tot_lm AS product_order_non_token_subtotal_excl_tariff_amount_month_tot_lm
    ,dcbcja.product_order_non_token_subtotal_excl_tariff_amount_month_tot_ly AS product_order_non_token_subtotal_excl_tariff_amount_month_tot_ly

    ,dcbcja.product_order_non_token_unit_count AS product_order_non_token_unit_count
    ,dcbcja.product_order_non_token_unit_count_mtd AS product_order_non_token_unit_count_mtd
    ,dcbcja.product_order_non_token_unit_count_mtd_ly AS product_order_non_token_unit_count_mtd_ly
    ,dcbcja.product_order_non_token_unit_count_month_tot_lm AS product_order_non_token_unit_count_month_tot_lm
    ,dcbcja.product_order_non_token_unit_count_month_tot_ly AS product_order_non_token_unit_count_month_tot_ly

    ,dcbcja.nonactivating_product_order_non_token_subtotal_excl_tariff_amount AS nonactivating_product_order_non_token_subtotal_excl_tariff_amount
    ,dcbcja.nonactivating_product_order_non_token_subtotal_excl_tariff_amount_mtd AS nonactivating_product_order_non_token_subtotal_excl_tariff_amount_mtd
    ,dcbcja.nonactivating_product_order_non_token_subtotal_excl_tariff_amount_mtd_ly AS nonactivating_product_order_non_token_subtotal_excl_tariff_amount_mtd_ly
    ,dcbcja.nonactivating_product_order_non_token_subtotal_excl_tariff_amount_month_tot_lm AS nonactivating_product_order_non_token_subtotal_excl_tariff_amount_month_tot_lm
    ,dcbcja.nonactivating_product_order_non_token_subtotal_excl_tariff_amount_month_tot_ly AS nonactivating_product_order_non_token_subtotal_excl_tariff_amount_month_tot_ly

    ,dcbcja.nonactivating_product_order_non_token_unit_count AS nonactivating_product_order_non_token_unit_count
    ,dcbcja.nonactivating_product_order_non_token_unit_count_mtd AS nonactivating_product_order_non_token_unit_count_mtd
    ,dcbcja.nonactivating_product_order_non_token_unit_count_mtd_ly AS nonactivating_product_order_non_token_unit_count_mtd_ly
    ,dcbcja.nonactivating_product_order_non_token_unit_count_month_tot_lm AS nonactivating_product_order_non_token_unit_count_month_tot_lm
    ,dcbcja.nonactivating_product_order_non_token_unit_count_month_tot_ly AS nonactivating_product_order_non_token_unit_count_month_tot_ly

    ,po.pending_order_count
    ,po.pending_order_local_amount
    ,po.pending_order_usd_amount
    ,po.pending_order_eur_amount
    ,po.pending_order_cash_margin_pre_return_local_amount
    ,po.pending_order_cash_margin_pre_return_usd_amount
    ,po.pending_order_cash_margin_pre_return_eur_amount
/*Acquisition*/

    ,dcbcja.leads AS leads
    ,dcbcja.leads_mtd AS leads_mtd
    ,dcbcja.leads_mtd_ly AS leads_mtd_ly
    ,dcbcja.leads_month_tot_lm AS leads_month_tot_lm
    ,dcbcja.leads_month_tot_ly AS leads_month_tot_ly
    ,bgt.leads AS leads_budget
    ,bgt_alt.leads AS leads_budget_alt
    ,fcst.leads AS leads_forecast

    ,dcbcja.primary_leads AS primary_leads
    ,dcbcja.primary_leads_mtd AS primary_leads_mtd
    ,dcbcja.primary_leads_mtd_ly AS primary_leads_mtd_ly
    ,dcbcja.primary_leads_month_tot_lm AS primary_leads_month_tot_lm
    ,dcbcja.primary_leads_month_tot_ly AS primary_leads_month_tot_ly

    ,dcbcja.reactivated_leads AS reactivated_leads
    ,dcbcja.reactivated_leads_mtd AS reactivated_leads_mtd
    ,dcbcja.reactivated_leads_mtd_ly AS reactivated_leads_mtd_ly
    ,dcbcja.reactivated_leads_month_tot_lm AS reactivated_leads_month_tot_lm
    ,dcbcja.reactivated_leads_month_tot_ly AS reactivated_leads_month_tot_ly

    ,dcbcja.new_vips AS new_vips
    ,dcbcja.new_vips_mtd AS new_vips_mtd
    ,dcbcja.new_vips_mtd_ly AS new_vips_mtd_ly
    ,dcbcja.new_vips_month_tot_lm AS new_vips_month_tot_lm
    ,dcbcja.new_vips_month_tot_ly AS new_vips_month_tot_ly
    ,bgt.total_new_vips AS new_vips_budget
    ,bgt_alt.total_new_vips AS new_vips_budget_alt
    ,fcst.total_new_vips AS new_vips_forecast

    ,dcbcja.reactivated_vips AS reactivated_vips
    ,dcbcja.reactivated_vips_mtd AS reactivated_vips_mtd
    ,dcbcja.reactivated_vips_mtd_ly AS reactivated_vips_mtd_ly
    ,dcbcja.reactivated_vips_month_tot_lm AS reactivated_vips_month_tot_lm
    ,dcbcja.reactivated_vips_month_tot_ly AS reactivated_vips_month_tot_ly

    ,dcbcja.vips_from_reactivated_leads_m1 AS vips_from_reactivated_leads_m1
    ,dcbcja.vips_from_reactivated_leads_m1_mtd AS vips_from_reactivated_leads_m1_mtd
    ,dcbcja.vips_from_reactivated_leads_m1_mtd_ly AS vips_from_reactivated_leads_m1_mtd_ly
    ,dcbcja.vips_from_reactivated_leads_m1_month_tot_lm AS vips_from_reactivated_leads_m1_month_tot_lm
    ,dcbcja.vips_from_reactivated_leads_m1_month_tot_ly AS vips_from_reactivated_leads_m1_month_tot_ly

    ,dcbcja.paid_vips AS paid_vips
    ,dcbcja.paid_vips_mtd AS paid_vips_mtd
    ,dcbcja.paid_vips_mtd_ly AS paid_vips_mtd_ly
    ,dcbcja.paid_vips_month_tot_lm AS paid_vips_month_tot_lm
    ,dcbcja.paid_vips_month_tot_ly AS paid_vips_month_tot_ly

    ,dcbcja.unpaid_vips AS unpaid_vips
    ,dcbcja.unpaid_vips_mtd AS unpaid_vips_mtd
    ,dcbcja.unpaid_vips_mtd_ly AS unpaid_vips_mtd_ly
    ,dcbcja.unpaid_vips_month_tot_lm AS unpaid_vips_month_tot_lm
    ,dcbcja.unpaid_vips_month_tot_ly AS unpaid_vips_month_tot_ly

    ,dcbcja.new_vips_m1 AS new_vips_m1
    ,dcbcja.new_vips_m1_mtd AS new_vips_m1_mtd
    ,dcbcja.new_vips_m1_mtd_ly AS new_vips_m1_mtd_ly
    ,dcbcja.new_vips_m1_month_tot_lm AS new_vips_m1_month_tot_lm
    ,dcbcja.new_vips_m1_month_tot_ly AS new_vips_m1_month_tot_ly
    ,bgt.m1_vips AS new_vips_m1_budget
    ,bgt_alt.m1_vips AS new_vips_m1_budget_alt
    ,fcst.m1_vips AS new_vips_m1_forecast

    ,dcbcja.paid_vips_m1 AS paid_vips_m1
    ,dcbcja.paid_vips_m1_mtd AS paid_vips_m1_mtd
    ,dcbcja.paid_vips_m1_mtd_ly AS paid_vips_m1_mtd_ly
    ,dcbcja.paid_vips_m1_month_tot_lm AS paid_vips_m1_month_tot_lm
    ,dcbcja.paid_vips_m1_month_tot_ly AS paid_vips_m1_month_tot_ly

    ,dcbcja.cancels AS cancels
    ,dcbcja.cancels_mtd AS cancels_mtd
    ,dcbcja.cancels_mtd_ly AS cancels_mtd_ly
    ,dcbcja.cancels_month_tot_lm AS cancels_month_tot_lm
    ,dcbcja.cancels_month_tot_ly AS cancels_month_tot_ly
    ,bgt.cancels AS cancels_budget
    ,bgt_alt.cancels AS cancels_budget_alt
    ,fcst.cancels AS cancels_forecast

    ,dcbcja.m1_cancels AS m1_cancels
    ,dcbcja.m1_cancels_mtd AS m1_cancels_mtd
    ,dcbcja.m1_cancels_mtd_ly AS m1_cancels_mtd_ly
    ,dcbcja.m1_cancels_month_tot_lm AS m1_cancels_month_tot_lm
    ,dcbcja.m1_cancels_month_tot_ly AS m1_cancels_month_tot_ly

    ,dcbcja.bop_vips AS bop_vips
    ,dcbcja.bop_vips_mtd AS bop_vips_mtd
    ,dcbcja.bop_vips_mtd_ly AS bop_vips_mtd_ly
    ,dcbcja.bop_vips_month_tot_lm AS bop_vips_month_tot_lm
    ,dcbcja.bop_vips_month_tot_ly AS bop_vips_month_tot_ly
    ,bgt.bom_vips AS bop_vips_budget
    ,bgt_alt.bom_vips AS bop_vips_budget_alt
    ,fcst.bom_vips AS bop_vips_forecast

    ,dcbcja.media_spend AS media_spend
    ,dcbcja.media_spend_mtd AS media_spend_mtd
    ,dcbcja.media_spend_mtd_ly AS media_spend_mtd_ly
    ,dcbcja.media_spend_month_tot_lm AS media_spend_month_tot_lm
    ,dcbcja.media_spend_month_tot_ly AS media_spend_month_tot_ly
    ,bgt.media_spend AS media_spend_budget
    ,bgt_alt.media_spend AS media_spend_budget_alt
    ,fcst.media_spend AS media_spend_forecast

    ,dcbcja.product_order_return_unit_count AS product_order_return_unit_count
    ,dcbcja.product_order_return_unit_count_mtd AS product_order_return_unit_count_mtd
    ,dcbcja.product_order_return_unit_count_mtd_ly AS product_order_return_unit_count_mtd_ly
    ,dcbcja.product_order_return_unit_count_month_tot_lm AS product_order_return_unit_count_month_tot_lm
    ,dcbcja.product_order_return_unit_count_month_tot_ly AS product_order_return_unit_count_month_tot_ly

    ,dcbcja.merch_purchase_count AS merch_purchase_count
    ,dcbcja.merch_purchase_count_mtd AS merch_purchase_count_mtd
    ,dcbcja.merch_purchase_count_mtd_ly AS merch_purchase_count_mtd_ly
    ,dcbcja.merch_purchase_count_month_tot_lm AS merch_purchase_count_month_tot_lm
    ,dcbcja.merch_purchase_count_month_tot_ly AS merch_purchase_count_month_tot_ly

    ,dcbcja.merch_purchase_hyperion_count AS merch_purchase_hyperion_count
    ,dcbcja.merch_purchase_hyperion_count_mtd AS merch_purchase_hyperion_count_mtd
    ,dcbcja.merch_purchase_hyperion_count_mtd_ly AS merch_purchase_hyperion_count_mtd_ly
    ,dcbcja.merch_purchase_hyperion_count_month_tot_lm AS merch_purchase_hyperion_count_month_tot_lm
    ,dcbcja.merch_purchase_hyperion_count_month_tot_ly AS merch_purchase_hyperion_count_month_tot_ly
    ,bv.bom_vips AS bom_vips
    ,sc.skip_count AS skip_count
/*Meta*/
    ,'To Date' AS report_date_type
    ,bgt.meta_update_datetime AS snapshot_datetime_budget
    ,fcst.meta_update_datetime AS snapshot_datetime_forecast
    ,$execution_start_time AS meta_update_datetime
    ,$execution_start_time AS meta_create_datetime
FROM _dcbc_j_agg AS dcbcja
LEFT JOIN (SELECT DISTINCT bu, report_mapping FROM reference.finance_bu_mapping) AS fbm
   ON dcbcja.report_mapping = fbm.report_mapping
LEFT JOIN (SELECT * FROM reference.finance_budget_forecast WHERE version='Budget') AS bgt
   ON CASE WHEN dcbcja.currency_object='usd' THEN 'usdbtfx' ELSE dcbcja.currency_object END = lower(bgt.currency_type)
       AND fbm.bu = bgt.bu
       AND dcbcja.currency_type = bgt.currency
       AND date_trunc('month',dcbcja.date) = bgt.financial_date::date
LEFT JOIN (SELECT * FROM reference.finance_budget_forecast WHERE version='CapRaise') AS bgt_alt
   ON CASE WHEN dcbcja.currency_object='usd' THEN 'usdbtfx' ELSE dcbcja.currency_object END = lower(bgt_alt.currency_type)
       AND fbm.bu = bgt_alt.bu
       AND dcbcja.currency_type = bgt_alt.currency
       AND date_trunc('month',dcbcja.date) = bgt_alt.financial_date::date
LEFT JOIN (SELECT * FROM reference.finance_budget_forecast WHERE version='Forecast') AS fcst
   ON CASE WHEN dcbcja.currency_object='usd' THEN 'usdbtfx' ELSE dcbcja.currency_object END = lower(fcst.currency_type)
       AND fbm.bu = fcst.bu
       AND dcbcja.currency_type = fcst.currency
       AND date_trunc('month',dcbcja.date) = fcst.financial_date::date
LEFT JOIN _pending_order AS po
    ON dcbcja.date = po.calc_date
    AND dcbcja.report_mapping = po.report_mapping
LEFT JOIN _bom_vips bv ON bv.date = DATE_TRUNC('MONTH', dcbcja.date)
    AND bv.report_mapping = dcbcja.report_mapping
    AND bv.currency_object = dcbcja.currency_object
    AND bv.currency_type = dcbcja.currency_type
LEFT JOIN _skip_count sc ON sc.date = dcbcja.date
    AND sc.report_mapping = dcbcja.report_mapping
    AND sc.currency_object = dcbcja.currency_object
    AND sc.currency_type = dcbcja.currency_type
WHERE
    dcbcja.date <= $execution_start_date_calc

UNION ALL

SELECT
/*Daily Cash Version*/
     dcbcja.date
    ,dcbcja.date_object
    ,dcbcja.currency_object
    ,dcbcja.currency_type
/*Segment*/
    ,dcbcja.store_brand
    ,dcbcja.business_unit
    ,dcbcja.report_mapping
    ,dcbcja.is_daily_cash_usd
    ,dcbcja.is_daily_cash_eur
/*Measures*/

    ,0 AS product_order_subtotal_excl_tariff_amount
    ,0 AS product_order_subtotal_excl_tariff_amount_mtd
    ,0 AS product_order_subtotal_excl_tariff_amount_mtd_ly
    ,0 AS product_order_subtotal_excl_tariff_amount_month_tot_lm
    ,0 AS product_order_subtotal_excl_tariff_amount_month_tot_ly

    ,0 AS activating_product_order_subtotal_excl_tariff_amount
    ,0 AS activating_product_order_subtotal_excl_tariff_amount_mtd
    ,0 AS activating_product_order_subtotal_excl_tariff_amount_mtd_ly
    ,0 AS activating_product_order_subtotal_excl_tariff_amount_month_tot_lm
    ,0 AS activating_product_order_subtotal_excl_tariff_amount_month_tot_ly

    ,0 AS nonactivating_product_order_subtotal_excl_tariff_amount
    ,0 AS nonactivating_product_order_subtotal_excl_tariff_amount_mtd
    ,0 AS nonactivating_product_order_subtotal_excl_tariff_amount_mtd_ly
    ,0 AS nonactivating_product_order_subtotal_excl_tariff_amount_month_tot_lm
    ,0 AS nonactivating_product_order_subtotal_excl_tariff_amount_month_tot_ly

    ,0 AS guest_product_order_subtotal_excl_tariff_amount
    ,0 AS guest_product_order_subtotal_excl_tariff_amount_mtd
    ,0 AS guest_product_order_subtotal_excl_tariff_amount_mtd_ly
    ,0 AS guest_product_order_subtotal_excl_tariff_amount_month_tot_lm
    ,0 AS guest_product_order_subtotal_excl_tariff_amount_month_tot_ly

    ,0 AS repeat_vip_product_order_subtotal_excl_tariff_amount
    ,0 AS repeat_vip_product_order_subtotal_excl_tariff_amount_mtd
    ,0 AS repeat_vip_product_order_subtotal_excl_tariff_amount_mtd_ly
    ,0 AS repeat_vip_product_order_subtotal_excl_tariff_amount_month_tot_lm
    ,0 AS repeat_vip_product_order_subtotal_excl_tariff_amount_month_tot_ly

    ,0 AS product_order_product_subtotal_amount
    ,0 AS product_order_product_subtotal_amount_mtd
    ,0 AS product_order_product_subtotal_amount_mtd_ly
    ,0 AS product_order_product_subtotal_amount_month_tot_lm
    ,0 AS product_order_product_subtotal_amount_month_tot_ly

    ,0 AS activating_product_order_product_subtotal_amount
    ,0 AS activating_product_order_product_subtotal_amount_mtd
    ,0 AS activating_product_order_product_subtotal_amount_mtd_ly
    ,0 AS activating_product_order_product_subtotal_amount_month_tot_lm
    ,0 AS activating_product_order_product_subtotal_amount_month_tot_ly

    ,0 AS nonactivating_product_order_product_subtotal_amount
    ,0 AS nonactivating_product_order_product_subtotal_amount_mtd
    ,0 AS nonactivating_product_order_product_subtotal_amount_mtd_ly
    ,0 AS nonactivating_product_order_product_subtotal_amount_month_tot_lm
    ,0 AS nonactivating_product_order_product_subtotal_amount_month_tot_ly

    ,0 AS product_order_product_discount_amount
    ,0 AS product_order_product_discount_amount_mtd
    ,0 AS product_order_product_discount_amount_mtd_ly
    ,0 AS product_order_product_discount_amount_month_tot_lm
    ,0 AS product_order_product_discount_amount_month_tot_ly

    ,0 AS activating_product_order_product_discount_amount
    ,0 AS activating_product_order_product_discount_amount_mtd
    ,0 AS activating_product_order_product_discount_amount_mtd_ly
    ,0 AS activating_product_order_product_discount_amount_month_tot_lm
    ,0 AS activating_product_order_product_discount_amount_month_tot_ly

    ,0 AS nonactivating_product_order_product_discount_amount
    ,0 AS nonactivating_product_order_product_discount_amount_mtd
    ,0 AS nonactivating_product_order_product_discount_amount_mtd_ly
    ,0 AS nonactivating_product_order_product_discount_amount_month_tot_lm
    ,0 AS nonactivating_product_order_product_discount_amount_month_tot_ly

    ,0 AS guest_product_order_product_discount_amount
    ,0 AS guest_product_order_product_discount_amount_mtd
    ,0 AS guest_product_order_product_discount_amount_mtd_ly
    ,0 AS guest_product_order_product_discount_amount_month_tot_lm
    ,0 AS guest_product_order_product_discount_amount_month_tot_ly

    ,0 AS repeat_vip_product_order_product_discount_amount
    ,0 AS repeat_vip_product_order_product_discount_amount_mtd
    ,0 AS repeat_vip_product_order_product_discount_amount_mtd_ly
    ,0 AS repeat_vip_product_order_product_discount_amount_month_tot_lm
    ,0 AS repeat_vip_product_order_product_discount_amount_month_tot_ly

    ,0 AS shipping_revenue
    ,0 AS shipping_revenue_mtd
    ,0 AS shipping_revenue_mtd_ly
    ,0 AS shipping_revenue_month_tot_lm
    ,0 AS shipping_revenue_month_tot_ly

    ,0 AS activating_shipping_revenue
    ,0 AS activating_shipping_revenue_mtd
    ,0 AS activating_shipping_revenue_mtd_ly
    ,0 AS activating_shipping_revenue_month_tot_lm
    ,0 AS activating_shipping_revenue_month_tot_ly

    ,0 AS nonactivating_shipping_revenue
    ,0 AS nonactivating_shipping_revenue_mtd
    ,0 AS nonactivating_shipping_revenue_mtd_ly
    ,0 AS nonactivating_shipping_revenue_month_tot_lm
    ,0 AS nonactivating_shipping_revenue_month_tot_ly

    ,0 AS guest_shipping_revenue
    ,0 AS guest_shipping_revenue_mtd
    ,0 AS guest_shipping_revenue_mtd_ly
    ,0 AS guest_shipping_revenue_month_tot_lm
    ,0 AS guest_shipping_revenue_month_tot_ly

    ,0 AS product_order_noncash_credit_redeemed_amount
    ,0 AS product_order_noncash_credit_redeemed_amount_mtd
    ,0 AS product_order_noncash_credit_redeemed_amount_mtd_ly
    ,0 AS product_order_noncash_credit_redeemed_amount_month_tot_lm
    ,0 AS product_order_noncash_credit_redeemed_amount_month_tot_ly

    ,0 AS activating_product_order_noncash_credit_redeemed_amount
    ,0 AS activating_product_order_noncash_credit_redeemed_amount_mtd
    ,0 AS activating_product_order_noncash_credit_redeemed_amount_mtd_ly
    ,0 AS activating_product_order_noncash_credit_redeemed_amount_month_tot_lm
    ,0 AS activating_product_order_noncash_credit_redeemed_amount_month_tot_ly

    ,0 AS nonactivating_product_order_noncash_credit_redeemed_amount
    ,0 AS nonactivating_product_order_noncash_credit_redeemed_amount_mtd
    ,0 AS nonactivating_product_order_noncash_credit_redeemed_amount_mtd_ly
    ,0 AS nonactivating_product_order_noncash_credit_redeemed_amount_month_tot_lm
    ,0 AS nonactivating_product_order_noncash_credit_redeemed_amount_month_tot_ly

    ,0 AS repeat_vip_product_order_noncash_credit_redeemed_amount
    ,0 AS repeat_vip_product_order_noncash_credit_redeemed_amount_mtd
    ,0 AS repeat_vip_product_order_noncash_credit_redeemed_amount_mtd_ly
    ,0 AS repeat_vip_product_order_noncash_credit_redeemed_amount_month_tot_lm
    ,0 AS repeat_vip_product_order_noncash_credit_redeemed_amount_month_tot_ly

    ,0 AS product_order_count
    ,0 AS product_order_count_mtd
    ,0 AS product_order_count_mtd_ly
    ,0 AS product_order_count_month_tot_lm
    ,0 AS product_order_count_month_tot_ly

    ,0 AS activating_product_order_count
    ,0 AS activating_product_order_count_mtd
    ,0 AS activating_product_order_count_mtd_ly
    ,0 AS activating_product_order_count_month_tot_lm
    ,0 AS activating_product_order_count_month_tot_ly
    ,bgt.activating_order_count AS activating_product_order_count_budget
    ,0 AS activating_product_order_count_budget_alt
    ,fcst.activating_order_count AS activating_product_order_count_forecast

    ,0 AS nonactivating_product_order_count
    ,0 AS nonactivating_product_order_count_mtd
    ,0 AS nonactivating_product_order_count_mtd_ly
    ,0 AS nonactivating_product_order_count_month_tot_lm
    ,0 AS nonactivating_product_order_count_month_tot_ly
    ,bgt.repeat_order_count AS nonactivating_product_order_count_budget
    ,0 AS nonactivating_product_order_count_budget_alt
    ,fcst.repeat_order_count AS nonactivating_product_order_count_forecast

    ,0 AS guest_product_order_count
    ,0 AS guest_product_order_count_mtd
    ,0 AS guest_product_order_count_mtd_ly
    ,0 AS guest_product_order_count_month_tot_lm
    ,0 AS guest_product_order_count_month_tot_ly

    ,0 AS repeat_vip_product_order_count
    ,0 AS repeat_vip_product_order_count_mtd
    ,0 AS repeat_vip_product_order_count_mtd_ly
    ,0 AS repeat_vip_product_order_count_month_tot_lm
    ,0 AS repeat_vip_product_order_count_month_tot_ly

    ,0 AS reactivated_vip_product_order_count
    ,0 AS reactivated_vip_product_order_count_mtd
    ,0 AS reactivated_vip_product_order_count_mtd_ly
    ,0 AS reactivated_vip_product_order_count_month_tot_lm
    ,0 AS reactivated_vip_product_order_count_month_tot_ly

    ,0 AS product_order_count_excl_seeding
    ,0 AS product_order_count_excl_seeding_mtd
    ,0 AS product_order_count_excl_seeding_mtd_ly
    ,0 AS product_order_count_excl_seeding_month_tot_lm
    ,0 AS product_order_count_excl_seeding_month_tot_ly

    ,0 AS activating_product_order_count_excl_seeding
    ,0 AS activating_product_order_count_excl_seeding_mtd
    ,0 AS activating_product_order_count_excl_seeding_mtd_ly
    ,0 AS activating_product_order_count_excl_seeding_month_tot_lm
    ,0 AS activating_product_order_count_excl_seeding_month_tot_ly

    ,0 AS nonactivating_product_order_count_excl_seeding
    ,0 AS nonactivating_product_order_count_excl_seeding_mtd
    ,0 AS nonactivating_product_order_count_excl_seeding_mtd_ly
    ,0 AS nonactivating_product_order_count_excl_seeding_month_tot_lm
    ,0 AS nonactivating_product_order_count_excl_seeding_month_tot_ly

    ,0 AS guest_product_order_count_excl_seeding
    ,0 AS guest_product_order_count_excl_seeding_mtd
    ,0 AS guest_product_order_count_excl_seeding_mtd_ly
    ,0 AS guest_product_order_count_excl_seeding_month_tot_lm
    ,0 AS guest_product_order_count_excl_seeding_month_tot_ly

    ,0 AS repeat_vip_product_order_count_excl_seeding
    ,0 AS repeat_vip_product_order_count_excl_seeding_mtd
    ,0 AS repeat_vip_product_order_count_excl_seeding_mtd_ly
    ,0 AS repeat_vip_product_order_count_excl_seeding_month_tot_lm
    ,0 AS repeat_vip_product_order_count_excl_seeding_month_tot_ly

    ,0 AS reactivated_vip_product_order_count_excl_seeding
    ,0 AS reactivated_vip_product_order_count_excl_seeding_mtd
    ,0 AS reactivated_vip_product_order_count_excl_seeding_mtd_ly
    ,0 AS reactivated_vip_product_order_count_excl_seeding_month_tot_lm
    ,0 AS reactivated_vip_product_order_count_excl_seeding_month_tot_ly

    ,0 AS product_margin_pre_return_excl_seeding
    ,0 AS product_margin_pre_return_excl_seeding_mtd
    ,0 AS product_margin_pre_return_excl_seeding_mtd_ly
    ,0 AS product_margin_pre_return_excl_seeding_month_tot_lm
    ,0 AS product_margin_pre_return_excl_seeding_month_tot_ly

    ,0 AS activating_product_margin_pre_return_excl_seeding
    ,0 AS activating_product_margin_pre_return_excl_seeding_mtd
    ,0 AS activating_product_margin_pre_return_excl_seeding_mtd_ly
    ,0 AS activating_product_margin_pre_return_excl_seeding_month_tot_lm
    ,0 AS activating_product_margin_pre_return_excl_seeding_month_tot_ly

    ,0 AS nonactivating_product_margin_pre_return_excl_seeding
    ,0 AS nonactivating_product_margin_pre_return_excl_seeding_mtd
    ,0 AS nonactivating_product_margin_pre_return_excl_seeding_mtd_ly
    ,0 AS nonactivating_product_margin_pre_return_excl_seeding_month_tot_lm
    ,0 AS nonactivating_product_margin_pre_return_excl_seeding_month_tot_ly

    ,0 AS guest_product_margin_pre_return_excl_seeding
    ,0 AS guest_product_margin_pre_return_excl_seeding_mtd
    ,0 AS guest_product_margin_pre_return_excl_seeding_mtd_ly
    ,0 AS guest_product_margin_pre_return_excl_seeding_month_tot_lm
    ,0 AS guest_product_margin_pre_return_excl_seeding_month_tot_ly

    ,0 AS repeat_vip_product_margin_pre_return_excl_seeding
    ,0 AS repeat_vip_product_margin_pre_return_excl_seeding_mtd
    ,0 AS repeat_vip_product_margin_pre_return_excl_seeding_mtd_ly
    ,0 AS repeat_vip_product_margin_pre_return_excl_seeding_month_tot_lm
    ,0 AS repeat_vip_product_margin_pre_return_excl_seeding_month_tot_ly

    ,0 AS reactivated_vip_product_margin_pre_return_excl_seeding
    ,0 AS reactivated_vip_product_margin_pre_return_excl_seeding_mtd
    ,0 AS reactivated_vip_product_margin_pre_return_excl_seeding_mtd_ly
    ,0 AS reactivated_vip_product_margin_pre_return_excl_seeding_month_tot_lm
    ,0 AS reactivated_vip_product_margin_pre_return_excl_seeding_month_tot_ly

    ,0 AS unit_count
    ,0 AS unit_count_mtd
    ,0 AS unit_count_mtd_ly
    ,0 AS unit_count_month_tot_lm
    ,0 AS unit_count_month_tot_ly
    ,bgt.total_units_shipped AS unit_count_budget
    ,0 AS unit_count_budget_alt
    ,fcst.total_units_shipped AS unit_count_forecast

    ,0 AS activating_unit_count
    ,0 AS activating_unit_count_mtd
    ,0 AS activating_unit_count_mtd_ly
    ,0 AS activating_unit_count_month_tot_lm
    ,0 AS activating_unit_count_month_tot_ly
    ,bgt.activating_units AS activating_unit_count_budget
    ,0 AS activating_unit_count_budget_alt
    ,fcst.activating_units AS activating_unit_count_forecast

    ,0 AS nonactivating_unit_count
    ,0 AS nonactivating_unit_count_mtd
    ,0 AS nonactivating_unit_count_mtd_ly
    ,0 AS nonactivating_unit_count_month_tot_lm
    ,0 AS nonactivating_unit_count_month_tot_ly
    ,bgt.repeat_units AS nonactivating_unit_count_budget
    ,0 AS nonactivating_unit_count_budget_alt
    ,fcst.repeat_units AS nonactivating_unit_count_forecast

    ,0 AS guest_unit_count
    ,0 AS guest_unit_count_mtd
    ,0 AS guest_unit_count_mtd_ly
    ,0 AS guest_unit_count_month_tot_lm
    ,0 AS guest_unit_count_month_tot_ly

    ,0 AS repeat_vip_unit_count
    ,0 AS repeat_vip_unit_count_mtd
    ,0 AS repeat_vip_unit_count_mtd_ly
    ,0 AS repeat_vip_unit_count_month_tot_lm
    ,0 AS repeat_vip_unit_count_month_tot_ly

    ,0 AS activating_product_order_air_vip_price
    ,0 AS activating_product_order_air_vip_price_mtd
    ,0 AS activating_product_order_air_vip_price_mtd_ly
    ,0 AS activating_product_order_air_vip_price_month_tot_lm
    ,0 AS activating_product_order_air_vip_price_month_tot_ly

    ,0 AS nonactivating_product_order_air_price
    ,0 AS nonactivating_product_order_air_price_mtd
    ,0 AS nonactivating_product_order_air_price_mtd_ly
    ,0 AS nonactivating_product_order_air_price_month_tot_lm
    ,0 AS nonactivating_product_order_air_price_month_tot_ly

    ,0 AS repeat_vip_product_order_air_vip_price
    ,0 AS repeat_vip_product_order_air_vip_price_mtd
    ,0 AS repeat_vip_product_order_air_vip_price_mtd_ly
    ,0 AS repeat_vip_product_order_air_vip_price_month_tot_lm
    ,0 AS repeat_vip_product_order_air_vip_price_month_tot_ly

    ,0 AS guest_product_order_air_vip_price
    ,0 AS guest_product_order_air_vip_price_mtd
    ,0 AS guest_product_order_air_vip_price_mtd_ly
    ,0 AS guest_product_order_air_vip_price_month_tot_lm
    ,0 AS guest_product_order_air_vip_price_month_tot_ly

    ,0 AS guest_product_order_retail_unit_price
    ,0 AS guest_product_order_retail_unit_price_mtd
    ,0 AS guest_product_order_retail_unit_price_mtd_ly
    ,0 AS guest_product_order_retail_unit_price_month_tot_lm
    ,0 AS guest_product_order_retail_unit_price_month_tot_ly

    ,0 AS activating_product_order_price_offered_amount
    ,0 AS activating_product_order_price_offered_amount_mtd
    ,0 AS activating_product_order_price_offered_amount_mtd_ly
    ,0 AS activating_product_order_price_offered_amount_month_tot_lm
    ,0 AS activating_product_order_price_offered_amount_month_tot_ly

    ,0 AS nonactivating_product_order_price_offered_amount
    ,0 AS nonactivating_product_order_price_offered_amount_mtd
    ,0 AS nonactivating_product_order_price_offered_amount_mtd_ly
    ,0 AS nonactivating_product_order_price_offered_amount_month_tot_lm
    ,0 AS nonactivating_product_order_price_offered_amount_month_tot_ly

    ,0 AS guest_product_order_price_offered_amount
    ,0 AS guest_product_order_price_offered_amount_mtd
    ,0 AS guest_product_order_price_offered_amount_mtd_ly
    ,0 AS guest_product_order_price_offered_amount_month_tot_lm
    ,0 AS guest_product_order_price_offered_amount_month_tot_ly

    ,0 AS repeat_vip_product_order_price_offered_amount
    ,0 AS repeat_vip_product_order_price_offered_amount_mtd
    ,0 AS repeat_vip_product_order_price_offered_amount_mtd_ly
    ,0 AS repeat_vip_product_order_price_offered_amount_month_tot_lm
    ,0 AS repeat_vip_product_order_price_offered_amount_month_tot_ly

    ,0 AS product_order_landed_product_cost_amount
    ,0 AS product_order_landed_product_cost_amount_mtd
    ,0 AS product_order_landed_product_cost_amount_mtd_ly
    ,0 AS product_order_landed_product_cost_amount_month_tot_lm
    ,0 AS product_order_landed_product_cost_amount_month_tot_ly

    ,0 AS activating_product_order_landed_product_cost_amount
    ,0 AS activating_product_order_landed_product_cost_amount_mtd
    ,0 AS activating_product_order_landed_product_cost_amount_mtd_ly
    ,0 AS activating_product_order_landed_product_cost_amount_month_tot_lm
    ,0 AS activating_product_order_landed_product_cost_amount_month_tot_ly

    ,0 AS nonactivating_product_order_landed_product_cost_amount
    ,0 AS nonactivating_product_order_landed_product_cost_amount_mtd
    ,0 AS nonactivating_product_order_landed_product_cost_amount_mtd_ly
    ,0 AS nonactivating_product_order_landed_product_cost_amount_month_tot_lm
    ,0 AS nonactivating_product_order_landed_product_cost_amount_month_tot_ly

    ,0 AS guest_product_order_landed_product_cost_amount
    ,0 AS guest_product_order_landed_product_cost_amount_mtd
    ,0 AS guest_product_order_landed_product_cost_amount_mtd_ly
    ,0 AS guest_product_order_landed_product_cost_amount_month_tot_lm
    ,0 AS guest_product_order_landed_product_cost_amount_month_tot_ly

    ,0 AS repeat_vip_product_order_landed_product_cost_amount
    ,0 AS repeat_vip_product_order_landed_product_cost_amount_mtd
    ,0 AS repeat_vip_product_order_landed_product_cost_amount_mtd_ly
    ,0 AS repeat_vip_product_order_landed_product_cost_amount_month_tot_lm
    ,0 AS repeat_vip_product_order_landed_product_cost_amount_month_tot_ly

    ,0 AS product_order_landed_product_cost_amount_accounting
    ,0 AS product_order_landed_product_cost_amount_accounting_mtd
    ,0 AS product_order_landed_product_cost_amount_accounting_mtd_ly
    ,0 AS product_order_landed_product_cost_amount_accounting_month_tot_lm
    ,0 AS product_order_landed_product_cost_amount_accounting_month_tot_ly

    ,0 AS activating_product_order_landed_product_cost_amount_accounting
    ,0 AS activating_product_order_landed_product_cost_amount_accounting_mtd
    ,0 AS activating_product_order_landed_product_cost_amount_accounting_mtd_ly
    ,0 AS activating_product_order_landed_product_cost_amount_accounting_month_tot_lm
    ,0 AS activating_product_order_landed_product_cost_amount_accounting_month_tot_ly

    ,0 AS nonactivating_product_order_landed_product_cost_amount_accounting
    ,0 AS nonactivating_product_order_landed_product_cost_amount_accounting_mtd
    ,0 AS nonactivating_product_order_landed_product_cost_amount_accounting_mtd_ly
    ,0 AS nonactivating_product_order_landed_product_cost_amount_accounting_month_tot_lm
    ,0 AS nonactivating_product_order_landed_product_cost_amount_accounting_month_tot_ly

    ,0 AS oracle_product_order_landed_product_cost_amount
    ,0 AS oracle_product_order_landed_product_cost_amount_mtd
    ,0 AS oracle_product_order_landed_product_cost_amount_mtd_ly
    ,0 AS oracle_product_order_landed_product_cost_amount_month_tot_lm
    ,0 AS oracle_product_order_landed_product_cost_amount_month_tot_ly

    ,0 AS product_order_shipping_cost_amount
    ,0 AS product_order_shipping_cost_amount_mtd
    ,0 AS product_order_shipping_cost_amount_mtd_ly
    ,0 AS product_order_shipping_cost_amount_month_tot_lm
    ,0 AS product_order_shipping_cost_amount_month_tot_ly

    ,0 AS product_order_shipping_supplies_cost_amount
    ,0 AS product_order_shipping_supplies_cost_amount_mtd
    ,0 AS product_order_shipping_supplies_cost_amount_mtd_ly
    ,0 AS product_order_shipping_supplies_cost_amount_month_tot_lm
    ,0 AS product_order_shipping_supplies_cost_amount_month_tot_ly

    ,0 AS product_order_direct_cogs_amount
    ,0 AS product_order_direct_cogs_amount_mtd
    ,0 AS product_order_direct_cogs_amount_mtd_ly
    ,0 AS product_order_direct_cogs_amount_month_tot_lm
    ,0 AS product_order_direct_cogs_amount_month_tot_ly

    ,0 AS product_order_cash_refund_amount
    ,0 AS product_order_cash_refund_amount_mtd
    ,0 AS product_order_cash_refund_amount_mtd_ly
    ,0 AS product_order_cash_refund_amount_month_tot_lm
    ,0 AS product_order_cash_refund_amount_month_tot_ly

    ,0 AS product_order_cash_chargeback_amount
    ,0 AS product_order_cash_chargeback_amount_mtd
    ,0 AS product_order_cash_chargeback_amount_mtd_ly
    ,0 AS product_order_cash_chargeback_amount_month_tot_lm
    ,0 AS product_order_cash_chargeback_amount_month_tot_ly

    ,0 AS product_cost_returned_resaleable_incl_reship_exch
    ,0 AS product_cost_returned_resaleable_incl_reship_exch_mtd
    ,0 AS product_cost_returned_resaleable_incl_reship_exch_mtd_ly
    ,0 AS product_cost_returned_resaleable_incl_reship_exch_month_tot_lm
    ,0 AS product_cost_returned_resaleable_incl_reship_exch_month_tot_ly

    ,0 AS product_cost_returned_resaleable_incl_reship_exch_accounting
    ,0 AS product_cost_returned_resaleable_incl_reship_exch_accounting_mtd
    ,0 AS product_cost_returned_resaleable_incl_reship_exch_accounting_mtd_ly
    ,0 AS product_cost_returned_resaleable_incl_reship_exch_accounting_month_tot_lm
    ,0 AS product_cost_returned_resaleable_incl_reship_exch_accounting_month_tot_ly

    ,0 AS product_order_cost_product_returned_damaged_amount
    ,0 AS product_order_cost_product_returned_damaged_amount_mtd
    ,0 AS product_order_cost_product_returned_damaged_amount_mtd_ly
    ,0 AS product_order_cost_product_returned_damaged_amount_month_tot_lm
    ,0 AS product_order_cost_product_returned_damaged_amount_month_tot_ly

    ,0 AS return_shipping_costs_incl_reship_exch
    ,0 AS return_shipping_costs_incl_reship_exch_mtd
    ,0 AS return_shipping_costs_incl_reship_exch_mtd_ly
    ,0 AS return_shipping_costs_incl_reship_exch_month_tot_lm
    ,0 AS return_shipping_costs_incl_reship_exch_month_tot_ly
    ,bgt.returns_shipping_cost AS return_shipping_costs_incl_reship_exch_budget
    ,0 AS return_shipping_costs_incl_reship_exch_budget_alt
    ,fcst.returns_shipping_cost AS return_shipping_costs_incl_reship_exch_forecast

    ,0 AS product_order_reship_order_count
    ,0 AS product_order_reship_order_count_mtd
    ,0 AS product_order_reship_order_count_mtd_ly
    ,0 AS product_order_reship_order_count_month_tot_lm
    ,0 AS product_order_reship_order_count_month_tot_ly

    ,0 AS product_order_reship_unit_count
    ,0 AS product_order_reship_unit_count_mtd
    ,0 AS product_order_reship_unit_count_mtd_ly
    ,0 AS product_order_reship_unit_count_month_tot_lm
    ,0 AS product_order_reship_unit_count_month_tot_ly

    ,0 AS activating_product_order_reship_unit_count
    ,0 AS activating_product_order_reship_unit_count_mtd
    ,0 AS activating_product_order_reship_unit_count_mtd_ly
    ,0 AS activating_product_order_reship_unit_count_month_tot_lm
    ,0 AS activating_product_order_reship_unit_count_month_tot_ly

    ,0 AS nonactivating_product_order_reship_unit_count
    ,0 AS nonactivating_product_order_reship_unit_count_mtd
    ,0 AS nonactivating_product_order_reship_unit_count_mtd_ly
    ,0 AS nonactivating_product_order_reship_unit_count_month_tot_lm
    ,0 AS nonactivating_product_order_reship_unit_count_month_tot_ly

    ,0 AS guest_product_order_reship_unit_count
    ,0 AS guest_product_order_reship_unit_count_mtd
    ,0 AS guest_product_order_reship_unit_count_mtd_ly
    ,0 AS guest_product_order_reship_unit_count_month_tot_lm
    ,0 AS guest_product_order_reship_unit_count_month_tot_ly

    ,0 AS repeat_vip_product_order_reship_unit_count
    ,0 AS repeat_vip_product_order_reship_unit_count_mtd
    ,0 AS repeat_vip_product_order_reship_unit_count_mtd_ly
    ,0 AS repeat_vip_product_order_reship_unit_count_month_tot_lm
    ,0 AS repeat_vip_product_order_reship_unit_count_month_tot_ly

    ,bgt.total_cogs_minus_cash AS total_cogs_budget
    ,0 AS total_cogs_budget_alt
    ,fcst.total_cogs_minus_cash AS total_cogs_forecast

    ,0 AS product_order_reship_product_cost_amount
    ,0 AS product_order_reship_product_cost_amount_mtd
    ,0 AS product_order_reship_product_cost_amount_mtd_ly
    ,0 AS product_order_reship_product_cost_amount_month_tot_lm
    ,0 AS product_order_reship_product_cost_amount_month_tot_ly

    ,0 AS activating_product_order_reship_product_cost_amount
    ,0 AS activating_product_order_reship_product_cost_amount_mtd
    ,0 AS activating_product_order_reship_product_cost_amount_mtd_ly
    ,0 AS activating_product_order_reship_product_cost_amount_month_tot_lm
    ,0 AS activating_product_order_reship_product_cost_amount_month_tot_ly

    ,0 AS nonactivating_product_order_reship_product_cost_amount
    ,0 AS nonactivating_product_order_reship_product_cost_amount_mtd
    ,0 AS nonactivating_product_order_reship_product_cost_amount_mtd_ly
    ,0 AS nonactivating_product_order_reship_product_cost_amount_month_tot_lm
    ,0 AS nonactivating_product_order_reship_product_cost_amount_month_tot_ly

    ,0 AS guest_product_order_reship_product_cost_amount
    ,0 AS guest_product_order_reship_product_cost_amount_mtd
    ,0 AS guest_product_order_reship_product_cost_amount_mtd_ly
    ,0 AS guest_product_order_reship_product_cost_amount_month_tot_lm
    ,0 AS guest_product_order_reship_product_cost_amount_month_tot_ly

    ,0 AS repeat_vip_product_order_reship_product_cost_amount
    ,0 AS repeat_vip_product_order_reship_product_cost_amount_mtd
    ,0 AS repeat_vip_product_order_reship_product_cost_amount_mtd_ly
    ,0 AS repeat_vip_product_order_reship_product_cost_amount_month_tot_lm
    ,0 AS repeat_vip_product_order_reship_product_cost_amount_month_tot_ly

    ,0 AS product_order_reship_product_cost_amount_accounting
    ,0 AS product_order_reship_product_cost_amount_accounting_mtd
    ,0 AS product_order_reship_product_cost_amount_accounting_mtd_ly
    ,0 AS product_order_reship_product_cost_amount_accounting_month_tot_lm
    ,0 AS product_order_reship_product_cost_amount_accounting_month_tot_ly

    ,0 AS activating_product_order_reship_product_cost_amount_accounting
    ,0 AS activating_product_order_reship_product_cost_amount_accounting_mtd
    ,0 AS activating_product_order_reship_product_cost_amount_accounting_mtd_ly
    ,0 AS activating_product_order_reship_product_cost_amount_accounting_month_tot_lm
    ,0 AS activating_product_order_reship_product_cost_amount_accounting_month_tot_ly

    ,0 AS nonactivating_product_order_reship_product_cost_amount_accounting
    ,0 AS nonactivating_product_order_reship_product_cost_amount_accounting_mtd
    ,0 AS nonactivating_product_order_reship_product_cost_amount_accounting_mtd_ly
    ,0 AS nonactivating_product_order_reship_product_cost_amount_accounting_month_tot_lm
    ,0 AS nonactivating_product_order_reship_product_cost_amount_accounting_month_tot_ly

    ,0 AS oracle_product_order_reship_product_cost_amount
    ,0 AS oracle_product_order_reship_product_cost_amount_mtd
    ,0 AS oracle_product_order_reship_product_cost_amount_mtd_ly
    ,0 AS oracle_product_order_reship_product_cost_amount_month_tot_lm
    ,0 AS oracle_product_order_reship_product_cost_amount_month_tot_ly

    ,0 AS product_order_reship_shipping_cost_amount
    ,0 AS product_order_reship_shipping_cost_amount_mtd
    ,0 AS product_order_reship_shipping_cost_amount_mtd_ly
    ,0 AS product_order_reship_shipping_cost_amount_month_tot_lm
    ,0 AS product_order_reship_shipping_cost_amount_month_tot_ly

    ,0 AS product_order_reship_shipping_supplies_cost_amount
    ,0 AS product_order_reship_shipping_supplies_cost_amount_mtd
    ,0 AS product_order_reship_shipping_supplies_cost_amount_mtd_ly
    ,0 AS product_order_reship_shipping_supplies_cost_amount_month_tot_lm
    ,0 AS product_order_reship_shipping_supplies_cost_amount_month_tot_ly

    ,0 AS product_order_exchange_order_count
    ,0 AS product_order_exchange_order_count_mtd
    ,0 AS product_order_exchange_order_count_mtd_ly
    ,0 AS product_order_exchange_order_count_month_tot_lm
    ,0 AS product_order_exchange_order_count_month_tot_ly

    ,bgt.reship_exch_orders_shipped AS product_order_reship_exch_order_count_budget
    ,0 AS product_order_reship_exch_order_count_budget_alt
    ,fcst.reship_exch_orders_shipped AS product_order_reship_exch_order_count_forecast

    ,0 AS product_order_exchange_unit_count
    ,0 AS product_order_exchange_unit_count_mtd
    ,0 AS product_order_exchange_unit_count_mtd_ly
    ,0 AS product_order_exchange_unit_count_month_tot_lm
    ,0 AS product_order_exchange_unit_count_month_tot_ly

    ,0 AS activating_product_order_exchange_unit_count
    ,0 AS activating_product_order_exchange_unit_count_mtd
    ,0 AS activating_product_order_exchange_unit_count_mtd_ly
    ,0 AS activating_product_order_exchange_unit_count_month_tot_lm
    ,0 AS activating_product_order_exchange_unit_count_month_tot_ly

    ,0 AS nonactivating_product_order_exchange_unit_count
    ,0 AS nonactivating_product_order_exchange_unit_count_mtd
    ,0 AS nonactivating_product_order_exchange_unit_count_mtd_ly
    ,0 AS nonactivating_product_order_exchange_unit_count_month_tot_lm
    ,0 AS nonactivating_product_order_exchange_unit_count_month_tot_ly

    ,0 AS guest_product_order_exchange_unit_count
    ,0 AS guest_product_order_exchange_unit_count_mtd
    ,0 AS guest_product_order_exchange_unit_count_mtd_ly
    ,0 AS guest_product_order_exchange_unit_count_month_tot_lm
    ,0 AS guest_product_order_exchange_unit_count_month_tot_ly

    ,0 AS repeat_vip_product_order_exchange_unit_count
    ,0 AS repeat_vip_product_order_exchange_unit_count_mtd
    ,0 AS repeat_vip_product_order_exchange_unit_count_mtd_ly
    ,0 AS repeat_vip_product_order_exchange_unit_count_month_tot_lm
    ,0 AS repeat_vip_product_order_exchange_unit_count_month_tot_ly

    ,0 AS product_order_exchange_product_cost_amount
    ,0 AS product_order_exchange_product_cost_amount_mtd
    ,0 AS product_order_exchange_product_cost_amount_mtd_ly
    ,0 AS product_order_exchange_product_cost_amount_month_tot_lm
    ,0 AS product_order_exchange_product_cost_amount_month_tot_ly

    ,0 AS activating_product_order_exchange_product_cost_amount
    ,0 AS activating_product_order_exchange_product_cost_amount_mtd
    ,0 AS activating_product_order_exchange_product_cost_amount_mtd_ly
    ,0 AS activating_product_order_exchange_product_cost_amount_month_tot_lm
    ,0 AS activating_product_order_exchange_product_cost_amount_month_tot_ly

    ,0 AS nonactivating_product_order_exchange_product_cost_amount
    ,0 AS nonactivating_product_order_exchange_product_cost_amount_mtd
    ,0 AS nonactivating_product_order_exchange_product_cost_amount_mtd_ly
    ,0 AS nonactivating_product_order_exchange_product_cost_amount_month_tot_lm
    ,0 AS nonactivating_product_order_exchange_product_cost_amount_month_tot_ly

    ,0 AS guest_product_order_exchange_product_cost_amount
    ,0 AS guest_product_order_exchange_product_cost_amount_mtd
    ,0 AS guest_product_order_exchange_product_cost_amount_mtd_ly
    ,0 AS guest_product_order_exchange_product_cost_amount_month_tot_lm
    ,0 AS guest_product_order_exchange_product_cost_amount_month_tot_ly

    ,0 AS repeat_vip_product_order_exchange_product_cost_amount
    ,0 AS repeat_vip_product_order_exchange_product_cost_amount_mtd
    ,0 AS repeat_vip_product_order_exchange_product_cost_amount_mtd_ly
    ,0 AS repeat_vip_product_order_exchange_product_cost_amount_month_tot_lm
    ,0 AS repeat_vip_product_order_exchange_product_cost_amount_month_tot_ly

    ,0 AS product_order_exchange_product_cost_amount_accounting
    ,0 AS product_order_exchange_product_cost_amount_accounting_mtd
    ,0 AS product_order_exchange_product_cost_amount_accounting_mtd_ly
    ,0 AS product_order_exchange_product_cost_amount_accounting_month_tot_lm
    ,0 AS product_order_exchange_product_cost_amount_accounting_month_tot_ly

    ,0 AS activating_product_order_exchange_product_cost_amount_accounting
    ,0 AS activating_product_order_exchange_product_cost_amount_accounting_mtd
    ,0 AS activating_product_order_exchange_product_cost_amount_accounting_mtd_ly
    ,0 AS activating_product_order_exchange_product_cost_amount_accounting_month_tot_lm
    ,0 AS activating_product_order_exchange_product_cost_amount_accounting_month_tot_ly

    ,0 AS nonactivating_product_order_exchange_product_cost_amount_accounting
    ,0 AS nonactivating_product_order_exchange_product_cost_amount_accounting_mtd
    ,0 AS nonactivating_product_order_exchange_product_cost_amount_accounting_mtd_ly
    ,0 AS nonactivating_product_order_exchange_product_cost_amount_accounting_month_tot_lm
    ,0 AS nonactivating_product_order_exchange_product_cost_amount_accounting_month_tot_ly

    ,0 AS oracle_product_order_exchange_product_cost_amount
    ,0 AS oracle_product_order_exchange_product_cost_amount_mtd
    ,0 AS oracle_product_order_exchange_product_cost_amount_mtd_ly
    ,0 AS oracle_product_order_exchange_product_cost_amount_month_tot_lm
    ,0 AS oracle_product_order_exchange_product_cost_amount_month_tot_ly

    ,0 AS product_order_exchange_shipping_cost_amount
    ,0 AS product_order_exchange_shipping_cost_amount_mtd
    ,0 AS product_order_exchange_shipping_cost_amount_mtd_ly
    ,0 AS product_order_exchange_shipping_cost_amount_month_tot_lm
    ,0 AS product_order_exchange_shipping_cost_amount_month_tot_ly

    ,0 AS product_order_exchange_shipping_supplies_cost_amount
    ,0 AS product_order_exchange_shipping_supplies_cost_amount_mtd
    ,0 AS product_order_exchange_shipping_supplies_cost_amount_mtd_ly
    ,0 AS product_order_exchange_shipping_supplies_cost_amount_month_tot_lm
    ,0 AS product_order_exchange_shipping_supplies_cost_amount_month_tot_ly

    ,0 AS product_gross_revenue
    ,0 AS product_gross_revenue_mtd
    ,0 AS product_gross_revenue_mtd_ly
    ,0 AS product_gross_revenue_month_tot_lm
    ,0 AS product_gross_revenue_month_tot_ly

    ,0 AS activating_product_gross_revenue
    ,0 AS activating_product_gross_revenue_mtd
    ,0 AS activating_product_gross_revenue_mtd_ly
    ,0 AS activating_product_gross_revenue_month_tot_lm
    ,0 AS activating_product_gross_revenue_month_tot_ly
    ,bgt.activating_gaap_gross_revenue AS activating_product_gross_revenue_budget
    ,0 AS activating_product_gross_revenue_budget_alt
    ,fcst.activating_gaap_gross_revenue AS activating_product_gross_revenue_forecast

    ,0 AS nonactivating_product_gross_revenue
    ,0 AS nonactivating_product_gross_revenue_mtd
    ,0 AS nonactivating_product_gross_revenue_mtd_ly
    ,0 AS nonactivating_product_gross_revenue_month_tot_lm
    ,0 AS nonactivating_product_gross_revenue_month_tot_ly
    ,bgt.repeat_gaap_gross_revenue AS nonactivating_product_gross_revenue_budget
    ,0 AS nonactivating_product_gross_revenue_budget_alt
    ,fcst.repeat_gaap_gross_revenue AS nonactivating_product_gross_revenue_forecast

    ,0 AS guest_product_gross_revenue
    ,0 AS guest_product_gross_revenue_mtd
    ,0 AS guest_product_gross_revenue_mtd_ly
    ,0 AS guest_product_gross_revenue_month_tot_lm
    ,0 AS guest_product_gross_revenue_month_tot_ly

    ,0 AS repeat_vip_product_gross_revenue
    ,0 AS repeat_vip_product_gross_revenue_mtd
    ,0 AS repeat_vip_product_gross_revenue_mtd_ly
    ,0 AS repeat_vip_product_gross_revenue_month_tot_lm
    ,0 AS repeat_vip_product_gross_revenue_month_tot_ly

    ,0 AS product_net_revenue
    ,0 AS product_net_revenue_mtd
    ,0 AS product_net_revenue_mtd_ly
    ,0 AS product_net_revenue_month_tot_lm
    ,0 AS product_net_revenue_month_tot_ly

    ,0 AS activating_product_net_revenue
    ,0 AS activating_product_net_revenue_mtd
    ,0 AS activating_product_net_revenue_mtd_ly
    ,0 AS activating_product_net_revenue_month_tot_lm
    ,0 AS activating_product_net_revenue_month_tot_ly
    ,bgt.activating_gaap_net_revenue AS activating_product_net_revenue_budget
    ,0 AS activating_product_net_revenue_budget_alt
    ,fcst.activating_gaap_net_revenue AS activating_product_net_revenue_forecast

    ,0 AS nonactivating_product_net_revenue
    ,0 AS nonactivating_product_net_revenue_mtd
    ,0 AS nonactivating_product_net_revenue_mtd_ly
    ,0 AS nonactivating_product_net_revenue_month_tot_lm
    ,0 AS nonactivating_product_net_revenue_month_tot_ly
    ,bgt.repeat_gaap_net_revenue AS nonactivating_product_net_revenue_budget
    ,0 AS nonactivating_product_net_revenue_budget_alt
    ,fcst.repeat_gaap_net_revenue AS nonactivating_product_net_revenue_forecast

    ,0 AS guest_product_net_revenue
    ,0 AS guest_product_net_revenue_mtd
    ,0 AS guest_product_net_revenue_mtd_ly
    ,0 AS guest_product_net_revenue_month_tot_lm
    ,0 AS guest_product_net_revenue_month_tot_ly

    ,0 AS repeat_vip_product_net_revenue
    ,0 AS repeat_vip_product_net_revenue_mtd
    ,0 AS repeat_vip_product_net_revenue_mtd_ly
    ,0 AS repeat_vip_product_net_revenue_month_tot_lm
    ,0 AS repeat_vip_product_net_revenue_month_tot_ly

    ,0 AS product_margin_pre_return
    ,0 AS product_margin_pre_return_mtd
    ,0 AS product_margin_pre_return_mtd_ly
    ,0 AS product_margin_pre_return_month_tot_lm
    ,0 AS product_margin_pre_return_month_tot_ly

    ,0 AS activating_product_margin_pre_return
    ,0 AS activating_product_margin_pre_return_mtd
    ,0 AS activating_product_margin_pre_return_mtd_ly
    ,0 AS activating_product_margin_pre_return_month_tot_lm
    ,0 AS activating_product_margin_pre_return_month_tot_ly
    ,bgt.acquisition_margin_$ AS activating_product_margin_pre_return_budget
    ,0 AS activating_product_margin_pre_return_budget_alt
    ,fcst.acquisition_margin_$ AS activating_product_margin_pre_return_forecast

    ,0 AS first_guest_product_margin_pre_return
    ,0 AS first_guest_product_margin_pre_return_mtd
    ,0 AS first_guest_product_margin_pre_return_mtd_ly
    ,0 AS first_guest_product_margin_pre_return_month_tot_lm
    ,0 AS first_guest_product_margin_pre_return_month_tot_ly

    ,0 AS product_gross_profit
    ,0 AS product_gross_profit_mtd
    ,0 AS product_gross_profit_mtd_ly
    ,0 AS product_gross_profit_month_tot_lm
    ,0 AS product_gross_profit_month_tot_ly

    ,0 AS activating_product_gross_profit
    ,0 AS activating_product_gross_profit_mtd
    ,0 AS activating_product_gross_profit_mtd_ly
    ,0 AS activating_product_gross_profit_month_tot_lm
    ,0 AS activating_product_gross_profit_month_tot_ly
    ,bgt.activating_gross_margin_$ AS activating_product_gross_profit_budget
    ,0 AS activating_product_gross_profit_budget_alt
    ,fcst.activating_gross_margin_$ AS activating_product_gross_profit_forecast

    ,0 AS nonactivating_product_gross_profit
    ,0 AS nonactivating_product_gross_profit_mtd
    ,0 AS nonactivating_product_gross_profit_mtd_ly
    ,0 AS nonactivating_product_gross_profit_month_tot_lm
    ,0 AS nonactivating_product_gross_profit_month_tot_ly
    ,bgt.repeat_gaap_gross_margin_$ AS nonactivating_product_gross_profit_budget
    ,0 AS nonactivating_product_gross_profit_budget_alt
    ,fcst.repeat_gaap_gross_margin_$ AS nonactivating_product_gross_profit_forecast

    ,0 AS guest_product_gross_profit
    ,0 AS guest_product_gross_profit_mtd
    ,0 AS guest_product_gross_profit_mtd_ly
    ,0 AS guest_product_gross_profit_month_tot_lm
    ,0 AS guest_product_gross_profit_month_tot_ly

    ,0 AS repeat_vip_product_gross_profit
    ,0 AS repeat_vip_product_gross_profit_mtd
    ,0 AS repeat_vip_product_gross_profit_mtd_ly
    ,0 AS repeat_vip_product_gross_profit_month_tot_lm
    ,0 AS repeat_vip_product_gross_profit_month_tot_ly

    ,0 AS product_order_cash_gross_revenue
    ,0 AS product_order_cash_gross_revenue_mtd
    ,0 AS product_order_cash_gross_revenue_mtd_ly
    ,0 AS product_order_cash_gross_revenue_month_tot_lm
    ,0 AS product_order_cash_gross_revenue_month_tot_ly

    ,0 AS activating_product_order_cash_gross_revenue
    ,0 AS activating_product_order_cash_gross_revenue_mtd
    ,0 AS activating_product_order_cash_gross_revenue_mtd_ly
    ,0 AS activating_product_order_cash_gross_revenue_month_tot_lm
    ,0 AS activating_product_order_cash_gross_revenue_month_tot_ly

    ,0 AS nonactivating_product_order_cash_gross_revenue
    ,0 AS nonactivating_product_order_cash_gross_revenue_mtd
    ,0 AS nonactivating_product_order_cash_gross_revenue_mtd_ly
    ,0 AS nonactivating_product_order_cash_gross_revenue_month_tot_lm
    ,0 AS nonactivating_product_order_cash_gross_revenue_month_tot_ly
    ,bgt.repeat_shipped_order_cash_collected AS nonactivating_product_order_cash_gross_revenue_budget
    ,0 AS nonactivating_product_order_cash_gross_revenue_budget_alt
    ,fcst.repeat_shipped_order_cash_collected AS nonactivating_product_order_cash_gross_revenue_forecast

    ,0 AS cash_gross_revenue
    ,0 AS cash_gross_revenue_mtd
    ,0 AS cash_gross_revenue_mtd_ly
    ,0 AS cash_gross_revenue_month_tot_lm
    ,0 AS cash_gross_revenue_month_tot_ly
    ,bgt.gross_cash_revenue AS cash_gross_revenue_budget
    ,0 AS cash_gross_revenue_budget_alt
    ,fcst.gross_cash_revenue AS cash_gross_revenue_forecast

    ,0 AS activating_cash_gross_revenue
    ,0 AS activating_cash_gross_revenue_mtd
    ,0 AS activating_cash_gross_revenue_mtd_ly
    ,0 AS activating_cash_gross_revenue_month_tot_lm
    ,0 AS activating_cash_gross_revenue_month_tot_ly

    ,0 AS nonactivating_cash_gross_revenue
    ,0 AS nonactivating_cash_gross_revenue_mtd
    ,0 AS nonactivating_cash_gross_revenue_mtd_ly
    ,0 AS nonactivating_cash_gross_revenue_month_tot_lm
    ,0 AS nonactivating_cash_gross_revenue_month_tot_ly

    ,0 AS cash_net_revenue
    ,0 AS cash_net_revenue_mtd
    ,0 AS cash_net_revenue_mtd_ly
    ,0 AS cash_net_revenue_month_tot_lm
    ,0 AS cash_net_revenue_month_tot_ly
    ,bgt.net_cash_revenue_total AS cash_net_revenue_budget
    ,0 AS cash_net_revenue_budget_alt
    ,fcst.net_cash_revenue_total AS cash_net_revenue_forecast

    ,0 AS activating_cash_net_revenue
    ,0 AS activating_cash_net_revenue_mtd
    ,0 AS activating_cash_net_revenue_mtd_ly
    ,0 AS activating_cash_net_revenue_month_tot_lm
    ,0 AS activating_cash_net_revenue_month_tot_ly
    ,bgt.activating_gaap_gross_revenue AS activating_cash_net_revenue_budget
    ,0 AS activating_cash_net_revenue_budget_alt
    ,fcst.activating_gaap_gross_revenue AS activating_cash_net_revenue_forecast

    ,0 AS nonactivating_cash_net_revenue
    ,0 AS nonactivating_cash_net_revenue_mtd
    ,0 AS nonactivating_cash_net_revenue_mtd_ly
    ,0 AS nonactivating_cash_net_revenue_month_tot_lm
    ,0 AS nonactivating_cash_net_revenue_month_tot_ly
    ,bgt.repeat_net_cash_revenue AS nonactivating_cash_net_revenue_budget
    ,0 AS nonactivating_cash_net_revenue_budget_alt
    ,fcst.repeat_net_cash_revenue AS nonactivating_cash_net_revenue_forecast

    ,0 AS cash_net_revenue_budgeted_fx
    ,0 AS cash_net_revenue_budgeted_fx_mtd
    ,0 AS cash_net_revenue_budgeted_fx_mtd_ly
    ,0 AS cash_net_revenue_budgeted_fx_month_tot_lm
    ,0 AS cash_net_revenue_budgeted_fx_month_tot_ly

    ,0 AS cash_gross_profit
    ,0 AS cash_gross_profit_mtd
    ,0 AS cash_gross_profit_mtd_ly
    ,0 AS cash_gross_profit_month_tot_lm
    ,0 AS cash_gross_profit_month_tot_ly
    ,bgt.cash_gross_margin AS cash_gross_profit_budget
    ,0 AS cash_gross_profit_budget_alt
    ,fcst.cash_gross_margin AS cash_gross_profit_forecast

    ,0 AS nonactivating_cash_gross_profit
    ,0 AS nonactivating_cash_gross_profit_mtd
    ,0 AS nonactivating_cash_gross_profit_mtd_ly
    ,0 AS nonactivating_cash_gross_profit_month_tot_lm
    ,0 AS nonactivating_cash_gross_profit_month_tot_ly
    ,bgt.repeat_cash_gross_margin AS nonactivating_cash_gross_profit_budget
    ,0 AS nonactivating_cash_gross_profit_budget_alt
    ,fcst.repeat_cash_gross_margin AS nonactivating_cash_gross_profit_forecast

    ,0 AS billed_credit_cash_transaction_count
    ,0 AS billed_credit_cash_transaction_count_mtd
    ,0 AS billed_credit_cash_transaction_count_mtd_ly
    ,0 AS billed_credit_cash_transaction_count_month_tot_lm
    ,0 AS billed_credit_cash_transaction_count_month_tot_ly

    ,0 AS billed_credits_successful_on_retry
    ,0 AS billed_credits_successful_on_retry_mtd
    ,0 AS billed_credits_successful_on_retry_mtd_ly
    ,0 AS billed_credits_successful_on_retry_month_tot_lm
    ,0 AS billed_credits_successful_on_retry_month_tot_ly

    ,0 AS billing_cash_refund_amount
    ,0 AS billing_cash_refund_amount_mtd
    ,0 AS billing_cash_refund_amount_mtd_ly
    ,0 AS billing_cash_refund_amount_month_tot_lm
    ,0 AS billing_cash_refund_amount_month_tot_ly

    ,0 AS billing_cash_chargeback_amount
    ,0 AS billing_cash_chargeback_amount_mtd
    ,0 AS billing_cash_chargeback_amount_mtd_ly
    ,0 AS billing_cash_chargeback_amount_month_tot_lm
    ,0 AS billing_cash_chargeback_amount_month_tot_ly

    ,0 AS billed_credit_cash_transaction_amount
    ,0 AS billed_credit_cash_transaction_amount_mtd
    ,0 AS billed_credit_cash_transaction_amount_mtd_ly
    ,0 AS billed_credit_cash_transaction_amount_month_tot_lm
    ,0 AS billed_credit_cash_transaction_amount_month_tot_ly
    ,bgt.membership_credits_charged AS billed_credit_cash_transaction_amount_budget
    ,0 AS billed_credit_cash_transaction_amount_budget_alt
    ,fcst.membership_credits_charged AS billed_credit_cash_transaction_amount_forecast

    ,0 AS billed_credit_cash_refund_chargeback_amount
    ,0 AS billed_credit_cash_refund_chargeback_amount_mtd
    ,0 AS billed_credit_cash_refund_chargeback_amount_mtd_ly
    ,0 AS billed_credit_cash_refund_chargeback_amount_month_tot_lm
    ,0 AS billed_credit_cash_refund_chargeback_amount_month_tot_ly
    ,bgt.membership_credits_refunded_plus_chargebacks AS billed_credit_cash_refund_chargeback_amount_budget
    ,0 AS billed_credit_cash_refund_chargeback_amount_budget_alt
    ,fcst.membership_credits_refunded_plus_chargebacks AS billed_credit_cash_refund_chargeback_amount_forecast

    ,0 AS activating_billed_cash_credit_redeemed_amount

    ,0 AS billed_cash_credit_redeemed_amount
    ,0 AS billed_cash_credit_redeemed_amount_mtd
    ,0 AS billed_cash_credit_redeemed_amount_mtd_ly
    ,0 AS billed_cash_credit_redeemed_amount_month_tot_lm
    ,0 AS billed_cash_credit_redeemed_amount_month_tot_ly
    ,bgt.membership_credits_redeemed AS billed_cash_credit_redeemed_amount_budget
    ,0 AS billed_cash_credit_redeemed_amount_budget_alt
    ,fcst.membership_credits_redeemed AS billed_cash_credit_redeemed_amount_forecast

    ,0 AS misc_cogs_amount
    ,0 AS misc_cogs_amount_mtd
    ,0 AS misc_cogs_amount_mtd_ly
    ,0 AS misc_cogs_amount_month_tot_lm
    ,0 AS misc_cogs_amount_month_tot_ly
    ,bgt.product_cost_markdown AS misc_cogs_amount_budget
    ,0 AS misc_cogs_amount_budget_alt
    ,fcst.product_cost_markdown AS misc_cogs_amount_forecast

    ,0 AS product_order_cash_gross_profit
    ,0 AS product_order_cash_gross_profit_mtd
    ,0 AS product_order_cash_gross_profit_mtd_ly
    ,0 AS product_order_cash_gross_profit_month_tot_lm
    ,0 AS product_order_cash_gross_profit_month_tot_ly

    ,0 AS nonactivating_product_order_cash_gross_profit
    ,0 AS nonactivating_product_order_cash_gross_profit_mtd
    ,0 AS nonactivating_product_order_cash_gross_profit_mtd_ly
    ,0 AS nonactivating_product_order_cash_gross_profit_month_tot_lm
    ,0 AS nonactivating_product_order_cash_gross_profit_month_tot_ly
    ,bgt.repeat_cash_gross_margin AS nonactivating_product_order_cash_gross_profit_budget
    ,0 AS nonactivating_product_order_cash_gross_profit_budget_alt
    ,fcst.repeat_cash_gross_margin AS nonactivating_product_order_cash_gross_profit_forecast

    ,0 AS same_month_billed_credit_redeemed
    ,0 AS same_month_billed_credit_redeemed_mtd
    ,0 AS same_month_billed_credit_redeemed_mtd_ly
    ,0 AS same_month_billed_credit_redeemed_month_tot_lm
    ,0 AS same_month_billed_credit_redeemed_month_tot_ly

    ,0 AS billed_credit_cash_refund_count
    ,0 AS billed_credit_cash_refund_count_mtd
    ,0 AS billed_credit_cash_refund_count_mtd_ly
    ,0 AS billed_credit_cash_refund_count_month_tot_lm
    ,0 AS billed_credit_cash_refund_count_month_tot_ly

    ,0 AS cash_variable_contribution_profit
    ,0 AS cash_variable_contribution_profit_mtd
    ,0 AS cash_variable_contribution_profit_mtd_ly
    ,0 AS cash_variable_contribution_profit_month_tot_lm
    ,0 AS cash_variable_contribution_profit_month_tot_ly

    ,bgt.cash_contribution_after_media AS cash_contribution_after_media_budget
    ,0 AS cash_contribution_after_media_budget_alt
    ,fcst.cash_contribution_after_media AS cash_contribution_after_media_forecast

    ,0 AS billed_cash_credit_redeemed_equivalent_count
    ,0 AS billed_cash_credit_redeemed_equivalent_count_mtd
    ,0 AS billed_cash_credit_redeemed_equivalent_count_mtd_ly
    ,0 AS billed_cash_credit_redeemed_equivalent_count_month_tot_lm
    ,0 AS billed_cash_credit_redeemed_equivalent_count_month_tot_ly

    ,bgt.membership_credits_redeemed_count AS membership_credits_redeemed_count_budget
    ,0 AS membership_credits_redeemed_count_budget_alt
    ,fcst.membership_credits_redeemed_count AS membership_credits_redeemed_count_forecast

    ,0 AS billed_cash_credit_cancelled_equivalent_count
    ,0 AS billed_cash_credit_cancelled_equivalent_count_mtd
    ,0 AS billed_cash_credit_cancelled_equivalent_count_mtd_ly
    ,0 AS billed_cash_credit_cancelled_equivalent_count_month_tot_lm
    ,0 AS billed_cash_credit_cancelled_equivalent_count_month_tot_ly

    ,bgt.membership_credits_cancelled_count AS membership_credits_cancelled_count_budget
    ,0 AS membership_credits_cancelled_count_budget_alt
    ,fcst.membership_credits_cancelled_count AS membership_credits_cancelled_count_forecast

    ,bgt.cash_gross_margin_percent AS cash_gross_profit_percent_of_net_cash_budget
    ,0 AS cash_gross_profit_percent_of_net_cash_budget_alt
    ,fcst.cash_gross_margin_percent AS cash_gross_profit_percent_of_net_cash_forecast

    ,bgt.cash_refunds * -1 AS product_order_and_billing_cash_refund_budget
    ,0 AS product_order_and_billing_cash_refund_budget_alt
    ,fcst.cash_refunds * -1 AS product_order_and_billing_cash_refund_forecast

    ,bgt.chargebacks * -1 AS product_order_and_billing_chargebacks_budget
    ,0 AS product_order_and_billing_chargebacks_budget_alt
    ,fcst.chargebacks * -1 AS product_order_and_billing_chargebacks_forecast

    ,bgt.refunds_and_chargebacks_as_percent_of_gross_cash AS refunds_plus_chargebacks_as_percent_of_cash_gross_rev_budget
    ,0 AS refunds_plus_chargebacks_as_percent_of_cash_gross_rev_budget_alt
    ,fcst.refunds_and_chargebacks_as_percent_of_gross_cash AS refunds_plus_chargebacks_as_percent_of_cash_gross_rev_forecast

    ,bgt.activating_gaap_gross_revenue/NULLIFZERO(bgt.activating_order_count) AS activating_aov_budget
    ,0 AS activating_aov_budget_alt
    ,fcst.activating_gaap_gross_revenue/NULLIFZERO(fcst.activating_order_count) AS activating_aov_forecast

    ,bgt.activating_units_per_transaction AS activating_upt_budget
    ,0 AS activating_upt_budget_alt
    ,fcst.activating_units_per_transaction AS activating_upt_forecast

    ,bgt.activating_discount_percent AS activating_discount_percent_budget
    ,0 AS activating_discount_percent_budget_alt
    ,fcst.activating_discount_percent AS activating_discount_percent_forecast

    ,bgt.activating_gross_margin_percent AS activating_product_gross_profit_percent_budget
    ,0 AS activating_product_gross_profit_percent_budget_alt
    ,fcst.activating_gross_margin_percent AS activating_product_gross_profit_percent_forecast

    ,bgt.acquisition_margin_$__div__order AS activating_product_margin_pre_return_per_order_budget
    ,0 AS activating_product_margin_pre_return_per_order_budget_alt
    ,fcst.acquisition_margin_$__div__order AS activating_product_margin_pre_return_per_order_forecast

    ,bgt.acquisition_margin_percent AS activating_product_margin_pre_return_percent_budget
    ,0 AS activating_product_margin_pre_return_percent_budget_alt
    ,fcst.acquisition_margin_percent AS activating_product_margin_pre_return_percent_forecast

    ,bgt.repeat_gaap_gross_revenue/NULLIFZERO(bgt.repeat_order_count) AS nonactivating_aov_budget
    ,0 AS nonactivating_aov_budget_alt
    ,fcst.repeat_gaap_gross_revenue/NULLIFZERO(fcst.repeat_order_count) AS nonactivating_aov_forecast

    ,bgt.repeat_units_per_transaction AS nonactivating_upt_budget
    ,0 AS nonactivating_upt_budget_alt
    ,fcst.repeat_units_per_transaction AS nonactivating_upt_forecast

    ,bgt.repeat_discount_percent AS nonactivating_discount_percent_budget
    ,0 AS nonactivating_discount_percent_budget_alt
    ,fcst.repeat_discount_percent AS nonactivating_discount_percent_forecast

    ,bgt.repeat_gaap_gross_margin_percent AS nonactivating_product_gross_profit_percent_budget
    ,0 AS nonactivating_product_gross_profit_percent_budget_alt
    ,fcst.repeat_gaap_gross_margin_percent AS nonactivating_product_gross_profit_percent_forecast

    ,bgt.freight_out_cost AS shipping_cost_incl_reship_exch_budget
    ,0 AS shipping_cost_incl_reship_exch_budget_alt
    ,fcst.freight_out_cost AS shipping_cost_incl_reship_exch_forecast

    ,bgt.outbound_shipping_supplies AS shipping_supplies_cost_incl_reship_exch_budget
    ,0 AS shipping_supplies_cost_incl_reship_exch_budget_alt
    ,fcst.outbound_shipping_supplies AS shipping_supplies_cost_incl_reship_exch_forecast

    ,bgt.product_cost_calc AS product_cost_calc_budget
    ,0 AS product_cost_calc_budget_alt
    ,fcst.product_cost_calc AS product_cost_calc_forecast

    ,0 AS product_order_tariff_amount

    ,0 AS product_order_shipping_discount_amount

    ,0 AS product_order_shipping_revenue_before_discount_amount

    ,0 AS product_order_tax_amount

    ,0 AS product_order_cash_transaction_amount

    ,0 AS product_order_cash_credit_redeemed_amount

    ,0 AS product_order_loyalty_unit_count

    ,0 AS product_order_outfit_component_unit_count

    ,0 AS product_order_outfit_count

    ,0 AS product_order_discounted_unit_count

    ,0 AS product_order_zero_revenue_unit_count

    ,0 AS product_order_cash_credit_refund_amount
    ,0 AS product_order_cash_credit_refund_amount_mtd
    ,0 AS product_order_cash_credit_refund_amount_mtd_ly
    ,0 AS product_order_cash_credit_refund_amount_month_tot_lm
    ,0 AS product_order_cash_credit_refund_amount_month_tot_ly
    ,NULL AS product_order_cash_credit_refund_amount_budget
    ,NULL AS product_order_cash_credit_refund_amount_budget_alt
    ,NULL AS product_order_cash_credit_refund_amount_forecast

    ,0 AS activating_product_order_cash_credit_refund_amount
    ,0 AS activating_product_order_cash_credit_refund_amount_mtd
    ,0 AS activating_product_order_cash_credit_refund_amount_mtd_ly
    ,0 AS activating_product_order_cash_credit_refund_amount_month_tot_lm
    ,0 AS activating_product_order_cash_credit_refund_amount_month_tot_ly
    ,NULL AS activating_product_order_cash_credit_refund_amount_budget
    ,NULL AS activating_product_order_cash_credit_refund_amount_budget_alt
    ,NULL AS activating_product_order_cash_credit_refund_amount_forecast

    ,0 AS nonactivating_product_order_cash_credit_refund_amount
    ,0 AS nonactivating_product_order_cash_credit_refund_amount_mtd
    ,0 AS nonactivating_product_order_cash_credit_refund_amount_mtd_ly
    ,0 AS nonactivating_product_order_cash_credit_refund_amount_month_tot_lm
    ,0 AS nonactivating_product_order_cash_credit_refund_amount_month_tot_ly
    ,NULL AS nonactivating_product_order_cash_credit_refund_amount_budget
    ,NULL AS nonactivating_product_order_cash_credit_refund_amount_budget_alt
    ,NULL AS nonactivating_product_order_cash_credit_refund_amount_forecast

    ,0 AS product_order_noncash_credit_refund_amount
    ,0 AS product_order_noncash_credit_refund_amount_mtd
    ,0 AS product_order_noncash_credit_refund_amount_mtd_ly
    ,0 AS product_order_noncash_credit_refund_amount_month_tot_lm
    ,0 AS product_order_noncash_credit_refund_amount_month_tot_ly
    ,NULL AS product_order_noncash_credit_refund_amount_budget
    ,NULL AS product_order_noncash_credit_refund_amount_budget_alt
    ,NULL AS product_order_noncash_credit_refund_amount_forecast

    ,0 AS product_order_exchange_direct_cogs_amount

    ,0 AS product_order_selling_expenses_amount

    ,0 AS product_order_payment_processing_cost_amount

    ,0 AS product_order_variable_gms_cost_amount

    ,0 AS product_order_variable_warehouse_cost_amount

    ,0 AS billing_selling_expenses_amount

    ,0 AS billing_payment_processing_cost_amount

    ,0 AS billing_variable_gms_cost_amount

    ,0 AS product_order_amount_to_pay

    ,0 AS product_gross_revenue_excl_shipping
    ,0 AS product_gross_revenue_excl_shipping_mtd
    ,0 AS product_gross_revenue_excl_shipping_mtd_ly
    ,0 AS product_gross_revenue_excl_shipping_month_tot_lm
    ,0 AS product_gross_revenue_excl_shipping_month_tot_ly

    ,0 AS activating_product_gross_revenue_excl_shipping
    ,0 AS activating_product_gross_revenue_excl_shipping_mtd
    ,0 AS activating_product_gross_revenue_excl_shipping_mtd_ly
    ,0 AS activating_product_gross_revenue_excl_shipping_month_tot_lm
    ,0 AS activating_product_gross_revenue_excl_shipping_month_tot_ly

    ,0 AS nonactivating_product_gross_revenue_excl_shipping
    ,0 AS nonactivating_product_gross_revenue_excl_shipping_mtd
    ,0 AS nonactivating_product_gross_revenue_excl_shipping_mtd_ly
    ,0 AS nonactivating_product_gross_revenue_excl_shipping_month_tot_lm
    ,0 AS nonactivating_product_gross_revenue_excl_shipping_month_tot_ly

    ,0 AS guest_product_gross_revenue_excl_shipping
    ,0 AS guest_product_gross_revenue_excl_shipping_mtd
    ,0 AS guest_product_gross_revenue_excl_shipping_mtd_ly
    ,0 AS guest_product_gross_revenue_excl_shipping_month_tot_lm
    ,0 AS guest_product_gross_revenue_excl_shipping_month_tot_ly

    ,0 AS repeat_vip_product_gross_revenue_excl_shipping
    ,0 AS repeat_vip_product_gross_revenue_excl_shipping_mtd
    ,0 AS repeat_vip_product_gross_revenue_excl_shipping_mtd_ly
    ,0 AS repeat_vip_product_gross_revenue_excl_shipping_month_tot_lm
    ,0 AS repeat_vip_product_gross_revenue_excl_shipping_month_tot_ly

    ,0 AS product_margin_pre_return_excl_shipping
    ,0 AS activating_product_margin_pre_return_excl_shipping

    ,0 AS product_variable_contribution_profit

    ,0 AS product_order_cash_net_revenue

    ,0 AS product_order_cash_margin_pre_return

    ,0 AS billing_cash_gross_revenue

    ,0 AS billing_cash_net_revenue

    ,0 AS billing_order_transaction_count
    ,0 AS billing_order_transaction_count_mtd
    ,0 AS billing_order_transaction_count_mtd_ly
    ,0 AS billing_order_transaction_count_month_tot_lm
    ,0 AS billing_order_transaction_count_month_tot_ly
    ,bgt.membership_credits_charged_count AS billing_order_transaction_count_budget
    ,0 AS billing_order_transaction_count_budget_alt
    ,fcst.membership_credits_charged_count AS billing_order_transaction_count_forecast

    ,0 AS membership_fee_cash_transaction_amount
    ,0 AS membership_fee_cash_transaction_amount_mtd
    ,0 AS membership_fee_cash_transaction_amount_mtd_ly
    ,0 AS membership_fee_cash_transaction_amount_month_tot_lm
    ,0 AS membership_fee_cash_transaction_amount_month_tot_ly

    ,0 AS gift_card_transaction_amount
    ,0 AS gift_card_transaction_amount_mtd
    ,0 AS gift_card_transaction_amount_mtd_ly
    ,0 AS gift_card_transaction_amount_month_tot_lm
    ,0 AS gift_card_transaction_amount_month_tot_ly

    ,0 AS legacy_credit_cash_transaction_amount
    ,0 AS legacy_credit_cash_transaction_amount_mtd
    ,0 AS legacy_credit_cash_transaction_amount_mtd_ly
    ,0 AS legacy_credit_cash_transaction_amount_month_tot_lm
    ,0 AS legacy_credit_cash_transaction_amount_month_tot_ly

    ,0 AS membership_fee_cash_refund_chargeback_amount
    ,0 AS membership_fee_cash_refund_chargeback_amount_mtd
    ,0 AS membership_fee_cash_refund_chargeback_amount_mtd_ly
    ,0 AS membership_fee_cash_refund_chargeback_amount_month_tot_lm
    ,0 AS membership_fee_cash_refund_chargeback_amount_month_tot_ly

    ,0 AS gift_card_cash_refund_chargeback_amount
    ,0 AS gift_card_cash_refund_chargeback_amount_mtd
    ,0 AS gift_card_cash_refund_chargeback_amount_mtd_ly
    ,0 AS gift_card_cash_refund_chargeback_amount_month_tot_lm
    ,0 AS gift_card_cash_refund_chargeback_amount_month_tot_ly

    ,0 AS legacy_credit_cash_refund_chargeback_amount
    ,0 AS legacy_credit_cash_refund_chargeback_amount_mtd
    ,0 AS legacy_credit_cash_refund_chargeback_amount_mtd_ly
    ,0 AS legacy_credit_cash_refund_chargeback_amount_month_tot_lm
    ,0 AS legacy_credit_cash_refund_chargeback_amount_month_tot_ly

    ,0 AS billed_cash_credit_issued_amount

    ,0 AS billed_cash_credit_cancelled_amount

    ,0 AS billed_cash_credit_expired_amount

    ,0 AS billed_cash_credit_issued_equivalent_count
    ,0 AS billed_cash_credit_issued_equivalent_count_mtd
    ,0 AS billed_cash_credit_issued_equivalent_count_mtd_ly
    ,0 AS billed_cash_credit_issued_equivalent_count_month_tot_lm
    ,0 AS billed_cash_credit_issued_equivalent_count_month_tot_ly

    ,0 AS billed_cash_credit_expired_equivalent_count

    ,0 AS refund_cash_credit_issued_amount
    ,0 AS refund_cash_credit_issued_amount_mtd
    ,0 AS refund_cash_credit_issued_amount_mtd_ly
    ,0 AS refund_cash_credit_issued_amount_month_tot_lm
    ,0 AS refund_cash_credit_issued_amount_month_tot_ly
    ,bgt.refunded_as_credit AS refund_cash_credit_issued_amount_budget
    ,0 AS refund_cash_credit_issued_amount_budget_alt
    ,fcst.refunded_as_credit AS refund_cash_credit_issued_amount_forecast

    ,0 AS refund_cash_credit_redeemed_amount
    ,0 AS refund_cash_credit_redeemed_amount_mtd
    ,0 AS refund_cash_credit_redeemed_amount_mtd_ly
    ,0 AS refund_cash_credit_redeemed_amount_month_tot_lm
    ,0 AS refund_cash_credit_redeemed_amount_month_tot_ly
    ,bgt.refund_credit_redeemed AS refund_cash_credit_redeemed_amount_budget
    ,0 AS refund_cash_credit_redeemed_amount_budget_alt
    ,fcst.refund_credit_redeemed AS refund_cash_credit_redeemed_amount_forecast

    ,0 AS refund_cash_credit_cancelled_amount
    ,0 AS refund_cash_credit_cancelled_amount_mtd
    ,0 AS refund_cash_credit_cancelled_amount_mtd_ly
    ,0 AS refund_cash_credit_cancelled_amount_month_tot_lm
    ,0 AS refund_cash_credit_cancelled_amount_month_tot_ly

    ,bgt.net_unredeemed_refund_credit AS net_unredeemed_refund_credit_budget
    ,0 AS net_unredeemed_refund_credit_budget_alt
    ,fcst.net_unredeemed_refund_credit AS net_unredeemed_refund_credit_forecast

    ,0 AS refund_cash_credit_expired_amount

    ,0 AS other_cash_credit_issued_amount

    ,0 AS other_cash_credit_redeemed_amount
    ,0 AS other_cash_credit_redeemed_amount_mtd
    ,0 AS other_cash_credit_redeemed_amount_mtd_ly
    ,0 AS other_cash_credit_redeemed_amount_month_tot_lm
    ,0 AS other_cash_credit_redeemed_amount_month_tot_ly

    ,0 AS other_cash_credit_cancelled_amount

    ,0 AS other_cash_credit_expired_amount

    ,0 AS cash_gift_card_redeemed_amount
    ,0 AS activating_cash_gift_card_redeemed_amount
    ,0 AS nonactivating_cash_gift_card_redeemed_amount
    ,0 AS guest_cash_gift_card_redeemed_amount

    ,0 AS noncash_credit_issued_amount
    ,0 AS noncash_credit_issued_amount_mtd
    ,0 AS noncash_credit_issued_amount_mtd_ly
    ,0 AS noncash_credit_issued_amount_month_tot_lm
    ,0 AS noncash_credit_issued_amount_month_tot_ly

    ,0 AS noncash_credit_cancelled_amount

    ,0 AS noncash_credit_expired_amount

    ,bgt.net_unredeemed_credit_billings AS net_unredeemed_credit_billings_budget
    ,0 AS net_unredeemed_credit_billings_budget_alt
    ,fcst.net_unredeemed_credit_billings AS net_unredeemed_credit_billings_forecast

    ,0 AS product_order_non_token_subtotal_excl_tariff_amount
    ,0 AS product_order_non_token_subtotal_excl_tariff_amount_mtd
    ,0 AS product_order_non_token_subtotal_excl_tariff_amount_mtd_ly
    ,0 AS product_order_non_token_subtotal_excl_tariff_amount_month_tot_lm
    ,0 AS product_order_non_token_subtotal_excl_tariff_amount_month_tot_ly

    ,0 AS product_order_non_token_unit_count
    ,0 AS product_order_non_token_unit_count_mtd
    ,0 AS product_order_non_token_unit_count_mtd_ly
    ,0 AS product_order_non_token_unit_count_month_tot_lm
    ,0 AS product_order_non_token_unit_count_month_tot_ly

    ,0 AS nonactivating_product_order_non_token_subtotal_excl_tariff_amount
    ,0 AS nonactivating_product_order_non_token_subtotal_excl_tariff_amount_mtd
    ,0 AS nonactivating_product_order_non_token_subtotal_excl_tariff_amount_mtd_ly
    ,0 AS nonactivating_product_order_non_token_subtotal_excl_tariff_amount_month_tot_lm
    ,0 AS nonactivating_product_order_non_token_subtotal_excl_tariff_amount_month_tot_ly

    ,0 AS nonactivating_product_order_non_token_unit_count
    ,0 AS nonactivating_product_order_non_token_unit_count_mtd
    ,0 AS nonactivating_product_order_non_token_unit_count_mtd_ly
    ,0 AS nonactivating_product_order_non_token_unit_count_month_tot_lm
    ,0 AS nonactivating_product_order_non_token_unit_count_month_tot_ly

    ,0 AS pending_order_count
    ,0 AS pending_order_local_amount
    ,0 AS pending_order_usd_amount
    ,0 AS pending_order_eur_amount
    ,0 AS pending_order_cash_margin_pre_return_local_amount
    ,0 AS pending_order_cash_margin_pre_return_usd_amount
    ,0 AS pending_order_cash_margin_pre_return_eur_amount

/*Acquisition*/
    ,0 AS leads
    ,0 AS leads_mtd
    ,0 AS leads_mtd_ly
    ,0 AS leads_month_tot_lm
    ,0 AS leads_month_tot_ly
    ,bgt.leads AS leads_budget
    ,0 AS leads_budget_alt
    ,fcst.leads AS leads_forecast

    ,0 AS primary_leads
    ,0 AS primary_leads_mtd
    ,0 AS primary_leads_mtd_ly
    ,0 AS primary_leads_month_tot_lm
    ,0 AS primary_leads_month_tot_ly

    ,0 AS reactivated_leads
    ,0 AS reactivated_leads_mtd
    ,0 AS reactivated_leads_mtd_ly
    ,0 AS reactivated_leads_month_tot_lm
    ,0 AS reactivated_leads_month_tot_ly

    ,0 AS new_vips
    ,0 AS new_vips_mtd
    ,0 AS new_vips_mtd_ly
    ,0 AS new_vips_month_tot_lm
    ,0 AS new_vips_month_tot_ly
    ,bgt.total_new_vips AS new_vips_budget
    ,0 AS new_vips_budget_alt
    ,fcst.total_new_vips AS new_vips_forecast

    ,0 AS reactivated_vips
    ,0 AS reactivated_vips_mtd
    ,0 AS reactivated_vips_mtd_ly
    ,0 AS reactivated_vips_month_tot_lm
    ,0 AS reactivated_vips_month_tot_ly

    ,0 AS vips_from_reactivated_leads_m1
    ,0 AS vips_from_reactivated_leads_m1_mtd
    ,0 AS vips_from_reactivated_leads_m1_mtd_ly
    ,0 AS vips_from_reactivated_leads_m1_month_tot_lm
    ,0 AS vips_from_reactivated_leads_m1_month_tot_ly

    ,0 AS paid_vips
    ,0 AS paid_vips_mtd
    ,0 AS paid_vips_mtd_ly
    ,0 AS paid_vips_month_tot_lm
    ,0 AS paid_vips_month_tot_ly

    ,0 AS unpaid_vips
    ,0 AS unpaid_vips_mtd
    ,0 AS unpaid_vips_mtd_ly
    ,0 AS unpaid_vips_month_tot_lm
    ,0 AS unpaid_vips_month_tot_ly

    ,0 AS new_vips_m1
    ,0 AS new_vips_m1_mtd
    ,0 AS new_vips_m1_mtd_ly
    ,0 AS new_vips_m1_month_tot_lm
    ,0 AS new_vips_m1_month_tot_ly
    ,bgt.m1_vips AS new_vips_m1_budget
    ,0 AS new_vips_m1_budget_alt
    ,fcst.m1_vips AS new_vips_m1_forecast

    ,0 AS paid_vips_m1
    ,0 AS paid_vips_m1_mtd
    ,0 AS paid_vips_m1_mtd_ly
    ,0 AS paid_vips_m1_month_tot_lm
    ,0 AS paid_vips_m1_month_tot_ly

    ,0 AS cancels
    ,0 AS cancels_mtd
    ,0 AS cancels_mtd_ly
    ,0 AS cancels_month_tot_lm
    ,0 AS cancels_month_tot_ly
    ,bgt.cancels AS cancels_budget
    ,0 AS cancels_budget_alt
    ,fcst.cancels AS cancels_forecast

    ,0 AS m1_cancels
    ,0 AS m1_cancels_mtd
    ,0 AS m1_cancels_mtd_ly
    ,0 AS m1_cancels_month_tot_lm
    ,0 AS m1_cancels_month_tot_ly

    ,0 AS bop_vips
    ,0 AS bop_vips_mtd
    ,0 AS bop_vips_mtd_ly
    ,0 AS bop_vips_month_tot_lm
    ,0 AS bop_vips_month_tot_ly
    ,bgt.bom_vips AS bop_vips_budget
    ,0 AS bop_vips_budget_alt
    ,fcst.bom_vips AS bop_vips_forecast

    ,0 AS media_spend
    ,0 AS media_spend_mtd
    ,0 AS media_spend_mtd_ly
    ,0 AS media_spend_month_tot_lm
    ,0 AS media_spend_month_tot_ly
    ,bgt.media_spend AS media_spend_budget
    ,0 AS media_spend_budget_alt
    ,fcst.media_spend AS media_spend_forecast

    ,0 AS product_order_return_unit_count
    ,0 AS product_order_return_unit_count_mtd
    ,0 AS product_order_return_unit_count_mtd_ly
    ,0 AS product_order_return_unit_count_month_tot_lm
    ,0 AS product_order_return_unit_count_month_tot_ly

    ,0 AS merch_purchase_count
    ,0 AS merch_purchase_count_mtd
    ,0 AS merch_purchase_count_mtd_ly
    ,0 AS merch_purchase_count_month_tot_lm
    ,0 AS merch_purchase_count_month_tot_ly

    ,0 AS merch_purchase_hyperion_count
    ,0 AS merch_purchase_hyperion_count_mtd
    ,0 AS merch_purchase_hyperion_count_mtd_ly
    ,0 AS merch_purchase_hyperion_count_month_tot_lm
    ,0 AS merch_purchase_hyperion_count_month_tot_ly

    ,0 AS bom_vips

    ,0 AS skip_count
/*Meta*/
    ,'Extended' AS report_date_type
    ,bgt.meta_update_datetime AS snapshot_datetime_budget
    ,fcst.meta_update_datetime AS snapshot_datetime_forecast
    ,$execution_start_time AS meta_update_datetime
    ,$execution_start_time AS meta_create_datetime
FROM _future_budget_forecast_scaffold dcbcja
LEFT JOIN (SELECT DISTINCT bu, report_mapping FROM reference.finance_bu_mapping) AS fbm
       ON dcbcja.report_mapping = fbm.report_mapping
LEFT JOIN (SELECT * FROM reference.finance_budget_forecast WHERE version='Budget') AS bgt
   ON CASE WHEN dcbcja.currency_object='usd' THEN 'usdbtfx' ELSE dcbcja.currency_object END = lower(bgt.currency_type)
       AND fbm.bu = bgt.bu
       AND dcbcja.currency_type = bgt.currency
       AND date_trunc('month',dcbcja.date) = bgt.financial_date::date
LEFT JOIN (SELECT * FROM reference.finance_budget_forecast WHERE version='Forecast') AS fcst
   ON CASE WHEN dcbcja.currency_object='usd' THEN 'usdbtfx' ELSE dcbcja.currency_object END = lower(fcst.currency_type)
       AND fbm.bu = fcst.bu
       AND dcbcja.currency_type = fcst.currency
       AND date_trunc('month',dcbcja.date) = fcst.financial_date::date
;

/*
    We are updating the report_date for Daily Cash and Weekly KPI to NULL to generate the latest report
 */
--UPDATE reference.config_ssrs_date_param
--SET report_date = NULL,
--    meta_update_datetime = CURRENT_TIMESTAMP()
--WHERE report IN ('Daily Cash', 'Weekly KPI')
--    AND report_date IS NOT NULL;
--
--UPDATE stg.meta_table_dependency_watermark
--SET
--    high_watermark_datetime = $execution_start_time,
--    new_high_watermark_datetime = $execution_start_time
--WHERE table_name = $target_table;
