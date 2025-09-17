CREATE OR REPLACE VIEW reporting.media_payback AS
SELECT ds.store_id,
       ds.store_brand,
       ds.store_region,
       ds.store_country,
       clvm.vip_cohort_month_date,
       clvm.month_date,
       clvm.gender,
       SUM(customer_acquisition_cost)                                                            AS media_spend,
       COUNT(IFF(is_bop_vip, 1, NULL))                                                           AS bop_vips,
       COUNT(IFF(is_cancel, 1, NULL))                                                            AS cancels,
       COUNT(IFF(fa.customer_id IS NOT NULL, 1, NULL))                                           AS cancels_comp,
       COUNT(DISTINCT clvm.customer_id)                                                          AS initial_cohort_size,
       COUNT(DISTINCT IFF(clvm.month_date = clvl.vip_cohort_month_date, clvl.customer_id, NULL)) AS vips,
       SUM(clvm.cash_gross_revenue)                                                              AS cash_gross_revenue,
       SUM(clvm.cash_net_revenue)                                                                AS cash_net_revenue,
       SUM(clvm.cash_gross_profit)                                                               AS cash_gross_profit,
       SUM(clvm.cash_variable_contribution_profit)                                               AS cash_variable_contribution_profit,
       SUM(clvm.product_margin_pre_return)                                                       AS product_margin_pre_return,
       SUM(clvm.product_gross_revenue)                                                           AS product_gross_revenue,
       SUM(clvm.product_net_revenue)                                                             AS product_net_revenue,
       SUM(clvm.product_gross_profit)                                                            AS product_gross_profit,
       SUM(clvm.product_order_count)                                                             AS product_order_count,
       SUM(clvm.product_order_cash_credit_refund_amount)                                         AS refunded_cash_amount_product_orders,
       SUM(clvm.monthly_billed_credit_cash_refund_chargeback_amount)                             AS refund_chargeback_cash_amount_credit_billings,
       MAX(clvm.meta_update_datetime)                                                            AS last_refresh_datetime
FROM analytics_base.customer_lifetime_value_monthly clvm
         JOIN data_model.fact_activation fa ON fa.activation_key = clvm.activation_key
         JOIN data_model.dim_store ds ON ds.store_id = clvm.store_id
         LEFT JOIN analytics_base.customer_lifetime_value_ltd clvl ON clvl.vip_cohort_month_date = clvm.month_date
    AND clvl.customer_id = clvm.customer_id
    AND clvl.vip_cohort_month_date = clvm.vip_cohort_month_date
WHERE clvm.vip_cohort_month_date >= '2018-01-01'
GROUP BY ds.store_id,
         ds.store_brand,
         ds.store_region,
         ds.store_country,
         clvm.vip_cohort_month_date,
         clvm.month_date,
         clvm.gender;
