CREATE OR REPLACE SECURE VIEW public.historical_customer_collections_request_sxf (
    customer_id,
    customer_segment,
    brand,
    vip_cohort,
    payment_date,
    billing_type,
    store_region,
    cash_gross_revenue,
    cash_net_rev,
    cash_margin_pre_return,
    cash_gross_profit,
    refunds_and_chargebacks,
    total_cogs,
    total_expenses
)
AS
SELECT DISTINCT fso.customer_id,
    'SXF'                                                                                   AS customer_segment,
    st.store_brand                                                                          AS brand,
    DATE_TRUNC('month', fo.order_completion_local_datetime::DATE)                           AS vip_cohort,
    fso.date                                                                                AS payment_date,
    'transaction or monthly billing'                                                        AS billing_type,
    store_region,
    SUM(fso.cash_gross_revenue)                                                             AS cash_gross_revenue,
    SUM(fso.cash_net_revenue)                                                               AS cash_net_rev,
    SUM(fso.product_order_cash_margin_pre_return)
        + SUM(billing_cash_gross_revenue)                                                   AS cash_margin_pre_return,
    SUM(cash_gross_profit)                                                                  AS cash_gross_profit,
    SUM(product_order_cash_refund_amount)
        + SUM(product_order_cash_chargeback_amount)
        + SUM(billing_cash_refund_amount)
        + SUM(billing_cash_chargeback_amount)                                               AS refunds_and_chargebacks,
    SUM(product_order_landed_product_cost_amount
        + product_order_exchange_product_cost_amount
        + product_order_reship_product_cost_amount
        + product_order_shipping_cost_amount
        + product_order_reship_shipping_cost_amount
        + product_order_exchange_shipping_cost_amount
        + product_order_return_shipping_cost_amount
        + product_order_shipping_supplies_cost_amount
        + product_order_exchange_shipping_supplies_cost_amount
        + product_order_reship_shipping_supplies_cost_amount
        + product_order_misc_cogs_amount
        - product_order_cost_product_returned_resaleable_amount)                            AS total_cogs,
    SUM(product_order_variable_gms_cost_amount)
        + SUM(billing_variable_gms_cost_amount)
        + SUM(product_order_payment_processing_cost_amount)
        + SUM(billing_payment_processing_cost_amount)
        + SUM(product_order_variable_warehouse_cost_amount)
        + (gc.selling_exp_pct * (cash_net_rev))
        + (gc.gfc_fixed_pct * (cash_net_rev))
        + (gc.gms_fixed_pct * (cash_net_rev))                                               AS total_expenses
FROM edw_prod.analytics_base.finance_sales_ops fso
JOIN edw_prod.stg.fact_activation a ON a.activation_key = fso.activation_key
JOIN edw_prod.stg.fact_order fo ON fo.order_id = a.order_id
JOIN edw_prod.stg.dim_store st ON st.store_id = fso.store_id
JOIN edw_prod.stg.dim_customer c ON c.customer_id = fso.customer_id
JOIN (
    SELECT *
    FROM reporting_prod.fabletics.gc_fixed_variable_expenses_sxf
    WHERE segment = 'SXNA'
) gc ON gc.month_date = DATE_TRUNC('month', payment_date)
WHERE currency_object = 'usd'
    AND date_object = 'shipped'
    AND store_region = 'NA'
    AND store_brand IN ('Savage X')
    AND vip_cohort >= '2021-01-01'
GROUP BY fso.customer_id,
    customer_segment,
    brand,
    vip_cohort,
    payment_date,
    store_region,
    gc.selling_exp_pct,
    gc.gfc_fixed_pct,
    gc.gms_fixed_pct
UNION
SELECT DISTINCT fso.customer_id,
    'SXF'                                                                                   AS customer_segment,
    st.store_brand                                                                          AS brand,
    DATE_TRUNC('month', fo.order_completion_local_datetime::DATE)                           AS vip_cohort,
    fso.date                                                                                AS payment_date,
    'transaction or monthly billing'                                                        AS billing_type,
    store_region,
    SUM(fso.cash_gross_revenue)                                                             AS cash_gross_revenue,
    SUM(fso.cash_net_revenue)                                                               AS cash_net_rev,
    SUM(fso.product_order_cash_margin_pre_return)
        + SUM(billing_cash_gross_revenue)                                                   AS cash_margin_pre_return,
    SUM(cash_gross_profit)                                                                  AS cash_gross_profit,
    SUM(product_order_cash_refund_amount)
        + SUM(product_order_cash_chargeback_amount)
        + SUM(billing_cash_refund_amount)
        + SUM(billing_cash_chargeback_amount)                                               AS refunds_and_chargebacks,
    SUM(product_order_landed_product_cost_amount
        + product_order_exchange_product_cost_amount
        + product_order_reship_product_cost_amount
        + product_order_shipping_cost_amount
        + product_order_reship_shipping_cost_amount
        + product_order_exchange_shipping_cost_amount
        + product_order_return_shipping_cost_amount
        + product_order_shipping_supplies_cost_amount
        + product_order_exchange_shipping_supplies_cost_amount
        + product_order_reship_shipping_supplies_cost_amount
        + product_order_misc_cogs_amount
        - product_order_cost_product_returned_resaleable_amount)                            AS total_cogs,
    SUM(product_order_variable_gms_cost_amount)
        + SUM(billing_variable_gms_cost_amount)
        + SUM(product_order_payment_processing_cost_amount)
        + SUM(billing_payment_processing_cost_amount)
        + SUM(product_order_variable_warehouse_cost_amount)
        + (gc.selling_exp_pct * (cash_net_rev))
        + (gc.gfc_fixed_pct * (cash_net_rev))
        + (gc.gms_fixed_pct * (cash_net_rev))                                               AS total_expenses
FROM edw_prod.analytics_base.finance_sales_ops fso
JOIN edw_prod.stg.fact_activation a ON a.activation_key = fso.activation_key
JOIN edw_prod.stg.fact_order fo ON fo.order_id = a.order_id
JOIN edw_prod.stg.dim_store st ON st.store_id = fso.store_id
JOIN edw_prod.stg.dim_customer c ON c.customer_id = fso.customer_id
JOIN (
    SELECT *
    FROM reporting_prod.fabletics.gc_fixed_variable_expenses_sxf
    WHERE segment = 'SXEU'
) gc ON gc.month_date = DATE_TRUNC('month', payment_date)
WHERE currency_object = 'usd'
    AND date_object = 'shipped'
    AND store_region = 'EU'
    AND store_brand IN ('Savage X')
    AND vip_cohort >= '2021-01-01'
GROUP BY fso.customer_id,
    customer_segment,
    brand,
    vip_cohort,
    payment_date,
    store_region,
    gc.selling_exp_pct,
    gc.gfc_fixed_pct,
    gc.gms_fixed_pct;
