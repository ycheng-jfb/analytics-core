SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMPORARY TABLE _fso_agg AS
SELECT 'SXF'                                                                            AS customer_segment,
    ds.store_brand                                                                      AS brand,
    DATE_TRUNC('month', fo.order_completion_local_datetime::DATE)                       AS vip_cohort,
    fso.date                                                                            AS payment_date,
    'transaction or monthly billing'                                                    AS billing_type,
    ds.store_region                                                                     AS store_region,
    COUNT(DISTINCT fso.customer_id)                                                     AS customer_count,
    SUM(fso.cash_gross_revenue)                                                         AS cash_gross_revenue,
    SUM(fso.cash_net_revenue)                                                           AS cash_net_rev,
    SUM(fso.product_order_cash_margin_pre_return) + SUM(fso.billing_cash_gross_revenue) AS cash_margin_pre_return,
    SUM(fso.cash_gross_profit)                                                          AS cash_gross_profit,
    SUM(fso.product_order_cash_refund_amount)                                           AS product_order_cash_refund_amount,
    SUM(fso.product_order_cash_chargeback_amount)                                       AS product_order_cash_chargeback_amount,
    SUM(fso.billing_cash_refund_amount)                                                 AS billing_cash_refund_amount,
    SUM(fso.billing_cash_chargeback_amount)                                             AS billing_cash_chargeback_amount,
    SUM(fso.product_order_landed_product_cost_amount)                                   AS product_order_landed_product_cost_amount,
    SUM(fso.product_order_exchange_product_cost_amount)                                 AS product_order_exchange_product_cost_amount,
    SUM(fso.product_order_reship_product_cost_amount)                                   AS product_order_reship_product_cost_amount,
    SUM(fso.product_order_shipping_cost_amount)                                         AS product_order_shipping_cost_amount,
    SUM(fso.product_order_reship_shipping_cost_amount)                                  AS product_order_reship_shipping_cost_amount,
    SUM(fso.product_order_exchange_shipping_cost_amount)                                AS product_order_exchange_shipping_cost_amount,
    SUM(fso.product_order_return_shipping_cost_amount)                                  AS product_order_return_shipping_cost_amount,
    SUM(fso.product_order_shipping_supplies_cost_amount)                                AS product_order_shipping_supplies_cost_amount,
    SUM(fso.product_order_exchange_shipping_supplies_cost_amount)                       AS product_order_exchange_shipping_supplies_cost_amount,
    SUM(fso.product_order_reship_shipping_supplies_cost_amount)                         AS product_order_reship_shipping_supplies_cost_amount,
    SUM(fso.product_order_misc_cogs_amount)                                             AS product_order_misc_cogs_amount,
    SUM(fso.product_order_cost_product_returned_resaleable_amount)                      AS product_order_cost_product_returned_resaleable_amount,
    SUM(fso.product_order_variable_gms_cost_amount)                                     AS product_order_variable_gms_cost_amount,
    SUM(fso.billing_variable_gms_cost_amount)                                           AS billing_variable_gms_cost_amount,
    SUM(fso.product_order_payment_processing_cost_amount)                               AS product_order_payment_processing_cost_amount,
    SUM(fso.billing_payment_processing_cost_amount)                                     AS billing_payment_processing_cost_amount,
    SUM(fso.product_order_variable_warehouse_cost_amount)                               AS product_order_variable_warehouse_cost_amount
FROM edw_prod.analytics_base.finance_sales_ops fso
JOIN edw_prod.stg.fact_activation fa ON fa.activation_key = fso.activation_key
JOIN edw_prod.stg.fact_order fo ON fo.order_id = fa.order_id
JOIN edw_prod.stg.dim_store ds ON ds.store_id = fso.store_id
JOIN edw_prod.stg.dim_customer c ON c.customer_id = fso.customer_id
WHERE currency_object = 'usd'
    AND date_object = 'shipped'
    AND store_region IN ('NA', 'EU')
    AND store_brand IN ('Savage X')
    AND vip_cohort >= '2021-01-01'
GROUP BY customer_segment,
    brand,
    vip_cohort,
    payment_date,
    store_region;

CREATE OR REPLACE TEMPORARY TABLE _gc_expenses_sxf AS
SELECT month_date,
    segment,
    gms_fixed,
    gfc_fixed,
    selling_expenses,
    cash_net_revenue,
    gms_fixed_pct,
    gfc_fixed_pct,
    selling_exp_pct,
    back_office,
    gms_back_office,
    gfc_back_office,
    gms_back_office_pct,
    gfc_back_office_pct,
    CASE
        WHEN segment = 'SXNA' THEN 'NA'
        WHEN segment = 'SXEU' THEN 'EU'
    END AS store_region
FROM fabletics.gc_fixed_variable_expenses_sxf;

BEGIN TRANSACTION;

TRUNCATE TABLE sxf.gc_historical_customer_request_sxf;

INSERT INTO sxf.gc_historical_customer_request_sxf (
    customer_segment,
    brand,
    vip_cohort,
    payment_date,
    billing_type,
    store_region,
    customer_count,
    cash_gross_revenue,
    cash_net_rev,
    cash_margin_pre_return,
    cash_gross_profit,
    product_order_cash_refund_amount,
    product_order_cash_chargeback_amount,
    billing_cash_refund_amount,
    billing_cash_chargeback_amount,
    product_order_landed_product_cost_amount,
    product_order_exchange_product_cost_amount,
    product_order_reship_product_cost_amount,
    product_order_shipping_cost_amount,
    product_order_reship_shipping_cost_amount,
    product_order_exchange_shipping_cost_amount,
    product_order_return_shipping_cost_amount,
    product_order_shipping_supplies_cost_amount,
    product_order_exchange_shipping_supplies_cost_amount,
    product_order_reship_shipping_supplies_cost_amount,
    product_order_misc_cogs_amount,
    product_order_cost_product_returned_resaleable_amount,
    product_order_variable_gms_cost_amount,
    billing_variable_gms_cost_amount,
    product_order_payment_processing_cost_amount,
    billing_payment_processing_cost_amount,
    product_order_variable_warehouse_cost_amount,
    selling_expenses,
    gfc_fixed,
    gms_fixed,
    meta_create_datetime
)
SELECT
    fso.customer_segment,
    fso.brand,
    fso.vip_cohort,
    fso.payment_date,
    fso.billing_type,
    fso.store_region,
    fso.customer_count,
    fso.cash_gross_revenue,
    fso.cash_net_rev,
    fso.cash_margin_pre_return,
    fso.cash_gross_profit,
    fso.product_order_cash_refund_amount,
    fso.product_order_cash_chargeback_amount,
    fso.billing_cash_refund_amount,
    fso.billing_cash_chargeback_amount,
    fso.product_order_landed_product_cost_amount,
    fso.product_order_exchange_product_cost_amount,
    fso.product_order_reship_product_cost_amount,
    fso.product_order_shipping_cost_amount,
    fso.product_order_reship_shipping_cost_amount,
    fso.product_order_exchange_shipping_cost_amount,
    fso.product_order_return_shipping_cost_amount,
    fso.product_order_shipping_supplies_cost_amount,
    fso.product_order_exchange_shipping_supplies_cost_amount,
    fso.product_order_reship_shipping_supplies_cost_amount,
    fso.product_order_misc_cogs_amount,
    fso.product_order_cost_product_returned_resaleable_amount,
    fso.product_order_variable_gms_cost_amount,
    fso.billing_variable_gms_cost_amount,
    fso.product_order_payment_processing_cost_amount,
    fso.billing_payment_processing_cost_amount,
    fso.product_order_variable_warehouse_cost_amount,
    gc.selling_exp_pct * fso.cash_net_rev AS selling_expenses,
    gc.gfc_fixed_pct * fso.cash_net_rev AS gfc_fixed,
    gc.gms_fixed_pct * fso.cash_net_rev AS gms_fixed,
    $execution_start_time AS meta_create_datetime
FROM _fso_agg fso
JOIN _gc_expenses_sxf gc ON gc.month_date = DATE_TRUNC('MONTH', fso.payment_date)
    AND fso.store_region = gc.store_region;

COMMIT;
