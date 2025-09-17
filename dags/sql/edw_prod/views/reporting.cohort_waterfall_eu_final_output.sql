CREATE OR REPLACE VIEW reporting.cohort_waterfall_eu_final_output AS
SELECT clvm.vip_cohort_month_date,
    CASE WHEN ds.store_brand = 'Fabletics' AND clvm.gender = 'M' THEN 'Fabletics Mens'
        WHEN ds.store_brand = 'Fabletics' THEN 'Fabletics Womens'
        WHEN ds.store_brand = 'Yitty' THEN 'Yitty'
        ELSE ds.store_brand END AS store_brand,
    clvm.month_date,
    ds.store_region,
    ds.store_country,
    ds.store_name,
    COUNT(DISTINCT clvm.customer_id) AS initial_cohort_count,
    SUM(clvm.product_gross_revenue) AS product_gross_revenue,
    SUM(clvm.product_net_revenue) AS product_net_revenue,
    SUM(clvm.product_gross_profit) AS product_gross_profit,
    SUM(clvm.cash_gross_revenue) AS cash_gross_revenue,
    SUM(clvm.cash_net_revenue) AS cash_net_revenue,
    SUM(clvm.cash_gross_profit) AS cash_gross_profit,
    MAX(clvm.meta_update_datetime) AS last_refresh_datetime
FROM analytics_base.customer_lifetime_value_monthly_eu clvm
JOIN data_model.dim_store ds ON ds.store_id = clvm.store_id
WHERE ds.store_region = 'EU'
GROUP BY clvm.vip_cohort_month_date,
    CASE WHEN ds.store_brand = 'Fabletics' AND clvm.gender = 'M' THEN 'Fabletics Mens'
        WHEN ds.store_brand = 'Fabletics' THEN 'Fabletics Womens'
        WHEN ds.store_brand = 'Yitty' THEN 'Yitty'
        ELSE ds.store_brand END,
    clvm.month_date,
    ds.store_region,
    ds.store_country,
    ds.store_name;
