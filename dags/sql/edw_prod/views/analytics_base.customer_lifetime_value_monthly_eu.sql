CREATE OR REPLACE VIEW analytics_base.customer_lifetime_value_monthly_eu (
    month_date,
    customer_id,
    activation_key,
    gender,
    store_id,
    guest_cohort_month_date,
    vip_cohort_month_date,
    is_bop_vip,
    product_gross_revenue,
    product_net_revenue,
    product_margin_pre_return,
    product_gross_profit,
    cash_gross_revenue,
    cash_net_revenue,
    cash_gross_profit,
    meta_create_datetime,
    meta_update_datetime
    ) AS
SELECT
    c.month_date,
    c.customer_id,
    c.activation_key,
    c.gender,
    c.store_id,
    c.guest_cohort_month_date,
    c.vip_cohort_month_date,
    c.is_bop_vip,
    IFNULL(a.product_gross_revenue, 0) AS product_gross_revenue,
    IFNULL(a.product_net_revenue, 0) AS product_net_revenue,
    IFNULL(a.product_margin_pre_return, 0) AS product_margin_pre_return,
    IFNULL(a.product_gross_profit, 0) AS product_gross_profit,
    IFNULL(a.cash_gross_revenue, 0) AS cash_gross_revenue,
    IFNULL(a.cash_net_revenue, 0) AS cash_net_revenue,
    IFNULL(a.cash_gross_profit, 0) AS cash_gross_profit,
    LEAST(COALESCE(c.meta_create_datetime, '9999-12-31'), COALESCE(a.meta_create_datetime, '9999-12-31')) AS meta_create_datetime,
    GREATEST(COALESCE(c.meta_update_datetime, '1900-01-01'), COALESCE(a.meta_update_datetime, '1900-01-01')) AS meta_update_datetime
FROM analytics_base.customer_lifetime_value_monthly_cust_eu AS c
    LEFT JOIN analytics_base.customer_lifetime_value_monthly_agg_eu AS a
        ON a.month_date = c.month_date
        AND a.customer_id = c.customer_id
        AND a.activation_key = c.activation_key
