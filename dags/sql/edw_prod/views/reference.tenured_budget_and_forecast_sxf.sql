CREATE OR REPLACE VIEW reference.tenured_budget_and_forecast_sxf AS
SELECT
    date,
    type,
    region,
    'All' as country,
    tenure,
    attrition_rate * 100.00 as attrition_rate,
    successful_credit_billing_rate * 100.00 as successful_credit_billing_rate,
    credit_redemption_rate * 100.00 as credit_redemption_rate,
    credit_refund_rate * 100.00 as credit_refund_rate,
    orders_shipped_rate * 100.00 as orders_shipped_rate,
    cash_net_revenue_per_vip,
    cash_net_gross_margin_per_vip,
    cumulative_retention_rate,
    cum_cash_net_revenue_per_vip,
    cum_cash_net_gross_margin_per_vip,
    activating_aov,
    activating_upt,
    repeat_aov,
    repeat_upt,
    activating_discount_rate * 100.00 as activating_discount_rate,
    repeat_discount_rate * 100.00 as repeat_discount_rate,
    meta_create_datetime,
    meta_update_datetime
FROM lake_view.sharepoint.tenured_budget_and_forecast_sxf_by_region

UNION ALL

SELECT
    date,
    type,
    region,
    country,
    tenure,
    attrition_rate * 100.00 as attrition_rate,
    successful_credit_billing_rate * 100.00 as successful_credit_billing_rate,
    credit_redemption_rate * 100.00 as credit_redemption_rate,
    credit_refund_rate * 100.00 as credit_refund_rate,
    orders_shipped_rate * 100.00 as orders_shipped_rate,
    cash_net_revenue_per_vip,
    cash_net_gross_margin_per_vip,
    cumulative_retention_rate,
    cum_cash_net_revenue_per_vip,
    cum_cash_net_gross_margin_per_vip,
    activating_aov,
    activating_upt,
    repeat_aov,
    repeat_upt,
    activating_discount_rate * 100.00 as activating_discount_rate,
    repeat_discount_rate * 100.00 as repeat_discount_rate,
    meta_create_datetime,
    meta_update_datetime
FROM lake_view.sharepoint.tenured_budget_and_forecast_sxf_by_country;
