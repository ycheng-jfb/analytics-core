CREATE OR REPLACE VIEW reporting.vw_billing_cycle_rates_1st_through_5th AS
SELECT
    store_brand,
    store_region,
    store_country,
    is_retail_vip,
    period_month_date AS month,
    max_day,
    tenure,
    login_unique AS "Login - Unique",
    skip_unique AS "Skip - Unique",
    snooze_unique AS "Snooze - Unique",
    skip_no_login AS "Skip - No Login",
    purchase_prod_order_unique AS "Purchase (Prod Order) - Unique",
    Skip_or_Purchase_Unique AS "Skip or Purchase - Unique",
    bop_vips AS "bop vips",
    cancels_unique AS "Cancels - Unique",
    eligible_to_be_billed AS "Eligible to be Billed",
    product_gross_revenue AS "Product Gross Revenue"
FROM reporting.billing_cycle_rates_1st_through_5th
ORDER BY month;
