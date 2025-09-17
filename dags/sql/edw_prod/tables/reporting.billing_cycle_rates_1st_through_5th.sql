CREATE TABLE IF NOT EXISTS reporting.billing_cycle_rates_1st_through_5th
(
    store_brand                VARCHAR(20),
    store_region               VARCHAR(32),
    store_country              VARCHAR(50),
    is_retail_vip              BOOLEAN,
    period_month_date          DATE,
    max_day                    NUMBER(2, 0),
    tenure                     VARCHAR(10),
    login_unique               NUMBER(18, 0),
    skip_unique                NUMBER(18, 0),
    snooze_unique              NUMBER(18, 0),
    skip_no_login              NUMBER(18, 0),
    purchase_prod_order_unique NUMBER(18, 0),
    skip_or_purchase_unique    NUMBER(18, 0),
    bop_vips                   NUMBER(18, 0),
    cancels_unique             NUMBER(18, 0),
    eligible_to_be_billed      NUMBER(18, 0),
    product_gross_revenue      NUMBER(18, 0),
    meta_row_hash              NUMBER(36, 0),
    meta_create_datetime       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(3),
    meta_update_datetime       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(3)
);
