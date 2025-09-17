-- DROP TABLE IF EXISTS analytics_base.customer_lifetime_value_monthly_cust;

CREATE TABLE analytics_base.customer_lifetime_value_monthly_cust_eu (
    month_date DATE,
    customer_id NUMBER(38,0),
    activation_key NUMBER(38,0),
    gender VARCHAR(16),
    store_id NUMBER(38,0),
    vip_store_id NUMBER(38,0),
    guest_cohort_month_date DATE,
    vip_cohort_month_date DATE,
    is_bop_vip BOOLEAN,
    meta_row_hash NUMBER(19,0),
    meta_create_datetime TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(3),
    meta_update_datetime TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (month_date, customer_id, activation_key),
    FOREIGN KEY (customer_id) REFERENCES stg.dim_customer (customer_id),
    FOREIGN KEY (store_id) REFERENCES stg.dim_store (store_id)
) CLUSTER BY (month_date);

GRANT OWNERSHIP ON TABLE analytics_base.customer_lifetime_value_monthly_cust_eu TO ROLE __EDW_ANALYTICS_BASE_RWC COPY CURRENT GRANTS;

-- ALTER TABLE analytics_base.customer_lifetime_value_monthly_cust ADD COLUMN guest_cohort_month_date DATE;

/*
-- Insert unknown record
INSERT INTO analytics_base.customer_lifetime_value_monthly_cust (month_date, customer_id, activation_key, meta_create_datetime, meta_update_datetime)
SELECT '1900-01-01' AS month_date, -1 AS customer_id, -1 AS activation_key, CURRENT_TIMESTAMP AS meta_create_datetime, CURRENT_TIMESTAMP AS meta_update_datetime
WHERE NOT EXISTS (SELECT TRUE AS is_exists FROM analytics_base.customer_lifetime_value_monthly_cust WHERE month_date = '1900-01-01' AND customer_id = -1 AND activation_key = -1);

-- Update unknown record
UPDATE analytics_base.customer_lifetime_value_monthly_cust
SET
    --month_date = '1900-01-01',
    --customer_id = -1,
    --activation_key = -1,
    first_activation_key = -1,
    gender = 'Unknown',
    store_id = -1,
    vip_store_id = -1,
    finance_specialty_store = -1,
    guest_cohort_month_date = '1900-01-01',
    vip_cohort_month_date = '1900-01-01',
    membership_type = 'Unknown',
    membership_type_entering_month = 'Unknown',
    is_bop_vip = FALSE,
    is_reactivated_vip = FALSE,
    is_cancel = FALSE,
    is_cancel_before_6th = FALSE,
    is_passive_cancel = FALSE,
    is_skip = FALSE,
    is_snooze = FALSE,
    is_login = FALSE,
    is_merch_purchaser = FALSE,
    is_successful_billing = FALSE,
    is_pending_billing = FALSE,
    is_failed_billing = FALSE,
    is_cross_promo = FALSE,
    is_retail_vip = FALSE,
    customer_action_category = 'Unknown',
    cumulative_cash_gross_profit = 0.0000000000,
    cumulative_product_gross_profit = 0.0000000000,
    cumulative_cash_gross_profit_decile = 0,
    cumulative_product_gross_profit_decile = 0
WHERE month_date = '1900-01-01' AND customer_id = -1 AND activation_key = -1;
*/
