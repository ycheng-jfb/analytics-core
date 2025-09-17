CREATE OR REPLACE VIEW data_model_jfb.dim_customer_action_category AS
SELECT
    customer_action_category,
    is_cash_purchase,
    is_non_cash_purchase,
    is_merch_purchase,
    is_purchase,
    is_skip,
    is_credit_billing,
    is_failed_billing,
    --effective_start_datetime,
    --effective_end_datetime,
    --is_current,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_customer_action_category
WHERE is_current;
