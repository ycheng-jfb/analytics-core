CREATE TABLE IF NOT EXISTS stg.dim_customer_action_category (
    customer_action_category_key INT IDENTITY (1,1),
    customer_action_category VARCHAR(200),
    is_cash_purchase BOOLEAN,
    is_non_cash_purchase BOOLEAN,
    is_merch_purchase BOOLEAN,
    is_purchase BOOLEAN,
    is_skip BOOLEAN,
    is_credit_billing BOOLEAN,
    is_failed_billing BOOLEAN,
    effective_start_datetime TIMESTAMP_LTZ,
    effective_end_datetime TIMESTAMP_LTZ,
    is_current BOOLEAN,
    meta_create_datetime TIMESTAMP_LTZ,
    meta_update_datetime TIMESTAMP_LTZ,
    PRIMARY KEY (customer_action_category)
);
