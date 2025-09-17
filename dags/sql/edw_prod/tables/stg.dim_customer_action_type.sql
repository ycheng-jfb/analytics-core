CREATE TABLE IF NOT EXISTS stg.dim_customer_action_type (
    customer_action_type_key INT IDENTITY (1,1) NOT NULL,
    customer_action_type VARCHAR(50) NOT NULL,
    effective_start_datetime TIMESTAMP_LTZ,
    effective_end_datetime TIMESTAMP_LTZ,
    is_current BOOLEAN,
    meta_create_datetime TIMESTAMP_LTZ,
    meta_update_datetime TIMESTAMP_LTZ,
    PRIMARY KEY (customer_action_type_key)
);
