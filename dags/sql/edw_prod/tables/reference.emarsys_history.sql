CREATE OR REPLACE TABLE reference.emarsys_history
(
    customer_id               NUMBER(38, 0),
    meta_original_customer_id NUMBER(38, 0),
    effective_start_datetime  TIMESTAMP_LTZ(9),
    is_opt_out                BOOLEAN,
    prev_is_opt_out           BOOLEAN
);
