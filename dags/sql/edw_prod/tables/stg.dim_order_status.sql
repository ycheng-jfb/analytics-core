CREATE TABLE IF NOT EXISTS stg.dim_order_status (
    order_status_key INT IDENTITY (1,1) NOT NULL,
    order_status VARCHAR(50) NOT NULL,
    effective_start_datetime TIMESTAMP_LTZ,
    effective_end_datetime TIMESTAMP_LTZ,
    is_current BOOLEAN,
    meta_create_datetime TIMESTAMP_LTZ,
    meta_update_datetime TIMESTAMP_LTZ,
    PRIMARY KEY (order_status_key)
);
