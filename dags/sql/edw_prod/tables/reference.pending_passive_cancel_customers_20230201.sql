CREATE TABLE IF NOT EXISTS reference.pending_passive_cancel_customers_20230201 (
    customer_id INT,
    meta_original_customer_id INT,
    meta_create_datetime TIMESTAMPLTZ(3) DEFAULT CURRENT_TIMESTAMP(3),
    meta_update_datetime TIMESTAMPLTZ(3) DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (customer_id)
    );
