CREATE TABLE reference.yitty_fl_lead_fix_20240304 (
    membership_id INT,
    customer_id INT,
    store_id INT,
    meta_create_datetime TIMESTAMP_LTZ DEFAULT current_timestamp(3),
    meta_update_datetime TIMESTAMP_LTZ DEFAULT current_timestamp(3),
    PRIMARY KEY (membership_id)
);
