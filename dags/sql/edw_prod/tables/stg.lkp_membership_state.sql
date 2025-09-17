CREATE TABLE IF NOT EXISTS  stg.lkp_membership_state(
    customer_id NUMBER NOT NULL,
    meta_original_customer_id NUMBER,
    store_id NUMBER,
    membership_event_type_key NUMBER,
    membership_event_type VARCHAR(50),
    membership_type_detail VARCHAR(20),
    event_local_datetime TIMESTAMP_TZ(3),
    membership_state VARCHAR(50),
    is_ignored_activation BOOLEAN,
    is_ignored_cancellation BOOLEAN,
    is_hard_cancellation_from_ecom BOOLEAN,
    is_deleted BOOLEAN,
    meta_row_hash NUMBER,
    meta_create_datetime TIMESTAMP_LTZ DEFAULT current_timestamp(3),
    meta_update_datetime TIMESTAMP_LTZ DEFAULT current_timestamp(3),
    PRIMARY KEY (customer_id, event_local_datetime)
);
