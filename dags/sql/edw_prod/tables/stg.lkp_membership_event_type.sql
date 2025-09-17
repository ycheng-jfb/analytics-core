-- DROP TABLE IF EXISTS stg.lkp_membership_event_type;

CREATE TABLE stg.lkp_membership_event_type (
    customer_id NUMBER NOT NULL,
    store_id NUMBER,
    membership_event_type VARCHAR(50),
    membership_type_detail VARCHAR(20),
    event_local_datetime TIMESTAMP_TZ(3),
    is_deleted BOOLEAN,
    meta_row_hash NUMBER,
    meta_create_datetime TIMESTAMP_LTZ DEFAULT current_timestamp(3),
    meta_update_datetime TIMESTAMP_LTZ DEFAULT current_timestamp(3),
    PRIMARY KEY (customer_id, event_local_datetime)
);

-- ALTER TABLE stg.lkp_membership_event_type ADD COLUMN is_deleted BOOLEAN;
