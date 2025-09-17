-- DROP TABLE IF EXISTS stg.lkp_membership_event;

CREATE TABLE stg.lkp_membership_event (
    customer_id NUMBER NOT NULL,
    meta_original_customer_id NUMBER(38,0),
    store_id NUMBER,
    membership_event_type_key NUMBER,
    membership_event_type VARCHAR(50),
    membership_type_detail VARCHAR(20),
    event_local_datetime TIMESTAMP_TZ(3),
    is_deleted BOOLEAN,
    meta_row_hash NUMBER,
    meta_create_datetime TIMESTAMP_LTZ DEFAULT current_timestamp(3),
    meta_update_datetime TIMESTAMP_LTZ DEFAULT current_timestamp(3),
    PRIMARY KEY (customer_id, event_local_datetime)
);

-- DROP TABLE IF EXISTS excp.lkp_membership_event;

CREATE TABLE excp.lkp_membership_event (
    customer_id NUMBER NOT NULL,
    meta_original_customer_id NUMBER(38,0),
    store_id NUMBER,
    membership_event_type_key NUMBER,
    membership_event_type VARCHAR(50),
    membership_type_detail VARCHAR(20),
    event_local_datetime TIMESTAMP_TZ(3),
    is_deleted BOOLEAN,
    meta_row_hash NUMBER,
    meta_data_quality VARCHAR(10),
    meta_create_datetime TIMESTAMP_LTZ DEFAULT current_timestamp(3),
    meta_update_datetime TIMESTAMP_LTZ DEFAULT current_timestamp(3),
    excp_message VARCHAR,
    meta_is_current_excp BOOLEAN
);

-- ALTER TABLE stg.lkp_membership_event ADD COLUMN is_deleted BOOLEAN;
-- ALTER TABLE excp.lkp_membership_event ADD COLUMN is_deleted BOOLEAN;
