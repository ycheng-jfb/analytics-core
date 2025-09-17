CREATE TABLE IF NOT EXISTS stg.dim_membership_event_type (
    membership_event_type_key INT IDENTITY (1,1) NOT NULL,
    membership_event_type VARCHAR(60) NOT NULL,
    effective_start_datetime TIMESTAMP_LTZ,
    effective_end_datetime TIMESTAMP_LTZ,
    is_current BOOLEAN,
    meta_create_datetime TIMESTAMP_LTZ,
    meta_update_datetime TIMESTAMP_LTZ,
    PRIMARY KEY (membership_event_type_key)
);
