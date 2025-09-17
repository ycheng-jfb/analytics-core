CREATE TABLE IF NOT EXISTS validation.fact_credit_event_orphan_records
(
    credit_id            INTEGER,
    meta_create_datetime TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP,
    meta_update_datetime TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP
);
