CREATE TABLE IF NOT EXISTS shared.session_ab_test_start (
    session_id                      NUMBER(38,0),
    ab_test_type                    VARCHAR(16),
    test_start_membership_state     VARCHAR,
    ab_test_key                     VARCHAR,
    ab_test_segment                 VARCHAR,
    ab_test_start_local_datetime    TIMESTAMP_TZ,
    test_start_lead_daily_tenure    NUMBER(38,0),
    test_start_vip_month_tenure     NUMBER(38,0),
    meta_original_session_id        NUMBER(38,0),
    meta_row_hash                   NUMBER(38,0),
    meta_create_datetime            TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    meta_update_datetime            TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);
