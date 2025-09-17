CREATE TRANSIENT TABLE staging.segment_branch_open_events
(
    session_id NUMBER(38,0)  NOT NULL PRIMARY KEY,
    meta_original_session_id NUMBER(38,0),
    ma_utm_source VARCHAR,
    ma_utm_medium VARCHAR,
    ma_utm_campaign VARCHAR,
    ma_utm_term VARCHAR,
    ma_utm_content VARCHAR,
    meta_row_hash NUMERIC,
    meta_create_datetime TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP,
    meta_update_datetime TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP
);
