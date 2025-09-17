CREATE OR REPLACE TABLE reference.finance_segment_mapping_rank
(
    report_version VARCHAR(250),
    report_mapping VARCHAR(250),
    master_rank    INT,
    set_rank       INT,
    meta_create_datetime TIMESTAMP_LTZ(9) DEFAULT current_timestamp,
    meta_update_datetime TIMESTAMP_LTZ(9) DEFAULT current_timestamp
);
