CREATE OR REPLACE TABLE reference.credit_report_mapping
(
    original_credit_tender       VARCHAR(255),
    original_credit_type         VARCHAR(255),
    original_credit_reason       VARCHAR(255),
    report_mapping               VARCHAR(255),
    original_credit_match_reason VARCHAR(255),
    report_sub_mapping           VARCHAR(255),
    meta_create_datetime         TIMESTAMPLTZ DEFAULT current_timestamp,
    meta_update_datetime         TIMESTAMPLTZ DEFAULT current_timestamp
);
