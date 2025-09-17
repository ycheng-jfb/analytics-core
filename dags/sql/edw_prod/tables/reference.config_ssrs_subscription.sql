CREATE OR REPLACE TABLE reference.config_ssrs_subscription (
    report_version VARCHAR(250),
    report_name VARCHAR(250),
    param_name VARCHAR(250),
    param_value VARCHAR(1000),
    meta_create_datetime TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP,
    meta_update_datetime TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP
);
