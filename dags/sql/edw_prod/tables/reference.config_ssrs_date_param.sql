CREATE OR REPLACE TABLE reference.config_ssrs_date_param (
	report VARCHAR(150),
	report_date DATE,
    meta_create_datetime TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP,
    meta_update_datetime TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP
);
