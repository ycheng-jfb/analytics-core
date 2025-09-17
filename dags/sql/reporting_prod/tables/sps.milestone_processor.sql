CREATE TRANSIENT TABLE IF NOT EXISTS REPORTING_PROD.SPS.MILESTONE_PROCESSOR
(
    ship_bol VARCHAR(100),
    container_label VARCHAR(100),
    status_code VARCHAR(100),
    status_date VARCHAR(100),
    status_time VARCHAR(100),
    status_datetime VARCHAR(100),
    file_name VARCHAR(1000),
    meta_create_datetime TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
)
