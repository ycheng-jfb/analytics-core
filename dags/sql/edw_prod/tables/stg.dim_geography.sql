CREATE TABLE IF NOT EXISTS stg.dim_geography (
    geography_key INT IDENTITY (1,1) NOT NULL,
    sub_region VARCHAR(7) NOT NULL,
    region VARCHAR(7) NOT NULL,
    effective_start_datetime TIMESTAMP_LTZ,
    effective_end_datetime TIMESTAMP_LTZ,
    is_current BOOLEAN,
    meta_create_datetime TIMESTAMP_LTZ,
    meta_update_datetime TIMESTAMP_LTZ,
    PRIMARY KEY (geography_key)
);
