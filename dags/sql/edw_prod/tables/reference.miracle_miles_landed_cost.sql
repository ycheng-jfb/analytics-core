CREATE OR REPLACE TABLE reference.miracle_miles_landed_cost
(
    style                    VARCHAR(1000),
    colorway_cnl             VARCHAR(1000),
    landed_cost              FLOAT,
    effective_start_datetime TIMESTAMP_LTZ(3),
    effective_end_datetime   TIMESTAMP_LTZ(9),
    is_current               BOOLEAN,
    meta_row_hash            VARCHAR(1000),
    meta_create_datetime     TIMESTAMP_LTZ(9),
    meta_update_datetime     TIMESTAMP_LTZ(9)
);
