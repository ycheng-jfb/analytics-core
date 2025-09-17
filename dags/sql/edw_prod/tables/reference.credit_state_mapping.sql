CREATE OR REPLACE TABLE reference.credit_state_mapping
(
    state_abbreviation VARCHAR(5),
    state_name         VARCHAR(55),
    giftco_recognition VARCHAR(55),
    region             VARCHAR(55),
    meta_create_datetime TIMESTAMP_LTZ default current_timestamp,
    meta_update_datetime TIMESTAMP_LTZ default current_timestamp
);
