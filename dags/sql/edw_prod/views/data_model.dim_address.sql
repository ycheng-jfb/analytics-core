CREATE OR REPLACE VIEW data_model.dim_address (
    address_key,
    address_id,
    street_address_1,
    street_address_2,
    city,
    state,
    zip_code,
    country_code,
    is_state_valid,
    is_zip_code_valid,
    meta_create_datetime,
    meta_update_datetime
    ) AS
SELECT
    address_key,
    address_id,
    street_address_1,
    street_address_2,
    city,
    state,
    zip_code,
    country_code,
    is_state_valid,
    is_zip_code_valid,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_address;
