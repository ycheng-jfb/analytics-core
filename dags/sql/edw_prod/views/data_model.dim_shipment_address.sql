CREATE OR REPLACE VIEW data_model.dim_shipment_address
(
    address_id,
    firstname,
    lastname,
    address1,
    address2,
    city,
    state,
    zip,
    country_code,
    phone,
    company,
    address_type_id,
    meta_create_datetime,
    meta_update_datetime
)
AS
SELECT address_id,
       firstname,
       lastname,
       address1,
       address2,
       city,
       state,
       zip,
       country_code,
       phone,
       company,
       address_type_id,
       meta_create_datetime,
       meta_update_datetime
FROM dbt_edw_prod.stg.dim_shipment_address;
