CREATE OR REPLACE VIEW data_model.dim_shipment_carrier_service
(
    carrier_service_id,
    carrier,
    service_type,
    carrier_code,
    meta_create_datetime,
    meta_update_datetime
)
AS
SELECT
    carrier_service_id,
    carrier,
    service_type,
    carrier_code,
    meta_create_datetime,
    meta_update_datetime
FROM dbt_edw_prod.stg.dim_shipment_carrier_service;
