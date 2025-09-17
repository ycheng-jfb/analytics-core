CREATE OR REPLACE VIEW data_model_fl.dim_order_processing_status
(
    order_processing_status_key,
    order_processing_status_code,
    order_processing_status,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    order_processing_status_key,
    order_processing_status_code,
    order_processing_status,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_order_processing_status;
