CREATE OR REPLACE VIEW data_model_sxf.dim_order_payment_status
(
    order_payment_status_key,
    order_payment_status_code,
    order_payment_status,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    order_payment_status_key,
    order_payment_status_code,
    order_payment_status,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_order_payment_status;
