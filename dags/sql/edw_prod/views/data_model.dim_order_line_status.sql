CREATE OR REPLACE VIEW data_model.dim_order_line_status
(
    order_line_status_key,
    order_line_status_code,
    order_line_status,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    order_line_status_key,
    order_line_status_code,
    order_line_status,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_order_line_status;
