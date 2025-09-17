CREATE OR REPLACE VIEW data_model_sxf.dim_refund_status
(
    refund_status_key,
    refund_status_code,
    refund_status,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    refund_status_key,
    refund_status_code,
    refund_status,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_refund_status;
