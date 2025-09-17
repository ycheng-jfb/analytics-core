CREATE OR REPLACE VIEW data_model_jfb.dim_refund_reason
(
    refund_reason_key,
    refund_reason,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    refund_reason_key,
    refund_reason,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_refund_reason;
