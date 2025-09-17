CREATE OR REPLACE VIEW data_model_jfb.dim_refund_payment_method (
    refund_payment_method_key,
    refund_payment_method,
    refund_payment_method_type,
    meta_create_datetime,
    meta_update_datetime
    ) AS
SELECT
    refund_payment_method_key,
    refund_payment_method,
    refund_payment_method_type,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_refund_payment_method;
