CREATE OR REPLACE VIEW data_model_sxf.dim_chargeback_reason
(
    chargeback_reason_key,
    chargeback_reason,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    chargeback_reason_key,
    chargeback_reason,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_chargeback_reason;
