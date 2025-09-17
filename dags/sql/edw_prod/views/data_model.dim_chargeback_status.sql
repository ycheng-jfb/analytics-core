CREATE OR REPLACE VIEW data_model.dim_chargeback_status
(
    chargeback_status_key,
    chargeback_status_code,
    chargeback_status,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    chargeback_status_key,
    chargeback_status_code,
    chargeback_status,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_chargeback_status;
