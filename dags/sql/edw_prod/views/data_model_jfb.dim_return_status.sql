CREATE OR REPLACE VIEW data_model_jfb.dim_return_status
(
    return_status_key,
    return_status_code,
    return_status,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    return_status_key,
    return_status_code,
    return_status,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_return_status;
