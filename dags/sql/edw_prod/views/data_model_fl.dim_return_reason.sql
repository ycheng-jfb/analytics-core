
CREATE VIEW IF NOT EXISTS data_model_fl.dim_return_reason
(
    return_reason_id,
    return_reason,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    return_reason_id,
    return_reason,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_return_reason;
