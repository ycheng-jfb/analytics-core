CREATE OR REPLACE VIEW data_model.dim_return_condition
(
    return_condition_key,
    return_condition,
    return_disposition,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    return_condition_key,
    return_condition,
    return_disposition,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_return_condition;
