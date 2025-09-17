CREATE OR REPLACE VIEW data_model_fl.dim_discount
(
    discount_id,
    discount_applied_to,
    discount_calculation_method,
    discount_label,
    discount_percentage,
    discount_rate,
    discount_date_expires,
    discount_status_code,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    discount_id,
    discount_applied_to,
    discount_calculation_method,
    discount_label,
    discount_percentage,
    discount_rate,
    discount_date_expires,
    discount_status_code,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_discount;
