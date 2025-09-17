CREATE OR REPLACE VIEW data_model_jfb.dim_product_type
(
    product_type_key,
    product_type_id,
    product_type_name,
    is_free,
    is_source_free,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    product_type_key,
    product_type_id,
    product_type_name,
    is_free,
    source_is_free,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_product_type;
