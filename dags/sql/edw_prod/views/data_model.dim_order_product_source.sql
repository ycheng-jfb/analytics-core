CREATE OR REPLACE VIEW data_model.dim_order_product_source
(
    order_product_source_key,
    order_product_source_name,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    order_product_source_key,
    order_product_source_name,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_order_product_source;
