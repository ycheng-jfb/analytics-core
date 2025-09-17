CREATE OR REPLACE VIEW data_model.dim_sku
(
    sku_key,
    sku,
    product_sku,
    base_sku,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    sku_key,
    sku,
    product_sku,
    base_sku,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_sku;
