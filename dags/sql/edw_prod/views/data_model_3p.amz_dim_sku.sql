CREATE OR REPLACE VIEW data_model_3p.amz_dim_sku(
    asin,
    amz_sku,
    tfg_sku,
    tfg_product_sku,
    tfg_base_sku,
    meta_create_datetime,
    meta_update_datetime
)
AS
(
SELECT asin,
       amz_sku,
       tfg_sku,
       tfg_product_sku,
       tfg_base_sku,
       meta_create_datetime,
       meta_update_datetime
FROM dbt_edw_prod.stg.amz_dim_sku
);
