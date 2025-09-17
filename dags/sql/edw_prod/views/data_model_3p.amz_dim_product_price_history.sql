CREATE OR REPLACE VIEW data_model_3p.amz_dim_product_price_history(
    product_price_history_key,
    product_key,
    seller_sku,
    order_marketplace_channel_key,
    asin,
    list_local_price,
    open_local_datetime,
    open_pst_datetime,
    status,
    effective_start_datetime,
    effective_end_datetime,
    is_current,
    meta_create_datetime,
    meta_update_datetime
)
AS
(
SELECT product_price_history_key,
    product_key,
    seller_sku,
    order_marketplace_channel_key,
    asin,
    list_local_price,
    open_local_datetime,
    open_pst_datetime,
    status,
    effective_start_datetime,
    effective_end_datetime,
    is_current,
    meta_create_datetime,
    meta_update_datetime
FROM dbt_edw_prod.stg.amz_dim_product_price_history
    );

