CREATE OR REPLACE VIEW data_model.dim_product_price_history
(
    product_price_history_key,
    product_id,
    master_product_id,
    store_id,
    vip_unit_price,
    retail_unit_price,
    warehouse_unit_price,
    sale_price,
    effective_start_datetime,
    effective_end_datetime,
    is_current,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    product_price_history_key,
    product_id,
    master_product_id,
    store_id,
    vip_unit_price,
    retail_unit_price,
    warehouse_unit_price,
    sale_price,
    effective_start_datetime,
    effective_end_datetime,
    is_current,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_product_price_history
WHERE NOT is_deleted;
