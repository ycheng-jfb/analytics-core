CREATE OR REPLACE VIEW data_model_sxf.dim_product_price_history
(
    product_price_history_key,
    product_id,
    master_product_id,
    store_id,
    vip_unit_price,
    retail_unit_price,
    sale_price,
    warehouse_unit_price,
    effective_start_datetime,
    effective_end_datetime,
    is_current,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    product_price_history_key,
    stg.udf_unconcat_brand(product_id) AS product_id,
    stg.udf_unconcat_brand(master_product_id) AS master_product_id,
    store_id,
    vip_unit_price,
    retail_unit_price,
    sale_price,
    warehouse_unit_price,
    effective_start_datetime,
    effective_end_datetime,
    is_current,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_product_price_history
WHERE NOT is_deleted AND (substring(product_id, -2) = '30' OR product_id = -1);

