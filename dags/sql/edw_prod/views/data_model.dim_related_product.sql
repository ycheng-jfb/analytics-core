CREATE OR REPLACE VIEW data_model.dim_related_product
(
    related_product_key,
    master_product_id,
    master_store_group_id,
    store_group_id,
    product_type_id,
    default_store_id,
    default_warehouse_id,
    default_product_category_id,
    alias,
    group_code,
    label,
    related_group_code,
    date_expected,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    related_product_key,
    master_product_id,
    master_store_group_id,
    store_group_id,
    product_type_id,
    default_store_id,
    default_warehouse_id,
    default_product_category_id,
    alias,
    group_code,
    label,
    related_group_code,
    date_expected,
    meta_create_datetime,
    meta_update_datetime
FROM stg.dim_related_product
WHERE is_deleted = FALSE;
