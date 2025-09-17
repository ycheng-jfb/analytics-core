CREATE OR REPLACE VIEW data_model_fl.dim_item_price AS
SELECT
    item_price_key,
    meta_original_product_id AS product_id,
    meta_original_item_id AS item_id,
    stg.udf_unconcat_brand(master_product_id) AS master_product_id,
    store_id,
    sku,
    product_sku,
    base_sku,
    product_name,
    vip_unit_price,
    retail_unit_price,
    membership_brand_id,
    is_active,
	meta_create_datetime,
    meta_update_datetime
FROM stg.dim_item_price
WHERE substring(product_id, -2) = '20' OR item_price_key = -1;



