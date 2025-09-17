CREATE OR REPLACE VIEW data_model.dim_item_price AS
SELECT
    item_price_key,
    product_id,
    item_id,
    master_product_id,
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
FROM stg.dim_item_price;



