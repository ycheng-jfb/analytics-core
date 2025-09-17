CREATE TABLE IF NOT EXISTS lake.tiktok_shop.inventory_report (
    shop_name                 VARCHAR,
	product_id                VARCHAR,
	sku_id                    VARCHAR,
	seller_sku                VARCHAR,
	in_shop_inventory         INTEGER,
	total_available_quantity  INTEGER,
	total_committed_quantity  INTEGER,
    warehouse_ids             ARRAY,
    meta_row_hash             NUMBER(38, 0),
    meta_create_datetime      TIMESTAMP_LTZ,
    meta_update_datetime      TIMESTAMP_LTZ
);
