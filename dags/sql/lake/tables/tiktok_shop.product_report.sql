CREATE TABLE IF NOT EXISTS lake.tiktok_shop.product_report (
    shop_name              VARCHAR,
	product_id             VARCHAR,
	sku_id                 VARCHAR,
    seller_sku             VARCHAR,
	parent_title           VARCHAR,
	brand                  VARCHAR,
	color                  VARCHAR,
	size                   VARCHAR,
    department             VARCHAR,
    category               VARCHAR,
    subcategory            VARCHAR,
    main_image_url         VARCHAR,
    color_image_url        VARCHAR,
    unit_price             NUMBER(38, 4),
    meta_row_hash          NUMBER(38, 0),
    meta_create_datetime   TIMESTAMP_LTZ,
    meta_update_datetime   TIMESTAMP_LTZ
);
