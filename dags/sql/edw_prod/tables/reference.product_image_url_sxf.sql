create or replace table reference.product_image_url_sxf (
	product_sku varchar,
	returns_product_image number(38,0),
	product_type varchar,
	image_url varchar,
    meta_create_datetime timestamp_ltz default current_timestamp::timestampltz,
    meta_update_datetime timestamp_ltz default current_timestamp::timestamp_ltz
);
