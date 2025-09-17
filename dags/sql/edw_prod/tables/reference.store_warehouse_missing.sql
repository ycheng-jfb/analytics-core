CREATE TABLE reference.store_warehouse_missing
(
	store_id NUMBER,
	country VARCHAR(20),
	region VARCHAR(20),
	warehouse_id NUMBER,
	meta_create_datetime TIMESTAMP_LTZ default current_timestamp(3),
    meta_update_datetime TIMESTAMP_LTZ default current_timestamp(3)
);
