
CREATE TABLE reference.landed_cost_by_sku
(
	estimated_cogs_id INT IDENTITY (1,1),
	store_id INT,
	store_region VARCHAR,
	store_brand VARCHAR,
	sku VARCHAR,
	currency_code VARCHAR,
	unit_cost NUMBER(18,4),
	start_date DATE,
	end_date DATE,
	meta_create_datetime TIMESTAMP_LTZ,
	meta_update_datetime TIMESTAMP_LTZ
);
