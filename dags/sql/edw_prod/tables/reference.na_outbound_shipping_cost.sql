CREATE TABLE IF NOT EXISTS reference.na_outbound_shipping_cost (
	cost_date DATE,
	store_id INT,
	store_name VARCHAR(16777216),
	store_country VARCHAR(16777216),
	store_region VARCHAR(16777216),
	store_type VARCHAR(16777216),
	currency_code VARCHAR(16777216),
	units INT,
	outbound_shipping_cost NUMBER(18,6),
	meta_create_datetime TIMESTAMP_LTZ(9),
	meta_update_datetime TIMESTAMP_LTZ(9)
);
