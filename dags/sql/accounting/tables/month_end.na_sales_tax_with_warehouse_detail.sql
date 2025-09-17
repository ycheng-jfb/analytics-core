CREATE TRANSIENT TABLE IF NOT EXISTS month_end.na_sales_tax_with_warehouse_detail (
	segment VARCHAR(7),
	order_id NUMBER(38,0),
	refund_id NUMBER(38,0),
	business_unit VARCHAR(50),
	country VARCHAR(18),
	retail_location VARCHAR(100),
	month DATE,
	store_id NUMBER(38,0),
	store_type VARCHAR(20),
	state VARCHAR(16777216),
	warehouse_id NUMBER(38,0),
	ship_from_warehouse_info VARCHAR(255),
	product_dollars FLOAT,
	shipping FLOAT,
	tax FLOAT,
	total FLOAT,
	refund_type VARCHAR(25)
);
