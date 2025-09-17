CREATE TRANSIENT TABLE IF NOT EXISTS month_end.loyalty_items_sold_price_per_point_detail_snapshot (
	business_unit VARCHAR(71),
	shipped_month DATE,
	redeemed_month DATE,
	order_id NUMBER(38,0),
	order_status VARCHAR(50),
	points NUMBER(38,6),
	product_name VARCHAR(100),
	color VARCHAR(200),
	product_sku VARCHAR(30),
	vip_price NUMBER(19,4),
	avg_item_price_sold_after_discounts_last_12m NUMBER(38,10),
	count_items_sold_to_form_avg_price NUMBER(38,0),
	total_product_cost NUMBER(19,4),
	units_shipped NUMBER(38,0),
	snapshot_timestamp TIMESTAMP_LTZ(9)
);
