CREATE TRANSIENT TABLE IF NOT EXISTS month_end.loyalty_points_discount (
	promo_id NUMBER(38,0),
	month_date DATE,
	store_id NUMBER(38,0),
	gender VARCHAR(10),
	discount_amount NUMBER(31,4),
	order_count NUMBER(18,0)
);
