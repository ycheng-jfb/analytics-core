CREATE TRANSIENT TABLE IF NOT EXISTS month_end.retail_discount_order (
	month_date DATE,
	store_brand VARCHAR(20),
	store_region VARCHAR(32),
	store_full_name VARCHAR(50),
	store_id NUMBER(38,0),
	is_retail_ship_only_order BOOLEAN,
	order_id NUMBER(38,0),
	promo_code VARCHAR(255),
	discount_type VARCHAR(17),
	currency_type VARCHAR(13),
	product_subtotal_local_amount NUMBER(20,4),
	product_discount_local_amount NUMBER(19,4),
	shipping_revenue_local_amount NUMBER(20,4),
	non_cash_credit_local_amount NUMBER(19,4),
	product_gross_revenue_local_amount NUMBER(24,4)
);
