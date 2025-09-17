CREATE TRANSIENT TABLE IF NOT EXISTS month_end.credit_billing_refund_waterfalls_order_detail (
	store_id NUMBER(38,0),
	individual_bu VARCHAR(71),
	store_country_abbr VARCHAR(50),
	payment_month DATE,
	order_id NUMBER(38,0),
	cash_collected NUMBER(32,10),
	payment_method VARCHAR(25),
	capture_payment_transaction_id NUMBER(38,0)
);
