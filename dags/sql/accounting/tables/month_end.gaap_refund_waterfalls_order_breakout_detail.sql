CREATE TRANSIENT TABLE IF NOT EXISTS month_end.gaap_refund_waterfalls_order_breakout_detail (
	individual_bu VARCHAR(101),
	store_id NUMBER(38,0),
	membership_store_id NUMBER(38,0),
	membership_store VARCHAR(101),
	order_type VARCHAR(34),
	date_shipped DATE,
	order_id NUMBER(38,0),
	rate NUMBER(19,6),
	gross_rev_after_discount NUMBER(32,10),
	total_cash_and_cash_credit NUMBER(33,10),
	subtotal NUMBER(31,10),
	discount NUMBER(32,10),
	tax NUMBER(31,10),
	shipping NUMBER(31,10),
	credit NUMBER(31,10),
	actual_ship_date TIMESTAMP_NTZ(0)
);
