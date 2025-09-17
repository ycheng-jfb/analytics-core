CREATE TRANSIENT TABLE IF NOT EXISTS month_end.gaap_refund_waterfalls_order_detail (
	membership_store_id NUMBER(38,0),
	membership_store VARCHAR(101),
	order_type VARCHAR(34),
	date_shipped DATE,
	order_id NUMBER(38,0),
	rate NUMBER(19,6),
	gross_rev_after_discount NUMBER(32,10),
	total_cash_and_cash_credit NUMBER(33,10),
	refund_type VARCHAR(20)
);
