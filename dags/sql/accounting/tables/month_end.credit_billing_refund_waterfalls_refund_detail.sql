CREATE TRANSIENT TABLE IF NOT EXISTS month_end.credit_billing_refund_waterfalls_refund_detail (
	order_id NUMBER(38,0),
	refund_id NUMBER(38,0),
	store_id NUMBER(38,0),
	individual_bu VARCHAR(71),
	date_refunded DATE,
	total_refund NUMBER(21,4),
	tax_refund NUMBER(19,4),
	credit_billing_refund_as_cash NUMBER(34,10),
	capture_payment_transaction_id NUMBER(38,0),
	payment_method VARCHAR(25),
	payment_month DATE,
	refund_month DATE,
	refund_month_offset NUMBER(10,0)
);
