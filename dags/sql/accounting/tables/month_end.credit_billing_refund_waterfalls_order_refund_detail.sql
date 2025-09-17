CREATE TRANSIENT TABLE IF NOT EXISTS month_end.credit_billing_refund_waterfalls_order_refund_detail (
	individual_bu VARCHAR(71),
	payment_month DATE,
	refund_month DATE,
	refund_month_offset NUMBER(10,0),
	date_refunded DATE,
	date_refunded_not_null DATE,
	cash_collected NUMBER(38,10),
	credit_billing_refund_as_cash NUMBER(38,10)
);
