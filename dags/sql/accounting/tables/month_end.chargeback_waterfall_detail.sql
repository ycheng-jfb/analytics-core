CREATE TRANSIENT TABLE IF NOT EXISTS month_end.chargeback_waterfall_detail (
	store VARCHAR(101),
	order_classification VARCHAR(50),
	order_id NUMBER(38,0),
	transaction_month DATE,
	chargeback_month DATE,
	monthoffset NUMBER(9,0),
	chargeback_amount_gross_vat NUMBER(20,4),
	chargeback_amount_net_vat NUMBER(32,10)
);
