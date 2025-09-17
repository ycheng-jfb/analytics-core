CREATE TRANSIENT TABLE IF NOT EXISTS month_end.cash_noncash_credit_redemptions (
	order_id NUMBER(38,0),
	store_id NUMBER(38,0),
	date_shipped DATE,
	credit NUMBER(19,4),
	order_credit_amount NUMBER(31,4),
	cash_credit_amount NUMBER(31,4),
	noncash_credit_amount NUMBER(31,4),
	missing_credit_amount NUMBER(31,4),
	datetime_added DATE
);
