CREATE TRANSIENT TABLE IF NOT EXISTS month_end.retail_ship_only_refunds (
	refund_id NUMBER(38,0),
	sales_retail_store NUMBER(38,0),
	retail_sales_location VARCHAR(50),
	store_brand VARCHAR(20),
	store_country VARCHAR(50),
	order_id NUMBER(38,0),
	payment_transaction_id NUMBER(38,0),
	return_payment_transaction_id NUMBER(38,0),
	date_shipped TIMESTAMP_TZ(9),
	month_shipped DATE,
	return_location VARCHAR(6),
	store_credit_refund NUMBER(20,4),
	creditcard_refund NUMBER(20,4),
	cash_refund NUMBER(20,4),
	date_refunded DATE,
	month_refunded DATE,
	total_refund NUMBER(20,4)
);
