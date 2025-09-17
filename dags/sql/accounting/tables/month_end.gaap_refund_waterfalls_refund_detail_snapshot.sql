CREATE TRANSIENT TABLE IF NOT EXISTS month_end.gaap_refund_waterfalls_refund_detail_snapshot (
	individual_bu VARCHAR(61),
	credit_billing_store_id NUMBER(38,0),
	membership_store_id VARCHAR(16777216),
	membership_store VARCHAR(16777216),
	refund_type VARCHAR(19),
	order_type VARCHAR(34),
	order_id NUMBER(38,0),
	refund_id NUMBER(38,0),
	payment_method VARCHAR(25),
	date_shipped DATE,
	refund_month DATE,
	monthoffset NUMBER(10,0),
	refund_total NUMBER(20,6),
	snapshot_timestamp TIMESTAMP_LTZ(9)
);
