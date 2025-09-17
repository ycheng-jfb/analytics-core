CREATE TRANSIENT TABLE IF NOT EXISTS month_end.gaap_refund_waterfalls_order_detail_snapshot (
	individual_bu VARCHAR(61),
	store_id NUMBER(38,0),
	membership_store_id VARCHAR(16777216),
	membership_store VARCHAR(16777216),
	order_type VARCHAR(34),
	date_shipped DATE,
	order_id NUMBER(38,0),
	gross_rev_after_discount NUMBER(32,10),
	total_cash_and_cash_credit NUMBER(33,10),
	refund_type VARCHAR(20),
	snapshot_timestamp TIMESTAMP_LTZ(9)
);
