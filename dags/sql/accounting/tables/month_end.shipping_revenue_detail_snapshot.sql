CREATE TRANSIENT TABLE IF NOT EXISTS month_end.shipping_revenue_detail_snapshot (
	store VARCHAR(31),
	shipped_month DATE,
	order_id NUMBER(38,0),
	shipping_amt NUMBER(19,4),
	snapshot_timestamp TIMESTAMP_LTZ(9)
);
