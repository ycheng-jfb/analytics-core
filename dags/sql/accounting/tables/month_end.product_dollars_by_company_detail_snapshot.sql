CREATE TRANSIENT TABLE IF NOT EXISTS month_end.product_dollars_by_company_detail_snapshot (
	business_unit VARCHAR(61),
	store VARCHAR(61),
	company VARCHAR(2),
	date_shipped DATE,
	order_id NUMBER(38,0),
	orders_shipped NUMBER(1,0),
	orders_shipped_reship_exch NUMBER(1,0),
	units_shipped NUMBER(38,0),
	units_shipped_reship_exch NUMBER(38,0),
	product_dollars NUMBER(32,4),
	product_dollars_reship_exch NUMBER(32,4),
	snapshot_timestamp TIMESTAMP_LTZ(9)
);
