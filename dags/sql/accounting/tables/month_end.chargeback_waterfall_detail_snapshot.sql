CREATE TRANSIENT TABLE IF NOT EXISTS month_end.chargeback_waterfall_detail_snapshot (
	store VARCHAR(61),
	order_classification VARCHAR(50),
	order_id NUMBER(38,0),
	transation_month DATE,
	chargeback_month DATE,
	monthoffset NUMBER(9,0),
	chargeback_amount_gross_vat NUMBER(20,4),
	chargeback_amount_net_vat NUMBER(32,10),
	snapshot_timestamp TIMESTAMP_LTZ(9)
);
