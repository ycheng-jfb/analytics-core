CREATE TRANSIENT TABLE IF NOT EXISTS breakage.breakage_by_state_actuals_snapshot (
	brand VARCHAR(155),
	country VARCHAR(155),
	state VARCHAR(155),
	original_issued_month DATE,
	original_credit_type VARCHAR(155),
	activity_month DATE,
	issued_to_date NUMBER(32,4),
	redeemed_to_date NUMBER(32,4),
	cancelled_to_date NUMBER(32,4),
	unredeemed_to_date NUMBER(32,4),
	snapshot_timestamp TIMESTAMP_LTZ(9)
);
