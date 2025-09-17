CREATE TRANSIENT TABLE IF NOT EXISTS month_end.emp_token_outstanding (
	brand VARCHAR(155),
	store_region VARCHAR(55),
	store_country VARCHAR(55),
	current_membership_state VARCHAR(55),
	membership_token_id NUMBER(38,0),
	token_reason_label VARCHAR(55),
	credit_tender VARCHAR(55),
	statuscode NUMBER(38,0),
	is_converted_credit BOOLEAN,
	original_issued_month DATE,
	optin_month DATE,
	credit_start_date DATE,
	credit_expire_date DATE,
	credit_expiration_month NUMBER(38,0),
	outstanding_amount NUMBER(20,4),
	datetime_updated TIMESTAMP_NTZ(9)
);
