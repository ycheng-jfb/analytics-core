CREATE TRANSIENT TABLE IF NOT EXISTS month_end.fin008_by_customer (
	store_brand VARCHAR(16777216),
	store_country VARCHAR(16777216),
	store_region VARCHAR(16777216),
	member_status VARCHAR(16777216),
	customer_id NUMBER(38,0),
	first_vip_cohort DATE,
	membership_month_date DATE,
	credit_activity_month_date_max DATE,
	credit_issuance_month_date_max DATE,
	date_flag NUMBER(38,0),
	outstanding_amount NUMBER(38,0),
	outstanding_count NUMBER(38,0)
);
