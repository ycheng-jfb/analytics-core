CREATE TRANSIENT TABLE IF NOT EXISTS month_end.fin008_outstanding_credit_count_by_status (
	credit_outstanding_date VARCHAR(16777216),
	credit_issued_max_date VARCHAR(16777216),
	vip_cohort_max_date VARCHAR(16777216),
	member_status_classification_date VARCHAR(16777216),
	store_brand VARCHAR(20),
	store_country VARCHAR(10),
	store_region VARCHAR(10),
	member_status VARCHAR(9),
	first_vip_cohort DATE,
	outstanding_count_bucket VARCHAR(16777216),
	customer_count NUMBER(18,0),
	outstanding_amount NUMBER(38,4),
	outstanding_count NUMBER(38,0)
);
