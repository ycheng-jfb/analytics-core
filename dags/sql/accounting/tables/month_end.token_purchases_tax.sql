CREATE TRANSIENT TABLE IF NOT EXISTS month_end.token_purchases_tax (
	store_brand_name VARCHAR(20),
	store_country_abbr VARCHAR(10),
	shipping_country VARCHAR(6),
	shipping_state VARCHAR(75),
	county VARCHAR(50),
	city VARCHAR(35),
	brand VARCHAR(31),
	store_type VARCHAR(20),
	credit_id NUMBER(38,0),
	credit_type VARCHAR(15),
	credit_reason VARCHAR(50),
	issued_date DATE,
	credit_activity_type VARCHAR(16777216),
	activity_date DATE,
	credit_activity_local_amount NUMBER(38,10)
);
