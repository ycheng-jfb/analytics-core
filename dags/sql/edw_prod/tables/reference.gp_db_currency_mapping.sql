CREATE TABLE reference.gp_db_currency_mapping (
	db VARCHAR(4) NOT NULL,
	iso_currency_code VARCHAR(4) NOT NULL,
	meta_create_datetime TIMESTAMP_LTZ DEFAULT current_timestamp(3),
    meta_update_datetime TIMESTAMP_LTZ DEFAULT current_timestamp(3),
	PRIMARY KEY (db)
);
