CREATE TABLE IF NOT EXISTS stg.dim_date (
	date_key INT,
	full_date DATE,
	day_of_week INT,
	day_name_of_week VARCHAR(10),
	day_of_month INT,
	day_of_year INT,
	week_of_year INT,
	month_name VARCHAR(10),
	month_date DATE,
	calendar_quarter INT,
	calendar_year INT,
	calendar_year_quarter VARCHAR(10),
	effective_start_datetime TIMESTAMP_LTZ,
	effective_end_datetime TIMESTAMP_LTZ,
	is_current BOOLEAN,
	meta_create_datetime TIMESTAMP_LTZ,
	meta_update_datetime TIMESTAMP_LTZ
);
