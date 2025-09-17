USE DATABASE lake;

CREATE TABLE IF NOT EXISTS lake.sps.carrier_invoice(
	invoice_number varchar(100),
	payment_method varchar(100),
	invoice_date date,
	net_amount_due varchar(100),
	invoice_type varchar(100),
	scac varchar(100),
	date_current date,
	settlement_option varchar(100),
	currency_code varchar(100),
	terms_type varchar(1000),
	terms_basis_date varchar(50),
	terms_net_due_date date,
	terms_net_days varchar(30),
	invoice_reference varchar(100),
	invoice_note varchar(100),
	account_number varchar(100),
	package_count varchar(100),
	total_charged varchar(100),
	sac_code varchar(100),
	billing variant,
	package variant,
	reference variant,
	file_name varchar(1000),
	datetime_added timestamp_ntz(9),
	meta_create_datetime TIMESTAMP_LTZ DEFAULT current_timestamp
);
