CREATE TABLE IF NOT EXISTS reporting_prod.sps.carrier_invoice_package
(
	carrier_invoice_package_id number(38,0) NOT NULL Identity(1,1),
	carrier_invoice_id number(38,0),
	package_number varchar(50) NOT NULL,
	tracking_number varchar(50),
	description varchar(80),
	date date,
	packaging_type varchar(50),
	weight number(10,2),
	weight_measurement varchar(50),
	quantity number(7,0),
	packaging_form varchar(100),
	weight_unit_code varchar(50),
	charge_count number(15,0),
	length number(8,3),
	width number(8,3),
	height number(8,3),
	dimension_measurement varchar(50),
	remarks_1 varchar(30),
	remarks_2 varchar(30),
	meta_create_datetime timestamp_ltz DEFAULT current_timestamp,
	meta_update_datetime timestamp_ltz DEFAULT current_timestamp
);
ALTER TABLE reporting_prod.sps.carrier_invoice_package ADD CONSTRAINT primary_key PRIMARY KEY (carrier_invoice_id, package_number, tracking_number);
