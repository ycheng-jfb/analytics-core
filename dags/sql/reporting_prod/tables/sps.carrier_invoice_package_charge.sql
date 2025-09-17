CREATE TABLE IF NOT EXISTS reporting_prod.sps.carrier_invoice_package_charge (
	carrier_invoice_package_charge_id number(38,0) NOT NULL Identity(1,1),
	carrier_invoice_package_id number(38,0),
	charge_amount number(12,2),
	charge_code varchar,
	charge_description varchar,
	declared_value number(12,2),
	percent_fuel_surchage_fac number(10,5),
	currency_code varchar(3),
	rate_scale varchar,
	meta_create_datetime timestamp_ltz DEFAULT current_timestamp,
	meta_update_datetime timestamp_ltz DEFAULT current_timestamp
);
ALTER TABLE reporting_prod.sps.carrier_invoice_package_charge ADD CONSTRAINT primary_key PRIMARY KEY (carrier_invoice_package_id, charge_code, rate_scale);
