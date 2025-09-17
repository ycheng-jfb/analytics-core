USE DATABASE lake;

CREATE TABLE IF NOT EXISTS lake.sps.carrier_milestone (
	scac VARCHAR(50),
	shipper_identification VARCHAR(30),
	shipper_reference VARCHAR(30),
	ship_date date,
	address VARIANT,
	reference VARIANT,
	shipment VARIANT,
	filename VARCHAR(1000),
	filename_datetime TIMESTAMP_NTZ(9),
	meta_create_datetime TIMESTAMP_LTZ DEFAULT current_timestamp
);
