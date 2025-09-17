CREATE TRANSIENT TABLE IF NOT EXISTS reporting_prod.sps.carrier_milestone (
    carrier_milestone_id NUMBER(38, 0) NOT NULL IDENTITY (1,1) UNIQUE,
    shipper_identification VARCHAR(30),
    shipper_reference VARCHAR(30),
    scac VARCHAR(50),
    ship_date DATE,
    filename_datetime TIMESTAMP_NTZ(9),
    meta_create_datetime TIMESTAMP_LTZ DEFAULT current_timestamp,
    meta_update_datetime TIMESTAMP_LTZ DEFAULT current_timestamp
);
ALTER TABLE reporting_prod.sps.carrier_milestone ADD CONSTRAINT primary_key PRIMARY KEY (shipper_identification, shipper_reference, scac);
