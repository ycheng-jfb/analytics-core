CREATE TRANSIENT TABLE IF NOT EXISTS reporting_prod.sps.carrier_milestone_address (
    carrier_milestone_address_id NUMBER(38, 0) NOT NULL IDENTITY (1,1) UNIQUE,
    carrier_milestone_id NUMBER(38, 0) NOT NULL
        FOREIGN KEY REFERENCES reporting.sps.carrier_milestone (carrier_milestone_id),
    entity_identifier VARCHAR(50),
    entity_name VARCHAR(60),
    identity_qualifier VARCHAR(100),
    identity_code VARCHAR(50),
    name VARCHAR(60),
    address_line VARCHAR(55),
    city VARCHAR(30),
    state VARCHAR(2),
    zipcode VARCHAR(15),
    country VARCHAR(3),
    filename_datetime TIMESTAMP_NTZ(9),
    meta_create_datetime TIMESTAMP_LTZ DEFAULT current_timestamp,
    meta_update_datetime TIMESTAMP_LTZ DEFAULT current_timestamp
);
ALTER TABLE reporting_prod.sps.carrier_milestone_address ADD CONSTRAINT primary_key PRIMARY KEY (carrier_milestone_id, entity_identifier, entity_name);
