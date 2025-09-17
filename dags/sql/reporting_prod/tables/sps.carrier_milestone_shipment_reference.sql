CREATE TRANSIENT TABLE IF NOT EXISTS reporting_prod.sps.carrier_milestone_shipment_reference (
    carrier_milestone_shipment_reference_id NUMBER(38, 0) NOT NULL IDENTITY (1,1) UNIQUE,
    carrier_milestone_shipment_id NUMBER(38, 0) NOT NULL
        FOREIGN KEY REFERENCES reporting.sps.carrier_milestone_shipment (carrier_milestone_shipment_id),
    reference_value VARCHAR,
    reference_qualifier VARCHAR(50),
    filename_datetime TIMESTAMP_NTZ(9),
    meta_create_datetime TIMESTAMP_LTZ DEFAULT current_timestamp,
    meta_update_datetime TIMESTAMP_LTZ DEFAULT current_timestamp,
    PRIMARY KEY (carrier_milestone_shipment_id, reference_value, reference_qualifier)
);
