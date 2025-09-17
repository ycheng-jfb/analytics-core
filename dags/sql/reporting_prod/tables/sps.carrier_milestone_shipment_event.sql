CREATE TRANSIENT TABLE IF NOT EXISTS reporting_prod.sps.carrier_milestone_shipment_event (
    carrier_milestone_shipment_event_id NUMBER(38, 0) NOT NULL IDENTITY (1,1) UNIQUE,
    carrier_milestone_shipment_id NUMBER(38, 0) NOT NULL
        FOREIGN KEY REFERENCES reporting.sps.carrier_milestone_shipment (carrier_milestone_shipment_id),
    status_code VARCHAR(80) NOT NULL,
    reason_code VARCHAR(80) NULL,
    scan_datetime TIMESTAMP_NTZ(8) NOT NULL,
    city VARCHAR(30),
    state VARCHAR(2),
    country VARCHAR(3),
    filename_datetime TIMESTAMP_NTZ(9),
    meta_create_datetime TIMESTAMP_LTZ DEFAULT current_timestamp,
    meta_update_datetime TIMESTAMP_LTZ DEFAULT current_timestamp
);
ALTER TABLE reporting_prod.sps.carrier_milestone_shipment_event ADD CONSTRAINT primary_key PRIMARY KEY (carrier_milestone_shipment_id, scan_datetime, status_code, reason_code);
