/*
Process overview

    * watermark off lake.sps.carrier_milestone
    * each time pull delta into temp table _delta
    * merge into downstream tables
    * everything should be run in one transaction

Process detail

    * load delta from lake.sps.carrier_milestone into _delta
    * merge _delta into reporting_prod.sps.carrier_milestone
    * using _delta, lookup key from carrier_milestone to produce temp table _base
        - base is same grain as _delta, but with surrogate key
    * merge _base into reporting_prod.sps.carrier_milestone_address
    * using _base, flatten the shipment column to construct _delta_shipment
    * merge _delta_shipment into reporting_prod.sps.carrier_milestone_shipment
    * using _delta_shipment, lookup key from carrier_milestone_shipment to construct _shipment_base
        - _shipment_base is same grain as _shipment_delta but with surrogate key
    * merge _shipment_base into reporting_prod.sps.carrier_milestone_shipment_event
    * merge _shipment_base into reporting_prod.sps.carrier_milestone_shipment_reference

Table list:

    * reporting_prod.sps.carrier_milestone;
    * reporting_prod.sps.carrier_milestone_reference;
    * reporting_prod.sps.carrier_milestone_address;
    * reporting_prod.sps.carrier_milestone_shipment;
    * reporting_prod.sps.carrier_milestone_shipment_event;
    * reporting_prod.sps.carrier_milestone_shipment_reference;

*/

SET low_watermark_datetime = %(low_watermark)s :: TIMESTAMP_LTZ;


CREATE OR REPLACE TEMP TABLE _delta LIKE lake.sps.carrier_milestone;
CREATE OR REPLACE TEMP TABLE _base (
    carrier_milestone_id NUMBER(38, 0),
    scac VARCHAR(50),
    shipper_identification VARCHAR(30),
    shipper_reference VARCHAR(30),
    ship_date DATE,
    address VARIANT,
    reference VARIANT,
    shipment VARIANT,
    filename VARCHAR(1000),
    filename_datetime TIMESTAMP_NTZ(9),
    meta_create_datetime TIMESTAMP_LTZ(9)
);
CREATE OR REPLACE TEMP TABLE _delta_shipment (
    carrier_milestone_id INT,
    assigned_number INT,
    weight_qualifier VARCHAR,
    weight_uom VARCHAR,
    weight VARCHAR,
    total_packages INT,
    shipment_events VARIANT,
    shipment_reference VARIANT,
    filename_datetime TIMESTAMP_NTZ(9)
);
CREATE OR REPLACE TEMP TABLE _shipment_base (
    carrier_milestone_shipment_id INT,
    filename_datetime TIMESTAMP_NTZ(9),
    shipment_events VARIANT,
    shipment_reference VARIANT
);

BEGIN;

INSERT INTO _delta
SELECT
    l.*
FROM lake.sps.carrier_milestone l
WHERE
    l.meta_create_datetime > $low_watermark_datetime;

MERGE INTO reporting_prod.sps.carrier_milestone t
    USING (
        SELECT
            shipper_identification,
            shipper_reference,
            scac,
            ship_date,
            filename_datetime
        FROM (
            SELECT
                s.shipper_identification,
                s.shipper_reference,
                s.scac,
                s.ship_date,
                s.filename_datetime,
                row_number() OVER (PARTITION BY s.shipper_identification, s.shipper_reference, s.scac
                    ORDER BY s.filename_datetime DESC, s.ship_date DESC) AS rn
            FROM _delta s
        ) a
        WHERE
            rn = 1
    ) s
    ON equal_null(t.scac, s.scac)
        AND equal_null(t.shipper_reference, s.shipper_reference)
        AND equal_null(t.shipper_identification, s.shipper_identification)
    WHEN NOT MATCHED THEN INSERT (
                                  shipper_identification,
                                  shipper_reference,
                                  scac,
                                  ship_date,
                                  filename_datetime
        )
        VALUES
        (
            shipper_identification,
            shipper_reference,
            scac,
            ship_date,
            filename_datetime)
    WHEN MATCHED AND s.filename_datetime > t.filename_datetime THEN UPDATE
        SET t.ship_date = s.ship_date,
            t.filename_datetime = s.filename_datetime,
            t.meta_update_datetime = current_timestamp;

-- key lookup: carrier_milestone_id
INSERT INTO _base
SELECT
    ecm.carrier_milestone_id,
    l.*
FROM _delta l
JOIN reporting_prod.sps.carrier_milestone ecm ON equal_null(ecm.scac, l.scac)
        AND equal_null(ecm.shipper_reference, l.shipper_reference)
        AND equal_null(ecm.shipper_identification, l.shipper_identification);

MERGE INTO reporting_prod.sps.carrier_milestone_reference t
    USING (
        SELECT *
        FROM (
            SELECT
                l.carrier_milestone_id,
                c.value:reference_qualifier::STRING AS reference_qualifier,
                c.value:reference_value::STRING AS reference_value,
                l.filename_datetime,
                row_number() OVER (PARTITION BY
                    l.carrier_milestone_id, reference_qualifier,reference_value ORDER BY l.filename_datetime DESC ) AS rn
            FROM _base l,
                LATERAL flatten(INPUT => reference) c
        ) a
        WHERE
            a.rn = 1
    ) s
    ON equal_null(s.carrier_milestone_id, t.carrier_milestone_id)
        AND equal_null(s.reference_value, t.reference_value)
        AND equal_null(s.reference_qualifier, t.reference_qualifier)
    WHEN NOT MATCHED THEN INSERT
        (
         carrier_milestone_id,
         reference_value,
         reference_qualifier,
         filename_datetime)
        VALUES
        (
            carrier_milestone_id,
            reference_value,
            reference_qualifier,
            filename_datetime);


MERGE INTO reporting_prod.sps.carrier_milestone_address t
    USING (
        SELECT *
        FROM (
            SELECT
                l.carrier_milestone_id,
                to_varchar(a.value:entity_identifier) AS entity_identifier,
                to_varchar(a.value:entity_name) AS entity_name,
                to_varchar(a.value:identity_qualifier) AS identity_qualifier,
                to_varchar(a.value:identity_code) AS identity_code,
                to_varchar(a.value:"name") AS name,
                to_varchar(a.value:address_line) AS address_line,
                to_varchar(a.value:city) AS city,
                to_varchar(a.value:state) AS state,
                to_varchar(a.value:zipcode) AS zipcode,
                to_varchar(a.value:country) AS country,
                l.filename_datetime,
                row_number()
                    OVER (PARTITION BY l.carrier_milestone_id, entity_identifier, entity_name
                        ORDER BY l.filename_datetime DESC ) AS rn
            FROM _base l,
                LATERAL flatten(INPUT => l.address) a
        ) l
        WHERE
            l.rn = 1
    ) s ON equal_null(s.carrier_milestone_id, t.carrier_milestone_id)
        AND equal_null(s.entity_identifier, t.entity_identifier)
        AND equal_null(s.entity_name, t.entity_name)
    WHEN NOT MATCHED THEN INSERT
        (
         carrier_milestone_id,
         entity_identifier,
         entity_name,
         identity_qualifier,
         identity_code,
         name,
         address_line,
         city,
         state,
         zipcode,
         country,
         filename_datetime)
        VALUES
        (
            carrier_milestone_id,
            entity_identifier,
            entity_name,
            identity_qualifier,
            identity_code,
            name,
            address_line,
            city,
            state,
            zipcode,
            country,
            filename_datetime)
    WHEN MATCHED AND s.filename_datetime > t.filename_datetime THEN UPDATE
        SET t.address_line = s.address_line,
            t.city = s.city,
            t.state = s.state,
            t.zipcode = s.zipcode,
            t.country = s.country,
            t.filename_datetime = s.filename_datetime,
            t.meta_update_datetime = current_timestamp;

INSERT INTO _delta_shipment
SELECT
    l.carrier_milestone_id,
    to_number(c.value:assigned_number) AS assigned_number,
    to_varchar(c.value:weight_qualifier) AS weight_qualifier,
    to_varchar(c.value:weight_uom) AS weight_uom,
    to_number(nullif(c.value:weight, '')) AS weight,
    to_number(nullif(c.value:total_packages, ''), 10, 0) AS total_packages,
    c.value:event AS shipment_events,
    c.value:shipment_reference AS shipment_reference,
    l.filename_datetime
FROM _base l,
    LATERAL flatten(INPUT => l.shipment) c;


MERGE INTO reporting_prod.sps.carrier_milestone_shipment t
    USING (
        SELECT *
        FROM (
            SELECT *,
                row_number()
                    OVER (PARTITION BY l.carrier_milestone_id, assigned_number ORDER BY l.filename_datetime DESC ) AS rn
            FROM _delta_shipment l
        ) a
        WHERE
            a.rn = 1
    ) s
    ON equal_null(s.carrier_milestone_id, t.carrier_milestone_id)
        AND equal_null(s.assigned_number, t.assigned_number)
    WHEN NOT MATCHED THEN
        INSERT
            (
             carrier_milestone_id,
             assigned_number,
             weight_qualifier,
             weight_uom,
             weight,
             total_packages,
             filename_datetime
                )
            VALUES
            (
                carrier_milestone_id,
                assigned_number,
                weight_qualifier,
                weight_uom,
                weight,
                total_packages,
                filename_datetime);


-- key lookup: carrier_milestone_shipment_id
INSERT INTO _shipment_base
SELECT
    cms.carrier_milestone_shipment_id,
    l.filename_datetime,
    l.shipment_events,
    l.shipment_reference
FROM _delta_shipment l
JOIN reporting_prod.sps.carrier_milestone_shipment cms ON equal_null(cms.carrier_milestone_id, l.carrier_milestone_id)
        AND equal_null(cms.assigned_number, l.assigned_number);


MERGE INTO reporting_prod.sps.carrier_milestone_shipment_event t
    USING (
        SELECT *
        FROM (
            SELECT
                sb.carrier_milestone_shipment_id,
                to_varchar(c.value:status_code) AS status_code,
                to_varchar(c.value:reason_code) AS reason_code,
                to_timestamp_ntz(to_varchar(c.value:scan_date), 'YYYY-MM-DDHH24:MI:SS.FF') AS scan_datetime,
                to_varchar(c.value:city) AS city,
                to_varchar(c.value:state) AS state,
                to_varchar(c.value:country) AS country,
                sb.filename_datetime,
                row_number() OVER (
                    PARTITION BY sb.carrier_milestone_shipment_id, scan_datetime, status_code, reason_code
                    ORDER BY sb.filename_datetime DESC
                    ) AS rn
            FROM _shipment_base sb,
                LATERAL flatten(INPUT => sb.shipment_events) c
        ) a
        WHERE
            a.rn = 1
    ) s
    ON equal_null(s.carrier_milestone_shipment_id, t.carrier_milestone_shipment_id)
        AND equal_null(s.scan_datetime, t.scan_datetime)
        AND equal_null(s.status_code, t.status_code)
        AND equal_null(s.reason_code, t.reason_code)
    WHEN NOT MATCHED THEN INSERT
        (
         carrier_milestone_shipment_id,
         status_code,
         reason_code,
         scan_datetime,
         city,
         state,
         country,
         filename_datetime)
        VALUES
        (
            carrier_milestone_shipment_id,
            status_code,
            reason_code,
            scan_datetime,
            city,
            state,
            country,
            filename_datetime);


MERGE INTO reporting_prod.sps.carrier_milestone_shipment_reference t
    USING (
        SELECT *
        FROM (
            SELECT
                sb.carrier_milestone_shipment_id,
                to_varchar(c.value:reference_value) AS reference_value,
                to_varchar(c.value:reference_qualifier) AS reference_qualifier,
                sb.filename_datetime,
                row_number()
                    OVER (PARTITION BY sb.carrier_milestone_shipment_id, reference_value, reference_qualifier ORDER BY sb.filename_datetime DESC ) AS rn
            FROM _shipment_base sb,
                LATERAL flatten(INPUT => sb.shipment_reference) c
        ) a
        WHERE
            a.rn = 1
    ) s
    ON equal_null(s.carrier_milestone_shipment_id, t.carrier_milestone_shipment_id)
        AND equal_null(s.reference_value, t.reference_value)
        AND equal_null(s.reference_qualifier, t.reference_qualifier)
    WHEN NOT MATCHED THEN INSERT
        (
         carrier_milestone_shipment_id,
         reference_value,
         reference_qualifier,
         filename_datetime
            )
        VALUES
        (
            carrier_milestone_shipment_id,
            reference_value,
            reference_qualifier,
            filename_datetime);

COMMIT;
