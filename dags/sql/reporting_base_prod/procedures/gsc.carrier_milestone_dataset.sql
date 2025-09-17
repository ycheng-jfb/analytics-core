BEGIN TRANSACTION NAME gsc_carrier_milestone_dataset;

SET date_filter = (
    SELECT COALESCE(DATEADD(DAY, -4, CAST(MIN(DATE_FILTER) AS DATE)), '1900-01-01') AS DATE_FILTER
    FROM (
          SELECT MAX(estimated_delivery_date_time) AS DATE_FILTER FROM REPORTING_BASE_PROD.GSC.CARRIER_MILESTONE_SCAN_DATETIME
          UNION ALL
          SELECT MAX(shipment_acknowledged_date_time) AS DATE_FILTER FROM REPORTING_BASE_PROD.GSC.CARRIER_MILESTONE_SCAN_DATETIME
          UNION ALL
          SELECT MAX(shipment_cancelled_date_time) AS DATE_FILTER FROM REPORTING_BASE_PROD.GSC.CARRIER_MILESTONE_SCAN_DATETIME
          UNION ALL
          SELECT MAX(arrived_at_terminal_date_time) AS DATE_FILTER FROM REPORTING_BASE_PROD.GSC.CARRIER_MILESTONE_SCAN_DATETIME
          UNION ALL
          SELECT MAX(en_route_date_time) AS DATE_FILTER FROM REPORTING_BASE_PROD.GSC.CARRIER_MILESTONE_SCAN_DATETIME
          UNION ALL
          SELECT MAX(shipment_delayed_date_time) AS DATE_FILTER FROM REPORTING_BASE_PROD.GSC.CARRIER_MILESTONE_SCAN_DATETIME
          UNION ALL
          SELECT MAX(shipment_damaged_date_time) AS DATE_FILTER FROM REPORTING_BASE_PROD.GSC.CARRIER_MILESTONE_SCAN_DATETIME
          UNION ALL
          SELECT MAX(returned_date_time) AS DATE_FILTER FROM REPORTING_BASE_PROD.GSC.CARRIER_MILESTONE_SCAN_DATETIME
          UNION ALL
          SELECT MAX(delivered_date_time) AS DATE_FILTER FROM REPORTING_BASE_PROD.GSC.CARRIER_MILESTONE_SCAN_DATETIME
          UNION ALL
          SELECT MAX(ingested_into_network_date_time) AS DATE_FILTER FROM REPORTING_BASE_PROD.GSC.CARRIER_MILESTONE_SCAN_DATETIME
        ) AS A
    );

CREATE OR REPLACE TEMPORARY TABLE _tracking_number_filter AS
SELECT DISTINCT
    CASE
        WHEN m.SCAC in ('PBPS','NGST')
          THEN msr.reference_value
        WHEN m.scac = 'PRLA' and m.shipper_reference ILIKE '%Order #:%' THEN m.shipper_identification
        WHEN m.SCAC in ('PRLA','PCLA','BPO')
          THEN m.SHIPPER_REFERENCE
        ELSE m.SHIPPER_IDENTIFICATION
        END AS TRACKING_NUMBER
from REPORTING_PROD.SPS.CARRIER_MILESTONE m
    LEFT JOIN REPORTING_PROD.SPS.CARRIER_MILESTONE_ADDRESS ma
        ON m.carrier_milestone_id = ma.carrier_milestone_id
        AND ma.entity_identifier = 'Ship To'
    LEFT JOIN REPORTING_PROD.SPS.CARRIER_MILESTONE_REFERENCE mr
        ON mr.carrier_milestone_id = m.carrier_milestone_id
    LEFT JOIN REPORTING_PROD.SPS.CARRIER_MILESTONE_SHIPMENT ms
        ON ms.carrier_milestone_id = m.carrier_milestone_id
    LEFT JOIN REPORTING_PROD.SPS.CARRIER_MILESTONE_SHIPMENT_REFERENCE msr
        ON msr.carrier_milestone_shipment_id = ms.carrier_milestone_shipment_id
    LEFT JOIN REPORTING_BASE_PROD.GSC.CARRIER_MILESTONE_DETAIL CMD
        ON CASE
              WHEN m.SCAC in ('PBPS','NGST')
                THEN msr.reference_value
              WHEN m.scac = 'PRLA' and m.shipper_reference ILIKE '%Order #:%' THEN m.shipper_identification
              WHEN m.SCAC in ('PRLA','PCLA','BPO')
                THEN m.SHIPPER_REFERENCE
              ELSE m.SHIPPER_IDENTIFICATION
          END = CMD.tracking_number
    LEFT JOIN REPORTING_BASE_PROD.GSC.CARRIER_MILESTONE_SCAN_DATETIME SD
        ON  CASE
              WHEN m.SCAC in ('PBPS','NGST')
                THEN msr.reference_value
              WHEN m.scac = 'PRLA' and m.shipper_reference ILIKE '%Order #:%' THEN m.shipper_identification
              WHEN m.SCAC in ('PRLA','PCLA','BPO')
                THEN m.SHIPPER_REFERENCE
              ELSE m.SHIPPER_IDENTIFICATION
          END = SD.tracking_number
WHERE (
                  m.meta_update_datetime >= $date_filter
              OR ma.meta_update_datetime >= $date_filter
              OR mr.meta_update_datetime >= $date_filter
              OR ms.meta_update_datetime >= $date_filter
              OR msr.meta_update_datetime >= $date_filter
              OR cmd.meta_update_datetime >= $date_filter
              OR sd.meta_update_datetime >= $date_filter
          )
ORDER BY tracking_number ASC;

/* Delete "old" data */
DELETE FROM REPORTING_BASE_PROD.gsc.carrier_milestone_dataset
WHERE TRACKING_NUMBER IN (SELECT TRACKING_NUMBER FROM _tracking_number_filter WHERE TRACKING_NUMBER IS NOT NULL)
;

/* Insert new copies of data */
insert into REPORTING_BASE_PROD.gsc.carrier_milestone_dataset (
    FC,
    TRACKING_NUMBER,
    REFERENCE_FIELD_1,
    SCAC,
    CARRIER,
    SHIP_DATE,
    REFERENCE_FIELD_2,
    REFERENCE_TYPE_2,
    MILESTONE_WEIGHT_QUALIFIER,
    MILESTONE_WEIGHT_UOM,
    MILESTONE_WEIGHT,
    TOTAL_PACKAGES,
    MILESTONE_WEIGHT_POUNDS,
    ESTIMATED_DELIVERY_DATE_TIME,
    SHIPMENT_ACKNOWLEDGED_DATE_TIME,
    SHIPMENT_CANCELLED_DATE_TIME,
    ARRIVED_AT_TERMINAL_DATE_TIME,
    ARRIVED_AT_TERMINAL_CITY,
    ARRIVED_AT_TERMINAL_STATE,
    ARRIVED_AT_TERMINAL_COUNTRY,
    EN_ROUTE_DATE_TIME,
    EN_ROUTE_CITY,
    EN_ROUTE_STATE,
    EN_ROUTE_COUNTRY,
    SHIPMENT_DELAYED_DATE_TIME,
    SHIPMENT_DELAYED_CITY,
    SHIPMENT_DELAYED_STATE,
    SHIPMENT_DELAYED_COUNTRY,
    SHIPMENT_DAMAGED_TIME,
    SHIPMENT_DAMAGED_CITY,
    SHIPMENT_DAMAGED_STATE,
    SHIPMENT_DAMAGED_COUNTRY,
    RETURNED_DATE_TIME,
    RETURNED_CITY,
    RETURNED_STATE,
    RETURNED_COUNTRY,
    DELIVERED_DATE_TIME,
    DELIVERED_CITY,
    DELIVERED_STATE,
    DELIVERED_COUNTRY,
    REASON_CODE,
    REFERENCE_FIELD_3,
    REFERENCE_TYPE_3,
    SHIPMENT_TYPE,
    INGESTED_INTO_NETWORK_DATE_TIME)
select DISTINCT
    FC,
    tracking_number,
    first_value(reference_field_1) over (partition by tracking_number order by carrier_milestone_shipment_reference_id desc) as reference_field_1,
    SCAC,
    CASE
        WHEN SCAC = 'AUS' THEN 'ATPost'
        WHEN SCAC = 'BPO' THEN 'Bpost'
        WHEN SCAC = 'COL' THEN 'Colissimo'
        WHEN SCAC in ('DHL','DPEE') THEN 'DHL'
        WHEN SCAC = 'DPN' THEN 'DPD'
        WHEN SCAC in ('FDE','FDEG','FXSP') THEN 'FedEx'
        WHEN SCAC in ('PBPS','NGST') THEN 'Pitney Bowes'
        WHEN SCAC in ('TPP','PNL') THEN 'PostNL'
        WHEN SCAC in ('PRLA','PURQ') THEN 'Purolator'
    END as carrier,
    SHIP_DATE,
    first_value(reference_field_2) over (partition by tracking_number order by carrier_milestone_shipment_reference_id desc) as reference_field_2
    ,first_value(reference_type_2) over (partition by tracking_number order by carrier_milestone_shipment_reference_id desc) as reference_type_2,
    first_value(milestone_weight_qualifier) over (partition by tracking_number order by milestone_weight_qualifier asc) AS milestone_weight_qualifier,
    first_value(milestone_weight_uom) over (partition by tracking_number order by milestone_weight_uom asc) AS milestone_weight_uom,
    first_value(milestone_weight) over (partition by tracking_number order by milestone_weight asc) AS milestone_weight,
    first_value(total_packages) over (partition by tracking_number order by total_packages asc) AS total_packages,
    first_value(milestone_weight_pounds) over (partition by tracking_number order by milestone_weight_pounds asc) AS milestone_weight_pounds,
    estimated_delivery_date_time
    ,shipment_acknowledged_date_time
    ,shipment_cancelled_date_time
    ,arrived_at_terminal_date_time
    ,arrived_at_terminal_city
    ,arrived_at_terminal_state
    ,arrived_at_terminal_country
    ,en_route_date_time
    ,en_route_city
    ,en_route_state
    ,en_route_country
    ,shipment_delayed_date_time
    ,shipment_delayed_city
    ,shipment_delayed_state
    ,shipment_delayed_country
    ,shipment_damaged_time
    ,shipment_damaged_city
    ,shipment_damaged_state
    ,shipment_damaged_country
    ,returned_date_time
    ,returned_city
    ,returned_state
    ,returned_country
    ,delivered_date_time
    ,delivered_city
    ,delivered_state
    ,delivered_country
    ,reason_code
    ,first_value(reference_field_3) over (partition by tracking_number order by carrier_milestone_shipment_reference_id desc) as reference_field_3
    ,first_value(reference_type_3) over (partition by tracking_number order by carrier_milestone_shipment_reference_id desc) as reference_type_3,
    shipment_type,
    ingested_into_network_date_time
from (
    select distinct
        CASE
            WHEN ma.entity_identifier = 'Ship To' and ma.address_line ilike '290 E%' and ma.address_line ilike '%Markham S%' and ma.city ilike '%PERRIS%' and ma.zipcode like '92571%' THEN 'JF - Perris (JFPERRIS)'
            WHEN ma.entity_identifier = 'Ship To' and ma.address_line ilike '7865 Nat%' and (ma.address_line ilike '%Turnpike%' or ma.address_line ilike '&Tpke%') and ma.city ilike '%LOUISVILLE%' and ma.zipcode ilike '%40214%' THEN 'JF - Kentucky (JFW001)'
            WHEN ma.entity_identifier = 'Ship To' and ma.address_line ilike '8161 Nat%' and (ma.address_line ilike '%Turnpike%' or ma.address_line ilike '&Tpke%') and ma.city ilike '%LOUISVILLE%' and ma.zipcode ilike '%40214%' THEN 'JF - Kentucky (JFW002)'
            WHEN ma.entity_identifier = 'Ship To' and ma.address_line ilike '350 Admiral B%' and ma.city ilike '%MISSISSAUGA%' and ma.zipcode like '%L5T 2N6%' THEN 'JF Canada (CAN001)'
            WHEN ma.entity_identifier = 'Ship To' and ma.address_line ilike '%JOHN HICKSSTRAAT%' and ma.city ilike '%VENLO%' and ma.zipcode ilike '%5928SJ%' THEN 'JF - Netherlands (JFEU0001)'
        END as FC,
        CASE
          WHEN m.SCAC in ('PBPS','NGST')
            THEN msr.reference_value
          WHEN m.scac = 'PRLA' and m.shipper_reference ILIKE '%Order #:%'
            THEN m.shipper_identification
          WHEN m.SCAC in ('PRLA','PCLA','BPO')
            THEN m.SHIPPER_REFERENCE
          ELSE m.SHIPPER_IDENTIFICATION
        END AS tracking_number,
        CASE WHEN m.SCAC in ('PRLA','PCLA','BPO') THEN m.SHIPPER_IDENTIFICATION ELSE m.SHIPPER_REFERENCE END AS reference_field_1,
        m.SCAC,
        m.SHIP_DATE,
        mr.reference_value AS reference_field_2,
        mr.reference_qualifier AS reference_type_2,
        ms.weight_qualifier AS milestone_weight_qualifier,
        ms.weight_uom AS milestone_weight_uom,
        ms.weight AS milestone_weight,
        ms.total_packages,
        CASE ms.weight_uom WHEN 'Pounds' THEN ms.weight
        WHEN 'Kilograms' THEN ms.weight * 2.20462
        ELSE NULL END AS milestone_weight_pounds,
        COALESCE(SD.estimated_delivery_date_time, '1900-01-01') AS estimated_delivery_date_time,
        COALESCE(SD.shipment_acknowledged_date_time, '1900-01-01') AS shipment_acknowledged_date_time,
        COALESCE(SD.shipment_cancelled_date_time, '1900-01-01') AS shipment_cancelled_date_time,
        COALESCE(SD.arrived_at_terminal_date_time, '1900-01-01') AS arrived_at_terminal_date_time,
        CMD.arrived_at_terminal_city,
        CMD.arrived_at_terminal_state,
        CMD.arrived_at_terminal_country,
        COALESCE(SD.en_route_date_time, '1900-01-01') AS en_route_date_time,
        CMD.en_route_city,
        CMD.en_route_state,
        CMD.en_route_country,
        COALESCE(SD.shipment_delayed_date_time, '1900-01-01') AS shipment_delayed_date_time,
        CMD.shipment_delayed_city,
        CMD.shipment_delayed_state,
        CMD.shipment_delayed_country,
        COALESCE(SD.shipment_damaged_date_time, '1900-01-01') AS shipment_damaged_time,
        CMD.shipment_damaged_city,
        CMD.shipment_damaged_state,
        CMD.shipment_damaged_country,
        COALESCE(SD.returned_date_time, '1900-01-01') AS returned_date_time,
        CMD.returned_city,
        CMD.returned_state,
        CMD.returned_country,
        COALESCE(SD.delivered_date_time, '1900-01-01') AS delivered_date_time,
        CMD.delivered_city,
        CMD.delivered_state,
        CMD.delivered_country,
        CMD.delivered_reason as reason_code,
        msr.reference_value AS reference_field_3,
        msr.reference_qualifier AS reference_type_3,
        CASE
            WHEN m.SCAC in ('PBPS','NGST') THEN 'Inbound'
            WHEN ma.entity_identifier = 'Ship To' and ma.address_line ilike '290 E%' and ma.address_line ilike '%Markham S%' and ma.city ilike '%PERRIS%' and ma.zipcode like '92571%' THEN 'Inbound'
            WHEN ma.entity_identifier = 'Ship To' and (ma.address_line ilike '7865 Nat%' or ma.address_line ilike '8161 Nat%') and (ma.address_line ilike '%Turnpike%' or ma.address_line ilike '&Tpke%') and ma.city ilike '%LOUISVILLE%' and ma.zipcode ilike '%40214%' THEN 'Inbound'
            WHEN ma.entity_identifier = 'Ship To' and ma.address_line ilike '350 Admiral B%' and ma.city ilike '%MISSISSAUGA%' and ma.zipcode like '%L5T 2N6%' THEN 'Inbound'
            WHEN ma.entity_identifier = 'Ship To' and ma.address_line ilike '%JOHN HICKSSTRAAT%' and ma.city ilike '%VENLO%' and ma.zipcode ilike '%5928SJ%' THEN 'Inbound'
            ELSE 'Outbound' END
        AS shipment_type,
        COALESCE(SD.ingested_into_network_date_time,SD.arrived_at_terminal_date_time) AS ingested_into_network_date_time
        ,ms.carrier_milestone_shipment_id
        ,carrier_milestone_shipment_reference_id
    from REPORTING_PROD.SPS.CARRIER_MILESTONE m
    LEFT JOIN REPORTING_PROD.SPS.CARRIER_MILESTONE_ADDRESS ma
        ON m.carrier_milestone_id = ma.carrier_milestone_id
        AND ma.entity_identifier = 'Ship To'
    LEFT JOIN REPORTING_PROD.SPS.CARRIER_MILESTONE_REFERENCE mr
        ON mr.carrier_milestone_id = m.carrier_milestone_id
    LEFT JOIN REPORTING_PROD.SPS.CARRIER_MILESTONE_SHIPMENT ms
        ON ms.carrier_milestone_id = m.carrier_milestone_id
    LEFT JOIN REPORTING_PROD.SPS.CARRIER_MILESTONE_SHIPMENT_REFERENCE msr
        ON msr.carrier_milestone_shipment_id = ms.carrier_milestone_shipment_id
    LEFT JOIN REPORTING_BASE_PROD.GSC.CARRIER_MILESTONE_DETAIL CMD
        ON CASE
              WHEN m.SCAC in ('PBPS','NGST')
                THEN msr.reference_value
              WHEN m.scac = 'PRLA' and m.shipper_reference ILIKE '%Order #:%'
                THEN m.shipper_identification
              WHEN m.SCAC in ('PRLA','PCLA','BPO')
                THEN m.SHIPPER_REFERENCE
              ELSE m.SHIPPER_IDENTIFICATION
          END = CMD.tracking_number
    LEFT JOIN REPORTING_BASE_PROD.GSC.CARRIER_MILESTONE_SCAN_DATETIME SD
        ON CASE
              WHEN m.SCAC in ('PBPS','NGST')
                THEN msr.reference_value
              WHEN m.scac = 'PRLA' and m.shipper_reference ILIKE '%Order #:%'
                THEN m.shipper_identification
              WHEN m.SCAC in ('PRLA','PCLA','BPO')
                THEN m.SHIPPER_REFERENCE
              ELSE m.SHIPPER_IDENTIFICATION
          END = SD.tracking_number

    WHERE
        CASE
            WHEN m.SCAC in ('PBPS','NGST')
            THEN msr.reference_value
            WHEN m.scac = 'PRLA' and m.shipper_reference ILIKE '%Order #:%'
            THEN m.shipper_identification
            WHEN m.SCAC in ('PRLA','PCLA','BPO')
            THEN m.SHIPPER_REFERENCE
            ELSE m.SHIPPER_IDENTIFICATION
            END
        IN (SELECT DISTINCT TRACKING_NUMBER FROM _tracking_number_filter WHERE TRACKING_NUMBER IS NOT NULL)
    ) AS A
ORDER BY tracking_number ASC
;


/*******************
add Hermes data
********************/

insert into REPORTING_BASE_PROD.gsc.carrier_milestone_dataset
(tracking_number, delivered_date_time)
select
    tracking_id,
    date_time
from REPORTING_BASE_PROD.GSC.HERMES_DELIVERED_EVENTS
WHERE tracking_id not in(
    SELECT nvl(tracking_number, '0') as tracking_id
    FROM REPORTING_BASE_PROD.gsc.carrier_milestone_dataset
    WHERE estimated_delivery_date_time IS NULL
    /* The first INSERT COALESCEs this column so we can use it as a proxy for the HERMES rows */
)
ORDER BY tracking_id;

COMMIT;
