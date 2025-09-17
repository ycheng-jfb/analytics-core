create or replace temporary table _scan_dates as
select
    CASE
        WHEN m.SCAC in ('PBPS','NGST')
        THEN msr.reference_value
        WHEN m.scac = 'PRLA' and m.shipper_reference ILIKE '%Order #:%'
        THEN m.shipper_identification
        WHEN m.SCAC in ('PRLA','PCLA','BPO')
        THEN m.SHIPPER_REFERENCE
        ELSE m.SHIPPER_IDENTIFICATION
    END AS tracking_number,
    se.scan_datetime
, case
    when se.status_code in ('Estimated Delivery','EstimatedDelivery')
    then 'estimated_delivery_date_time'
    when se.status_code in ('Shipment Acknowledged','ShipmentAcknowledged')
    then 'shipment_acknowledged_date_time'
    when se.status_code in ('Shipment Cancelled','Refused by Consignee')
    then 'shipment_cancelled_date_time'
    when se.status_code in ('Arrived at Terminal Location','Arrived at Pick-up Location','ArrivedatPick-upLocation','ArrivedatTerminalLocation')
    then 'arrived_at_terminal_date_time'
    when se.status_code in ('En Route to Delivery Location','Departed Terminal Location','Tendered for Delivery','DepartedTerminalLocation')
    then 'en_route_date_time'
    when se.status_code in ('Shipment Delayed', 'Delivery Not Completed', 'Attempted Delivery','ShipmentDelayed')
    then 'shipment_delayed_date_time'
    when se.status_code = 'Shipment Damaged'
    then 'shipment_damaged_date_time'
    when se.status_code = 'Shipment Returned to Shipper'
    then 'returned_date_time'
    when se.status_code in ('Delivered','Arrived at Delivery Location','Completed Unloading at Delivery Location','Carrier Departed Delivery Location','CompletedUnloadingatDeliveryLocation')
    then 'delivered_date_time'
    when se.status_code in ('Completed Loading at Pick-up Location','Carrier Departed Pick-up Location with Shipment','Loading','Loaded on Truck','Out-Gate','Available for Delivery','CarrierDepartedPick-upLocationwithShipment','LoadedonTruck')
    then 'ingested_into_network_date_time'
    end as status_code
FROM REPORTING_PROD.SPS.CARRIER_MILESTONE m
LEFT JOIN REPORTING_PROD.SPS.CARRIER_MILESTONE_SHIPMENT MS
    ON M.CARRIER_MILESTONE_ID = MS.CARRIER_MILESTONE_ID
LEFT JOIN REPORTING_PROD.SPS.CARRIER_MILESTONE_SHIPMENT_EVENT SE
    ON MS.CARRIER_MILESTONE_SHIPMENT_ID = SE.CARRIER_MILESTONE_SHIPMENT_ID
LEFT JOIN REPORTING_PROD.SPS.CARRIER_MILESTONE_SHIPMENT_REFERENCE MSR
    ON MS.CARRIER_MILESTONE_SHIPMENT_ID = MSR.CARRIER_MILESTONE_SHIPMENT_ID
;

create or replace temporary table _scan_pivot
as
select *
from(
    select
        tracking_number,
        scan_datetime,
        status_code
    from _scan_dates
) AS M
pivot(
min(scan_datetime)
for status_code in (
    'estimated_delivery_date_time',
    'shipment_acknowledged_date_time',
    'shipment_cancelled_date_time',
    'arrived_at_terminal_date_time',
    'en_route_date_time',
    'shipment_delayed_date_time',
    'shipment_damaged_date_time',
    'returned_date_time',
    'delivered_date_time',
    'ingested_into_network_date_time'
        )
    ) AS p (
    tracking_number,
    estimated_delivery_date_time,
    shipment_acknowledged_date_time,
    shipment_cancelled_date_time,
    arrived_at_terminal_date_time,
    en_route_date_time,
    shipment_delayed_date_time,
    shipment_damaged_date_time,
    returned_date_time,
    delivered_date_time,
    ingested_into_network_date_time
);

MERGE INTO REPORTING_BASE_PROD.GSC.CARRIER_MILESTONE_SCAN_DATETIME TGT
USING (select
       tracking_number,
       estimated_delivery_date_time,
       shipment_acknowledged_date_time,
       shipment_cancelled_date_time,
       arrived_at_terminal_date_time,
       en_route_date_time,
       shipment_delayed_date_time,
       shipment_damaged_date_time,
       returned_date_time,
       delivered_date_time,
       ingested_into_network_date_time
        from _SCAN_PIVOT
       )SRC
    ON TGT.TRACKING_NUMBER = SRC.TRACKING_NUMBER
WHEN MATCHED AND (
    COALESCE(TGT.estimated_delivery_date_time, '1970-01-01 00:00:00') <> COALESCE(SRC.estimated_delivery_date_time, '1970-01-01 00:00:00')
    OR COALESCE(TGT.shipment_acknowledged_date_time, '1970-01-01 00:00:00') <> COALESCE(SRC.shipment_acknowledged_date_time, '1970-01-01 00:00:00')
    OR COALESCE(TGT.shipment_cancelled_date_time, '1970-01-01 00:00:00') <> COALESCE(SRC.shipment_cancelled_date_time, '1970-01-01 00:00:00')
    OR COALESCE(TGT.arrived_at_terminal_date_time, '1970-01-01 00:00:00') <> COALESCE(SRC.arrived_at_terminal_date_time, '1970-01-01 00:00:00')
    OR COALESCE(TGT.en_route_date_time, '1970-01-01 00:00:00') <> COALESCE(SRC.en_route_date_time, '1970-01-01 00:00:00')
    OR COALESCE(TGT.shipment_delayed_date_time, '1970-01-01 00:00:00') <> COALESCE(SRC.shipment_delayed_date_time, '1970-01-01 00:00:00')
    OR COALESCE(TGT.shipment_damaged_date_time, '1970-01-01 00:00:00') <> COALESCE(SRC.shipment_damaged_date_time, '1970-01-01 00:00:00')
    OR COALESCE(TGT.returned_date_time, '1970-01-01 00:00:00') <> COALESCE(SRC.returned_date_time, '1970-01-01 00:00:00')
    OR COALESCE(TGT.delivered_date_time, '1970-01-01 00:00:00') <> COALESCE(SRC.delivered_date_time, '1970-01-01 00:00:00')
    OR COALESCE(TGT.ingested_into_network_date_time, '1970-01-01 00:00:00') <> COALESCE(SRC.ingested_into_network_date_time, '1970-01-01 00:00:00')
    )
THEN UPDATE
SET
    TGT.estimated_delivery_date_time = SRC.estimated_delivery_date_time,
    TGT.shipment_acknowledged_date_time = SRC.shipment_acknowledged_date_time,
    TGT.shipment_cancelled_date_time = SRC.shipment_cancelled_date_time,
    TGT.arrived_at_terminal_date_time = SRC.arrived_at_terminal_date_time,
    TGT.en_route_date_time = SRC.en_route_date_time,
    TGT.shipment_delayed_date_time = SRC.shipment_delayed_date_time,
    TGT.shipment_damaged_date_time = SRC.shipment_damaged_date_time,
    TGT.returned_date_time = SRC.returned_date_time,
    TGT.delivered_date_time = SRC.delivered_date_time,
    TGT.ingested_into_network_date_time = SRC.ingested_into_network_date_time,
    TGT.META_UPDATE_DATETIME = CURRENT_TIMESTAMP()::TIMESTAMP_LTZ
WHEN NOT MATCHED THEN INSERT(
    TRACKING_NUMBER,
    estimated_delivery_date_time,
    shipment_acknowledged_date_time,
    shipment_cancelled_date_time,
    arrived_at_terminal_date_time,
    en_route_date_time,
    shipment_delayed_date_time,
    shipment_damaged_date_time,
    returned_date_time,
    delivered_date_time,
    ingested_into_network_date_time
)
VALUES(
    SRC.TRACKING_NUMBER,
    SRC.estimated_delivery_date_time,
    SRC.shipment_acknowledged_date_time,
    SRC.shipment_cancelled_date_time,
    SRC.arrived_at_terminal_date_time,
    SRC.en_route_date_time,
    SRC.shipment_delayed_date_time,
    SRC.shipment_damaged_date_time,
    SRC.returned_date_time,
    SRC.delivered_date_time,
    SRC.ingested_into_network_date_time
);

/*************************
_tracking_numbers TEMP TABLE
**************************/

SET min_timestamp = (SELECT MAX(META_UPDATE_DATETIME) FROM REPORTING_BASE_PROD.GSC.CARRIER_MILESTONE_SCAN_DATETIME);

CREATE OR REPLACE TEMPORARY TABLE _tracking_numbers AS
SELECT DISTINCT
    CARRIER_MILESTONE_SHIPMENT_ID,
    tracking_number
FROM (
    select distinct
        MS.CARRIER_MILESTONE_SHIPMENT_ID,
        CASE
          WHEN m.SCAC in ('PBPS','NGST')
            THEN msr.reference_value
          WHEN m.scac = 'PRLA' and m.shipper_reference ILIKE '%Order #:%'
            THEN m.shipper_identification
          WHEN m.SCAC in ('PRLA','PCLA','BPO')
            THEN m.SHIPPER_REFERENCE
          ELSE m.SHIPPER_IDENTIFICATION
        END AS tracking_number
    FROM REPORTING_PROD.SPS.CARRIER_MILESTONE m
    JOIN REPORTING_PROD.SPS.CARRIER_MILESTONE_SHIPMENT MS
        ON M.CARRIER_MILESTONE_ID = MS.CARRIER_MILESTONE_ID
    JOIN REPORTING_PROD.SPS.CARRIER_MILESTONE_SHIPMENT_EVENT SE
        ON MS.CARRIER_MILESTONE_SHIPMENT_ID = SE.CARRIER_MILESTONE_SHIPMENT_ID
    LEFT JOIN REPORTING_PROD.SPS.CARRIER_MILESTONE_SHIPMENT_REFERENCE MSR
        ON MS.CARRIER_MILESTONE_SHIPMENT_ID = MSR.CARRIER_MILESTONE_SHIPMENT_ID
    ) AS A
WHERE tracking_number IN (
    SELECT DISTINCT tracking_number
    FROM (
        SELECT tracking_number
        FROM _scan_dates
        WHERE scan_datetime >= DATEADD(HOUR, -3, $min_timestamp)

        UNION ALL
        SELECT tracking_number
        FROM REPORTING_BASE_PROD.GSC.CARRIER_MILESTONE_SCAN_DATETIME
        WHERE META_UPDATE_DATETIME >= DATEADD(HOUR, -1, $min_timestamp)

        UNION ALL
        SELECT tracking_number
        FROM REPORTING_BASE_PROD.GSC.CARRIER_MILESTONE_DETAIL
        WHERE META_UPDATE_DATETIME >= DATEADD(HOUR, -3, $min_timestamp)
        ) AS A
    WHERE tracking_number IS NOT NULL
    )
ORDER BY CARRIER_MILESTONE_SHIPMENT_ID ASC;


/*************************
CARRIER_MILESTONE_DETAIL
**************************/

create or replace temporary table _milestone
(
  TRACKING_NUMBER VARCHAR(255),
  arrived_at_terminal_city varchar(255),
  arrived_at_terminal_state varchar(255),
  arrived_at_terminal_country varchar(255),
  arrived_at_terminal_reason varchar(255),
  en_route_city varchar(255),
  en_route_state varchar(255),
  en_route_country varchar(255),
  en_route_reason varchar(255),
  shipment_delayed_city varchar(255),
  shipment_delayed_state varchar(255),
  shipment_delayed_country varchar(255),
  shipment_delayed_reason varchar(255),
  shipment_damaged_city varchar(255),
  shipment_damaged_state varchar(255),
  shipment_damaged_country varchar(255),
  shipment_damaged_reason varchar(255),
  returned_city varchar(255),
  returned_state varchar(255),
  returned_country varchar(255),
  returned_reason varchar(255),
  delivered_city varchar(255),
  delivered_state varchar(255),
  delivered_country varchar(255),
  delivered_reason varchar(255)
);

/***********
arrived_at_terminal
************/
Merge into _milestone tgt
using (
    select distinct
        t.tracking_number
        ,first_value(se.city) over (partition by t.tracking_number order by se.scan_datetime desc) as city
        ,first_value(se.state) over (partition by t.tracking_number order by se.scan_datetime desc) as state
        ,first_value(se.country) over (partition by t.tracking_number order by se.scan_datetime desc) as country
        ,first_value(se.reason_code) over (partition by t.tracking_number order by se.scan_datetime desc) as reason_code
    FROM _tracking_numbers as t
    JOIN REPORTING_PROD.SPS.CARRIER_MILESTONE_SHIPMENT_EVENT SE
        ON t.CARRIER_MILESTONE_SHIPMENT_ID = SE.CARRIER_MILESTONE_SHIPMENT_ID
    where se.status_code in ('Arrived at Terminal Location')
) src
    on tgt.tracking_number = src.tracking_number
when matched
then update
set tgt.arrived_at_terminal_city = src.city,
    tgt.arrived_at_terminal_state = src.state,
    tgt.arrived_at_terminal_country = src.country,
    tgt.arrived_at_terminal_reason = src.reason_code
when not matched
then insert(
    TRACKING_NUMBER, arrived_at_terminal_city, arrived_at_terminal_state, arrived_at_terminal_country, arrived_at_terminal_reason)
values (
    src.TRACKING_NUMBER, src.city, src.state, src.country, src.reason_code)
;

/***********
enroute
************/
Merge into _milestone tgt
using (
    select distinct
        t.tracking_number
        ,first_value(se.city) over (partition by t.tracking_number order by se.scan_datetime desc) as city
        ,first_value(se.state) over (partition by t.tracking_number order by se.scan_datetime desc) as state
        ,first_value(se.country) over (partition by t.tracking_number order by se.scan_datetime desc) as country
        ,first_value(se.reason_code) over (partition by t.tracking_number order by se.scan_datetime desc) as reason_code
    FROM _tracking_numbers as t
    JOIN REPORTING_PROD.SPS.CARRIER_MILESTONE_SHIPMENT_EVENT SE
        ON t.CARRIER_MILESTONE_SHIPMENT_ID = SE.CARRIER_MILESTONE_SHIPMENT_ID
    where se.status_code in ('En Route to Delivery Location',
                             'Carrier Departed Pick-up Location with Shipment',
                             'Departed Terminal Location','Loaded on Truck',
                             'Out-Gate')
) src
    on tgt.TRACKING_NUMBER = src.TRACKING_NUMBER
when matched
then update
set tgt.en_route_city = src.city,
    tgt.en_route_state = src.state,
    tgt.en_route_country = src.country,
    tgt.en_route_reason = src.reason_code
when not matched
then insert (
    TRACKING_NUMBER, en_route_city, en_route_state, en_route_country, en_route_reason)
values (
    src.TRACKING_NUMBER, src.city, src.state, src.country, src.reason_code)
;

/***************
delay
****************/

Merge into _milestone tgt
using (
    select distinct
        t.tracking_number
        ,first_value(se.city) over (partition by t.tracking_number order by se.scan_datetime desc) as city
        ,first_value(se.state) over (partition by t.tracking_number order by se.scan_datetime desc) as state
        ,first_value(se.country) over (partition by t.tracking_number order by se.scan_datetime desc) as country
        ,first_value(se.reason_code) over (partition by t.tracking_number order by se.scan_datetime desc) as reason_code
    FROM _tracking_numbers as t
    JOIN REPORTING_PROD.SPS.CARRIER_MILESTONE_SHIPMENT_EVENT SE
        ON t.CARRIER_MILESTONE_SHIPMENT_ID = SE.CARRIER_MILESTONE_SHIPMENT_ID
    where se.status_code in ('Shipment Delayed',
                             'Delivery Not Completed',
                             'Attempted Delivery')
) src
    on tgt.TRACKING_NUMBER = src.TRACKING_NUMBER
when matched
then update
set tgt.shipment_delayed_city = src.city,
    tgt.shipment_delayed_state = src.state,
    tgt.shipment_delayed_country = src.country,
    tgt.shipment_delayed_reason = src.reason_code
when not matched
then insert(
    TRACKING_NUMBER, shipment_delayed_city, shipment_delayed_state, shipment_delayed_country, shipment_delayed_reason)
values (
    src.TRACKING_NUMBER, src.city, src.state, src.country, src.reason_code)
;

/***************
damaged
****************/
Merge into _milestone tgt
using (
    select distinct
        t.tracking_number
        ,first_value(se.city) over (partition by t.tracking_number order by se.scan_datetime desc) as city
        ,first_value(se.state) over (partition by t.tracking_number order by se.scan_datetime desc) as state
        ,first_value(se.country) over (partition by t.tracking_number order by se.scan_datetime desc) as country
        ,first_value(se.reason_code) over (partition by t.tracking_number order by se.scan_datetime desc) as reason_code
    FROM _tracking_numbers as t
    JOIN REPORTING_PROD.SPS.CARRIER_MILESTONE_SHIPMENT_EVENT SE
        ON t.CARRIER_MILESTONE_SHIPMENT_ID = SE.CARRIER_MILESTONE_SHIPMENT_ID
    where se.status_code in ('Shipment Damaged')
) SRC
    on tgt.TRACKING_NUMBER = src.TRACKING_NUMBER
when matched
then update
set tgt.shipment_damaged_city = src.city,
    tgt.shipment_damaged_state = src.state,
    tgt.shipment_damaged_country = src.country,
    tgt.shipment_damaged_reason = src.reason_code

when not matched
then insert(
    TRACKING_NUMBER, shipment_damaged_city, shipment_damaged_state, shipment_damaged_country, shipment_damaged_reason)
values (
    src.TRACKING_NUMBER, src.city, src.state, src.country, src.reason_code)
;

/***************
returned
****************/
Merge into _milestone tgt
using (
    select distinct
        t.tracking_number
        ,first_value(se.city) over (partition by t.tracking_number order by se.scan_datetime desc) as city
        ,first_value(se.state) over (partition by t.tracking_number order by se.scan_datetime desc) as state
        ,first_value(se.country) over (partition by t.tracking_number order by se.scan_datetime desc) as country
        ,first_value(se.reason_code) over (partition by t.tracking_number order by se.scan_datetime desc) as reason_code
    FROM _tracking_numbers as t
    JOIN REPORTING_PROD.SPS.CARRIER_MILESTONE_SHIPMENT_EVENT SE
        ON t.CARRIER_MILESTONE_SHIPMENT_ID = SE.CARRIER_MILESTONE_SHIPMENT_ID
    where se.status_code in ('Shipment Returned to Shipper')
) src
    on tgt.TRACKING_NUMBER = src.TRACKING_NUMBER
when matched
then update
set tgt.returned_city = src.city,
    tgt.returned_state = src.state,
    tgt.returned_country = src.country,
    tgt.returned_reason = src.reason_code
when not matched
then insert(
    TRACKING_NUMBER, returned_city, returned_state, returned_country, returned_reason)
values (
    src.TRACKING_NUMBER, src.city, src.state, src.country, src.reason_code)
;

/***************
delivered
****************/

Merge into _milestone tgt
using (
    select distinct
        t.tracking_number
        ,first_value(se.city) over (partition by t.tracking_number order by se.scan_datetime desc) as city
        ,first_value(se.state) over (partition by t.tracking_number order by se.scan_datetime desc) as state
        ,first_value(se.country) over (partition by t.tracking_number order by se.scan_datetime desc) as country
        ,first_value(se.reason_code) over (partition by t.tracking_number order by se.scan_datetime desc) as reason_code
    FROM _tracking_numbers as t
    JOIN REPORTING_PROD.SPS.CARRIER_MILESTONE_SHIPMENT_EVENT SE
        ON t.CARRIER_MILESTONE_SHIPMENT_ID = SE.CARRIER_MILESTONE_SHIPMENT_ID
    where se.status_code in ('Delivered','Arrived at Delivery Location')
) src
    on tgt.TRACKING_NUMBER = src.TRACKING_NUMBER
when matched
then update
set tgt.delivered_city = src.city,
    tgt.delivered_state = src.state,
    tgt.delivered_country = src.country,
    tgt.delivered_reason = src.reason_code
when not matched
then insert(
    TRACKING_NUMBER, delivered_city, delivered_state, delivered_country, delivered_reason)
values (
    src.TRACKING_NUMBER, src.city, src.state, src.country, src.reason_code)
;

MERGE INTO REPORTING_BASE_PROD.GSC.CARRIER_MILESTONE_DETAIL TGT
USING _MILESTONE SRC
    ON TGT.TRACKING_NUMBER = SRC.TRACKING_NUMBER
WHEN MATCHED AND (
    COALESCE(TGT.arrived_at_terminal_city, '') <> COALESCE(SRC.arrived_at_terminal_city, '')
    OR COALESCE(TGT.arrived_at_terminal_state, '') <> COALESCE(SRC.arrived_at_terminal_state, '')
    OR COALESCE(TGT.arrived_at_terminal_country, '') <> COALESCE(SRC.arrived_at_terminal_country, '')
    OR COALESCE(TGT.arrived_at_terminal_reason, '') <> COALESCE(SRC.arrived_at_terminal_reason, '')
    OR COALESCE(TGT.en_route_city, '') <> COALESCE(SRC.en_route_city, '')
    OR COALESCE(TGT.en_route_state, '') <> COALESCE(SRC.en_route_state, '')
    OR COALESCE(TGT.en_route_country, '') <> COALESCE(SRC.en_route_country, '')
    OR COALESCE(TGT.en_route_reason, '') <> COALESCE(SRC.en_route_reason, '')
    OR COALESCE(TGT.shipment_delayed_city, '') <> COALESCE(SRC.shipment_delayed_city, '')
    OR COALESCE(TGT.shipment_delayed_state, '') <> COALESCE(SRC.shipment_delayed_state, '')
    OR COALESCE(TGT.shipment_delayed_country, '') <> COALESCE(SRC.shipment_delayed_country, '')
    OR COALESCE(TGT.shipment_delayed_reason, '') <> COALESCE(SRC.shipment_delayed_reason, '')
    OR COALESCE(TGT.shipment_damaged_city, '') <> COALESCE(SRC.shipment_damaged_city, '')
    OR COALESCE(TGT.shipment_damaged_state, '') <> COALESCE(SRC.shipment_damaged_state, '')
    OR COALESCE(TGT.shipment_damaged_country, '') <> COALESCE(SRC.shipment_damaged_country, '')
    OR COALESCE(TGT.shipment_damaged_reason, '') <> COALESCE(SRC.shipment_damaged_reason, '')
    OR COALESCE(TGT.returned_city, '') <> COALESCE(SRC.returned_city, '')
    OR COALESCE(TGT.returned_state, '') <> COALESCE(SRC.returned_state, '')
    OR COALESCE(TGT.returned_country, '') <> COALESCE(SRC.returned_country, '')
    OR COALESCE(TGT.returned_reason, '') <> COALESCE(SRC.returned_reason, '')
    OR COALESCE(TGT.delivered_city, '') <> COALESCE(SRC.delivered_city, '')
    OR COALESCE(TGT.delivered_state, '') <> COALESCE(SRC.delivered_state, '')
    OR COALESCE(TGT.delivered_country, '') <> COALESCE(SRC.delivered_country, '')
    OR COALESCE(TGT.delivered_reason, '') <> COALESCE(SRC.delivered_reason, '')
    )
THEN UPDATE
SET
    TGT.arrived_at_terminal_city = SRC.arrived_at_terminal_city,
    TGT.arrived_at_terminal_state = SRC.arrived_at_terminal_state,
    TGT.arrived_at_terminal_country = SRC.arrived_at_terminal_country,
    TGT.arrived_at_terminal_reason = SRC.arrived_at_terminal_reason,
    TGT.en_route_city = SRC.en_route_city,
    TGT.en_route_state = SRC.en_route_state,
    TGT.en_route_country = SRC.en_route_country,
    TGT.en_route_reason = SRC.en_route_reason,
    TGT.shipment_delayed_city = SRC.shipment_delayed_city,
    TGT.shipment_delayed_state = SRC.shipment_delayed_state,
    TGT.shipment_delayed_country = SRC.shipment_delayed_country,
    TGT.shipment_delayed_reason = SRC.shipment_delayed_reason,
    TGT.shipment_damaged_city = SRC.shipment_damaged_city,
    TGT.shipment_damaged_state = SRC.shipment_damaged_state,
    TGT.shipment_damaged_country = SRC.shipment_damaged_country,
    TGT.shipment_damaged_reason = SRC.shipment_damaged_reason,
    TGT.returned_city = SRC.returned_city,
    TGT.returned_state = SRC.returned_state,
    TGT.returned_country = SRC.returned_country,
    TGT.returned_reason = SRC.returned_reason,
    TGT.delivered_city = SRC.delivered_city,
    TGT.delivered_state = SRC.delivered_state,
    TGT.delivered_country = SRC.delivered_country,
    TGT.delivered_reason = SRC.delivered_reason,
    TGT.META_UPDATE_DATETIME = CURRENT_TIMESTAMP()::TIMESTAMP_LTZ
WHEN NOT MATCHED THEN INSERT(
    TRACKING_NUMBER,
    arrived_at_terminal_city,
    arrived_at_terminal_state,
    arrived_at_terminal_country,
    arrived_at_terminal_reason,
    en_route_city,
    en_route_state,
    en_route_country,
    en_route_reason,
    shipment_delayed_city,
    shipment_delayed_state,
    shipment_delayed_country,
    shipment_delayed_reason,
    shipment_damaged_city,
    shipment_damaged_state,
    shipment_damaged_country,
    shipment_damaged_reason,
    returned_city,
    returned_state,
    returned_country,
    returned_reason,
    delivered_city,
    delivered_state,
    delivered_country,
    delivered_reason
)
VALUES(
    SRC.TRACKING_NUMBER,
    SRC.arrived_at_terminal_city,
    SRC.arrived_at_terminal_state,
    SRC.arrived_at_terminal_country,
    SRC.arrived_at_terminal_reason,
    SRC.en_route_city,
    SRC.en_route_state,
    SRC.en_route_country,
    SRC.en_route_reason,
    SRC.shipment_delayed_city,
    SRC.shipment_delayed_state,
    SRC.shipment_delayed_country,
    SRC.shipment_delayed_reason,
    SRC.shipment_damaged_city,
    SRC.shipment_damaged_state,
    SRC.shipment_damaged_country,
    SRC.shipment_damaged_reason,
    SRC.returned_city,
    SRC.returned_state,
    SRC.returned_country,
    SRC.returned_reason,
    SRC.delivered_city,
    SRC.delivered_state,
    SRC.delivered_country,
    SRC.delivered_reason
);
