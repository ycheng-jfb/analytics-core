create
or replace task UTIL.TASKS_CENTRAL_DP.GSC_PULSE_06_RPT_PIPELINE_DETAIL_AGGREGATION_DAILY
	warehouse=DA_WH_ANALYTICS
	after UTIL.TASKS_CENTRAL_DP.GSC_PULSE_05_YARD_CONTAINER_AGGREGATION_DAILY
	as
BEGIN


use schema reporting_base_prod.gsc;

/**************************
30 queries as of 20231018
mitigate duplicate po data
1
***************************/
create
or replace temporary table _jpd
as
select *
     , ROW_NUMBER() OVER (PARTITION BY PO_ID, sku, nvl(po_line_number, 0) ORDER BY sku, ordered_qty desc) AS ROW_NUMBER
from gsc.jf_portal_data jpd
where TO_DATE(JPD.SHOWROOM_DATE) >= DATEADD(month, -8, CURRENT_TIMESTAMP());


/*****
2
******/
delete
from _jpd
where row_number > 1;

/***************************
CALCULATE IN TRANSIT RECORD
3
****************************/
CREATE
OR REPLACE TEMPORARY TABLE _ITD
AS
SELECT itd.*
     , ROW_NUMBER()          OVER (PARTITION BY ITD.PO_NUMBER, ITD.item_id, itd.po_line_number,  ITD.type_code_id ORDER BY ITD.IN_TRANSIT_ID desc, ITD.IN_TRANSIT_CONTAINER_ID desc, ITD.IN_TRANSIT_CONTAINER_CASE_ID desc, ITD.IN_TRANSIT_CONTAINER_CASE_DETAIL_ID desc, itd.meta_update_datetime desc) AS ROW_NUMBER1
     , 0 as marker
     , 0                  as pre_asn
     , 0                  as ASN_Received
     , jpd.warehouse_id   as jpd_warehouse_id
     , 0                  as booking_so
     --,  iff( nvl(jpd.bc_po_line_number, -1) = nvl(itd.po_line_number, 0), jpd.bc_po_line_number,  jpd.po_dtl_id_merlin) as bc_po_line_number
     , jpd.bc_po_line_number
     , jpd.po_line_number as psd_po_line_number
     , jpd.po_dtl_id_merlin

FROM _JPD JPD
         JOIN gsc.in_transit_data ITD
              ON JPD.PO_NUMBER = ITD.PO_NUMBER
                  AND JPD.ITEM_ID = ITD.ITEM_ID
                  and (
                             nvl(jpd.bc_po_line_number, -1) = nvl(itd.po_line_number, 0)
                         or nvl(jpd.po_line_number, -1) = nvl(itd.po_line_number, 0)
                         or jpd.po_dtl_id_merlin = nvl(itd.po_line_number, 0)
                     )
WHERE NVL(itd.quantity_added, 0) = 0
;


/************************
capture first ASN record
received
4
*************************/
CREATE
OR REPLACE TEMPORARY TABLE _ITD_FINAL
AS
SELECT *
FROM _ITD
WHERE TYPE_CODE_ID = 1
  AND ROW_NUMBER1 = 1;


/**************************
mark first ASN record
5
***************************/
update _itd
set marker = 1
where in_transit_container_case_detail_id in (select in_transit_container_case_detail_id from _itd_final);

/***********************
remove first ASN record
6
*************************/
create
or replace temporary table _itd_delete
as
select distinct po_number, sku, po_line_number
from _itd
where marker = 1;

/**
7
**/
delete
from _itd using _itd_delete
where _itd.po_number = _itd_delete.po_number
  and _itd.sku = _itd_delete.sku
  and _itd.po_line_number = _itd_delete.po_line_number;

/******************************
capture first Pre ASN Record
received
8
********************************/
INSERT INTO _ITD_FINAL
SELECT *
FROM _ITD
WHERE _ITD.TYPE_CODE_ID = 1218
  AND _ITD.ROW_NUMBER1 = 1;


/******************************
mitigate duplicate UNLocCodes
9
*******************************/
create
or replace temporary table _warehouse as

select warehouse_id
     , code
     , row_number() over (partition by code order by warehouse_id desc) as row_num
from lake_view.ultra_warehouse.warehouse
where status_code_id = 4;

/**
10
**/
delete
from _warehouse
where row_num > 1;

/**
11
**/
create
or replace temporary table _booking as

select upper(trim(i.so))              as SO
     , upper(trim(i.po))              as PO
     , upper(trim(style_color))       as style_color
     , upper(trim(traffic_mode))      as traffic_mode
     , i.VENDOR_BOOKING_RECEIVED_DATE as vendor_booking_date
     , i.CARRIER_BKG_CONFIRM_DATE     as booking_confirmed_date
     , i.CARRIER_BKG_PLACE_DATE       as carrier_booking_placed_date
     , i.cargo_receive_date
     , i.cargo_ready_date
     , i.fnd_eta                      as fnd_date
     , i.pol_etd                      as vessel_depart_at_port_estimated_date
     , i.pod_eta                      as vessel_arrive_at_port_estimated_date
     , wl.warehouse_id                as pol_warehouse_id
     , wd.warehouse_id                as pod_warehouse_id
     , i.vol                          as booking_volume
     , i.gwt                          as booking_weight
     , i.units                        as booking_units
     , it.item_id
     , po_line_number
     , bc_po_line_number
     , psd_po_line_number
     , po_dtl_id_merlin
from reporting_prod.gsc.booking_dataset i
         left join _warehouse wl
                   on i.pol = wl.code
         left join _warehouse wd
                   on i.pod = wd.code
         left join lake_view.ultra_warehouse.item it
                   on upper(trim(i.style_color)) = upper(trim(it.item_number))
;


/***********************
propagate pre-asn data
across a PO

po/sku/po_line_number grain
12
*************************/
update _itd_final
set booking_so                           = iff(nvl(mdp.so, 'mikewashere') != 'mikewashere', 1, 0)
  , vendor_booking_date                  = mdp.vendor_booking_date
  , booking_confirmed_date               = mdp.booking_confirmed_date
  , carrier_booking_placed_date          = mdp.carrier_booking_placed_date
  , cargo_receive_date                   = mdp.cargo_receive_date
  , fnd_date                             = mdp.fnd_date
  , vessel_depart_at_port_estimated_date = mdp.vessel_depart_at_port_estimated_date
  , vessel_arrive_at_port_estimated_date = mdp.vessel_arrive_at_port_estimated_date
  , cargo_ready_date                     = mdp.cargo_ready_date
  , traffic_mode                         = NVL(_itd_final.traffic_mode, mdp.traffic_mode)
  , from_warehouse_id                    = NVL(_itd_final.from_warehouse_id, mdp.pol_warehouse_id)
  , pod_warehouse_id                     = NVL(_itd_final.pod_warehouse_id, mdp.pod_warehouse_id)
  , to_warehouse_id                      = Coalesce(_itd_final.to_warehouse_id, _itd_final.jpd_warehouse_id)
  , item_id                              = mdp.item_id
  , pre_asn                              = 1 from _booking mdp
where _itd_final.po_number = mdp.po
  and _itd_final.sku = mdp.style_color
  and (
    nvl(mdp.bc_po_line_number
    , -1) = nvl(_itd_final.bc_po_line_number
    , 0)
   or nvl(mdp.psd_po_line_number
    , -1) = nvl(_itd_final.po_line_number
    , 0)
   or mdp.po_dtl_id_merlin = nvl(_itd_final.po_dtl_id_merlin
    , 0)
    )
;


/*****************
po/sku/traffic_mode
grain
13
******************/
update _itd_final
set booking_so                           = iff(nvl(mdp.so, 'mikewashere') != 'mikewashere', 1, 0)
  , vendor_booking_date                  = mdp.vendor_booking_date
  , booking_confirmed_date               = mdp.booking_confirmed_date
  , carrier_booking_placed_date          = mdp.carrier_booking_placed_date
  , cargo_receive_date                   = mdp.cargo_receive_date
  , fnd_date                             = mdp.fnd_date
  , vessel_depart_at_port_estimated_date = mdp.vessel_depart_at_port_estimated_date
  , vessel_arrive_at_port_estimated_date = mdp.vessel_arrive_at_port_estimated_date
  , cargo_ready_date                     = mdp.cargo_ready_date
  --, traffic_mode                         = mdp.traffic_mode
  , from_warehouse_id                    = NVL(from_warehouse_id, mdp.pol_warehouse_id)
  , pod_warehouse_id                     = NVL(_itd_final.pod_warehouse_id, mdp.pod_warehouse_id)
  , to_warehouse_id                      = NVL(to_warehouse_id, jpd_warehouse_id)
  , pre_asn                              = 2 from _booking mdp
where _itd_final.po_number = mdp.po
  and upper (trim (nvl(_itd_final.traffic_mode
    , 'AIR'))) = upper (trim (nvl(mdp.traffic_mode
    , 'AIR')))
  and _itd_final.pre_asn != 1
;

/**
14
**/
insert into _itd_final
( booking_so
, po_number
, sku
, vendor_booking_date
, booking_confirmed_date
, carrier_booking_placed_date
, cargo_receive_date
, fnd_date
, vessel_depart_at_port_estimated_date
, vessel_arrive_at_port_estimated_date
, cargo_ready_date
, traffic_mode
, from_warehouse_id
, pod_warehouse_id
, to_warehouse_id
, po_line_number
, bc_po_line_number
, psd_po_line_number
, po_dtl_id_merlin)
select iff(nvl(mdp.so, 'mikewashere') != 'mikewashere', 1, 0) as booking_so
     , mdp.po
     , mdp.style_color
     , mdp.vendor_booking_date
     , mdp.booking_confirmed_date
     , mdp.carrier_booking_placed_date
     , mdp.cargo_receive_date
     , mdp.fnd_date
     , mdp.vessel_depart_at_port_estimated_date
     , mdp.vessel_arrive_at_port_estimated_date
     , mdp.cargo_ready_date
     , mdp.traffic_mode
     , mdp.pol_warehouse_id
     , mdp.pod_warehouse_id
     , NVL(to_warehouse_id, jpd_warehouse_id)
     , mdp.po_line_number
     , mdp.bc_po_line_number
     , mdp.psd_po_line_number
     , mdp.po_dtl_id_merlin
from _booking mdp
         left join _itd_final itd
                   on itd.po_number = mdp.po
                       and itd.sku = mdp.style_color
                       and (
                                  nvl(mdp.bc_po_line_number, -1) = nvl(itd.bc_po_line_number, 0)
                              or nvl(mdp.psd_po_line_number, -1) = nvl(itd.po_line_number, 0)
                              or mdp.po_dtl_id_merlin = nvl(itd.po_dtl_id_merlin, 0)
                          )
where itd.po_number is null;


/************************
set asn_received marker
15
*************************/
update _itd_final
set ASN_Received                         = 1
  , vessel_arrive_at_port_estimated_date = iff(transload_truck_eta_actual_date is null,
                                               vessel_arrive_at_port_estimated_date, transload_truck_eta_actual_date)
where po_number in (select distinct po_number from _itd_final where nvl(type_code_id, 0) = 1)
  and po_line_number in (select distinct po_line_number from _itd_final where nvl(type_code_id, 0) = 1);


/*******************************
calculate Yard Container record
16
********************************/
create
or replace temporary table _yard
as
select *
     , ROW_NUMBER() OVER (PARTITION BY UPPER(trim(PO_NUMBER)), trim(ITEM_ID), nvl(po_line_number, 1) ORDER BY LOAD_DATE asc) AS ROW_num
from gsc.yard_container_data
where TO_DATE(load_date) >= DATEADD(month, -4, CURRENT_TIMESTAMP());


/***************************
CALCULATE ON HAND QTY
17
****************************/
CREATE
OR REPLACE TEMPORARY TABLE _onhand_qty
AS
SELECT IW.ITEM_ID, IW.WAREHOUSE_ID, sum(qty_onhand) as ONHAND_QTY
FROM lake_view.ultra_warehouse.item_warehouse IW
WHERE QTY_ONHAND > 0
GROUP BY IW.ITEM_ID, IW.WAREHOUSE_ID;


/*******************
SET TRANSIT LOGIC
18
********************/
CREATE
OR REPLACE TEMPORARY TABLE _logic
AS
SELECT DISTINCT JPD.PO_NUMBER
              , JPD.SKU
              , jpd.bc_po_line_number as po_line_number
              , CASE
                    when UPPER(NVL(JPD.transport_mode, 'MikeWasHere')) = 'OCEAN'
                        then Case
                                 WHEN NVL(Y.CARRIER_ARRIVAL_DATE, '1900-01-01') != '1900-01-01'
                                     then 'Transload ETA Actual'
                                 WHEN NVL(Y.CARRIER_DEPARTURE_DATE, '1900-01-01') != '1900-01-01' AND
                             NVL(Y.CARRIER_ARRIVAL_DATE, '1900-01-01') = '1900-01-01'
    then 'Transload Departed'
                                 WHEN NVL(ITD.vessel_arrive_at_port_actual_date, '1900-01-01') != '1900-01-01'
                                     then 'Vessel Arrived'
                                 when NVL(ITD.vessel_arrive_at_port_estimated_date, '1900-01-01') != '1900-01-01'
                                     then 'Vessel Departed With ETA'
                                 WHEN NVL(ITD.vessel_depart_at_port_actual_date, '1900-01-01') != '1900-01-01'
                                     then 'Vessel Departed No ETA'
                                 WHEN JPD.END_SHIP_WINDOW > CURRENT_DATE() AND
                                      NVL(ITD.cargo_receive_date, '1900-01-01') != '1900-01-01'
                                     THEN 'Full Supply Chain'
                                 WHEN NVL(ITD.vessel_depart_at_port_actual_date, '1900-01-01') = '1900-01-01'
                                     and NVL(JPD.end_ship_window, '1900-01-01') < CURRENT_DATE() and
                                      NVL(ITD.vendor_booking_date, '1900-01-01') != '1900-01-01'
                                     then 'Full Supply Chain'
end
else 'NA'
end
AS LOGIC
FROM _JPD JPD
         LEFT JOIN _ITD_FINAL ITD
                   ON JPD.PO_NUMBER = ITD.PO_NUMBER
                       AND JPD.SKU = ITD.SKU
                   and (
                    nvl(jpd.bc_po_line_number, -1) = nvl(itd.bc_po_line_number, 0)
                    or nvl(jpd.po_line_number, -1) = nvl(itd.po_line_number, 0)
                    or jpd.po_dtl_id_merlin = nvl(itd.po_dtl_id_merlin, 0)
                   )

         left Join _yard Y
                   ON JPD.PO_NUMBER = Y.PO_NUMBER
                       AND JPD.ITEM_ID = Y.ITEM_ID
                       and NVL(jpd.po_line_number, 1) = NVL(y.po_line_number, 1)
                       AND Y.ROW_NUM = 1;



/**
19
**/
CREATE
OR REPLACE TEMPORARY TABLE _fnd_dates
AS
SELECT DISTINCT JPD.PO_NUMBER
              , JPD.SKU
              , jpd.bc_po_line_number as po_line_number
              , ITD.IN_TRANSIT_CONTAINER_ID
              , CASE
                    when jpd.warehouse_id_origin in (107, 154) and jpd.warehouse_id in (107, 154)
                        and UPPER(JPD.TRANSPORT_MODE) = 'TRUCK' and UPPER(JPD.trading_terms) = 'XWORKS'
                        then dateadd(day, 4, jpd.end_ship_window)

                    when jpd.warehouse_id_origin in (107, 154) and jpd.warehouse_id in (231, 466)
                        and UPPER(JPD.TRANSPORT_MODE) = 'TRUCK' and UPPER(JPD.trading_terms) = 'XWORKS'
                        then dateadd(day, 16, jpd.end_ship_window)

                    when jpd.warehouse_id in (221)
                        and UPPER(JPD.TRANSPORT_MODE) = 'TRUCK' and UPPER(JPD.trading_terms) = 'XWORKS'
                        then jpd.end_ship_window

                    when jpd.warehouse_id in (366)
                        and UPPER(JPD.TRANSPORT_MODE) = 'TRUCK' and UPPER(JPD.trading_terms) = 'XWORKS'
                        then dateadd(day, 6, jpd.end_ship_window)

                    when jpd.warehouse_id in (107, 154) and jpd.warehouse_id_origin in (231)
                        and UPPER(JPD.trading_terms) = 'DDP' and UPPER(JPD.TRANSPORT_MODE) = 'TRUCK'
                        then dateadd(day, 7, COALESCE(ITD.cargo_ready_date, JPD.END_SHIP_WINDOW))

                    when UPPER(JPD.trading_terms) = 'DDP' and UPPER(JPD.TRANSPORT_MODE) = 'DDP VENDOR'
                        then jpd.end_ship_window

                    WHEN NVL(ITD.DC_appointment_date, '1900-01-01') != '1900-01-01'
                        THEN ITD.DC_appointment_date

                    when UPPER(NVL(JPD.transport_mode, 'MikeWasHere')) = 'AIR (EXPEDITED)' and
                         NVL(ITD.cargo_receive_date, '1900-01-01') != '1900-01-01'
                        then dateadd(day, 7, cargo_receive_date)
                    when UPPER(NVL(JPD.transport_mode, 'MikeWasHere')) = 'AIR (STANDARD)' and
                         NVL(ITD.cargo_receive_date, '1900-01-01') != '1900-01-01'
                        then dateadd(day, 10, cargo_receive_date)
                    when UPPER(NVL(JPD.transport_mode, 'MikeWasHere')) = 'AIR (DEFERRED)' and
                         NVL(ITD.cargo_receive_date, '1900-01-01') != '1900-01-01'
                        then dateadd(day, 15, cargo_receive_date)
                    when UPPER(NVL(JPD.transport_mode, 'MikeWasHere')) = 'AIR (EXPEDITED)' and
                         NVL(ITD.cargo_receive_date, '1900-01-01') = '1900-01-01' and
                         NVL(ITD.cargo_ready_date, '1900-01-01') != '1900-01-01'
                        then dateadd(day, 7, cargo_ready_date)
                    when UPPER(NVL(JPD.transport_mode, 'MikeWasHere')) = 'AIR (STANDARD)' and
                         NVL(ITD.cargo_receive_date, '1900-01-01') = '1900-01-01' and
                         NVL(ITD.cargo_ready_date, '1900-01-01') != '1900-01-01'
                        then dateadd(day, 10, cargo_ready_date)
                    when UPPER(NVL(JPD.transport_mode, 'MikeWasHere')) = 'AIR (DEFERRED)' and
                         NVL(ITD.cargo_receive_date, '1900-01-01') = '1900-01-01' and
                         NVL(ITD.cargo_ready_date, '1900-01-01') != '1900-01-01'
                        then dateadd(day, 15, cargo_ready_date)
                    when UPPER(NVL(JPD.transport_mode, 'MikeWasHere')) = 'AIR (EXPEDITED)' and
                         NVL(ITD.cargo_ready_date, '1900-01-01') = '1900-01-01'
                        then dateadd(day, 7, end_ship_window)
                    when UPPER(NVL(JPD.transport_mode, 'MikeWasHere')) = 'AIR (STANDARD)' and
                         NVL(ITD.cargo_ready_date, '1900-01-01') = '1900-01-01'
                        then dateadd(day, 10, end_ship_window)
                    when UPPER(NVL(JPD.transport_mode, 'MikeWasHere')) = 'AIR (DEFERRED)' and
                         NVL(ITD.cargo_ready_date, '1900-01-01') = '1900-01-01'
                        then dateadd(day, 15, end_ship_window)

                    when UPPER(NVL(JPD.transport_mode, 'MikeWasHere')) = 'OCEAN' and
                         NVL(Y.CARRIER_ARRIVAL_DATE, '1900-01-01') != '1900-01-01' /*transload eta actual*/
                        then Y.CARRIER_ARRIVAL_DATE

                    WHEN UPPER(NVL(JPD.transport_mode, 'MikeWasHere')) = 'OCEAN' and
                         NVL(log.total_transit_time, 999) != 999
                        then CASE
                                                                                           WHEN NVL(Y.CARRIER_DEPARTURE_DATE, '1900-01-01') != '1900-01-01' AND
                             NVL(Y.CARRIER_ARRIVAL_DATE, '1900-01-01') = '1900-01-01' /*transload departed*/
    THEN dateadd(day, NVL(LOG.TOTAL_TRANSIT_TIME, 0), Y.CARRIER_DEPARTURE_DATE)

                                 WHEN NVL(ITD.vessel_arrive_at_port_actual_date, '1900-01-01') !=
                                      '1900-01-01' /*vessel arrived*/
                                     THEN dateadd(day, NVL(LOG.TOTAL_TRANSIT_TIME, 0),
                                                  ITD.vessel_arrive_at_port_actual_date)

                                 WHEN NVL(ITD.vessel_arrive_at_port_estimated_date, '1900-01-01') !=
                                      '1900-01-01' /*Vessel Departed With ETA*/
                                     THEN dateadd(day, NVL(LOG.TOTAL_TRANSIT_TIME, 0),
                                                  ITD.vessel_arrive_at_port_estimated_date)

                                 WHEN NVL(ITD.vessel_depart_at_port_actual_date, '1900-01-01') !=
                                      '1900-01-01' --'Vessel Departed No ETA'
                                     THEN DATEADD(DAY, NVL(LOG.TOTAL_TRANSIT_TIME, 0),
                                                  ITD.VESSEL_DEPART_AT_PORT_ACTUAL_DATE)

                                 WHEN JPD.END_SHIP_WINDOW > CURRENT_DATE() AND
                                      NVL(ITD.cargo_receive_date, '1900-01-01') != '1900-01-01' /*Full Supply Chain*/
                                     THEN DATEADD(DAY, NVL(LOG.TOTAL_TRANSIT_TIME, 0), ITD.CARGO_RECEIVE_DATE)

                                 WHEN NVL(ITD.vessel_depart_at_port_actual_date, '1900-01-01') =
                                      '1900-01-01' /*Full Supply Chain*/
                                     and NVL(JPD.end_ship_window, '2200-12-31') < CURRENT_DATE() and
                                      NVL(ITD.vendor_booking_date, '1900-01-01') != '1900-01-01'
                                     then dateadd(day, NVL(LOG.TOTAL_TRANSIT_TIME, 0), JPD.end_ship_window)
END
WHEN UPPER(transport_mode) = 'SMALL PARCEL'
                        then dateadd(day, 7, end_ship_window)
END
as FND_ETA_DATE_RAW
              , L.LOGIC
              , CASE
                    WHEN UPPER(NVL(JPD.transport_mode, 'MikeWasHere')) = 'OCEAN'
                        THEN JPD.PO_FND_ETA_DATE
END
AS PO_FND_ETA_DATE
              , to_timestamp('1900-01-01 00:00:00.000') AS FND_ETA_DATE
FROM _JPD JPD
         LEFT JOIN _ITD_FINAL ITD
                   ON JPD.PO_NUMBER = ITD.PO_NUMBER
                       AND JPD.SKU = ITD.SKU
                                         and (
                    nvl(jpd.bc_po_line_number, -1) = nvl(itd.bc_po_line_number, 0)
                    or nvl(jpd.po_line_number, -1) = nvl(itd.po_line_number, 0)
                    or jpd.po_dtl_id_merlin = nvl(itd.po_dtl_id_merlin, 0)
                   )
         LEFT JOIN _logic L
                   ON JPD.PO_NUMBER = L.PO_NUMBER
                       AND JPD.SKU = L.SKU
                       and nvl(jpd.bc_po_line_number,1) = nvl(l.po_line_number,1)
         LEFT JOIN reporting_base_prod.gsc.pol_pod_transit_dataset log
                   ON ITD.FROM_WAREHOUSE_ID = LOG.POL_WAREHOUSE_ID
                       AND ITD.POD_WAREHOUSE_ID = LOG.POD_WAREHOUSE_ID
                       AND ITD.TRAFFIC_MODE = LOG.TRAFFIC_MODE
                       AND JPD.WAREHOUSE_ID = LOG.DESTINATION_WAREHOUSE_ID
                       AND L.LOGIC = LOG.TRANSIT_LOGIC
         left Join _yard Y
                   ON JPD.PO_NUMBER = Y.PO_NUMBER
                       AND JPD.ITEM_ID = Y.ITEM_ID
                       and nvl(jpd.po_line_number,1) = nvl(y.po_line_number,1)
                       AND Y.ROW_NUM = 1
WHERE 1 = 1
ORDER BY JPD.PO_NUMBER, JPD.SKU;




/**
20
**/
UPDATE _fnd_dates
SET FND_ETA_DATE =
        CASE
            WHEN PO_FND_ETA_DATE IS NULL
                THEN FND_ETA_DATE_RAW
            ELSE NVL(FND_ETA_DATE_RAW, PO_FND_ETA_DATE)
            END;

/**
21
**/
CREATE
OR REPLACE TEMPORARY TABLE _eta_on_hand_date
AS
SELECT DISTINCT JPD.PO_NUMBER
              , JPD.SKU
              , jpd.bc_po_line_number as po_line_number
              , case
                    when jpd.received_date_est is not null
                        then jpd.received_date_est
                    when upper(destination) = 'US'
                        then
                        case
                            when fd.fnd_eta_date is not null
                                then
                                case
                                    when DAYOFWEEK(FD.FND_ETA_DATE) in (0, 6)
                                        then dateadd(day, 4, FD.FND_ETA_DATE)
                                    else dateadd(day, 2, FD.FND_ETA_DATE)
                                    end
                            when UPPER(transport_mode) = 'DDP VENDOR'
                                then jpd.end_ship_window
                            end

                    when upper(destination) in ('CA', 'EU', 'MX')
                        then
                        case
                            when fd.fnd_eta_date is not null
                                then
                                case
                                    when DAYOFWEEK(FD.FND_ETA_DATE) in (0, 6)
                                        then dateadd(day, 4, FD.FND_ETA_DATE)
                                    else dateadd(day, 2, FD.FND_ETA_DATE)
                                    end
                            when UPPER(transport_mode) = 'DDP VENDOR'
                                then jpd.end_ship_window
                            end
    end                               AS ETA_ON_HAND_DATE

              , CASE
                    WHEN NVL(JPD.RECEIVED_DATE_EST, '1900-01-01') = '1900-01-01'
                        THEN 'No'
                    ELSE 'Yes'
    END                               AS PO_RECEIVED

FROM _JPD JPD
         LEFT JOIN _fnd_dates FD
                   ON JPD.PO_NUMBER = FD.PO_NUMBER
                       AND JPD.SKU = FD.SKU
                       and nvl(jpd.bc_po_line_number, 1) = nvl(fd.po_line_number, 1);


/********************************************
move eta on hand date to following Monday
for any eta on hand falling on a weekend
22
*********************************************/
update _eta_on_hand_date
set eta_on_hand_date =
        case
            when DAYOFWEEK(ETA_ON_HAND_DATE) = 6
                then dateadd(day, 2, ETA_ON_HAND_DATE)
            when DAYOFWEEK(ETA_ON_HAND_DATE) = 0
                then dateadd(day, 1, ETA_ON_HAND_DATE)
            else ETA_ON_HAND_DATE
            end;

/****************************
create oocl wgt/vol table
23
*****************************/

create
or replace temporary table _oocl as
select ocl.po_number
     , ocl.item_number
     , ocl.container
     , ocl.bl
     , ocl.total_item_volume_cbm
     , ocl.total_item_weight_kg
     , ocl.units_shipped
     , iff(ocl.total_item_volume_cbm > 0 and ocl.units_shipped > 0, ocl.total_item_volume_cbm / ocl.units_shipped,
           0) as    unit_volume
     , row_number() over (partition by ocl.po_number, ocl.item_number order by ocl.po_number, ocl.item_number, ocl.meta_update_datetime desc) as row_ct
from _jpd jpd
         join (SELECT PO          AS PO_NUMBER,
                      STYLE_COLOR AS ITEM_NUMBER,
                      CONTAINER,
                      BL,
                      TOTAL_ITEM_VOLUME_CBM,
                      TOTAL_ITEM_WEIGHT_KG,
                      UNITS_SHIPPED,
                      meta_update_datetime
               FROM LAKE_VIEW.GSC.SHIPMENT_DATA_OOCL
               UNION
               SELECT PO               AS PO_NUMBER,
                      STYLE_COLOR      AS ITEM_NUMBER,
                      DOMESTIC_TRAILER AS CONTAINER,
                      DOMESTIC_BL      AS BL,
                      VOL_CBM          AS TOTAL_ITEM_VOLUME_CBM,
                      GWT_POUND        AS TOTAL_ITEM_WEIGHT_KG,
                      UNITS_ORDERED    AS UNITS_SHIPPED,
                      meta_update_datetime
               FROM LAKE_VIEW.GSC.SHIPMENT_DATA_DOMESTIC_OOCL) ocl
              on upper(jpd.po_number) = upper(ocl.po_number)
                  and upper(jpd.sku) = upper(ocl.item_number)
;


/***********************************************
avg unit volume
24
************************************************/
create
or replace temporary table _vol as
select jpd.business_unit,
       Nvl(upper(trim(jpd.centric_class)), upper(trim(jpd.style_type))) as class,
       AVG(iff(ocl.total_item_volume_cbm > 0 and ocl.units_shipped > 0, ocl.total_item_volume_cbm / ocl.units_shipped,
               0))                                                      as Avg_unit_volume
from _jpd jpd
         join LAKE_VIEW.GSC.SHIPMENT_DATA_OOCL ocl
              on upper(jpd.po_number) = upper(ocl.po)
                  and upper(jpd.sku) = upper(ocl.style_color)
                  and (
                             jpd.bc_po_line_number = ocl.line_num
                         or jpd.po_line_number = ocl.line_num
                         or nvl(ocl.line_num, 0) = 0
                     )
where 1 = 1
  and jpd.Ordered_qty > 0
  and NVL(jpd.is_cancelled, FALSE) = FALSE
  and jpd.showroom_date > dateadd(month, -48, current_timestamp())
  and ocl.total_item_volume_cbm !=0
group by
    jpd.business_unit,
    Nvl(upper (trim (jpd.centric_class)), upper (trim (jpd.style_type)))
;


/**********************
CREATE STAGING TABLE

GET DATA
25
**********************/
CREATE
OR REPLACE TEMPORARY TABLE _stg_gsc_pipeline
AS

SELECT JPD.PO_NUMBER
     , JPD.SKU
     , CASE
           WHEN NVL(JPD.received_date_EST, '1900-01-01') != '1900-01-01'
               THEN 'Received'
           WHEN (NVL(Y.CARRIER_ARRIVAL_DATE, '1900-01-01') != '1900-01-01' OR
                 NVL(ITD.delivered_to_consignee_actual_date, '1900-01-01') != '1900-01-01')
               THEN 'Delivered at FC'
           WHEN (NVL(ITD.in_gate_actual_date, '1900-01-01') != '1900-01-01' OR
                 NVL(Y.CARRIER_DEPARTURE_DATE, '1900-01-01') != '1900-01-01')
               THEN 'In-Transit'
           WHEN NVL(ITD.cargo_receive_date, '1900-01-01') != '1900-01-01'
               THEN 'Cargo Received'
    END            AS PO_SKU_STATUS
     , JPD.BUSINESS_UNIT
     , JPD.SHOWROOM_DATE
     , JPD.LAUNCH_DATE
     , JPD.COLOR
     , I.ITEM_ID   AS ITEM_ID
     , JPD.ITEM_CATEGORY
     , I.WMS_CLASS AS ITEM_WMS_CLASS
     , LEFT (JPD.SKU
     , 2) AS SKU_SEGMENT
     , JPD.DESCRIPTION
     , JPD.VENDOR_CODE
     , JPD.VENDOR_NAME
     , JPD.END_SHIP_WINDOW
     , JPD.TARGET_DELIVERY_DATE
     , JPD.DESTINATION
     , JPD.TRANSPORT_MODE
     , ITD.CARGO_READY_DATE
     , ITD.CARGO_RECEIVE_DATE
     , ITD.FROM_WAREHOUSE_ID
     , ITD.TO_WAREHOUSE_ID
     , ITD.BILL_OF_LADING
     , ITD.DOMESTIC_BILL_OF_LADING
     , ITD.in_transit_container_id AS CONTAINER_ID
     , ITD.CONTAINER_LABEL
     , Y.TRAILER_ID AS DOMESTIC_TRAILER_NUMBER
     , ITD.CARRIER_FND
     , ITD.CARRIER_FND_CODE
     , JPD.TRADING_TERMS
     , ITD.PORT_OF_LOADING
     , ITD.CARRIER_ID
     , ITD.CARRIER_LABEL
     , ITD.container_equipment_type AS EQUIPMENT_TYPE
     , ITD.container_equipment_type AS CONTAINER_TYPE
     , ITD.PORT_OF_DISCHARGE
     , ITD.CONTAINER_WEIGHT
     , ITD.SHIPMENT_VOLUME
     , JPD.ORDERED_QTY
     , ITD.QUANTITY AS SHIPPED_QTY
     , NVL(OH.ONHAND_QTY
     , 0) AS ONHAND_QTY
     , JPD.UNIT_COST
     , JPD.FREIGHT
     , JPD.DUTY
     , ITD.IN_TRANSIT_CONTAINER_ID
     , ITD.IN_GATE_ESTIMATED_DATE
     , ITD.IN_GATE_REVISED_DATE
     , ITD.IN_GATE_ACTUAL_DATE
     , ITD.VESSEL_DEPART_AT_PORT_ESTIMATED_DATE
     , ITD.VESSEL_DEPART_AT_PORT_REVISED_DATE
     , ITD.VESSEL_DEPART_AT_PORT_ACTUAL_DATE
     , ITD.VESSEL_ARRIVE_AT_PORT_ESTIMATED_DATE
     , ITD.VESSEL_ARRIVE_AT_PORT_REVISED_DATE
     , ITD.VESSEL_ARRIVE_AT_PORT_ACTUAL_DATE
     , ITD.LOADED_ON_RAIL_ESTIMATED_DATE
     , ITD.LOADED_ON_RAIL_REVISED_DATE
     , ITD.LOADED_ON_RAIL_ACTUAL_DATE
     , ITD.RAIL_ARR_DEST_INTML_REMP_ESTIMATED_DATE
     , ITD.RAIL_ARR_DEST_INTML_REMP_REVISED_DATE
     , ITD.RAIL_ARR_DEST_INTML_REMP_ACTUAL_DATE
     , ITD.PICKED_UP_FOR_DELIVERY_ESTIMATED_DATE
     , ITD.PICKED_UP_FOR_DELIVERY_REVISED_DATE
     , ITD.PICKED_UP_FOR_DELIVERY_ACTUAL_DATE
     , ITD.TRANSLOAD_DOMESTIC_DEPART_ESTIMATED_DATE
     , ITD.TRANSLOAD_DOMESTIC_DEPART_REVISED_DATE
     , Y.CARRIER_DEPARTURE_DATE AS TRANSLOAD_DOMESTIC_DEPART_ACTUAL_DATE
     , ITD.TRANSLOAD_TRUCK_ETA_ESTIMATED_DATE
     , ITD.TRANSLOAD_TRUCK_ETA_REVISED_DATE
     , Y.CARRIER_ARRIVAL_DATE AS TRANSLOAD_TRUCK_ETA_ACTUAL_DATE
     , ITD.DELIVERED_TO_CONSIGNEE_ESTIMATED_DATE
     , ITD.DELIVERED_TO_CONSIGNEE_REVISED_DATE
     , ITD.DELIVERED_TO_CONSIGNEE_ACTUAL_DATE
     , JPD.RECEIVED_DATE_EST
     , ITD.DC_APPOINTMENT_DATE
     , CASE
    WHEN NVL(ITD.transload_truck_ETA_estimated_date
     , '1900-01-01') = '1900-01-01'
    THEN
    CASE
    WHEN UPPER (JPD.destination) = 'US'
    THEN (DATEADD(DAY
     , 10
     , NVL(NVL(ITD.vessel_arrive_at_port_estimated_date
     , ITD.vessel_arrive_at_port_revised_date)
     , ITD.vessel_arrive_at_port_actual_date)))
    ELSE (NULL)
END
ELSE ITD.transload_truck_ETA_estimated_date
END
AS TRANSLOAD_TRUCK_ETA_C_DATE
     , NVL(ETA.PO_RECEIVED, 'NO')                                                 AS PO_RECEIVED
     , CASE
           WHEN UPPER(NVL(ETA.PO_RECEIVED, 'NO')) = 'YES' AND NVL(OH.ONHAND_QTY, 0) = 0
               THEN 'Arrived at DC/Out of Stock'
           WHEN UPPER(NVL(ETA.PO_RECEIVED, 'NO')) = 'YES' AND NVL(OH.ONHAND_QTY, 0) > 0
               THEN 'Received'
           WHEN UPPER(NVL(ETA.PO_RECEIVED, 'NO')) = 'NO' AND NVL(OH.ONHAND_QTY, 0) = 0
               THEN 'Not Received'
           ELSE 'SKU on Hand'
END
AS RECEIPT_STATUS
     , FD.FND_ETA_DATE
     , DAYOFWEEK(FD.FND_ETA_DATE)                                                 AS FND_ETA_WEEK_DAY_NO
     , to_date(ETA.ETA_ON_HAND_DATE)                                              as ETA_ON_HAND_DATE
     , WEEK(ETA.ETA_ON_HAND_DATE)                                                 AS ETA_ON_HAND_WEEK
     , MONTH(ETA.ETA_ON_HAND_DATE)                                                AS ETA_ON_HAND_MONTH
     , DAY(ETA.ETA_ON_HAND_DATE)                                                  AS ETA_ON_HAND_DAY
     , WEEK(FD.FND_ETA_DATE)                                                      AS WEEK_ETA
     , Case
           when jpd.received_date_est is not null and jpd.received_date_est >= JPD.LAUNCH_DATE
               then 'Late'
           when jpd.received_date_est is not null and jpd.received_date_est < JPD.LAUNCH_DATE
               then 'On Time'
           when upper(JPD.transport_mode) = 'TRUCK' AND ITD.BILL_OF_LADING is null
               then 'No Booking'
           when upper(JPD.transport_mode) IN ('DDP VENDOR', 'SMALL PARCEL', 'TRUCK')
               or itd.booking_confirmed_date is not null
               or itd.booking_so = 1
               then Case
                        WHEN ETA.ETA_on_hand_date >= dateadd(dd, -2, JPD.launch_date)
                            AND ETA.ETA_on_hand_date < JPD.launch_date
                            AND NVL(JPD.received_date_est, '1900-01-01') = '1900-01-01'
                            THEN 'At Risk'
                        WHEN JPD.launch_date = ETA.ETA_on_hand_date
                            OR ETA.ETA_on_hand_date > JPD.launch_date
                            or (JPD.launch_date < to_date(current_date()) and
                                NVL(JPD.received_date_est, '1900-01-01') = '1900-01-01' and
                                ETA.ETA_on_hand_date < to_date(current_date()))
                            OR JPD.received_date_EST > JPD.launch_date
                            THEN 'Late'
                        WHEN DATEADD(dd, -2, JPD.launch_date) > ETA.ETA_on_hand_date
                            OR coalesce(JPD.received_date_EST, to_date(current_date())) < JPD.launch_date
                            OR (UPPER(po_type) = 'RETAIL' AND DATEADD('DAY', -10, JPD.LAUNCH_DATE) > ETA_ON_HAND_DATE)
                            THEN 'On Time'
end

when upper(JPD.transport_mode) = 'OCEAN'
               OR upper(JPD.transport_mode) ilike 'AIR%'
               then case
                        when itd.vendor_booking_date is null and upper(JPD.transport_mode) = 'OCEAN' and
                             datediff('day', current_timestamp(), JPD.END_SHIP_WINDOW) > 21
                            then 'No Booking - Not Due'

                        when itd.vendor_booking_date is null and upper(JPD.transport_mode) ilike 'AIR%' and
                             datediff('day', current_timestamp(), JPD.END_SHIP_WINDOW) > 10
                            then 'No Booking - Not Due'

                        when itd.vendor_booking_date is null
                            then 'No Booking'
                        when itd.vendor_booking_date is not null
                            and itd.booking_confirmed_date is null
                            then 'Booking Requested'
end
end
as Call_Out
     , JPD.MODE
     , CASE
           WHEN UPPER(JPD.mode) = 'RAIL'
               THEN
               CASE
                   WHEN NVL(ITD.vessel_arrive_at_port_actual_date, '1900-01-01') = '1900-01-01'
                       THEN 'Not Arrived'
                   WHEN (NVL(ITD.vessel_arrive_at_port_actual_date, '1900-01-01') != '1900-01-01' AND
                         NVL(ITD.loaded_on_rail_actual_date, '1900-01-01') = '1900-01-01')
                       THEN 'Waiting to Load'
                   WHEN PO_received = 'Yes'
                       THEN 'Delivered'
                   ELSE 'In-Transit'
END
ELSE NULL
END
AS RAIL_STATUS
     , ITD.VENDOR_BOOKING_DATE
     , JPD.STYLE_NAME
     , NVL(ITD.SHIPMENT_TYPE, 'no-asn')                                           AS SHIPMENT_TYPE
     , ITD.POD_WAREHOUSE_ID
     , ITD.FND_DATE
     , ITD.TRANSPORT_MODE_INTRANSIT
     , JPD.MERLIN_PO_STATUS
     , ITD.TRAFFIC_MODE
     , ITD.QUANTITY_ADDED
     , ITD.CARTON_COUNT
     , iff(JPD.ORDERED_QTY = 0, 0, coalesce(nullif(ocl.total_item_volume_cbm, 0), nullif(ocl1.total_item_volume_cbm, 0)
    , nullif(bk.avg_booking_volume, 0), nullif(bk_po_avg.po_avg_booking_volume, 0), nullif(bk_sku_avg.sku_avg_booking_volume, 0)
    , nullif((JPD.ACTUAL_VOLUME * JPD.ORDERED_QTY), 0), nullif((JPD.AVG_VOLUME * JPD.ORDERED_QTY), 0), nullif(centric_cbm, 0), nullif(v.Avg_unit_volume * JPD.ORDERED_QTY,0))) AS VOLUME
     , iff(JPD.ORDERED_QTY = 0, 0,
           coalesce(ocl.total_item_weight_kg, ocl1.total_item_weight_kg, (JPD.ACTUAL_WEIGHT * JPD.ORDERED_QTY)
               , bk.avg_booking_weight, bk_po_avg.po_avg_booking_weight, bk_sku_avg.sku_avg_booking_weight
               , (JPD.AVG_WEIGHT * JPD.ORDERED_QTY)))                             AS WEIGHT
     , JPD.WAREHOUSE_ID
     , JPD.WAREHOUSE_ID_ORIGIN
     , JPD.OFFICIAL_FACTORY_NAME
     , JPD.ORIGINAL_END_SHIP_WINDOW_DATE
     , JPD.PO_TYPE
     , JPD.QTY_GHOSTED
     , JPD.BUY_RANK
     , FD.LOGIC
     , JPD.SOURCE_VENDOR_NAME
     , JPD.SOURCE_VENDOR_ID
     , JPD.GENDER
     , FD.FND_ETA_DATE_RAW
     , JPD.COO
     , Y.YARD_CONTAINER_ID                                                        AS DOMESTIC_TRAILER_ID
     , case
           when ITD.from_warehouse_id is null and ITD.pod_warehouse_id is null and ITD.to_warehouse_id is null
               then null
           when UPPER(TRIM(ITD.transport_mode_intransit)) = 'AIR'
               then null
           else ITD.IN_TRANSIT_CONTAINER_ID
end
as IT_CONTAINER_ID

     , case
           when ITD.from_warehouse_id is null and ITD.pod_warehouse_id is null and ITD.to_warehouse_id is null
               then null
           when UPPER(TRIM(ITD.transport_mode_intransit)) = 'AIR'
               then ITD.IN_TRANSIT_CONTAINER_ID
           else ITD.IN_TRANSIT_ID
end
as BL_ID

     , JPD.PO_ID
     , JPD.PO_DTL_ID
     , JPD.SC_CATEGORY
     , JPD.SHIPPING_PORT
     , coalesce(bk.so, ITD.in_transit_label)                                      AS SO
     , ITD.AIR_DSR_POD_DATE
     , y.order_number                                                             as DOMESTIC_ORDER_NUMBER
     , coalesce(ocl.unit_volume,
                ocl1.unit_volume,
                iff(jpd.ordered_qty > 0, bk.avg_booking_volume / jpd.ordered_qty, 0),
                iff(jpd.ordered_qty > 0, bk_po_avg.po_avg_booking_volume / jpd.ordered_qty, 0),
                iff(jpd.ordered_qty > 0, bk_sku_avg.sku_avg_booking_volume / jpd.ordered_qty, 0),
                JPD.ACTUAL_VOLUME,
                JPD.AVG_VOLUME,
                v.Avg_unit_volume
    )                                                                             as unit_volume
     , itd.booking_confirmed_date
     , itd.carrier_booking_placed_date
     , jpd.is_cancelled
     , jpd.received_date
     , itd.Vessel_Depart_at_Port_DSR_Actual
     , itd.Vessel_Arrive_at_Port_DSR_Actual
     , Case
           when jpd.received_date_est is not null and jpd.received_date_est >= JPD.SHOWROOM_DATE
               then 'Late'
           when jpd.received_date_est is not null and jpd.received_date_est < JPD.SHOWROOM_DATE
               then 'On Time'
           when upper(JPD.transport_mode) = 'TRUCK' AND ITD.BILL_OF_LADING is null
               then 'No Booking'
           when upper(JPD.transport_mode) IN ('DDP VENDOR', 'SMALL PARCEL', 'TRUCK')
               or itd.booking_confirmed_date is not null
               or itd.booking_so = 1
               then Case
                        WHEN ETA.ETA_on_hand_date >= dateadd(dd, -2, JPD.SHOWROOM_DATE)
                            AND ETA.ETA_on_hand_date < JPD.SHOWROOM_DATE
                            AND NVL(JPD.received_date_est, '1900-01-01') = '1900-01-01'
                            THEN 'At Risk'
                        WHEN JPD.SHOWROOM_DATE = ETA.ETA_on_hand_date
                            OR ETA.ETA_on_hand_date > JPD.SHOWROOM_DATE
                            or (JPD.SHOWROOM_DATE < to_date(current_date()) and
                                NVL(JPD.received_date_est, '1900-01-01') = '1900-01-01' and
                                ETA.ETA_on_hand_date < to_date(current_date()))
                            OR JPD.received_date_EST > JPD.SHOWROOM_DATE
                            THEN 'Late'
                        WHEN DATEADD(dd, -2, JPD.SHOWROOM_DATE) > ETA.ETA_on_hand_date
                            OR coalesce(JPD.received_date_EST, to_date(current_date())) < JPD.SHOWROOM_DATE
                            OR (UPPER(po_type) = 'RETAIL' AND DATEADD('DAY', -10, JPD.SHOWROOM_DATE) > ETA_ON_HAND_DATE)
                            THEN 'On Time'
end
when upper(JPD.transport_mode) = 'OCEAN'
               OR upper(JPD.transport_mode) ilike 'AIR%'
               then case
                        when itd.vendor_booking_date is null and upper(JPD.transport_mode) = 'OCEAN' and
                             datediff('day', current_timestamp(), JPD.END_SHIP_WINDOW) > 21
                            then 'No Booking - Not Due'

                        when itd.vendor_booking_date is null and upper(JPD.transport_mode) ilike 'AIR%' and
                             datediff('day', current_timestamp(), JPD.END_SHIP_WINDOW) > 10
                            then 'No Booking - Not Due'

                        when itd.vendor_booking_date is null
                            then 'No Booking'
                        when itd.vendor_booking_date is not null
                            and itd.booking_confirmed_date is null
                            then 'Booking Requested'
end
end
as Showroom_Call_Out

     , jpd.original_showroom
     , jpd.original_showroom_locked
     , Case
           when jpd.received_date_est is not null and jpd.received_date_est >= JPD.LAUNCH_DATE
               then 'Late'
           when jpd.received_date_est is not null and jpd.received_date_est < JPD.LAUNCH_DATE
               then 'On Time'
           when upper(JPD.transport_mode) = 'TRUCK' AND ITD.BILL_OF_LADING is null
               then 'No Booking'
           when upper(JPD.transport_mode) IN ('DDP VENDOR', 'SMALL PARCEL', 'TRUCK')
               or itd.booking_confirmed_date is not null
               or itd.ASN_Received = 1
               then Case
                        WHEN to_date(jpd.original_showroom || '-01') >= dateadd(dd, -2, JPD.launch_date)
                            AND to_date(jpd.original_showroom || '-01') < JPD.launch_date
                            AND NVL(JPD.received_date_est, '1900-01-01') = '1900-01-01'
                            THEN 'At Risk'
                        WHEN JPD.launch_date = to_date(jpd.original_showroom || '-01')
                            OR to_date(jpd.original_showroom || '-01') > JPD.launch_date
                            or (JPD.launch_date < to_date(current_date()) and
                                NVL(JPD.received_date_est, '1900-01-01') = '1900-01-01' and
                                to_date(jpd.original_showroom || '-01') < to_date(current_date()))
                            OR JPD.received_date_EST > JPD.launch_date
                            THEN 'Late'
                        WHEN DATEADD(dd, -2, JPD.launch_date) > to_date(jpd.original_showroom || '-01')
                            OR coalesce(JPD.received_date_EST, to_date(current_date())) < JPD.launch_date
                            OR (UPPER(po_type) = 'RETAIL' AND
                                DATEADD('DAY', -10, JPD.LAUNCH_DATE) > to_date(jpd.original_showroom || '-01'))
                            THEN 'On Time'
end

when upper(JPD.transport_mode) = 'OCEAN'
               OR upper(JPD.transport_mode) ilike 'AIR%'
               then case
                        when itd.vendor_booking_date is null
                            then 'No Booking'
                        when itd.vendor_booking_date is not null
                            and itd.booking_confirmed_date is null
                            then 'Booking Requested'
end
end
as Original_Showroom_Call_Out
     , jpd.size_name
     , ocl.total_item_volume_cbm                                                     oocl_vol
     , ocl1.total_item_volume_cbm                                                    oocl_vol1
     , bk.avg_booking_volume
     , bk_po_avg.po_avg_booking_volume
     , bk_sku_avg.sku_avg_booking_volume
     , (JPD.ACTUAL_VOLUME * JPD.ORDERED_QTY)                                      as jpd_actual_vol
     , (JPD.AVG_VOLUME * JPD.ORDERED_QTY)                                         as jpd_avg_vol
     , (v.Avg_unit_volume * JPD.ORDERED_QTY)                                         v_avg_unit_vol
     , jpd.centric_department
     , jpd.centric_subdepartment
     , jpd.centric_category
     , jpd.centric_class
     , jpd.centric_subclass
     , jpd.received_qty
     , case
           when nvl(ocl.total_item_volume_cbm, 0) != 0
               then 'OOCL Actual Vol'
           when nvl(ocl1.total_item_volume_cbm, 0) != 0
               then 'OOCL PO/SKU Avg Vol'
           when nvl(bk.avg_booking_volume, 0) != 0
               then 'Booking Vol'
           when nvl(bk_po_avg.po_avg_booking_volume, 0) != 0
               then 'PO Avg Booking Vol'
           when nvl(bk_sku_avg.sku_avg_booking_volume, 0) != 0
               then 'SKU Avg Booking Vol'
           when nvl((JPD.ACTUAL_VOLUME * JPD.ORDERED_QTY), 0) != 0
               then 'LC Avg Item Vol'
           when nvl((JPD.AVG_VOLUME * JPD.ORDERED_QTY), 0) != 0
               then 'LC Avg Vol'
            when nvl(jpd.centric_cbm, 0) !=0
            then 'Centric CBM'
           when nvl((v.Avg_unit_volume * JPD.ORDERED_QTY), 0) != 0
               then 'Avg Unit Vol'
end
as Volume_Source
     , case
           when nvl(ocl.total_item_weight_kg, 0) != 0
               then 'OOCL Actual Wgt'
           when nvl(ocl1.total_item_weight_kg, 0) != 0
               then 'OOCL PO/SKU Avg Wgt'
           when nvl(bk.avg_booking_weight, 0) != 0
               then 'Booking Vol'
           when nvl(bk_po_avg.po_avg_booking_weight, 0) != 0
               then 'PO Avg Booking Wgt'
           when nvl(bk_sku_avg.sku_avg_booking_weight, 0) != 0
               then 'SKU Avg Booking Wgt'
           when nvl((JPD.ACTUAL_WEIGHT * JPD.ORDERED_QTY), 0) != 0
               then 'LC Avg Item Wgt'
           when nvl((JPD.AVG_WEIGHT * JPD.ORDERED_QTY), 0) != 0
               then 'LC Avg Wgt'
end
as Weight_Source
    ,coalesce(jpd.bc_po_line_number, jpd.po_line_number, jpd.po_dtl_id) as po_line_number
    , jpd.centric_cbm
    ,ocl.unit_volume as actual_cbm

FROM _JPD JPD
         JOIN lake_view.ultra_warehouse.item I
              ON JPD.SKU = I.ITEM_NUMBER
         LEFT JOIN _ITD_FINAL ITD
                   ON JPD.PO_NUMBER = ITD.PO_NUMBER
                       AND JPD.SKU = ITD.SKU
                   and (
                    nvl(jpd.bc_po_line_number, -1) = nvl(itd.bc_po_line_number, 0)
                    or nvl(jpd.po_line_number, -1) = nvl(itd.po_line_number, 0)
                    or jpd.po_dtl_id_merlin = nvl(itd.po_dtl_id_merlin, 0)
                   )
          LEFT JOIN _fnd_dates FD
                   ON JPD.PO_NUMBER = FD.PO_NUMBER
                       AND JPD.SKU = FD.SKU
                       and nvl(jpd.bc_po_line_number, 1) = nvl(fd.po_line_number, 1)
         LEFT JOIN _eta_on_hand_date ETA
                   ON JPD.PO_NUMBER = ETA.PO_NUMBER
                       AND JPD.SKU = ETA.SKU
                       and nvl(jpd.bc_po_line_number, 1) = nvl(eta.po_line_number, 1)
         left Join _yard Y
                   ON JPD.PO_NUMBER = Y.PO_NUMBER
                       AND I.ITEM_ID = Y.ITEM_ID
                       and nvl(jpd.bc_po_line_number, 1) = nvl(y.po_line_number, 1)
                       AND Y.ROW_Num = 1
        LEFT JOIN _onhand_qty OH
                   ON JPD.WAREHOUSE_ID = OH.WAREHOUSE_ID
                       AND I.ITEM_ID = OH.ITEM_ID
      left join _oocl ocl
                   on upper(itd.po_number) = upper(ocl.po_number)
                       and upper(itd.sku) = upper(ocl.item_number)
                       and upper(itd.container_label) = upper(ocl.container)
                       and upper(itd.bill_of_lading) = upper(ocl.bl)
                       and ocl.row_ct = 1
         left join _oocl ocl1
                   on upper(itd.po_number) = upper(ocl1.po_number)
                       and upper(itd.sku) = upper(ocl1.item_number)
                       and ocl1.row_ct = 1

         left join _vol v
                   on jpd.BUSINESS_UNIT = v.business_unit
                and  Nvl(upper(trim(jpd.centric_class)), upper(trim(jpd.style_type))) = v.class

         left join (select so,
                           po,
                           style_color,
                           bc_po_line_number as po_line_number,
                           avg(booking_volume) as avg_booking_volume,
                           avg(booking_weight) as avg_booking_weight
                    from _booking bk
                    group by so, po, style_color,bc_po_line_number) bk
                   on jpd.po_number = bk.po
                       and jpd.sku = bk.style_color
                       and  jpd.bc_po_line_number = bk.po_line_number

         left join (select po,
                           avg(booking_volume) as po_avg_booking_volume,
                           avg(booking_weight) as po_avg_booking_weight
                    from _booking bk
                    group by po) bk_PO_avg
                   on jpd.po_number = bk_PO_avg.po

        left join (select style_color,
                           avg(booking_volume) as sku_avg_booking_volume,
                           avg(booking_weight) as sku_avg_booking_weight
                    from _booking bk
                    group by style_color) bk_sku_avg
                   on jpd.sku = bk_sku_avg.style_color

WHERE 1 = 1
--AND TO_DATE(JPD.SHOWROOM_DATE) >= DATEADD(month, -8, CURRENT_TIMESTAMP())
--and jpd.po_number = 'TK145800-315669'
--and jpd.sku = 'XO2253871-4985-57010'
;

/**
26
**/
create
or replace temporary table _stg_gsc_pipeline_a as

select p.*
     , iff(pol.traffic_mode is null, 'Failed', 'Passed') as Merlin_ports
     , iff(l.traffic_mode is null, 'Failed', 'Passed')   as ASN_ports
from _stg_gsc_pipeline p
         left join reporting_base_prod.gsc.pol_pod_transit_dataset l
                   on p.traffic_mode = l.traffic_mode
                       and p.from_warehouse_id = l.pol_warehouse_id
                       and p.pod_warehouse_id = l.pod_warehouse_id
                       and p.warehouse_id_origin = l.destination_warehouse_id
                       and p.logic = l.transit_logic
         left join (select distinct traffic_mode
                                  , upper(trim(case
                                                   when charindex(char(44), pol) > 0
                                                       then substring(pol, 1, charindex(char(44), pol) - 1)
                                                   when charindex(char(40), pol) > 0
                                                       then substring(pol, 1, charindex(char(40), pol) - 1)
                                                   else pol
        end)) as pol
                                  , destination_warehouse_id
                    from reporting_base_prod.gsc.pol_pod_transit_dataset
                    where 1 = 1
                      and traffic_mode = 'CFS-CY'
                      and transit_logic = 'Full Supply Chain') POL
                   ON upper(trim(case
                                     when charindex(char(44), P.SHIPPING_PORT) > 0
                                         then substring(P.SHIPPING_PORT, 1, charindex(char(44), P.SHIPPING_PORT) - 1)
                                     else P.SHIPPING_PORT
                       END)) = pol.pol
                       AND P.WAREHOUSE_ID = POL.DESTINATION_WAREHOUSE_ID
;

/*********************************
capture and remove duplicated data
**********************************/
insert into gsc.rpt_pipeline_detail_errors
(PO_NUMBER, SKU, PO_SKU_STATUS, BUSINESS_UNIT, SHOWROOM_DATE, LAUNCH_DATE, COLOR, ITEM_ID, ITEM_CATEGORY,
 ITEM_WMS_CLASS, SKU_SEGMENT, DESCRIPTION, VENDOR_CODE, VENDOR_NAME, END_SHIP_WINDOW, TARGET_DELIVERY_DATE,
 DESTINATION, TRANSPORT_MODE, CARGO_READY_DATE, CARGO_RECEIVE_DATE, FROM_WAREHOUSE_ID, TO_WAREHOUSE_ID,
 BILL_OF_LADING, DOMESTIC_BILL_OF_LADING, CONTAINER_ID, CONTAINER_LABEL, DOMESTIC_TRAILER_NUMBER, CARRIER_FND,
 CARRIER_FND_CODE, TRADING_TERMS, PORT_OF_LOADING, CARRIER_ID, CARRIER_LABEL, EQUIPMENT_TYPE, CONTAINER_TYPE,
 PORT_OF_DISCHARGE, CONTAINER_WEIGHT, SHIPMENT_VOLUME, ORDERED_QTY, SHIPPED_QTY, ONHAND_QTY, UNIT_COST, FREIGHT,
 DUTY, IN_TRANSIT_CONTAINER_ID, IN_GATE_ESTIMATED_DATE, IN_GATE_REVISED_DATE, IN_GATE_ACTUAL_DATE,
 VESSEL_DEPART_AT_PORT_ESTIMATED_DATE, VESSEL_DEPART_AT_PORT_REVISED_DATE, VESSEL_DEPART_AT_PORT_ACTUAL_DATE,
 VESSEL_ARRIVE_AT_PORT_ESTIMATED_DATE, VESSEL_ARRIVE_AT_PORT_REVISED_DATE, VESSEL_ARRIVE_AT_PORT_ACTUAL_DATE,
 LOADED_ON_RAIL_ESTIMATED_DATE, LOADED_ON_RAIL_REVISED_DATE, LOADED_ON_RAIL_ACTUAL_DATE,
 RAIL_ARR_DEST_INTML_REMP_ESTIMATED_DATE, RAIL_ARR_DEST_INTML_REMP_REVISED_DATE,
 RAIL_ARR_DEST_INTML_REMP_ACTUAL_DATE, PICKED_UP_FOR_DELIVERY_ESTIMATED_DATE,
 PICKED_UP_FOR_DELIVERY_REVISED_DATE, PICKED_UP_FOR_DELIVERY_ACTUAL_DATE,
 TRANSLOAD_DOMESTIC_DEPART_ESTIMATED_DATE, TRANSLOAD_DOMESTIC_DEPART_REVISED_DATE,
 TRANSLOAD_DOMESTIC_DEPART_ACTUAL_DATE, TRANSLOAD_TRUCK_ETA_ESTIMATED_DATE, TRANSLOAD_TRUCK_ETA_REVISED_DATE,
 TRANSLOAD_TRUCK_ETA_ACTUAL_DATE, DELIVERED_TO_CONSIGNEE_ESTIMATED_DATE, DELIVERED_TO_CONSIGNEE_REVISED_DATE,
 DELIVERED_TO_CONSIGNEE_ACTUAL_DATE, RECEIVED_DATE_EST, DC_APPOINTMENT_DATE, TRANSLOAD_TRUCK_ETA_C_DATE,
 PO_RECEIVED, RECEIPT_STATUS, FND_ETA_DATE, FND_ETA_WEEK_DAY_NO, ETA_ON_HAND_DATE, ETA_ON_HAND_WEEK,
 ETA_ON_HAND_MONTH, ETA_ON_HAND_DAY, WEEK_ETA, CALL_OUT, MODE, RAIL_STATUS, VENDOR_BOOKING_DATE, STYLE_NAME,
 SHIPMENT_TYPE, POD_WAREHOUSE_ID, FND_DATE, TRANSPORT_MODE_INTRANSIT, MERLIN_PO_STATUS, TRAFFIC_MODE,
 QUANTITY_ADDED, CARTON_COUNT, VOLUME, WAREHOUSE_ID, WAREHOUSE_ID_ORIGIN,
 OFFICIAL_FACTORY_NAME, ORIGINAL_END_SHIP_WINDOW_DATE, PO_TYPE, QTY_GHOSTED, BUY_RANK,
 LOGIC, SOURCE_VENDOR_NAME, SOURCE_VENDOR_ID, GENDER, COO, DOMESTIC_TRAILER_ID,
 IT_CONTAINER_ID, BL_ID, PO_ID, PO_DTL_ID, SC_CATEGORY, SHIPPING_PORT, WEIGHT, SO, AIR_DSR_POD_DATE,
 DOMESTIC_ORDER_NUMBER, unit_volume, booking_confirmed_date, carrier_booking_placed_date, is_cancelled,
 merlin_ports, asn_ports, received_date, Vessel_Depart_at_Port_DSR_Actual, Vessel_Arrive_at_Port_DSR_Actual,
 showroom_call_out, original_showroom, original_showroom_locked, Original_Showroom_Call_Out, size_name,
 oocl_vol, oocl_vol1, avg_booking_volume, po_avg_booking_volume, sku_avg_booking_volume, jpd_actual_vol,
 jpd_avg_vol, v_avg_unit_vol, centric_department, centric_subdepartment, centric_category, centric_class,
 centric_subclass, received_qty, volume_source, weight_source, po_line_number, centric_cbm, actual_cbm)
select PO_NUMBER,
       SKU,
       PO_SKU_STATUS,
       BUSINESS_UNIT,
       SHOWROOM_DATE,
       LAUNCH_DATE,
       COLOR,
       ITEM_ID,
       ITEM_CATEGORY,
       ITEM_WMS_CLASS,
       SKU_SEGMENT,
       DESCRIPTION,
       VENDOR_CODE,
       VENDOR_NAME,
       END_SHIP_WINDOW,
       TARGET_DELIVERY_DATE,
       DESTINATION,
       TRANSPORT_MODE,
       CARGO_READY_DATE,
       CARGO_RECEIVE_DATE,
       FROM_WAREHOUSE_ID,
       TO_WAREHOUSE_ID,
       BILL_OF_LADING,
       DOMESTIC_BILL_OF_LADING,
       CONTAINER_ID,
       CONTAINER_LABEL,
       DOMESTIC_TRAILER_NUMBER,
       CARRIER_FND,
       CARRIER_FND_CODE,
       TRADING_TERMS,
       PORT_OF_LOADING,
       CARRIER_ID,
       CARRIER_LABEL,
       EQUIPMENT_TYPE,
       CONTAINER_TYPE,
       PORT_OF_DISCHARGE,
       CONTAINER_WEIGHT,
       SHIPMENT_VOLUME,
       ORDERED_QTY,
       SHIPPED_QTY,
       ONHAND_QTY,
       UNIT_COST,
       FREIGHT,
       DUTY,
       IN_TRANSIT_CONTAINER_ID,
       IN_GATE_ESTIMATED_DATE,
       IN_GATE_REVISED_DATE,
       IN_GATE_ACTUAL_DATE,
       VESSEL_DEPART_AT_PORT_ESTIMATED_DATE,
       VESSEL_DEPART_AT_PORT_REVISED_DATE,
       VESSEL_DEPART_AT_PORT_ACTUAL_DATE,
       VESSEL_ARRIVE_AT_PORT_ESTIMATED_DATE,
       VESSEL_ARRIVE_AT_PORT_REVISED_DATE,
       VESSEL_ARRIVE_AT_PORT_ACTUAL_DATE,
       LOADED_ON_RAIL_ESTIMATED_DATE,
       LOADED_ON_RAIL_REVISED_DATE,
       LOADED_ON_RAIL_ACTUAL_DATE,
       RAIL_ARR_DEST_INTML_REMP_ESTIMATED_DATE,
       RAIL_ARR_DEST_INTML_REMP_REVISED_DATE,
       RAIL_ARR_DEST_INTML_REMP_ACTUAL_DATE,
       PICKED_UP_FOR_DELIVERY_ESTIMATED_DATE,
       PICKED_UP_FOR_DELIVERY_REVISED_DATE,
       PICKED_UP_FOR_DELIVERY_ACTUAL_DATE,
       TRANSLOAD_DOMESTIC_DEPART_ESTIMATED_DATE,
       TRANSLOAD_DOMESTIC_DEPART_REVISED_DATE,
       TRANSLOAD_DOMESTIC_DEPART_ACTUAL_DATE,
       TRANSLOAD_TRUCK_ETA_ESTIMATED_DATE,
       TRANSLOAD_TRUCK_ETA_REVISED_DATE,
       TRANSLOAD_TRUCK_ETA_ACTUAL_DATE,
       DELIVERED_TO_CONSIGNEE_ESTIMATED_DATE,
       DELIVERED_TO_CONSIGNEE_REVISED_DATE,
       DELIVERED_TO_CONSIGNEE_ACTUAL_DATE,
       RECEIVED_DATE_EST,
       DC_APPOINTMENT_DATE,
       TRANSLOAD_TRUCK_ETA_C_DATE,
       PO_RECEIVED,
       RECEIPT_STATUS,
       FND_ETA_DATE,
       FND_ETA_WEEK_DAY_NO,
       ETA_ON_HAND_DATE,
       ETA_ON_HAND_WEEK,
       ETA_ON_HAND_MONTH,
       ETA_ON_HAND_DAY,
       WEEK_ETA,
       CALL_OUT,
       MODE,
       RAIL_STATUS,
       VENDOR_BOOKING_DATE,
       STYLE_NAME,
       SHIPMENT_TYPE,
       POD_WAREHOUSE_ID,
       FND_DATE,
       TRANSPORT_MODE_INTRANSIT,
       MERLIN_PO_STATUS,
       TRAFFIC_MODE,
       QUANTITY_ADDED,
       CARTON_COUNT,
       VOLUME,
       WAREHOUSE_ID,
       WAREHOUSE_ID_ORIGIN,
       OFFICIAL_FACTORY_NAME,
       ORIGINAL_END_SHIP_WINDOW_DATE,
       PO_TYPE,
       QTY_GHOSTED,
       BUY_RANK,
       LOGIC,
       SOURCE_VENDOR_NAME,
       SOURCE_VENDOR_ID,
       GENDER,
       COO,
       DOMESTIC_TRAILER_ID,
       IT_CONTAINER_ID,
       BL_ID,
       PO_ID,
       PO_DTL_ID,
       SC_CATEGORY,
       SHIPPING_PORT,
       WEIGHT,
       SO,
       AIR_DSR_POD_DATE,
       DOMESTIC_ORDER_NUMBER,
       unit_volume,
       booking_confirmed_date,
       carrier_booking_placed_date,
       is_cancelled,
       merlin_ports,
       asn_ports,
       received_date,
       Vessel_Depart_at_Port_DSR_Actual,
       Vessel_Arrive_at_Port_DSR_Actual,
       showroom_call_out,
       original_showroom,
       original_showroom_locked,
       Original_Showroom_Call_Out,
       size_name,
       oocl_vol,
       oocl_vol1,
       avg_booking_volume,
       po_avg_booking_volume,
       sku_avg_booking_volume,
       jpd_actual_vol,
       jpd_avg_vol,
       v_avg_unit_vol,
       centric_department,
       centric_subdepartment,
       centric_category,
       centric_class,
       centric_subclass,
       received_qty,
       volume_source,
       weight_source,
       po_line_number,
       centric_cbm,
       actual_cbm
from _stg_gsc_pipeline_a
where po_dtl_id in (select po_dtl_id
                    from _stg_gsc_pipeline
                    group by po_dtl_id
                    having count(1) > 1)
;

delete
from _stg_gsc_pipeline_a
where po_dtl_id in (select po_dtl_id
                    from _stg_gsc_pipeline
                    group by po_dtl_id
                    having count(1) > 1)
;


/*******************************
MERGE STAGE TO PROD
27
********************************/

MERGE INTO gsc.rpt_pipeline_detail AS TGT
    USING _stg_gsc_pipeline_a AS SRC
    ON TGT.po_dtl_id = src.po_dtl_id
    WHEN MATCHED
        THEN UPDATE
        SET
            TGT.PO_SKU_STATUS = SRC.PO_SKU_STATUS
            , TGT.BUSINESS_UNIT = SRC.BUSINESS_UNIT
            , TGT.SHOWROOM_DATE = SRC.SHOWROOM_DATE
            , TGT.LAUNCH_DATE = SRC.LAUNCH_DATE
            , TGT.COLOR = SRC.COLOR
            , TGT.ITEM_CATEGORY = SRC.ITEM_CATEGORY
            , TGT.ITEM_WMS_CLASS = SRC.ITEM_WMS_CLASS
            , TGT.SKU_SEGMENT = SRC.SKU_SEGMENT
            , TGT.DESCRIPTION = SRC.DESCRIPTION
            , TGT.VENDOR_CODE = SRC.VENDOR_CODE
            , TGT.VENDOR_NAME = SRC.VENDOR_NAME
            , TGT.END_SHIP_WINDOW = SRC.END_SHIP_WINDOW
            , TGT.TARGET_DELIVERY_DATE = SRC.TARGET_DELIVERY_DATE
            , TGT.DESTINATION = SRC.DESTINATION
            , TGT.TRANSPORT_MODE = SRC.TRANSPORT_MODE
            , TGT.CARGO_READY_DATE = SRC.CARGO_READY_DATE
            , TGT.CARGO_RECEIVE_DATE = SRC.CARGO_RECEIVE_DATE
            , TGT.FROM_WAREHOUSE_ID = SRC.FROM_WAREHOUSE_ID
            , TGT.TO_WAREHOUSE_ID = SRC.TO_WAREHOUSE_ID
            , TGT.BILL_OF_LADING = SRC.BILL_OF_LADING
            , TGT.DOMESTIC_BILL_OF_LADING = SRC.DOMESTIC_BILL_OF_LADING
            , TGT.CONTAINER_ID = SRC.CONTAINER_ID
            , TGT.CONTAINER_LABEL = SRC.CONTAINER_LABEL
            , TGT.DOMESTIC_TRAILER_NUMBER = SRC.DOMESTIC_TRAILER_NUMBER
            , TGT.CARRIER_FND = SRC.CARRIER_FND
            , TGT.CARRIER_FND_CODE = SRC.CARRIER_FND_CODE
            , TGT.TRADING_TERMS = SRC.TRADING_TERMS
            , TGT.PORT_OF_LOADING = SRC.PORT_OF_LOADING
            , TGT.CARRIER_ID = SRC.CARRIER_ID
            , TGT.CARRIER_LABEL = SRC.CARRIER_LABEL
            , TGT.EQUIPMENT_TYPE = SRC.EQUIPMENT_TYPE
            , TGT.CONTAINER_TYPE = SRC.CONTAINER_TYPE
            , TGT.PORT_OF_DISCHARGE = SRC.PORT_OF_DISCHARGE
            , TGT.CONTAINER_WEIGHT = SRC.CONTAINER_WEIGHT
            , TGT.SHIPMENT_VOLUME = SRC.SHIPMENT_VOLUME
            , TGT.ORDERED_QTY = SRC.ORDERED_QTY
            , TGT.SHIPPED_QTY = SRC.SHIPPED_QTY
            , TGT.ONHAND_QTY = SRC.ONHAND_QTY
            , TGT.UNIT_COST = SRC.UNIT_COST
            , TGT.FREIGHT = SRC.FREIGHT
            , TGT.DUTY = SRC.DUTY
            , TGT.IN_TRANSIT_CONTAINER_ID = SRC.IN_TRANSIT_CONTAINER_ID
            , TGT.IN_GATE_ESTIMATED_DATE = SRC.IN_GATE_ESTIMATED_DATE
            , TGT.IN_GATE_REVISED_DATE = SRC.IN_GATE_REVISED_DATE
            , TGT.IN_GATE_ACTUAL_DATE = SRC.IN_GATE_ACTUAL_DATE
            , TGT.VESSEL_DEPART_AT_PORT_ESTIMATED_DATE = SRC.VESSEL_DEPART_AT_PORT_ESTIMATED_DATE
            , TGT.VESSEL_DEPART_AT_PORT_REVISED_DATE = SRC.VESSEL_DEPART_AT_PORT_REVISED_DATE
            , TGT.VESSEL_DEPART_AT_PORT_ACTUAL_DATE = SRC.VESSEL_DEPART_AT_PORT_ACTUAL_DATE
            , TGT.VESSEL_ARRIVE_AT_PORT_ESTIMATED_DATE = SRC.VESSEL_ARRIVE_AT_PORT_ESTIMATED_DATE
            , TGT.VESSEL_ARRIVE_AT_PORT_REVISED_DATE = SRC.VESSEL_ARRIVE_AT_PORT_REVISED_DATE
            , TGT.VESSEL_ARRIVE_AT_PORT_ACTUAL_DATE = SRC.VESSEL_ARRIVE_AT_PORT_ACTUAL_DATE
            , TGT.LOADED_ON_RAIL_ESTIMATED_DATE = SRC.LOADED_ON_RAIL_ESTIMATED_DATE
            , TGT.LOADED_ON_RAIL_REVISED_DATE = SRC.LOADED_ON_RAIL_REVISED_DATE
            , TGT.LOADED_ON_RAIL_ACTUAL_DATE = SRC.LOADED_ON_RAIL_ACTUAL_DATE
            , TGT.RAIL_ARR_DEST_INTML_REMP_ESTIMATED_DATE = SRC.RAIL_ARR_DEST_INTML_REMP_ESTIMATED_DATE
            , TGT.RAIL_ARR_DEST_INTML_REMP_REVISED_DATE = SRC.RAIL_ARR_DEST_INTML_REMP_REVISED_DATE
            , TGT.RAIL_ARR_DEST_INTML_REMP_ACTUAL_DATE = SRC.RAIL_ARR_DEST_INTML_REMP_ACTUAL_DATE
            , TGT.PICKED_UP_FOR_DELIVERY_ESTIMATED_DATE = SRC.PICKED_UP_FOR_DELIVERY_ESTIMATED_DATE
            , TGT.PICKED_UP_FOR_DELIVERY_REVISED_DATE = SRC.PICKED_UP_FOR_DELIVERY_REVISED_DATE
            , TGT.PICKED_UP_FOR_DELIVERY_ACTUAL_DATE = SRC.PICKED_UP_FOR_DELIVERY_ACTUAL_DATE
            , TGT.TRANSLOAD_DOMESTIC_DEPART_ESTIMATED_DATE = SRC.TRANSLOAD_DOMESTIC_DEPART_ESTIMATED_DATE
            , TGT.TRANSLOAD_DOMESTIC_DEPART_REVISED_DATE = SRC.TRANSLOAD_DOMESTIC_DEPART_REVISED_DATE
            , TGT.TRANSLOAD_DOMESTIC_DEPART_ACTUAL_DATE = SRC.TRANSLOAD_DOMESTIC_DEPART_ACTUAL_DATE
            , TGT.TRANSLOAD_TRUCK_ETA_ESTIMATED_DATE = SRC.TRANSLOAD_TRUCK_ETA_ESTIMATED_DATE
            , TGT.TRANSLOAD_TRUCK_ETA_REVISED_DATE = SRC.TRANSLOAD_TRUCK_ETA_REVISED_DATE
            , TGT.TRANSLOAD_TRUCK_ETA_ACTUAL_DATE = SRC.TRANSLOAD_TRUCK_ETA_ACTUAL_DATE
            , TGT.DELIVERED_TO_CONSIGNEE_ESTIMATED_DATE = SRC.DELIVERED_TO_CONSIGNEE_ESTIMATED_DATE
            , TGT.DELIVERED_TO_CONSIGNEE_REVISED_DATE = SRC.DELIVERED_TO_CONSIGNEE_REVISED_DATE
            , TGT.DELIVERED_TO_CONSIGNEE_ACTUAL_DATE = SRC.DELIVERED_TO_CONSIGNEE_ACTUAL_DATE
            , TGT.RECEIVED_DATE_EST = SRC.RECEIVED_DATE_EST
            , TGT.DC_APPOINTMENT_DATE = SRC.DC_APPOINTMENT_DATE
            , TGT.TRANSLOAD_TRUCK_ETA_C_DATE = SRC.TRANSLOAD_TRUCK_ETA_C_DATE
            , TGT.PO_RECEIVED = SRC.PO_RECEIVED
            , TGT.RECEIPT_STATUS = SRC.RECEIPT_STATUS
            , TGT.FND_ETA_DATE = SRC.FND_ETA_DATE
            , TGT.FND_ETA_WEEK_DAY_NO = SRC.FND_ETA_WEEK_DAY_NO
            , TGT.ETA_ON_HAND_DATE = SRC.ETA_ON_HAND_DATE
            , TGT.ETA_ON_HAND_WEEK = SRC.ETA_ON_HAND_WEEK
            , TGT.ETA_ON_HAND_MONTH = SRC.ETA_ON_HAND_MONTH
            , TGT.ETA_ON_HAND_DAY = SRC.ETA_ON_HAND_DAY
            , TGT.WEEK_ETA = SRC.WEEK_ETA
            , TGT.CALL_OUT = SRC.CALL_OUT
            , TGT.MODE = SRC.MODE
            , TGT.RAIL_STATUS = SRC.RAIL_STATUS
            , TGT.VENDOR_BOOKING_DATE = SRC.VENDOR_BOOKING_DATE
            , TGT.STYLE_NAME = SRC.STYLE_NAME
            , TGT.SHIPMENT_TYPE = SRC.SHIPMENT_TYPE
            , TGT.POD_WAREHOUSE_ID = SRC.POD_WAREHOUSE_ID
            , TGT.FND_DATE = SRC.FND_DATE
            , TGT.TRANSPORT_MODE_INTRANSIT = SRC.TRANSPORT_MODE_INTRANSIT
            , TGT.MERLIN_PO_STATUS = SRC.MERLIN_PO_STATUS
            , TGT.TRAFFIC_MODE = SRC.TRAFFIC_MODE
            , TGT.QUANTITY_ADDED = SRC.QUANTITY_ADDED
            , TGT.CARTON_COUNT = SRC.CARTON_COUNT
            , TGT.VOLUME = SRC.VOLUME
            , TGT.WAREHOUSE_ID = SRC.WAREHOUSE_ID
            , TGT.WAREHOUSE_ID_ORIGIN = SRC.WAREHOUSE_ID_ORIGIN
            , TGT.META_UPDATE_DATETIME = CURRENT_TIMESTAMP()::TIMESTAMP_LTZ
            , TGT.OFFICIAL_FACTORY_NAME = SRC.OFFICIAL_FACTORY_NAME
            , TGT.ORIGINAL_END_SHIP_WINDOW_DATE = SRC.ORIGINAL_END_SHIP_WINDOW_DATE
            , TGT.PO_TYPE = SRC.PO_TYPE
            , TGT.QTY_GHOSTED = SRC.QTY_GHOSTED
            , TGT.BUY_RANK = SRC.BUY_RANK
            , TGT.LOGIC = SRC.LOGIC
            , TGT.SOURCE_VENDOR_NAME = SRC.SOURCE_VENDOR_NAME
            , TGT.SOURCE_VENDOR_ID = SRC.SOURCE_VENDOR_ID
            , TGT.GENDER = SRC.GENDER
            , TGT.FND_ETA_DATE_ORIGINAL = CASE
                                              WHEN TGT.FND_ETA_DATE_ORIGINAL IS NULL AND SRC.FND_ETA_DATE_RAW IS NOT NULL
                                                  THEN SRC.FND_ETA_DATE_RAW
                                              ELSE TGT.FND_ETA_DATE_ORIGINAL
END
,TGT.COO = SRC.COO
            ,TGT.DOMESTIC_TRAILER_ID = SRC.DOMESTIC_TRAILER_ID
            ,TGT.IT_CONTAINER_ID = SRC.IT_CONTAINER_ID
            ,TGT.BL_ID = SRC.BL_ID
            ,TGT.PO_ID = SRC.PO_ID
            ,TGT.PO_DTL_ID = SRC.PO_DTL_ID
            ,TGT.SC_CATEGORY = SRC.SC_CATEGORY
            ,TGT.SHIPPING_PORT = SRC.SHIPPING_PORT
            ,TGT.WEIGHT = SRC.WEIGHT
            ,TGT.SO = SRC.SO
            ,TGT.AIR_DSR_POD_DATE = SRC.AIR_DSR_POD_DATE
            ,TGT.DOMESTIC_ORDER_NUMBER = SRC.DOMESTIC_ORDER_NUMBER
            ,tgt.unit_volume = src.unit_volume
            ,tgt.booking_confirmed_date = src.booking_confirmed_date
            ,tgt.carrier_booking_placed_date = src.carrier_booking_placed_date
            ,tgt.is_cancelled = src.is_cancelled
            ,tgt.merlin_ports = src.merlin_ports
            ,tgt.asn_ports = src.asn_ports
            ,tgt.received_date = src.received_date
            ,tgt.Vessel_Depart_at_Port_DSR_Actual = src.Vessel_Depart_at_Port_DSR_Actual
            ,tgt.Vessel_Arrive_at_Port_DSR_Actual = src.Vessel_Arrive_at_Port_DSR_Actual
            ,tgt.showroom_call_out = src.showroom_call_out
            ,tgt.original_showroom = src.original_showroom
            ,tgt.original_showroom_locked = src.original_showroom_locked
            ,tgt.Original_Showroom_Call_Out = src.Original_Showroom_Call_Out
            ,tgt.size_name = src.size_name
            ,tgt.oocl_vol = src.oocl_vol
            ,tgt.oocl_vol1 = src.oocl_vol1
            ,tgt.avg_booking_volume = src.avg_booking_volume
            ,tgt.po_avg_booking_volume = src.po_avg_booking_volume
            ,tgt.sku_avg_booking_volume = src.sku_avg_booking_volume
            ,tgt.jpd_actual_vol = src.jpd_actual_vol
            ,tgt.jpd_avg_vol = src.jpd_avg_vol
            ,tgt.v_avg_unit_vol = src.v_avg_unit_vol
            , tgt.centric_department = src.centric_department
            , tgt.centric_subdepartment = src.centric_subdepartment
            , tgt.centric_category = src.centric_category
            , tgt.centric_class = src.centric_class
            , tgt.centric_subclass = src.centric_subclass
            , tgt.received_qty = src.received_qty
            , tgt.volume_source = src.volume_source
            , tgt.weight_source = src.weight_source
            , tgt.po_line_number = src.po_line_number
            , tgt.centric_cbm = src.centric_cbm
            , tgt.actual_cbm = src.actual_cbm
    WHEN NOT MATCHED
        THEN INSERT
        (
         PO_NUMBER, SKU, PO_SKU_STATUS, BUSINESS_UNIT, SHOWROOM_DATE, LAUNCH_DATE, COLOR, ITEM_ID, ITEM_CATEGORY,
         ITEM_WMS_CLASS, SKU_SEGMENT, DESCRIPTION, VENDOR_CODE, VENDOR_NAME, END_SHIP_WINDOW, TARGET_DELIVERY_DATE,
         DESTINATION, TRANSPORT_MODE, CARGO_READY_DATE, CARGO_RECEIVE_DATE, FROM_WAREHOUSE_ID, TO_WAREHOUSE_ID,
         BILL_OF_LADING, DOMESTIC_BILL_OF_LADING, CONTAINER_ID, CONTAINER_LABEL, DOMESTIC_TRAILER_NUMBER, CARRIER_FND,
         CARRIER_FND_CODE, TRADING_TERMS, PORT_OF_LOADING, CARRIER_ID, CARRIER_LABEL, EQUIPMENT_TYPE, CONTAINER_TYPE,
         PORT_OF_DISCHARGE, CONTAINER_WEIGHT, SHIPMENT_VOLUME, ORDERED_QTY, SHIPPED_QTY, ONHAND_QTY, UNIT_COST, FREIGHT,
         DUTY, IN_TRANSIT_CONTAINER_ID, IN_GATE_ESTIMATED_DATE, IN_GATE_REVISED_DATE, IN_GATE_ACTUAL_DATE,
         VESSEL_DEPART_AT_PORT_ESTIMATED_DATE, VESSEL_DEPART_AT_PORT_REVISED_DATE, VESSEL_DEPART_AT_PORT_ACTUAL_DATE,
         VESSEL_ARRIVE_AT_PORT_ESTIMATED_DATE, VESSEL_ARRIVE_AT_PORT_REVISED_DATE, VESSEL_ARRIVE_AT_PORT_ACTUAL_DATE,
         LOADED_ON_RAIL_ESTIMATED_DATE, LOADED_ON_RAIL_REVISED_DATE, LOADED_ON_RAIL_ACTUAL_DATE,
         RAIL_ARR_DEST_INTML_REMP_ESTIMATED_DATE, RAIL_ARR_DEST_INTML_REMP_REVISED_DATE,
         RAIL_ARR_DEST_INTML_REMP_ACTUAL_DATE, PICKED_UP_FOR_DELIVERY_ESTIMATED_DATE,
         PICKED_UP_FOR_DELIVERY_REVISED_DATE, PICKED_UP_FOR_DELIVERY_ACTUAL_DATE,
         TRANSLOAD_DOMESTIC_DEPART_ESTIMATED_DATE, TRANSLOAD_DOMESTIC_DEPART_REVISED_DATE,
         TRANSLOAD_DOMESTIC_DEPART_ACTUAL_DATE, TRANSLOAD_TRUCK_ETA_ESTIMATED_DATE, TRANSLOAD_TRUCK_ETA_REVISED_DATE,
         TRANSLOAD_TRUCK_ETA_ACTUAL_DATE, DELIVERED_TO_CONSIGNEE_ESTIMATED_DATE, DELIVERED_TO_CONSIGNEE_REVISED_DATE,
         DELIVERED_TO_CONSIGNEE_ACTUAL_DATE, RECEIVED_DATE_EST, DC_APPOINTMENT_DATE, TRANSLOAD_TRUCK_ETA_C_DATE,
         PO_RECEIVED, RECEIPT_STATUS, FND_ETA_DATE, FND_ETA_WEEK_DAY_NO, ETA_ON_HAND_DATE, ETA_ON_HAND_WEEK,
         ETA_ON_HAND_MONTH, ETA_ON_HAND_DAY, WEEK_ETA, CALL_OUT, MODE, RAIL_STATUS, VENDOR_BOOKING_DATE, STYLE_NAME,
         SHIPMENT_TYPE, POD_WAREHOUSE_ID, FND_DATE, TRANSPORT_MODE_INTRANSIT, MERLIN_PO_STATUS, TRAFFIC_MODE,
         QUANTITY_ADDED, CARTON_COUNT, VOLUME, WAREHOUSE_ID, WAREHOUSE_ID_ORIGIN, META_CREATE_DATETIME,
         META_UPDATE_DATETIME, OFFICIAL_FACTORY_NAME, ORIGINAL_END_SHIP_WINDOW_DATE, PO_TYPE, QTY_GHOSTED, BUY_RANK,
         LOGIC, SOURCE_VENDOR_NAME, SOURCE_VENDOR_ID, GENDER, FND_ETA_DATE_ORIGINAL, COO, DOMESTIC_TRAILER_ID,
         IT_CONTAINER_ID, BL_ID, PO_ID, PO_DTL_ID, SC_CATEGORY, SHIPPING_PORT, WEIGHT, SO, AIR_DSR_POD_DATE,
         DOMESTIC_ORDER_NUMBER, unit_volume, booking_confirmed_date, carrier_booking_placed_date, is_cancelled,
         merlin_ports, asn_ports, received_date, Vessel_Depart_at_Port_DSR_Actual, Vessel_Arrive_at_Port_DSR_Actual,
         showroom_call_out, original_showroom, original_showroom_locked, Original_Showroom_Call_Out, size_name,
         oocl_vol, oocl_vol1, avg_booking_volume, po_avg_booking_volume, sku_avg_booking_volume, jpd_actual_vol,
         jpd_avg_vol, v_avg_unit_vol, centric_department, centric_subdepartment, centric_category, centric_class,
         centric_subclass, received_qty, volume_source, weight_source, po_line_number, centric_cbm, actual_cbm
            )
        VALUES ( SRC.PO_NUMBER
               , SRC.SKU
               , SRC.PO_SKU_STATUS
               , SRC.BUSINESS_UNIT
               , SRC.SHOWROOM_DATE
               , SRC.LAUNCH_DATE
               , SRC.COLOR
               , SRC.ITEM_ID
               , SRC.ITEM_CATEGORY
               , SRC.ITEM_WMS_CLASS
               , SRC.SKU_SEGMENT
               , SRC.DESCRIPTION
               , SRC.VENDOR_CODE
               , SRC.VENDOR_NAME
               , SRC.END_SHIP_WINDOW
               , SRC.TARGET_DELIVERY_DATE
               , SRC.DESTINATION
               , SRC.TRANSPORT_MODE
               , SRC.CARGO_READY_DATE
               , SRC.CARGO_RECEIVE_DATE
               , SRC.FROM_WAREHOUSE_ID
               , SRC.TO_WAREHOUSE_ID
               , SRC.BILL_OF_LADING
               , SRC.DOMESTIC_BILL_OF_LADING
               , SRC.CONTAINER_ID
               , SRC.CONTAINER_LABEL
               , SRC.DOMESTIC_TRAILER_NUMBER
               , SRC.CARRIER_FND
               , SRC.CARRIER_FND_CODE
               , SRC.TRADING_TERMS
               , SRC.PORT_OF_LOADING
               , SRC.CARRIER_ID
               , SRC.CARRIER_LABEL
               , SRC.EQUIPMENT_TYPE
               , SRC.CONTAINER_TYPE
               , SRC.PORT_OF_DISCHARGE
               , SRC.CONTAINER_WEIGHT
               , SRC.SHIPMENT_VOLUME
               , SRC.ORDERED_QTY
               , SRC.SHIPPED_QTY
               , SRC.ONHAND_QTY
               , SRC.UNIT_COST
               , SRC.FREIGHT
               , SRC.DUTY
               , SRC.IN_TRANSIT_CONTAINER_ID
               , SRC.IN_GATE_ESTIMATED_DATE
               , SRC.IN_GATE_REVISED_DATE
               , SRC.IN_GATE_ACTUAL_DATE
               , SRC.VESSEL_DEPART_AT_PORT_ESTIMATED_DATE
               , SRC.VESSEL_DEPART_AT_PORT_REVISED_DATE
               , SRC.VESSEL_DEPART_AT_PORT_ACTUAL_DATE
               , SRC.VESSEL_ARRIVE_AT_PORT_ESTIMATED_DATE
               , SRC.VESSEL_ARRIVE_AT_PORT_REVISED_DATE
               , SRC.VESSEL_ARRIVE_AT_PORT_ACTUAL_DATE
               , SRC.LOADED_ON_RAIL_ESTIMATED_DATE
               , SRC.LOADED_ON_RAIL_REVISED_DATE
               , SRC.LOADED_ON_RAIL_ACTUAL_DATE
               , SRC.RAIL_ARR_DEST_INTML_REMP_ESTIMATED_DATE
               , SRC.RAIL_ARR_DEST_INTML_REMP_REVISED_DATE
               , SRC.RAIL_ARR_DEST_INTML_REMP_ACTUAL_DATE
               , SRC.PICKED_UP_FOR_DELIVERY_ESTIMATED_DATE
               , SRC.PICKED_UP_FOR_DELIVERY_REVISED_DATE
               , SRC.PICKED_UP_FOR_DELIVERY_ACTUAL_DATE
               , SRC.TRANSLOAD_DOMESTIC_DEPART_ESTIMATED_DATE
               , SRC.TRANSLOAD_DOMESTIC_DEPART_REVISED_DATE
               , SRC.TRANSLOAD_DOMESTIC_DEPART_ACTUAL_DATE
               , SRC.TRANSLOAD_TRUCK_ETA_ESTIMATED_DATE
               , SRC.TRANSLOAD_TRUCK_ETA_REVISED_DATE
               , SRC.TRANSLOAD_TRUCK_ETA_ACTUAL_DATE
               , SRC.DELIVERED_TO_CONSIGNEE_ESTIMATED_DATE
               , SRC.DELIVERED_TO_CONSIGNEE_REVISED_DATE
               , SRC.DELIVERED_TO_CONSIGNEE_ACTUAL_DATE
               , SRC.RECEIVED_DATE_EST
               , SRC.DC_APPOINTMENT_DATE
               , SRC.TRANSLOAD_TRUCK_ETA_C_DATE
               , SRC.PO_RECEIVED
               , SRC.RECEIPT_STATUS
               , SRC.FND_ETA_DATE
               , SRC.FND_ETA_WEEK_DAY_NO
               , SRC.ETA_ON_HAND_DATE
               , SRC.ETA_ON_HAND_WEEK
               , SRC.ETA_ON_HAND_MONTH
               , SRC.ETA_ON_HAND_DAY
               , SRC.WEEK_ETA
               , SRC.CALL_OUT
               , SRC.MODE
               , SRC.RAIL_STATUS
               , SRC.VENDOR_BOOKING_DATE
               , SRC.STYLE_NAME
               , SRC.SHIPMENT_TYPE
               , SRC.POD_WAREHOUSE_ID
               , SRC.FND_DATE
               , SRC.TRANSPORT_MODE_INTRANSIT
               , SRC.MERLIN_PO_STATUS
               , SRC.TRAFFIC_MODE
               , SRC.QUANTITY_ADDED
               , SRC.CARTON_COUNT
               , SRC.VOLUME
               , SRC.WAREHOUSE_ID
               , SRC.WAREHOUSE_ID_ORIGIN
               , CURRENT_TIMESTAMP()::TIMESTAMP_ltz
               , CURRENT_TIMESTAMP()::TIMESTAMP_ltz
               , SRC.OFFICIAL_FACTORY_NAME
               , SRC.ORIGINAL_END_SHIP_WINDOW_DATE
               , SRC.PO_TYPE
               , SRC.QTY_GHOSTED
               , SRC.BUY_RANK
               , SRC.LOGIC
               , SRC.SOURCE_VENDOR_NAME
               , SRC.SOURCE_VENDOR_ID
               , SRC.GENDER
               , SRC.FND_ETA_DATE_RAW
               , SRC.COO
               , SRC.DOMESTIC_TRAILER_ID
               , SRC.IT_CONTAINER_ID
               , SRC.BL_ID
               , SRC.PO_ID
               , SRC.PO_DTL_ID
               , SRC.SC_CATEGORY
               , SRC.SHIPPING_PORT
               , SRC.WEIGHT
               , SRC.SO
               , SRC.AIR_DSR_POD_DATE
               , SRC.DOMESTIC_ORDER_NUMBER
               , src.unit_volume
               , src.booking_confirmed_date
               , src.carrier_booking_placed_date
               , src.is_cancelled
               , src.merlin_ports
               , src.asn_ports
               , src.received_date
               , src.Vessel_Depart_at_Port_DSR_Actual
               , src.Vessel_Arrive_at_Port_DSR_Actual
               , src.showroom_call_out
               , src.original_showroom
               , src.original_showroom_locked
               , src.Original_Showroom_Call_Out
               , src.size_name
               , src.oocl_vol
               , src.oocl_vol1
               , src.avg_booking_volume
               , src.po_avg_booking_volume
               , src.sku_avg_booking_volume
               , src.jpd_actual_vol
               , src.jpd_avg_vol
               , src.v_avg_unit_vol
               , src.centric_department
               , src.centric_subdepartment
               , src.centric_category
               , src.centric_class
               , src.centric_subclass
               , src.received_qty
               , src.volume_source
               , src.weight_source
               , src.po_line_number
               , src.centric_cbm
               , src.actual_cbm
               );

end;
