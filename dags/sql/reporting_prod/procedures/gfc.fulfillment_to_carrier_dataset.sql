BEGIN TRANSACTION NAME gfc_fulfillment_to_carrier;

-- Official Table: reporting_prod.gfc.fulfillment_to_carrier_dataset
SET last_order_placed = (SELECT COALESCE(max(datetime_placed),'2023-12-06')
                            FROM gfc.fulfillment_to_carrier_dataset);

-- Incorporate Invoices Datetime Ingestion // eventually we will have proper datafeed
CREATE OR REPLACE TEMP TABLE _invoice_ingestion AS
SELECT DISTINCT invoice_tracking_number tracking_number,
                source carrier,
                invoice_service_type,
                min(invoice_ingestion_date) invoice_ingestion_datetime
FROM reporting_base_prod.gsc.consolidated_ecom_carrier_invoices
WHERE invoice_date >= '2023-12-01'
GROUP BY 1,2,3;

CREATE OR REPLACE TEMP TABLE _base AS
SELECT DISTINCT f.fulfillment_id
    , f.foreign_order_id
    , ib.tracking_number
    -- , l.label AS Location
    , p.label AS Pallet_ID
    , pa.package_code
    , f.datetime_placed
    , f.datetime_process
    , ib.date_shipped
    , f.priority
    , MAX(CASE WHEN l.label = 'INTRN-482-0-0' THEN opl.datetime_added ELSE NULL END) AS loaded_on_truck --Stage_Date
    , MAX(CASE WHEN l.label = 'TL-SD-0-0' THEN opl.datetime_added ELSE NULL END) AS unloaded_at_crossdock --TL_Date_
    , MAX(CASE WHEN l.label like 'STG%' THEN opl.datetime_added ELSE NULL END) AS gglobal_staging -- CD_Stage In G-Global but loading onto truck soon/or on truck
    , MAX(CASE WHEN l.label like 'SHP%' THEN opl.datetime_added ELSE NULL END) AS loaded_onto_carrier_pickup --CD_Ship_ official Techstyle resp ends
    , to_timestamp_ntz(n.carrier_ingestion_datetime) carrier_ingestion_narvar
    , ii.invoice_ingestion_datetime carrier_ingestion_invoices
    , f.line_count
    , c.label AS Carrier
    , cs.label AS Service
    , co.label AS Brand
    , a.zip
    , t.shipping_zone AS zone
FROM LAKE_VIEW.ULTRA_WAREHOUSE.FULFILLMENT F
INNER JOIN LAKE_VIEW.ULTRA_WAREHOUSE.INVOICE AS IV
    ON IV.FOREIGN_ORDER_ID = F.FOREIGN_ORDER_ID AND IV.WAREHOUSE_ID = F.WAREHOUSE_ID
INNER JOIN LAKE_VIEW.ULTRA_WAREHOUSE.WAREHOUSE AS W
    ON W.WAREHOUSE_ID = F.WAREHOUSE_ID
INNER JOIN LAKE_VIEW.ULTRA_WAREHOUSE.COMPANY AS CO
    ON CO.COMPANY_ID = F.COMPANY_ID
INNER JOIN LAKE_VIEW.ULTRA_WAREHOUSE.ADDRESS AS A
    ON A.ADDRESS_ID = IV.SHIPPING_ADDRESS_ID
LEFT OUTER JOIN LAKE_VIEW.ULTRA_WAREHOUSE.INVOICE_BOX AS IB
    ON IB.INVOICE_ID = IV.INVOICE_ID
LEFT OUTER JOIN LAKE_VIEW.ULTRA_WAREHOUSE.PACKAGE AS PA
    ON PA.PACKAGE_ID = IB.PACKAGE_ID
LEFT OUTER JOIN LAKE_VIEW.ULTRA_WAREHOUSE.COMPANY_CARRIER_SERVICE AS CCS
    ON CCS.COMPANY_CARRIER_SERVICE_ID = IB.COMPANY_CARRIER_SERVICE_ID
LEFT OUTER JOIN LAKE_VIEW.ULTRA_WAREHOUSE.CARRIER_SERVICE AS CS
    ON CCS.CARRIER_SERVICE_ID = CS.CARRIER_SERVICE_ID
LEFT OUTER JOIN LAKE_VIEW.ULTRA_WAREHOUSE.CARRIER AS C
    ON C.CARRIER_ID = CS.CARRIER_ID
LEFT OUTER JOIN LAKE_VIEW.ULTRA_WAREHOUSE.OUTBOUND_PALLET_DETAIL AS OPD
    ON OPD.PACKAGE_ID = IB.PACKAGE_ID
LEFT OUTER JOIN LAKE_VIEW.ULTRA_WAREHOUSE.OUTBOUND_PALLET_LOG AS OPL
    ON OPL.OUTBOUND_PALLET_ID = OPD.OUTBOUND_PALLET_ID AND OPL.LOCATION_ID IS NOT NULL
LEFT OUTER JOIN LAKE_VIEW.ULTRA_WAREHOUSE.PALLET AS P
    ON P.PALLET_ID = OPD.OUTBOUND_PALLET_ID
LEFT OUTER JOIN LAKE_VIEW.ULTRA_WAREHOUSE.LOCATION AS L
    ON L.LOCATION_ID = OPL.LOCATION_ID
LEFT OUTER JOIN reporting_base_prod.reference.transit_time_sla T
    ON LEFT(a.zip,5) = t.zipcode AND T.airport_code = 'TIJ2'
LEFT JOIN reporting_base_prod.gsc.narvar_consolidated_milestones N
    ON n.tracking_number = ib.tracking_number AND n.order_id = f.foreign_order_id
LEFT JOIN _invoice_ingestion II
    ON ib.tracking_number = ii.tracking_number
WHERE w.airport_code in ('TIJ2','TIJ1')
    AND f.source = 'WMS'
    AND to_date(f.datetime_placed) >= to_date(dateadd(day,-5,$last_order_placed)) -- change to latest datetime already in the table
    AND f.status_code_id = 151
    AND ib.date_shipped is not NULL
GROUP BY f.fulfillment_id
    , f.foreign_order_id
    , ib.tracking_number
    -- , l.label
    , f.datetime_placed
    , ib.date_shipped
    , f.priority
    , n.carrier_ingestion_datetime
    , ii.invoice_ingestion_datetime
    , f.line_count
    , c.label
    , cs.label
    , co.label
    , p.label
    , pa.package_code
    , f.datetime_process
    , a.zip
    , t.shipping_zone;

-- packages sit at TIJ for 8 hours, take 1 day to travel, and Sundays don't pickup
-- 3 days felt sufficient for all 3 of the days to get fulfilled to carrier
-- increased number of days to account for week of rest or anomalies.
DELETE FROM gfc.fulfillment_to_carrier_dataset
WHERE to_date(datetime_placed) >= to_date(dateadd(day,-5,$last_order_placed));

INSERT INTO gfc.fulfillment_to_carrier_dataset (
    FULFILLMENT_ID,
    FOREIGN_ORDER_ID,
    PALLET_ID,
    PACKAGE_CODE,
    DATETIME_PLACED,
    DATETIME_PROCESS,
    DATE_SHIPPED,
    PRIORITY,
    LOADED_ON_TRUCK,
    UNLOADED_AT_CROSSDOCK,
    GGLOBAL_STAGING,
    CARRIER_TIME_OF_POSSESSION,
    LINE_COUNT,
    CARRIER,
    SERVICE,
    BRAND,
    ZIPCODE,
    ZONE,
    CARRIER_INGESTION_IDENTIFIER -- renamed from possession_indentier
)
SELECT fulfillment_id,
       foreign_order_id,
       pallet_id,
       package_code,
       datetime_placed,
       datetime_process,
       date_shipped,
       priority,
       loaded_on_truck,
       unloaded_at_crossdock,
       gglobal_staging,
       CASE
            WHEN carrier_ingestion_narvar > gglobal_staging
                 AND (carrier_ingestion_narvar is not null OR loaded_onto_carrier_pickup is null)
             THEN carrier_ingestion_narvar
            WHEN carrier_ingestion_invoices > gglobal_staging
                 AND (carrier_ingestion_invoices is not null OR loaded_onto_carrier_pickup is null)
             THEN carrier_ingestion_invoices
            ELSE loaded_onto_carrier_pickup
       END AS carrier_time_of_possession, --carrier pickup / possession
       line_count,
       carrier,
       service,
       brand,
       zip AS ZIPCODE,
       zone,
       CASE WHEN carrier_time_of_possession = carrier_ingestion_narvar THEN 'NARVAR'
            WHEN carrier_time_of_possession = loaded_onto_carrier_pickup THEN 'SHP'
            WHEN carrier_time_of_possession = carrier_ingestion_invoices THEN 'INVOICES'
            ELSE NULL END carrier_ingestion_identifier
FROM _base B;

CREATE OR REPLACE TEMP TABLE _agg_data AS
WITH data AS (
    SELECT package_code,
           line_count,
           pallet_id,
           iff(priority=1,'Rush','Standard') as priority,
           to_date(datetime_placed) date_placed,
           to_date(date_shipped) date_ship,
           to_date(loaded_on_truck) date_loaded_on_truck,
           to_date(unloaded_at_crossdock) date_unloaded_at_crossdock,
           to_date(gglobal_staging) date_at_gglobal,
           to_date(carrier_time_of_possession) date_at_carrier_possession,
           brand,
           carrier,
           service,
           zone,
           DAYOFWEEK(datetime_placed)+1 placed_DOW,
           DAYOFWEEK(date_shipped)+1 shipped_DOW,
           DAYOFWEEK(loaded_on_truck)+1 loaded_on_truck_DOW,
           DAYOFWEEK(unloaded_at_crossdock)+1 unload_at_crossdock_DOW,
           DAYOFWEEK(gglobal_staging)+1 gglobal_DOW,
           DAYOFWEEK(carrier_time_of_possession)+1 carrier_pickup_DOW,
           datediff(HOUR,datetime_placed,date_shipped) AS Placed_to_shipped,
           datediff(HOUR,date_shipped,loaded_on_truck) AS Shipped_to_trailer,
           datediff(HOUR,loaded_on_truck,gglobal_staging) AS Trailer_to_crossdock,
           datediff(HOUR,gglobal_staging,carrier_time_of_possession) AS Stage_to_pickup
    FROM reporting_prod.gfc.fulfillment_to_carrier_dataset
)
SELECT date_placed,
       placed_dow,
       date_ship,
       shipped_dow,
       loaded_on_truck_dow,
       gglobal_dow,
       carrier_pickup_dow,
       priority,
       count(package_code) packages,
       sum(line_count) units,
       sum(placed_to_shipped) as placed_to_shipped,
       sum(shipped_to_trailer) as shipped_to_trailer,
       sum(trailer_to_crossdock) as trailer_to_crossdock,
       sum(stage_to_pickup) as staged_to_pickup,
       brand,
       carrier,
       service,
       zone
FROM data
GROUP BY date_placed,
         placed_dow,
         date_ship,
         shipped_dow,
         loaded_on_truck_dow,
         gglobal_dow,
         carrier_pickup_dow,
         priority,
         brand,
         carrier,
         service,
         zone;

TRUNCATE TABLE reporting_base_prod.gfc.fulfillment_to_carrier_agg;

INSERT INTO reporting_base_prod.gfc.fulfillment_to_carrier_agg (
    DATE_PLACED,
    DATE_SHIP,
    DOW,
    ACTIVITY,
    PRIORITY,
    PACKAGES,
    UNITS,
    TOTAL_HOURS,
    BRAND,
    CARRIER,
    SERVICE,
    ZONE
)
SELECT date_placed,
       date_ship,
       placed_dow as DOW,
       'Placed to Shipped' AS activity,
       priority,
       packages,
       units,
       placed_to_shipped as total_hours,
       brand,
       carrier,
       service,
       zone
FROM _agg_data
UNION
SELECT date_placed,
       date_ship,
       shipped_dow as DOW,
       'Shipped to Trailer' AS activity,
       priority,
       packages,
       units,
       shipped_to_trailer as total_hours,
       brand,
       carrier,
       service,
       zone
FROM _agg_data
UNION
SELECT date_placed,
       date_ship,
       loaded_on_truck_dow as DOW,
       'Trailer to Crossdock' AS activity,
       priority,
       packages,
       units,
       trailer_to_crossdock as total_hours,
       brand,
       carrier,
       service,
       zone
FROM _agg_data
UNION
SELECT date_placed,
       date_ship,
       gglobal_dow as DOW,
       'Staged to Pick-up' AS activity,
       priority,
       packages,
       units,
       staged_to_pickup as total_hours,
       brand,
       carrier,
       service,
       zone
FROM _agg_data
;

COMMIT;
