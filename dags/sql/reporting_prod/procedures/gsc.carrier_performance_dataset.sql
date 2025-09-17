BEGIN TRANSACTION NAME GSC_CARRIER_PERFORMANCE_DATASET;

SET update_date = (SELECT to_date(coalesce(dateadd(month,-6,date_trunc(month,max(order_placed_datetime))),'2019-01-01'))
                   FROM reporting_prod.gsc.carrier_performance_dataset);

CREATE OR REPLACE TEMP TABLE _carrier_performance_dataset AS
WITH _FINAL_INVOICES AS (
    SELECT INVOICE_TRACKING_NUMBER,
           INVOICE_DATE,
           INVOICE_INGESTION_DATE,
           TOTAL_PACKAGE_COST,
           ROW_NUMBER() OVER (PARTITION BY INVOICE_TRACKING_NUMBER ORDER BY INVOICE_DATE ASC, TOTAL_PACKAGE_COST DESC) AS ROW_NUM
    FROM REPORTING_BASE_PROD.GSC.CONSOLIDATED_ECOM_CARRIER_INVOICES
)
SELECT DISTINCT
       o.order_id,
       o.is_activating_c AS is_activating,
       o.is_vip_c AS is_vip,
       o.business_unit,
       o.customer_selected_type,
       o.system_date_placed AS order_placed_datetime,
       CASE WHEN UPPER(S.SERVICE_TYPE) = 'HERMES EUROPE' THEN 'HERM'
            WHEN UPPER(S.SERVICE_TYPE) = 'HERMES RUSH' THEN 'HRMRUSH'
            ELSE S.SCAC END AS scac,
       s.fc,
       s.service_type,
       s.carrier,
       s.tracking_number,
       s.gender,
       s.units,
       s.estimated_shipped_date AS date_shipped,
       IFF(s.carrier_delivered_datetime < s.estimated_shipped_date,NULL,s.carrier_delivered_datetime) AS carrier_delivered_datetime,
        s.ACTUAL_WEIGHT_WMS,
        IFF(S.ACTUAL_WEIGHT_WMS < 1,'Y','N') AS IS_SUB_ONE_POUND,
        COALESCE(SLA.SHIPPING_ZONE,S.ZONE_RATE_SHOPPING) AS ZONE_RATE_SHOPPING,
        s.BASE_RATE_WMS,
        s.TOTAL_SURCHARGES_WMS,
        s.NET_CHARGE_AMOUNT_WMS,
        s.DISCOUNT_WMS,
        s.SHIPMENT_FREIGHT_CHARGE_BEFORE_DISCOUNT_WMS,
        s.FUEL_SURCHARGE_WMS,
        s.RESIDENTIAL_SURCHARGE_WMS,
        s.carrier_region AS REGION,
        upper(s.ship_to_country_code) AS COUNTRY,
        IFF(UPPER(S.SHIP_TO_COUNTRY_CODE) = 'US', UPPER(S.SHIP_TO_STATE_REGION), '*International*') AS STATE,
        upper(s.ship_to_zip) AS ZIP,
        s.is_split_order,
        s.is_out_of_region_order,
        s.order_type,
        o.order_status,
        o.payment_method,
        s.manifest_receipt_datetime,
        COALESCE(s.carrier_ingested_datetime,fi.invoice_ingestion_date)  AS CARRIER_INGESTION_DATETIME,
        s.contains_scrub,
        CASE WHEN O.IS_VIP_C = 'Y' AND UPPER(TRIM(O.BUSINESS_UNIT)) = 'FABLETICS' AND UPPER(TRIM(C.VALUE)) = 'M' THEN 'Mens'
             WHEN O.IS_VIP_C = 'N' AND UPPER(TRIM(O.BUSINESS_UNIT)) = 'FABLETICS' THEN 'Non-VIP'
             ELSE 'Womens' END AS VIP_GENDER,
        fi.total_package_cost,
        IFF(S.SERVICE_TYPE = 'FedEx 2 Day', 2, SLA.TRANSIT_TIME_SLA) AS SLA_TRANSIT_TIME,
        CASE WHEN COALESCE(s.carrier_ingested_datetime,fi.invoice_ingestion_date) = s.carrier_ingested_datetime THEN 'SHIPMENT DATASET'
             WHEN COALESCE(s.carrier_ingested_datetime,fi.invoice_ingestion_date) = fi.invoice_ingestion_date THEN 'INVOICE'
             ELSE NULL END AS CARRIER_INGESTION_SOURCE, -- Need to delete/account for
        s.package_length AS PACKAGE_LENGTH,
        s.package_width AS PACKAGE_WIDTH,
        s.package_height AS PACKAGE_HEIGHT,
        s.package_dim_unit AS PACKAGE_DIM_UNIT,
        s.only_shoes
FROM
    reporting_prod.gsc.shipment_dataset S
    LEFT JOIN reporting_prod.gsc.order_dataset O
        ON S.ORDER_ID = EDW_PROD.STG.UDF_UNCONCAT_BRAND(O.ORDER_ID)
    LEFT JOIN reporting_base_prod.reference.transit_time_sla SLA
        ON S.FC = SLA.AIRPORT_CODE
            AND S.SERVICE_TYPE = SLA.SERVICE_TYPE
            AND LEFT(S.SHIP_TO_ZIP,5) = SLA.ZIPCODE
    LEFT JOIN lake_view.ultra_warehouse.warehouse W
        ON W.AIRPORT_CODE = S.FC
    LEFT JOIN lake_view.ultra_warehouse.fulfillment F
        ON F.FOREIGN_ORDER_ID = S.ORDER_ID
            AND F.WAREHOUSE_ID = W.WAREHOUSE_ID
    LEFT JOIN lake_consolidated_view.ultra_merchant.customer_detail C
        ON EDW_PROD.STG.UDF_UNCONCAT_BRAND(C.CUSTOMER_ID) = F.FOREIGN_CUSTOMER_ID
            AND UPPER(TRIM(C.NAME)) = 'GENDER'
    LEFT JOIN _final_invoices FI
        ON FI.INVOICE_TRACKING_NUMBER = (IFF(S.service_type = 'DHL SmartMail Parcel / Parcel Plus Expedited' AND s.carrier = 'DHL ECS',right(s.tracking_number,22),S.TRACKING_NUMBER))
            AND FI.ROW_NUM = 1
            AND TO_DATE(S.ESTIMATED_SHIPPED_DATE) >= DATEADD(MONTH,-3,FI.INVOICE_DATE)
WHERE
    O.SYSTEM_DATE_PLACED >= $update_date
    and S.ORDER_TYPE in ('Ecom','Borderfree', 'Other')
    and O.ORDER_ID is not null;


CREATE OR REPLACE TEMP TABLE _order_to_shipped AS
SELECT tracking_number,
       order_placed_datetime start_date,
       date_shipped end_date,
       to_date(order_placed_datetime) order_placed_datetime_standard,
       to_date(date_shipped) date_shipped_standard,
       SUM(IFF(dd.day_name_of_week NOT IN ('Saturday', 'Sunday'),1,0)) - iff(dayname(start_date) IN ('Sat', 'Sun'),0,1) AS business_days,
       COUNT(distinct dd.full_date) - 1 calendar_days
FROM _carrier_performance_dataset cpd
JOIN edw_prod.data_model.dim_date dd
    ON dd.full_date BETWEEN to_date(order_placed_datetime) AND to_date(date_shipped)
GROUP BY tracking_number,
       order_placed_datetime,
       date_shipped,
       to_date(order_placed_datetime),
       to_date(date_shipped);

CREATE OR REPLACE TEMP TABLE _shipped_to_ingested AS
SELECT tracking_number,
       date_shipped start_date,
       carrier_ingestion_datetime end_date,
       to_date(date_shipped) date_shipped_standard,
       to_date(carrier_ingestion_datetime) carrier_ingestion_datetime_standard,
       SUM(IFF(dd.day_name_of_week NOT IN ('Saturday', 'Sunday'),1,0)) - iff(dayname(start_date) IN ('Sat', 'Sun'),0,1) AS business_days,
       COUNT(distinct dd.full_date) - 1 calendar_days
FROM _carrier_performance_dataset cpd
JOIN edw_prod.data_model.dim_date dd
    ON dd.full_date BETWEEN to_date(date_shipped) AND to_date(carrier_ingestion_datetime)
GROUP BY tracking_number,
       date_shipped,
       carrier_ingestion_datetime,
       to_date(date_shipped),
       to_date(carrier_ingestion_datetime);

CREATE OR REPLACE TEMP TABLE _ingested_to_delivered AS
SELECT tracking_number,
       carrier_ingestion_datetime start_date,
       carrier_delivered_datetime end_date,
       to_date(carrier_ingestion_datetime) carrier_ingestion_datetime_standard,
       to_date(carrier_delivered_datetime) carrier_delivered_datetime_standard,
       SUM(IFF(dd.day_name_of_week NOT IN ('Saturday', 'Sunday'),1,0)) - iff(dayname(start_date) IN ('Sat', 'Sun'),0,1) AS business_days,
       COUNT(distinct dd.full_date) - 1 calendar_days
FROM _carrier_performance_dataset cpd
JOIN edw_prod.data_model.dim_date dd
    ON dd.full_date BETWEEN to_date(carrier_ingestion_datetime) AND to_date(carrier_delivered_datetime)
GROUP BY tracking_number,
       carrier_ingestion_datetime,
       carrier_delivered_datetime,
       to_date(carrier_ingestion_datetime),
       to_date(carrier_delivered_datetime);

CREATE OR REPLACE TEMP TABLE _ordered_to_delivered AS
SELECT tracking_number,
       order_placed_datetime start_date,
       carrier_delivered_datetime end_date,
       to_date(order_placed_datetime) order_placed_datetime_standard,
       to_date(carrier_delivered_datetime) carrier_delivered_datetime_standard,
       SUM(IFF(dd.day_name_of_week NOT IN ('Saturday', 'Sunday'),1,0)) - iff(dayname(start_date) IN ('Sat', 'Sun'),0,1) AS business_days,
       COUNT(distinct dd.full_date) - 1 calendar_days
FROM _carrier_performance_dataset cpd
JOIN edw_prod.data_model.dim_date dd
    ON dd.full_date BETWEEN to_date(order_placed_datetime) AND to_date(carrier_delivered_datetime)
GROUP BY tracking_number,
       order_placed_datetime,
       carrier_delivered_datetime,
       to_date(order_placed_datetime),
       to_date(carrier_delivered_datetime);

CREATE OR REPLACE TEMP TABLE _shipped_to_delivered AS
SELECT tracking_number,
       date_shipped start_date,
       carrier_delivered_datetime end_date,
       to_date(date_shipped) order_placed_datetime_standard,
       to_date(carrier_delivered_datetime) carrier_delivered_datetime_standard,
       SUM(IFF(dd.day_name_of_week NOT IN ('Saturday', 'Sunday'),1,0)) - iff(dayname(start_date) IN ('Sat', 'Sun'),0,1) AS business_days,
       COUNT(distinct dd.full_date) - 1 calendar_days
FROM _carrier_performance_dataset cpd
JOIN edw_prod.data_model.dim_date dd
    ON dd.full_date BETWEEN to_date(date_shipped) AND to_date(carrier_delivered_datetime)
GROUP BY tracking_number,
       date_shipped,
       carrier_delivered_datetime,
       to_date(order_placed_datetime),
       to_date(carrier_delivered_datetime);

-- TRUNCATE REPORTING_PROD.GSC.CARRIER_PERFORMANCE_DATASET;
DELETE FROM REPORTING_PROD.GSC.CARRIER_PERFORMANCE_DATASET
WHERE to_date(order_placed_datetime) >= $update_date;

INSERT INTO REPORTING_PROD.GSC.CARRIER_PERFORMANCE_DATASET (
    ORDER_ID,
    IS_ACTIVATING,
    IS_VIP,
    BUSINESS_UNIT,
    CUSTOMER_SELECTED_TYPE,
    ORDER_PLACED_DATETIME,
    SCAC,
    FC,
    SERVICE_TYPE,
    CARRIER,
    TRACKING_NUMBER,
    GENDER,
    UNITS,
    DATE_SHIPPED,
    CARRIER_DELIVERED_DATETIME,
    ORDERED_TO_SHIPPED_CALENDAR_DAYS,
    ORDERED_TO_SHIPPED_BUSINESS_DAYS,
    SHIPPED_TO_DELIVERED_CALENDAR_DAYS,
    SHIPPED_TO_DELIVERED_BUSINESS_DAYS,
    ORDERED_TO_DELIVERED_CALENDAR_DAYS,
    ORDERED_TO_DELIVERED_BUSINESS_DAYS,
    ACTUAL_WEIGHT_WMS,
    IS_SUB_ONE_POUND,
    ZONE_RATE_SHOPPING,
    BASE_RATE_WMS,
    TOTAL_SURCHARGES_WMS,
    NET_CHARGE_AMOUNT_WMS,
    DISCOUNT_WMS,
    SHIPMENT_FREIGHT_CHARGE_BEFORE_DISCOUNT_WMS,
    FUEL_SURCHARGE_WMS,
    RESIDENTIAL_SURCHARGE_WMS,
    REGION,
    COUNTRY,
    STATE,
    ZIP,
    IS_SPLIT_ORDER,
    IS_OUT_OF_REGION_ORDER,
    ORDER_TYPE,
	ORDER_STATUS,
	PAYMENT_METHOD,
    MANIFEST_RECEIPT_DATETIME,
    SHIPPED_TO_MANIFEST_CALENDAR_DAYS,
    SHIPPED_TO_MANIFEST_BUSINESS_DAYS,
    MANIFEST_TO_DELIVERED_CALENDAR_DAYS,
    MANIFEST_TO_DELIVERED_BUSINESS_DAYS,
    CARRIER_INGESTION_DATETIME,
    SHIPPED_TO_INGESTED_CALENDAR_DAYS,
    SHIPPED_TO_INGESTED_BUSINESS_DAYS,
    INGESTED_TO_DELIVERED_CALENDAR_DAYS,
    INGESTED_TO_DELIVERED_BUSINESS_DAYS,
    CONTAINS_SCRUB,
    VIP_GENDER,
    TOTAL_PACKAGE_COST,
    SLA_TRANSIT_TIME,
    CARRIER_INGESTION_SOURCE,
    PACKAGE_LENGTH,
    PACKAGE_WIDTH,
    PACKAGE_HEIGHT,
    PACKAGE_DIM_UNIT,
    ONLY_SHOES
)
SELECT c.order_id,
       c.is_activating,
       c.is_vip,
       c.business_unit,
       c.customer_selected_type,
       c.order_placed_datetime,
       c.scac,
       c.fc,
       c.service_type,
       c.carrier,
       c.tracking_number,
       c.gender,
       c.units,
       c.date_shipped,
       c.carrier_delivered_datetime,
       COALESCE(iff(os.calendar_days<=0,0,os.calendar_days),0) AS ORDERED_TO_SHIPPED_CALENDAR_DAYS,
       COALESCE(iff(os.business_days<=0,0,os.business_days),0) AS ORDERED_TO_SHIPPED_BUSINESS_DAYS,
       COALESCE(iff(sd.calendar_days<=0,0,sd.calendar_days),0) AS SHIPPED_TO_DELIVERED_CALENDAR_DAYS,
       COALESCE(iff(sd.business_days<=0,0,sd.business_days),0) AS SHIPPED_TO_DELIVERED_BUSINESS_DAYS,
       COALESCE(iff(od.calendar_days<=0,0,od.calendar_days),0) AS ORDERED_TO_DELIVERED_CALENDAR_DAYS,
       COALESCE(iff(od.business_days<=0,0,od.business_days),0) AS ORDERED_TO_DELIVERED_BUSINESS_DAYS,
       c.actual_weight_wms,
       c.is_sub_one_pound,
       c.zone_rate_shopping,
       c.base_rate_wms,
       c.total_surcharges_wms,
       c.net_charge_amount_wms,
       c.discount_wms,
       c.shipment_freight_charge_before_discount_wms,
       c.fuel_surcharge_wms,
       c.residential_surcharge_wms,
       c.region,
       c.country,
       c.state,
       c.zip,
       c.is_split_order,
       c.is_out_of_region_order,
       c.order_type,
       c.order_status,
       c.payment_method,
       c.manifest_receipt_datetime,
       0 AS SHIPPED_TO_MANIFEST_CALENDAR_DAYS,
       0 AS SHIPPED_TO_MANIFEST_BUSINESS_DAYS,
       0 AS MANIFEST_TO_DELIVERED_CALENDAR_DAYS,
       0 AS MANIFEST_TO_DELIVERED_BUSINESS_DAYS,
       c.carrier_ingestion_datetime,
       COALESCE(iff(si.calendar_days<=0,0,si.calendar_days),0) AS SHIPPED_TO_INGESTED_CALENDAR_DAYS,
       COALESCE(iff(si.business_days<=0,0,si.business_days),0) AS SHIPPED_TO_INGESTED_BUSINESS_DAYS,
       COALESCE(iff(id.calendar_days<=0,0,id.calendar_days),0) AS INGESTED_TO_DELIVERED_CALENDAR_DAYS,
       COALESCE(iff(id.business_days<=0,0,id.business_days),0) AS INGESTED_TO_DELIVERED_BUSINESS_DAYS,
       c.contains_scrub,
       c.vip_gender,
       c.total_package_cost,
       c.sla_transit_time,
       c.carrier_ingestion_source,
       c.package_length,
       c.package_width,
       c.package_height,
       c.package_dim_unit,
       c.only_shoes
FROM _carrier_performance_dataset c
LEFT JOIN _order_to_shipped os
    ON c.tracking_number = os.tracking_number
    AND c.order_placed_datetime = os.start_date
    AND c.date_shipped = os.end_date
LEFT JOIN _shipped_to_ingested si
    ON c.tracking_number = si.tracking_number
    AND c.date_shipped = si.start_date
    AND c.carrier_ingestion_datetime = si.end_date
LEFT JOIN _ingested_to_delivered id
    ON c.tracking_number = id.tracking_number
    AND c.carrier_ingestion_datetime = id.start_date
    AND c.carrier_delivered_datetime = id.end_date
LEFT JOIN _ordered_to_delivered od
    ON c.tracking_number = od.tracking_number
    AND c.order_placed_datetime = od.start_date
    AND c.carrier_delivered_datetime = od.end_date
LEFT JOIN _shipped_to_delivered sd
    ON c.tracking_number = sd.tracking_number
    AND c.date_shipped = sd.start_date
    AND c.carrier_delivered_datetime = sd.end_date
;

COMMIT;
