CREATE TABLE IF NOT EXISTS reporting_prod.sxf.retail_traffic
(
    store_id NUMBER(38,0),
    store_name VARCHAR(50),
    store_full_name VARCHAR(50),
    date DATE,
    hour TIME(9),
    datetime TIMESTAMP_NTZ,
    dayname VARCHAR(10),
    open_time VARCHAR(35),
    close_time VARCHAR(35),
    during_business_hours BOOLEAN,
    incoming_visitor_traffic NUMBER(38,0),
    exiting_visitor_traffic NUMBER(38,0),
    meta_create_datetime TIMESTAMP_LTZ(3),
    meta_update_datetime TIMESTAMP_LTZ(3),
    meta_row_hash NUMBER(38, 0),
    DURING_BUSINESS_HOURS_EXCLUDING_OPEN_TIME BOOLEAN
);

set low_watermark = (
    select
        nvl(max(meta_update_datetime), '1900-01-01'::timestamp_ltz)
    from
        reporting_prod.sxf.retail_traffic
);

CREATE OR REPLACE TEMPORARY TABLE _retail_store_open_time AS
SELECT split_part(day,'_',0) AS dayname_open
    ,open_time
    ,rl.store_id
FROM lake.ultra_warehouse.retail_location rl
UNPIVOT(open_time FOR day IN (sunday_open,monday_open,tuesday_open,wednesday_open,thursday_open,friday_open,saturday_open))
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = rl.store_id
WHERE ds.store_brand_abbr = 'SX';

CREATE OR REPLACE TEMPORARY TABLE _retail_store_close_time AS
SELECT split_part(day,'_',0) AS dayname_close
    ,close_time
    ,rl.store_id
FROM lake.ultra_warehouse.retail_location rl
UNPIVOT(close_time FOR day IN (sunday_close,monday_close,tuesday_close,wednesday_close,thursday_close,friday_close,saturday_close))
JOIN edw_prod.data_model.dim_store ds ON ds.store_id = rl.store_id
WHERE ds.store_brand_abbr = 'SX';

CREATE OR REPLACE TEMPORARY TABLE _retail_store_open_close_time AS
SELECT o.store_id
    ,o.dayname_open AS dayname_final
    ,o.open_time,c.close_time
FROM _retail_store_open_time o
JOIN _retail_store_close_time c ON o.store_id = c.store_id
    AND LOWER(o.dayname_open) = LOWER(c.dayname_close);

CREATE OR REPLACE TEMPORARY TABLE _retail_traffic_stg AS
SELECT ds.store_id
    ,ds.store_name
    ,store_full_name
    ,date
    ,time AS hour
    ,timestamp_ntz_from_parts(date, time) AS datetime
    ,decode(
        extract('dayofweek_iso', tc.date),
        1, 'Monday',
        2, 'Tuesday',
        3, 'Wednesday',
        4, 'Thursday',
        5, 'Friday',
        6, 'Saturday',
        7, 'Sunday'
    ) AS dayname
    ,oct.open_time
    ,oct.close_time
    ,CASE WHEN hour BETWEEN TRY_TO_TIME(open_time) AND TRY_TO_TIME(close_time) THEN 1 ELSE 0
        END AS during_business_hours,
    CASE WHEN hour > TRY_TO_TIME(open_time) AND hour <= TRY_TO_TIME(close_time) THEN 1 ELSE 0
        END AS during_business_hours_excluding_open_time
    ,SUM(tc.traffic_enters) AS incoming_visitor_traffic
    ,SUM(tc.traffic_exits) AS exiting_visitor_traffic
    ,max(tc.meta_update_datetime) as Meta_update_datetime
    ,max(tc.meta_create_datetime) as Meta_create_datetime
FROM lake.shoppertrak.traffic_counter_sxf tc
JOIN edw_prod.data_model.dim_store ds ON concat('SXF-', ds.store_retail_location_code) = customer_site_id and STORE_BRAND_ABBR = 'SX'
JOIN _retail_store_open_close_time oct ON oct.store_id = ds.store_id
    AND LOWER(oct.dayname_final) = LOWER(
        decode(
            extract('dayofweek_iso', tc.date),
            1, 'Monday',
            2, 'Tuesday',
            3, 'Wednesday',
            4, 'Thursday',
            5, 'Friday',
            6, 'Saturday',
            7, 'Sunday'
        )
    )
WHERE ds.store_brand_abbr = 'SX'
AND tc.meta_update_datetime >= $low_watermark
GROUP BY ds.store_id
    ,ds.store_name
    ,store_full_name
    ,date
    ,hour
    ,datetime
    ,dayname
    ,open_time
    ,close_time
    ,during_business_hours
    ,during_business_hours_excluding_open_time;


MERGE INTO
    reporting_prod.sxf.retail_traffic a
USING (
    SELECT
        *,
        hash(*) as meta_row_hash
    FROM
        _retail_traffic_stg
) b
ON a.store_id = b.store_id
AND equal_null(a.datetime, b.datetime)
WHEN MATCHED AND a.meta_row_hash != b.meta_row_hash THEN UPDATE SET
    a.store_id = b.store_id,
    a.store_name = b.store_name,
    a.store_full_name = b.store_full_name,
    a.date = b.date,
    a.hour = b.hour,
    a.datetime = b.datetime,
    a.dayname = b.dayname,
    a.open_time = b.open_time,
    a.close_time = b.close_time,
    a.during_business_hours = b.during_business_hours,
    a.during_business_hours_excluding_open_time = b.during_business_hours_excluding_open_time,
    a.incoming_visitor_traffic = b.incoming_visitor_traffic,
    a.exiting_visitor_traffic = b.exiting_visitor_traffic,
    a.meta_update_datetime = b.meta_update_datetime,
    a.meta_row_hash = b.meta_row_hash
WHEN NOT MATCHED THEN INSERT (
    store_id,
    store_name,
    store_full_name,
    date,
    hour,
    datetime,
    dayname,
    open_time,
    close_time,
    during_business_hours,
    during_business_hours_excluding_open_time,
    incoming_visitor_traffic,
    exiting_visitor_traffic,
    meta_create_datetime,
    meta_update_datetime,
    meta_row_hash
) VALUES (
    b.store_id,
    b.store_name,
    b.store_full_name,
    b.date,
    b.hour,
    b.datetime,
    b.dayname,
    b.open_time,
    b.close_time,
    b.during_business_hours,
    b.during_business_hours_excluding_open_time,
    b.incoming_visitor_traffic,
    b.exiting_visitor_traffic,
    b.meta_create_datetime,
    b.meta_update_datetime,
    b.meta_row_hash
);
