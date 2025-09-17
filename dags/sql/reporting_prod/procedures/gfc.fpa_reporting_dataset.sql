set target_date = (select max(date_object) from reporting_prod.gfc.fpa_reporting_dataset
where  data_type not in ('Case On Hand', 'Warehouse Cap','Static On Hand','Inventory Units'));
set target_date_end = (select to_date(current_timestamp()));

CREATE OR REPLACE TEMPORARY TABLE _raw_data (
    data_type             VARCHAR(100),
    warehouse_id          INT,
    fc                    VARCHAR(20),
    city                  VARCHAR(100),
    country               VARCHAR(100),
    brand                 VARCHAR(100),
    brands                VARCHAR(100),
    date_object           DATE,
    order_volume_forecast INT,
    unit_volume_forecast  INT,
    date_volume_forecast  DATETIME,
    order_count           INT,
    unit_count            INT,
    units_cancelled       INT
);

INSERT INTO _raw_data (
    data_type,
    warehouse_id,
    fc,
    city,
    country,
    brand,
    brands,
    date_object,
    order_volume_forecast,
    unit_volume_forecast,
    date_volume_forecast,
    order_count,
    unit_count,
    units_cancelled
)
SELECT
    'Sales' AS data_type,
    a.warehouse_id,
    a.fc,
    a.city,
    a.country,
    a.brand,
    CASE
        WHEN a.brand = 'Savage X' OR a.brand = 'LINGERIE'
        THEN 'Savage'
        WHEN a.brand = 'Fabletics' AND a.order_source = 'Retail'
        THEN 'Fabletics Retail'
        WHEN a.brand = 'Fabletics' AND a.customer_gender = 'Men'
        THEN 'Fabletics Men'
        ELSE a.brand
    END AS brands,
    a.datetime_placed AS date_object,
    0,
    0,
    '1900-01-01',
    COUNT(1) AS order_count,
    SUM(a.line_count) AS unit_count,
    SUM(a.units_cancelled) AS units_cancelled
FROM (
    SELECT
        f.warehouse_id,
        w.airport_code AS fc,
        a.city,
        a.country_code AS country,
        f.fulfillment_id,
        c.label AS brand,
        CASE
            WHEN f.warehouse_id IN (107, 154)
            THEN TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', 'America/New_York', f.datetime_placed))
            WHEN f.warehouse_id IN (221)
            THEN TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', 'Europe/Amsterdam', f.datetime_placed))
            WHEN f.warehouse_id IN (366)
            THEN TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', 'Europe/London', f.datetime_placed))
            ELSE TO_DATE(f.datetime_placed)
        END AS datetime_placed,
        f.line_count,
        CASE
            WHEN f.source IN ('OMS-RETAIL', 'OUTBOUND-RETAIL')
            THEN 'Retail'
            WHEN f.source = 'OMS-SAMPLE'
            THEN 'Sample'
            WHEN f.source = 'WMS'
            THEN 'Outbound'
            ELSE 'Unknown'
            END AS order_source,
        IFF(UPPER(cd.value) = 'M', 'Men', 'Women') AS customer_gender,
        fi.units_cancelled
    FROM lake_view.ultra_warehouse.fulfillment f
    LEFT JOIN (
        SELECT
            fi.fulfillment_id,
            SUM(fi.quantity_cancelled) AS units_cancelled
        FROM lake_view.ultra_warehouse.fulfillment_item AS fi
        GROUP BY fi.fulfillment_id
        ) AS fi
        ON f.fulfillment_id = fi.fulfillment_id
    LEFT JOIN lake_view.ultra_warehouse.warehouse AS w
        ON f.warehouse_id = w.warehouse_id
    LEFT JOIN lake_view.ultra_warehouse.company AS c
        ON f.company_id = c.company_id
    LEFT JOIN lake_view.ultra_warehouse.address AS a
        ON w.address_id = a.address_id
    LEFT JOIN lake_consolidated_view.ultra_merchant.customer_detail AS cd
        ON f.foreign_customer_id = edw_prod.stg.udf_unconcat_brand(cd.customer_id)
        AND UPPER(cd.name) = 'GENDER'
    WHERE f.datetime_placed BETWEEN $target_date AND $target_date_end
        AND f.source NOT IN ('oms-sample')
    ) AS a
GROUP BY
    a.warehouse_id,
    a.fc,
    a.city,
    a.country,
    a.brand,
    CASE
        WHEN brand = 'Savage X' OR brand = 'LINGERIE'
        THEN 'Savage'
        WHEN brand = 'Fabletics' AND order_source = 'Retail'
        THEN 'Fabletics Retail'
        WHEN brand = 'Fabletics' AND customer_gender = 'Men'
        THEN 'Fabletics Men'
        ELSE brand
    END,
    a.datetime_placed
;


/**************************
aggregate shipped orders
***************************/
INSERT INTO _raw_data (
    data_type,
    warehouse_id,
    fc,
    city,
    country,
    brand,
    brands,
    date_object,
    order_volume_forecast,
    unit_volume_forecast,
    date_volume_forecast,
    order_count,
    unit_count,
    units_cancelled
)
SELECT
    'Shipped' AS data_type,
    p.warehouse_id,
    p.fc,
    p.city,
    p.country,
    p.brand,
    CASE
        WHEN brand = 'Savage X' OR brand = 'LINGERIE'
        THEN 'Savage'
        WHEN brand = 'Fabletics' AND p.order_source = 'Retail'
        THEN 'Fabletics Retail'
        WHEN brand = 'Fabletics' AND p.customer_gender = 'Men'
        THEN 'Fabletics Men'
        ELSE brand
    END AS brands,
    date_shipped AS date_object,
    NVL(f.order_volume_forecast, 0) AS order_volume_forecast,
    NVL(f.unit_volume_forecast, 0) AS unit_volume_forecast,
    f.date_volume_forecast,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(unit_count) AS unit_count,
    SUM(p.units_cancelled) AS units_cancelled
FROM (
    SELECT
        warehouse_id,
        fc,
        city,
        country,
        brand,
        date_shipped,
        order_source,
        order_id,
        COUNT(1) AS unit_count,
        customer_gender,
        units_cancelled
    FROM (
        SELECT
            f.warehouse_id,
            w.airport_code AS fc,
            a.city,
            a.country_code AS country,
            c.label AS brand,
            CASE
                WHEN f.warehouse_id IN (107, 154)
                THEN TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', 'America/New_York', ib.date_shipped))
                WHEN f.warehouse_id IN (221)
                THEN TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', 'Europe/Amsterdam', ib.date_shipped))
                WHEN f.warehouse_id IN (366)
                THEN TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', 'Europe/London', ib.date_shipped))
                ELSE TO_DATE(ib.date_shipped)
            END AS date_shipped,
            CASE
                WHEN f.source IN ('OMS-RETAIL', 'OUTBOUND-RETAIL')
                THEN 'Retail'
                WHEN f.source = 'OMS-SAMPLE'
                THEN 'Sample'
                WHEN f.source = 'WMS'
                THEN 'Outbound'
                ELSE 'Unknown'
            END AS order_source,
            f.foreign_order_id AS order_id,
            IFF(UPPER(cd.value) = 'M', 'Men', 'Women') AS customer_gender,
            units_cancelled
        FROM lake_view.ultra_warehouse.fulfillment AS f
        JOIN (
            SELECT
                fi.fulfillment_id,
                SUM(fi.quantity_cancelled) AS units_cancelled
            FROM lake_view.ultra_warehouse.fulfillment_item fi
            GROUP BY fi.fulfillment_id
        ) AS fi
            ON f.fulfillment_id = fi.fulfillment_id
        JOIN lake_view.ultra_warehouse.invoice i
            ON f.foreign_order_id = i.foreign_order_id
            AND f.warehouse_id = i.warehouse_id
        JOIN lake_view.ultra_warehouse.invoice_box ib
            ON i.invoice_id = ib.invoice_id
        JOIN lake_view.ultra_warehouse.invoice_box_item ibi
            ON ib.invoice_box_id = ibi.invoice_box_id
        JOIN lake_view.ultra_warehouse.warehouse w
            ON f.warehouse_id = w.warehouse_id
        JOIN lake_view.ultra_warehouse.company c
            ON f.company_id = c.company_id
        JOIN lake_view.ultra_warehouse.address a
            ON w.address_id = a.address_id
        LEFT JOIN lake_consolidated_view.ultra_merchant.customer_detail cd
            ON i.foreign_customer_id = edw_prod.stg.udf_unconcat_brand(cd.customer_id) --cd.meta_original_customer_id
            AND UPPER(cd.name) = 'GENDER'
        WHERE 1 = 1
            AND ib.date_shipped BETWEEN $target_date AND $target_date_end
            AND f.line_count > 0
            AND f.status_code_id != 144
            AND ibi.lpn_id IS NOT NULL
            AND f.source NOT IN ('oms-sample')
            ) a
        GROUP BY
            warehouse_id,
            fc,
            city,
            country,
            brand,
            date_shipped,
            order_source,
            order_id,
            customer_gender,
            units_cancelled
    ) p
    LEFT JOIN reporting_base_prod.gfc.sales_forecast_by_fc_bu AS f
        ON p.warehouse_id = f.warehouse_id
        AND CASE
                WHEN p.brand = 'Savage X' OR brand = 'LINGERIE'
                THEN 'Savage'
                WHEN p.brand = 'Fabletics' AND p.order_source = 'Retail'
                THEN 'Fabletics Retail'
                WHEN p.brand = 'Fabletics' AND p.customer_gender = 'Men'
                THEN 'Fabletics Men'
                ELSE p.brand
                END = f.bu
        AND p.date_shipped = f.date_volume_forecast
GROUP BY
    p.warehouse_id,
    p.fc,
    p.city,
    p.country,
    p.brand,
    CASE
        WHEN brand = 'Savage X' OR brand = 'LINGERIE'
        THEN 'Savage'
        WHEN brand = 'Fabletics' AND p.order_source = 'Retail'
        THEN 'Fabletics Retail'
        WHEN brand = 'Fabletics' AND customer_gender = 'Men'
        THEN 'Fabletics Men'
        ELSE brand
    END,
    p.date_shipped,
    NVL(f.order_volume_forecast, 0),
    NVL(f.unit_volume_forecast, 0),
    f.date_volume_forecast;

/*****************
received
******************/
CREATE OR REPLACE TEMPORARY TABLE _rec AS
SELECT
    'Received'       AS data_type,
    r.warehouse_id,
    c.label          AS brand,
    CASE
        WHEN c.label = 'Savage X' OR c.label = 'LINGERIE'
        THEN 'Savage'
        WHEN c.label = 'Fabletics' AND psd.is_retail = 1
        THEN 'Fabletics Retail'
        WHEN c.label = 'Fabletics' AND psd.gender = 'Men'
        THEN 'Fabletics Men'
        ELSE c.label
    END AS brands,
    CASE
        WHEN r.warehouse_id IN (107, 154)
        THEN TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', 'America/New_York', r.datetime_added))
        WHEN r.warehouse_id IN (221)
        THEN TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', 'Europe/Amsterdam', r.datetime_added))
        WHEN r.warehouse_id IN (366)
        THEN TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', 'Europe/London', r.datetime_added))
        ELSE TO_DATE(r.datetime_added)
    END AS date_object,
    0 AS order_volume_forecast,
    0 AS unit_volume_forecast,
    '1900-01-01' AS date_volume_forecast,
    0 AS order_count,
    SUM(ri.quantity) AS unit_count,
    0 AS units_cancelled,
    psd.warehouse_id AS po_warehouse_id
FROM lake_view.ultra_warehouse.receipt AS r
JOIN lake_view.ultra_warehouse.receipt_item AS ri
    ON r.receipt_id = ri.receipt_id
JOIN lake_view.ultra_warehouse.item AS i
    ON ri.item_id = i.item_id
JOIN lake_view.ultra_warehouse.company AS c
    ON i.company_id = c.company_id
join (select distinct po_number, is_retail, gender, warehouse_id from reporting_base_prod.gsc.po_skus_data) psd
    on upper(r.po_number) = upper(psd.po_number)
WHERE r.datetime_received BETWEEN $target_date AND $target_date_end
GROUP BY
    r.warehouse_id,
    c.label,
    CASE
        WHEN c.label = 'Savage X' OR c.label = 'LINGERIE'
        THEN 'Savage'
        WHEN c.label = 'Fabletics' AND psd.is_retail = 1
        THEN 'Fabletics Retail'
        WHEN c.label = 'Fabletics' AND psd.gender = 'Men'
        THEN 'Fabletics Men'
        ELSE c.label
    END,
    CASE
        WHEN r.warehouse_id IN (107, 154)
        THEN TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', 'America/New_York', r.datetime_added))
        WHEN r.warehouse_id IN (221)
        THEN TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', 'Europe/Amsterdam', r.datetime_added))
        WHEN r.warehouse_id IN (366)
        THEN TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', 'Europe/London', r.datetime_added))
        ELSE TO_DATE(r.datetime_added)
    END,
    psd.warehouse_id
;

INSERT INTO _raw_data (
    data_type,
     warehouse_id,
     fc,
     city,
     country,
     brand,
     brands,
     date_object,
     order_volume_forecast,
     unit_volume_forecast,
     date_volume_forecast,
     order_count,
     unit_count,
     units_cancelled
)
SELECT
    r.data_type,
    r.warehouse_id,
    w.airport_code AS fc,
    a.city,
    a.country_code,
    r.brand,
    r.brands,
    r.date_object,
    r.order_volume_forecast,
    r.unit_volume_forecast,
    r.date_volume_forecast,
    r.order_count,
    SUM(r.unit_count) AS unit_count,
    r.units_cancelled
FROM _rec r
         JOIN lake_view.ultra_warehouse.warehouse w
              ON r.warehouse_id = w.warehouse_id
         JOIN lake_view.ultra_warehouse.address a
              ON w.address_id = a.address_id
GROUP BY r.data_type,
         r.warehouse_id,
         w.airport_code,
         a.city,
         a.country_code,
         r.brand,
         r.brands,
         r.date_object,
         r.order_volume_forecast,
         r.unit_volume_forecast,
         r.date_volume_forecast,
         r.order_count,
         r.units_cancelled;

/***********************
Returned
***********************/
INSERT INTO _raw_data (
    data_type,
    warehouse_id,
    fc,
    city,
    country,
    brand,
    brands,
    date_object,
    order_volume_forecast,
    unit_volume_forecast,
    date_volume_forecast,
    order_count,
    unit_count,
    units_cancelled
)
SELECT
    'Returned' AS data_type,
    ol.warehouse_id,
    w.airport_code AS fc,
    a.city,
    a.country_code AS country,
    c.label AS brand,
    CASE
        WHEN brand = 'Savage X' OR brand = 'LINGERIE'
        THEN 'Savage'
        WHEN brand = 'Fabletics' AND UPPER(cd.value) = 'M'
        THEN 'Fabletics Men'
        ELSE brand
    END AS brands,
    CASE
        WHEN ol.warehouse_id IN (107, 154)
        THEN TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', 'America/New_York', r.date_added))
        WHEN ol.warehouse_id IN (221)
        THEN TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', 'Europe/Amsterdam', r.date_added))
        WHEN ol.warehouse_id IN (366)
        THEN TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', 'Europe/London', r.date_added))
        ELSE TO_DATE(r.date_added)
    END AS date_object,
    0 AS order_volume_forecast,
    0 AS unit_volume_forecast,
    '1900-01-01' AS date_volume_forecast,
    0 AS order_count,
    COUNT(DISTINCT r.return_id) AS unit_count,
    0 AS units_cancelled
FROM reporting_base_prod.ultra_warehouse.vw_inventory_log AS vil
JOIN lake_consolidated_view.ultra_merchant.return_product AS rp
    ON vil.lpn_code = rp.lpn_code
JOIN lake_consolidated_view.ultra_merchant.return AS r
    ON rp.return_id = r.return_id
JOIN lake_consolidated_view.ultra_merchant.order_line AS ol
    ON r.order_id = ol.order_id
    AND vil.lpn_code = ol.lpn_code
JOIN lake_consolidated_view.ultra_merchant."ORDER" AS o
    ON ol.order_id = o.order_id
LEFT JOIN lake_consolidated_view.ultra_merchant.customer_detail AS cd
    ON o.customer_id = cd.customer_id
    AND UPPER(cd.name) = 'GENDER'
JOIN lake_view.ultra_warehouse.warehouse AS w
    ON ol.warehouse_id = w.warehouse_id
JOIN lake_view.ultra_warehouse.address AS a
    ON w.address_id = a.address_id
JOIN lake_view.ultra_warehouse.item AS i
    ON vil.item_id = i.item_id
JOIN lake_view.ultra_warehouse.company AS c
    ON i.company_id = c.company_id
WHERE vil.type_code_id != 207
    AND vil.zone_label LIKE ('%Return%')
    AND vil.quantity > 0
    AND NVL(vil.object, '') NOT IN ('sku_relabel', 'pack_case', 'putaway_detail', 'receipt-error')
    AND vil.lpn_id IS NOT NULL
    AND CASE
        WHEN ol.warehouse_id IN (107, 154)
        THEN TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', 'America/New_York', r.date_added))
        WHEN ol.warehouse_id IN (221)
        THEN TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', 'Europe/Amsterdam', r.date_added))
        WHEN ol.warehouse_id IN (366)
        THEN TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', 'Europe/London', r.date_added))
        ELSE TO_DATE(r.date_added)
    END BETWEEN $target_date AND $target_date_end
GROUP BY
    ol.warehouse_id,
    w.airport_code,
    a.city,
    a.country_code,
    c.label,
    CASE
        WHEN brand = 'Savage X' OR brand = 'LINGERIE'
        THEN 'Savage'
        WHEN brand = 'Fabletics' AND UPPER(cd.value) = 'M'
        THEN 'Fabletics Men'
        ELSE brand
    END,
    CASE
        WHEN ol.warehouse_id IN (107, 154)
        THEN TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', 'America/New_York', r.date_added))
        WHEN ol.warehouse_id IN (221)
        THEN TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', 'Europe/Amsterdam', r.date_added))
        WHEN ol.warehouse_id IN (366)
        THEN TO_DATE(CONVERT_TIMEZONE('America/Los_Angeles', 'Europe/London', r.date_added))
        ELSE TO_DATE(r.date_added)
    END
;

/*****************
final data
******************/
DELETE FROM gfc.fpa_reporting_dataset
WHERE date_object > $target_date
    AND date_object < $target_date_end;

INSERT INTO gfc.fpa_reporting_dataset (
    WAREHOUSE_ID,
    FC,
    CITY,
    COUNTRY,
    BRAND,
    BRANDS,
    DATE_OBJECT,
    DATA_TYPE,
    ACTUAL,
    BDGT,
    ACTUAL_PM,
    BDGT_PM,
    ACTUAL_YOY,
    BDGT_YOY
)
SELECT
    a.warehouse_id,
    a.fc,
    a.city,
    a.country,
    a.brand,
    a.brands,
    a.date_object,
    'Shipped Orders' AS data_type,
    a.order_count AS actual,
    a.order_volume_forecast AS bdgt,
    pm.actual AS actual_pm,
    pm.bdgt AS bdgt_pm,
    y.actual AS actual_yoy,
    y.bdgt AS bdgt_yoy
FROM _raw_data a
LEFT JOIN gfc.fpa_reporting_dataset AS pm
    ON DATEADD('month', -1, a.date_object) = pm.date_object
    AND a.warehouse_id = pm.warehouse_id
    AND a.brands = pm.brands
    AND pm.data_type = 'Shipped Orders'
LEFT JOIN gfc.fpa_reporting_dataset AS y
    ON DATEADD('year', -1, a.date_object) = y.date_object
    AND a.warehouse_id = y.warehouse_id
    AND a.brands = y.brands
    AND y.data_type = 'Shipped Orders'
WHERE a.data_type = 'Shipped'
    AND a.date_object > $target_date
    AND a.date_object < $target_date_end

UNION
SELECT
    a.warehouse_id,
    a.fc,
    a.city,
    a.country,
    a.brand,
    a.brands,
    a.date_object,
    'Shipped Units' AS data_type,
    a.unit_count AS actual,
    a.unit_volume_forecast AS bdgt,
    pm.actual AS actual_pm,
    pm.bdgt AS bdgt_pm,
    y.actual AS actual_yoy,
    y.bdgt AS bdgt_yoy
FROM _raw_data AS a
LEFT JOIN gfc.fpa_reporting_dataset AS pm
    ON DATEADD('month', -1, a.date_object) = pm.date_object
    AND a.warehouse_id = pm.warehouse_id
    AND a.brands = pm.brands
    AND pm.data_type = 'Shipped Units'
LEFT JOIN gfc.fpa_reporting_dataset AS y
    ON DATEADD('year', -1, a.date_object) = y.date_object
    AND a.warehouse_id = y.warehouse_id
    AND a.brands = y.brands
    AND y.data_type = 'Shipped Units'
WHERE a.data_type = 'Shipped'
    AND a.date_object > $target_date
    AND a.date_object < $target_date_end

UNION
SELECT
    a.warehouse_id,
    a.fc,
    a.city,
    a.country,
    a.brand,
    a.brands,
    a.date_object,
    'Sales Volume Orders' AS data_type,
    a.order_count AS actual,
    a.order_volume_forecast AS bdgt,
    pm.actual AS actual_pm,
    pm.bdgt AS bdgt_pm,
    y.actual AS actual_yoy,
    y.bdgt AS bdgt_yoy
FROM _raw_data AS a
LEFT JOIN gfc.fpa_reporting_dataset AS pm
    ON DATEADD('month', -1, a.date_object) = pm.date_object
    AND a.warehouse_id = pm.warehouse_id
    AND a.brands = pm.brands
    AND pm.data_type = 'Sales Volume Orders'
LEFT JOIN gfc.fpa_reporting_dataset AS y
    ON DATEADD('year', -1, a.date_object) = y.date_object
    AND a.warehouse_id = y.warehouse_id
    AND a.brands = y.brands
    AND y.data_type = 'Sales Volume Orders'
WHERE a.data_type = 'Sales'
    AND a.date_object > $target_date
    AND a.date_object < $target_date_end

UNION
SELECT
    a.warehouse_id,
    a.fc,
    a.city,
    a.country,
    a.brand,
    a.brands,
    a.date_object,
    'Sales Volume Units' AS data_type,
    a.unit_count AS actual,
    a.unit_volume_forecast AS bdgt,
    pm.actual AS actual_pm,
    pm.bdgt AS bdgt_pm,
    y.actual AS actual_yoy,
    y.bdgt AS bdgt_yoy
FROM _raw_data AS a
LEFT JOIN gfc.fpa_reporting_dataset AS pm
    ON DATEADD('month', -1, a.date_object) = pm.date_object
    AND a.warehouse_id = pm.warehouse_id
    AND a.brands = pm.brands
    AND pm.data_type = 'Sales Volume Units'
LEFT JOIN gfc.fpa_reporting_dataset AS y
    ON DATEADD('year', -1, a.date_object) = y.date_object
    AND a.warehouse_id = y.warehouse_id
    AND a.brands = y.brands
    AND y.data_type = 'Sales Volume Units'
WHERE a.data_type = 'Sales'
    AND a.date_object > $target_date
    AND a.date_object < $target_date_end

UNION
SELECT
    a.warehouse_id,
    a.fc,
    a.city,
    a.country,
    a.brand,
    a.brands,
    a.date_object,
    'Received Units' AS data_type,
    a.unit_count AS actual,
    a.unit_volume_forecast AS bdgt,
    pm.actual AS actual_pm,
    pm.bdgt AS bdgt_pm,
    y.actual AS actual_yoy,
    y.bdgt AS bdgt_yoy
FROM _raw_data AS a
LEFT JOIN gfc.fpa_reporting_dataset AS pm
    ON DATEADD('month', -1, a.date_object) = pm.date_object
    AND a.warehouse_id = pm.warehouse_id
    AND a.brands = pm.brands
    AND pm.data_type = 'Received Units'
LEFT JOIN gfc.fpa_reporting_dataset AS y
    ON DATEADD('year', -1, a.date_object) = y.date_object
    AND a.warehouse_id = y.warehouse_id
    AND a.brands = y.brands
    AND y.data_type = 'Received Units'
WHERE a.data_type = 'Received'
    AND a.date_object > $target_date
    AND a.date_object < $target_date_end

UNION
SELECT
    a.warehouse_id,
    a.fc,
    a.city,
    a.country,
    a.brand,
    a.brands,
    a.date_object,
    'Returned Units' AS data_type,
    a.unit_count AS actual,
    a.unit_volume_forecast AS bdgt,
    pm.actual AS actual_pm,
    pm.bdgt AS bdgt_pm,
    y.actual AS actual_yoy,
    y.bdgt AS bdgt_yoy
FROM _raw_data AS a
LEFT JOIN gfc.fpa_reporting_dataset AS pm
    ON DATEADD('month', -1, a.date_object) = pm.date_object
    AND a.warehouse_id = pm.warehouse_id
    AND a.brands = pm.brands
    AND pm.data_type = 'Returned Units'
LEFT JOIN gfc.fpa_reporting_dataset AS y
    ON DATEADD('year', -1, a.date_object) = y.date_object
    AND a.warehouse_id = y.warehouse_id
    AND a.brands = y.brands
    AND y.data_type = 'Returned Units'
WHERE a.data_type = 'Returned'
    AND a.date_object > $target_date
    AND a.date_object < $target_date_end
;

/***********************
get capacity data
************************/
CREATE OR REPLACE TEMPORARY TABLE _fcap AS
SELECT
    CASE
    WHEN UPPER(TRIM(fc.building)) = 'LOU1'
       THEN 107
    WHEN UPPER(TRIM(fc.building)) = 'LOU2'
       THEN 154
    WHEN UPPER(TRIM(fc.building)) = 'LOU3'
       THEN 421
    WHEN UPPER(TRIM(fc.building)) = 'ONT'
       THEN 231
    WHEN UPPER(TRIM(fc.building)) = 'TIJ'
       THEN 466
    WHEN UPPER(TRIM(fc.building)) = 'DUS'
       THEN 221
    WHEN UPPER(TRIM(fc.building)) = 'LHR'
       THEN 366
    ELSE 1 END AS warehouse_id,
    UPPER(TRIM(fc.building)) AS building,
    w.airport_code AS fc,
    a.city,
    a.country_code AS country,
    fc.year || '-' || IFF(fc.month < 10, '0' || TO_VARCHAR(fc.month), TO_VARCHAR(fc.month)) || '-01' AS date_object,
    SUM(NVL(fc.lpn_rack_by_building_capacity, 0) +
        NVL(fc.case_rack_by_building_capacity, 0)) AS warehouse_cap,
    SUM(fc.lpn_rack_by_building) AS static_on_hand,
    SUM(fc.case_rack_by_building) AS case_on_hand,
    SUM(fc.lpn_rack_by_building_capacity) AS budget_static_on_hand,
    SUM(fc.case_rack_by_building_capacity) AS budget_case_on_hand,
    SUM(fc.act_on_hand) AS act_on_hand,
    SUM(fc.budget) AS budget
FROM lake_view.excel.fc_capacity AS fc
LEFT JOIN lake_view.ultra_warehouse.warehouse AS w
    ON CASE
        WHEN UPPER(TRIM(fc.building)) = 'LOU1'
        THEN 107
        WHEN UPPER(TRIM(fc.building)) = 'LOU2'
        THEN 154
        WHEN UPPER(TRIM(fc.building)) = 'LOU3'
        THEN 421
        WHEN UPPER(TRIM(fc.building)) = 'ONT'
        THEN 231
        WHEN UPPER(TRIM(fc.building)) = 'TIJ'
        THEN 466
        WHEN UPPER(TRIM(fc.building)) = 'DUS'
        THEN 221
        WHEN UPPER(TRIM(fc.building)) = 'LHR'
        THEN 366
        ELSE 1
        END = w.warehouse_id
LEFT JOIN lake_view.ultra_warehouse.address AS a
    ON w.address_id = a.address_id
GROUP BY
    CASE
    WHEN UPPER(TRIM(fc.building)) = 'LOU1'
    THEN 107
    WHEN UPPER(TRIM(fc.building)) = 'LOU2'
    THEN 154
    WHEN UPPER(TRIM(fc.building)) = 'LOU3'
    THEN 421
    WHEN UPPER(TRIM(fc.building)) = 'ONT'
    THEN 231
    WHEN UPPER(TRIM(fc.building)) = 'TIJ'
    THEN 466
    WHEN UPPER(TRIM(fc.building)) = 'DUS'
    THEN 221
    WHEN UPPER(TRIM(fc.building)) = 'LHR'
    THEN 366
    ELSE 1
    END,
    UPPER(TRIM(fc.building)),
    w.airport_code,
    a.city,
    a.country_code,
    fc.year || '-' || IFF(fc.month < 10, '0' || TO_VARCHAR(fc.month), TO_VARCHAR(fc.month)) || '-01';


/*************************
format the capacity data
**************************/
CREATE OR REPLACE TEMPORARY TABLE _fpa_reporting_dataset AS
SELECT
    a.warehouse_id,
    a.fc,
    a.city,
    a.country,
    'FC' AS brand,
    'FC' AS brands,
    a.date_object,
    'Warehouse Cap' AS data_type,
    a.warehouse_cap AS actual,
    0 AS bdgt,
    m.warehouse_cap AS actual_pm,
    0 AS bdgt_pm,
    y.warehouse_cap AS actual_yoy,
    0 AS bdgt_yoy
FROM _fcap AS a
LEFT JOIN _fcap AS m
    ON a.warehouse_id = m.warehouse_id
    AND DATEADD('Month', -1, a.date_object) = m.date_object
LEFT JOIN _fcap AS y
    ON a.warehouse_id = y.warehouse_id
    AND DATEADD('Year', -1, a.date_object) = m.date_object

UNION
SELECT
    a.warehouse_id,
    a.fc,
    a.city,
    a.country,
    'FC' AS brand,
    'FC' AS brands,
    a.date_object,
    'Static On Hand' AS data_type,
    a.static_on_hand AS actual,
    a.budget_static_on_hand AS bdgt,
    m.static_on_hand AS actual_pm,
    m.budget_static_on_hand AS bdgt_pm,
    y.static_on_hand AS actual_yoy,
    y.budget_static_on_hand AS bdgt_yoy
FROM _fcap AS a
LEFT JOIN _fcap AS m
    ON a.warehouse_id = m.warehouse_id
    AND DATEADD('Month', -1, a.date_object) = m.date_object
LEFT JOIN _fcap AS y
    ON a.warehouse_id = y.warehouse_id
    AND DATEADD('Year', -1, a.date_object) = m.date_object

UNION
SELECT
    a.warehouse_id,
    a.fc,
    a.city,
    a.country,
    'FC' AS brand,
    'FC' AS brands,
    a.date_object,
    'Case On Hand' AS data_type,
    a.case_on_hand AS actual,
    a.budget_case_on_hand AS bdgt,
    m.case_on_hand AS actual_pm,
    m.budget_case_on_hand AS bdgt_pm,
    y.case_on_hand AS actual_yoy,
    y.budget_case_on_hand AS bdgt_yoy
FROM _fcap AS a
LEFT JOIN _fcap AS m
    ON a.warehouse_id = m.warehouse_id
    AND DATEADD('Month', -1, a.date_object) = m.date_object
LEFT JOIN _fcap AS y
    ON a.warehouse_id = y.warehouse_id
    AND DATEADD('Year', -1, a.date_object) = m.date_object

UNION
SELECT
    a.warehouse_id,
    a.fc,
    a.city,
    a.country,
    'FC' AS brand,
    'FC' AS brands,
    a.date_object,
    'Inventory Units' AS data_type,
    a.act_on_hand AS actual,
    a.budget AS bdgt,
    m.act_on_hand AS actual_pm,
    m.budget AS bdgt_pm,
    y.act_on_hand AS actual_yoy,
    y.budget AS bdgt_yoy
FROM _fcap AS a
LEFT JOIN _fcap AS m
    ON a.warehouse_id = m.warehouse_id
    AND DATEADD('Month', -1, a.date_object) = m.date_object
LEFT JOIN _fcap AS y
    ON a.warehouse_id = y.warehouse_id
    AND DATEADD('Year', -1, a.date_object) = m.date_object
;

/***********************
merge capacity data
************************/
MERGE INTO gfc.fpa_reporting_dataset AS tgt
    USING _fpa_reporting_dataset AS src
    ON tgt.warehouse_id = src.warehouse_id
        AND tgt.brand = src.brand
        AND tgt.date_object = src.date_object
        AND tgt.data_type = src.data_type
    WHEN MATCHED THEN UPDATE
        SET
            tgt.actual = src.actual,
            tgt.bdgt = src.bdgt,
            tgt.actual_pm = src.actual_pm,
            tgt.bdgt_pm = src.bdgt_pm,
            tgt.actual_yoy = src.actual_yoy
    WHEN NOT MATCHED THEN INSERT (
    warehouse_id, fc, city, country, brand, brands, date_object, data_type, actual, bdgt, actual_pm, bdgt_pm,
    actual_yoy, bdgt_yoy
    )
    VALUES (
        src.warehouse_id,
        src.fc,
        src.city,
        src.country,
        src.brand,
        src.brands,
        src.date_object,
        src.data_type,
        src.actual,
        src.bdgt,
        src.actual_pm,
        src.bdgt_pm,
        src.actual_yoy,
        src.bdgt_yoy
    )
;
