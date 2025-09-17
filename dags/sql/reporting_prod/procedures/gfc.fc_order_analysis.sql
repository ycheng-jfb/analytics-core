SET target_date = (
    SELECT MAX(date_added) AS mda
    FROM gfc.fc_order_analysis
);

CREATE OR REPLACE TEMPORARY TABLE _data_raw AS
SELECT DISTINCT
    c.label AS brand,
    CASE
        WHEN c.label IN ('Savage X', 'LINGERIE')
        THEN 'Savage'
        ELSE c.label
    END AS brands,
    i.foreign_order_id AS order_id,
    it.item_number,
    ii.quantity AS qty_ordered,
    CASE
        WHEN UPPER(a.country_code) IN ('CA', 'MX')
        THEN 0
        WHEN i.warehouse_id IN (231, 465, 466, 467)
            AND (
                (SPLIT_PART(a.zip, '-', 1) >= '58600' AND SPLIT_PART(a.zip, '-', 1) < '60000')
                    OR (SPLIT_PART(a.zip, '-', 1) >= '78500' AND SPLIT_PART(a.zip, '-', 1) < '78600')
                OR (SPLIT_PART(a.zip, '-', 1) >= '79100'
                    AND SPLIT_PART(a.zip, '-', 1) < '79600') OR (SPLIT_PART(a.zip, '-', 1) >= '79700'))
        THEN 0
        WHEN i.warehouse_id IN (107, 154, 421)
            AND (
                (SPLIT_PART(a.zip, '-', 1) >= '00100' AND SPLIT_PART(a.zip, '-', 1) < '58600')
                OR (SPLIT_PART(a.zip, '-', 1) >= '60000' AND SPLIT_PART(a.zip, '-', 1) < '78500')
                OR (SPLIT_PART(a.zip, '-', 1) >= '78600' AND SPLIT_PART(a.zip, '-', 1) < '79100')
                OR (SPLIT_PART(a.zip, '-', 1) >= '79600' AND SPLIT_PART(a.zip, '-', 1) < '79700'))
        THEN 0
        WHEN i.warehouse_id IN (221, 366, 109)
        THEN 0
        ELSE 1
    END AS out_of_region,
    CASE
        WHEN i2.foreign_order_id IS NULL
        THEN 0
        ELSE 1
    END AS is_split,
    TO_DATE(i.datetime_added) AS date_added,
    i.warehouse_id,
    c.company_id,
    w.airport_code AS fc,
    CASE
        WHEN i.warehouse_id IN (231, 465, 466, 467)
            AND (
                (SPLIT_PART(a.zip, '-', 1) >= '58600' AND SPLIT_PART(a.zip, '-', 1) < '60000')
                OR (SPLIT_PART(a.zip, '-', 1) >= '78500' AND SPLIT_PART(a.zip, '-', 1) < '78600')
                OR (SPLIT_PART(a.zip, '-', 1) >= '79100' AND SPLIT_PART(a.zip, '-', 1) < '79600')
                OR (SPLIT_PART(a.zip, '-', 1) >= '79700')
                OR UPPER(a.country_code) IN ('CA', 'MX')
            )
        THEN i.warehouse_id
        WHEN i.warehouse_id IN (107, 154, 421)
            AND (
                (SPLIT_PART(a.zip, '-', 1) >= '00100' AND SPLIT_PART(a.zip, '-', 1) < '58600')
                OR (SPLIT_PART(a.zip, '-', 1) >= '60000' AND SPLIT_PART(a.zip, '-', 1) < '78500')
                OR (SPLIT_PART(a.zip, '-', 1) >= '78600' AND SPLIT_PART(a.zip, '-', 1) < '79100')
                OR (SPLIT_PART(a.zip, '-', 1) >= '79600' AND SPLIT_PART(a.zip, '-', 1) < '79700')
                OR UPPER(a.country_code) IN ('CA', 'MX')
            )
        THEN i.warehouse_id
        WHEN i.warehouse_id IN (221, 366, 109)
        THEN i.warehouse_id
    END AS owning_warehouse_id,
    SPLIT_PART(a.zip, '-', 1) AS zip
FROM lake_view.ultra_warehouse.invoice AS i
JOIN lake_view.ultra_warehouse.fulfillment AS f
    ON i.warehouse_id = f.warehouse_id
    AND i.foreign_order_id = f.foreign_order_id
JOIN lake_view.ultra_warehouse.warehouse AS w
    ON i.warehouse_id = w.warehouse_id
JOIN lake_view.ultra_warehouse.company AS c
    ON i.company_id = c.company_id
JOIN lake_view.ultra_warehouse.invoice_item AS ii
    ON i.invoice_id = ii.invoice_id
JOIN lake_view.ultra_warehouse.item AS it
    ON ii.item_id = it.item_id
JOIN lake_view.ultra_warehouse.address AS a
    ON i.shipping_address_id = a.address_id
LEFT JOIN lake_view.ultra_warehouse.invoice i2
    ON i.foreign_order_id = i2.foreign_order_id
    AND i.warehouse_id != i2.warehouse_id
WHERE TO_DATE(i.datetime_added) > $target_date
  AND TO_DATE(i.datetime_added) < TO_DATE(CURRENT_TIMESTAMP)
  --to_date(dateadd('day', -1, current_timestamp())) and to_date(i.datetime_added) < to_date(current_timestamp)
  AND UPPER(NVL(it.wms_class, 'UNK')) NOT IN ('UNK', 'CONSUMABLE')
  AND UPPER(f.source) = 'WMS';

UPDATE _data_raw
SET owning_warehouse_id = x.owning_warehouse_id
FROM (
    SELECT DISTINCT
        order_id,
        MIN(owning_warehouse_id) AS owning_warehouse_id
    FROM _data_raw
    WHERE owning_warehouse_id IS NOT NULL
    GROUP BY order_id
    ) AS x
WHERE _data_raw.order_id = x.order_id;

UPDATE _data_raw
SET owning_warehouse_id = x.owning_warehouse_id
FROM (
    SELECT DISTINCT
        order_id,
        item_number,
        CASE
            WHEN warehouse_id IN (465, 467, 221, 366, 109)
            THEN warehouse_id
            WHEN warehouse_id IN (231) AND brand IN ('JustFab', 'FabKids', 'Shoe Dazzle')
            THEN 107
            WHEN warehouse_id IN (231) AND brand IN ('Fabletics', 'Yitty', 'Savage X')
            THEN 154
            WHEN warehouse_id IN (466) AND brand IN ('Fabletics', 'Yitty', 'Savage X')
            THEN 154
            WHEN warehouse_id IN (107, 154, 421)
            THEN 231
        END AS owning_warehouse_id
    FROM _data_raw
    WHERE owning_warehouse_id IS NULL
    ) AS x
WHERE _data_raw.order_id = x.order_id
  AND _data_raw.item_number = x.item_number;

CREATE OR REPLACE TEMPORARY TABLE _fc_order_analysis AS
SELECT
    owning_warehouse_id,
    w.airport_code AS fc,
    brand,
    brands,
    date_added,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(qty_ordered) AS unit_count,
    'Is Split' AS data_type
FROM _data_raw AS dr
JOIN lake_view.ultra_warehouse.warehouse AS w
    ON dr.owning_warehouse_id = w.warehouse_id
WHERE is_split = 1
GROUP BY
    owning_warehouse_id,
    w.airport_code,
    brand,
    brands,
    date_added

UNION

SELECT
    dr.warehouse_id,
    w.airport_code AS fc,
    brand,
    brands,
    date_added,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(qty_ordered) AS unit_count,
    'Out of Region' AS data_type
FROM _data_raw AS dr
JOIN lake_view.ultra_warehouse.warehouse AS w
    ON dr.warehouse_id = w.warehouse_id
WHERE out_of_region = 1
    AND is_split = 0
GROUP BY
    dr.warehouse_id,
    w.airport_code,
    brand,
    brands,
    date_added

UNION

SELECT
    owning_warehouse_id,
    fc,
    brand,
    brands,
    date_added,
    COUNT(1) AS order_count,
    SUM(unit_count) AS unit_count,
    data_type
FROM (
    SELECT
        owning_warehouse_id,
        w.airport_code AS fc,
        brand,
        brands,
        date_added,
        order_id,
        SUM(qty_ordered) AS unit_count,
        'Singles' AS data_type
      FROM _data_raw AS dr
      JOIN lake_view.ultra_warehouse.warehouse AS w
          ON dr.owning_warehouse_id = w.warehouse_id
      GROUP BY
          owning_warehouse_id,
          w.airport_code,
          brand,
          brands,
          date_added,
          order_id
      HAVING SUM(qty_ordered) = 1
      ) AS a
GROUP BY
    owning_warehouse_id,
    fc,
    brand,
    brands,
    date_added,
    data_type

UNION

SELECT
    owning_warehouse_id,
    fc,
    brand,
    brands,
    date_added,
    COUNT(1) AS order_count,
    SUM(unit_count) AS unit_count,
    data_type
FROM (
    SELECT
        owning_warehouse_id,
        w.airport_code AS fc,
        brand,
        brands,
        date_added,
        order_id,
        SUM(qty_ordered) AS unit_count,
        'Multi' AS data_type
    FROM _data_raw AS dr
    JOIN lake_view.ultra_warehouse.warehouse AS w
        ON dr.owning_warehouse_id = w.warehouse_id
    GROUP BY
        owning_warehouse_id,
        w.airport_code,
        brand,
        brands,
        date_added,
        order_id
    HAVING SUM(qty_ordered) > 1
    ) AS a
GROUP BY
    owning_warehouse_id,
    fc,
    brand,
    brands,
    date_added,
    data_type

UNION

SELECT
    owning_warehouse_id,
    w.airport_code AS fc,
    brand,
    brands,
    date_added,
    COUNT(DISTINCT order_id) order_count,
    SUM(qty_ordered) AS unit_count,
    'Total' AS data_type
FROM _data_raw AS dr
JOIN lake_view.ultra_warehouse.warehouse AS w
    ON dr.owning_warehouse_id = w.warehouse_id
GROUP BY
    owning_warehouse_id,
    w.airport_code,
    brand,
    brands,
    date_added

UNION

SELECT
    owning_warehouse_id,
    w.airport_code AS fc,
    brand,
    brands,
    date_added,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(qty_ordered) AS unit_count,
    'West Coast Total' AS data_type
FROM _data_raw AS dr
JOIN lake_view.ultra_warehouse.warehouse AS w
    ON dr.owning_warehouse_id = w.warehouse_id
WHERE dr.warehouse_id IN (231, 465, 466, 467)
GROUP BY
    owning_warehouse_id,
    w.airport_code,
    brand,
    brands,
    date_added

UNION

SELECT
    owning_warehouse_id,
    w.airport_code AS fc,
    brand,
    brands,
    date_added,
    COUNT(DISTINCT order_id) order_count,
    SUM(qty_ordered)   AS    unit_count,
    'East Coast Total' AS    data_type
FROM _data_raw AS dr
JOIN lake_view.ultra_warehouse.warehouse AS w
    ON dr.owning_warehouse_id = w.warehouse_id
WHERE dr.warehouse_id IN (107, 154, 421)
GROUP BY
    owning_warehouse_id,
    w.airport_code,
    brand,
    brands,
    date_added

UNION

SELECT owning_warehouse_id,
       w.airport_code   AS      fc,
       brand,
       brands,
       date_added,
       COUNT(DISTINCT order_id) order_count,
       SUM(qty_ordered) AS      unit_count,
       'EU Total'       AS      data_type
FROM _data_raw AS dr
JOIN lake_view.ultra_warehouse.warehouse AS w
    ON dr.owning_warehouse_id = w.warehouse_id
WHERE dr.warehouse_id IN (221, 366)
GROUP BY owning_warehouse_id, w.airport_code, brand, brands, date_added;

CREATE OR REPLACE TEMPORARY TABLE _splits AS
SELECT
    owning_warehouse_id,
    fc,
    brand,
    brands,
    date_added,
    split_orders,
    warehouse_id,
    SUM(east_coast_split_units) AS east_coast_split_units,
    SUM(west_coast_split_units) AS west_coast_split_units
FROM (
    SELECT
        owning_warehouse_id,
        w.airport_code AS fc,
        brand,
        brands,
        date_added,
        dr.warehouse_id,
        COUNT(DISTINCT order_id) AS split_orders,
        CASE
            WHEN dr.warehouse_id IN (107, 154, 421)
            THEN SUM(qty_ordered)
            ELSE 0
        END AS east_coast_split_units,
        CASE
            WHEN dr.warehouse_id IN (231, 465, 466, 467)
            THEN SUM(qty_ordered)
            ELSE 0
        END AS west_coast_split_units
        FROM _data_raw AS dr
        JOIN lake_view.ultra_warehouse.warehouse AS w
            ON dr.owning_warehouse_id = w.warehouse_id
            AND is_split = 1
      GROUP BY
          owning_warehouse_id,
          w.airport_code,
          brand,
          brands,
          date_added,
          dr.warehouse_id
    ) AS a
GROUP BY
    owning_warehouse_id,
    fc,
    brand,
    brands,
    date_added,
    warehouse_id,
    split_orders;

CREATE OR REPLACE TEMPORARY TABLE _orders (
    owning_warehouse_id     NUMBER(38, 0),
    fc                      VARCHAR(5),
    brand                   VARCHAR(255),
    brands                  VARCHAR(255),
    date_added              DATE,
    east_coast_total_orders NUMBER(30, 0),
    eu_total_orders         NUMBER(30, 0),
    is_split_orders         NUMBER(30, 0),
    multi_orders            NUMBER(30, 0),
    out_of_region_orders    NUMBER(30, 0),
    singles_orders          NUMBER(30, 0),
    total_orders            NUMBER(30, 0),
    west_coast_total_orders NUMBER(30, 0),
    ec_split_orders         NUMBER(30, 0),
    ec_split_units          NUMBER(30, 0),
    wc_split_orders         NUMBER(30, 0),
    wc_split_units          NUMBER(30, 0)
);

INSERT INTO _orders
    (owning_warehouse_id, fc, brand, brands, date_added, east_coast_total_orders, eu_total_orders,
    is_split_orders, multi_orders, out_of_region_orders, singles_orders, total_orders, west_coast_total_orders
    )
SELECT
    owning_warehouse_id,
    fc,
    brand,
    brands,
    date_added,
    east_coast_total_orders,
    eu_total_orders,
    is_split_orders,
    multi_orders,
    out_of_region_orders,
    singles_orders,
    total_orders,
    west_coast_total_orders
FROM (
    SELECT
        owning_warehouse_id,
        fc, brand,
        brands,
        date_added,
        data_type,
        order_count
    FROM _fc_order_analysis
    ) AS a
PIVOT (SUM(order_count)
FOR data_type IN ('East Coast Total', 'EU Total', 'Is Split', 'Multi',
    'Out of Region', 'Singles', 'Total', 'West Coast Total')
    ) AS b
    (owning_warehouse_id, fc, brand, brands, date_added, east_coast_total_orders, eu_total_orders,
    is_split_orders, multi_orders, out_of_region_orders, singles_orders, total_orders, west_coast_total_orders)
;

UPDATE _orders
SET ec_split_orders = s.split_orders,
    ec_split_units  = s.east_coast_split_units,
    wc_split_orders = 0
FROM _splits AS s
WHERE _orders.owning_warehouse_id = s.owning_warehouse_id
  AND _orders.brand = s.brand
  AND _orders.date_added = s.date_added
  AND _orders.owning_warehouse_id IN (107, 154, 421)
  AND s.owning_warehouse_id = s.warehouse_id;

UPDATE _orders
SET wc_split_units = s.west_coast_split_units
FROM (
    SELECT
        owning_warehouse_id,
        brand,
        date_added,
        SUM(west_coast_split_units) AS west_coast_split_units
    FROM _splits AS s
    WHERE owning_warehouse_id IN (107, 154, 421)
        AND s.owning_warehouse_id != s.warehouse_id
    GROUP BY owning_warehouse_id, brand, date_added
    ) AS s
WHERE _orders.owning_warehouse_id = s.owning_warehouse_id
  AND _orders.brand = s.brand
  AND _orders.date_added = s.date_added;

UPDATE _orders
SET ec_split_orders = 0,
    ec_split_units  = s.east_coast_split_units,
    wc_split_orders = s.split_orders,
    wc_split_units  = s.west_coast_split_units
FROM _splits AS s
WHERE _orders.owning_warehouse_id = s.owning_warehouse_id
  AND _orders.brand = s.brand
  AND _orders.owning_warehouse_id IN (231, 465, 466, 467)
  AND s.owning_warehouse_id = s.warehouse_id
  AND _orders.date_added = s.date_added;

UPDATE _orders
SET ec_split_units = s.east_coast_split_units
FROM (
    SELECT
        owning_warehouse_id,
        brand,
        date_added,
        SUM(east_coast_split_units) AS east_coast_split_units
    FROM _splits AS s
    WHERE owning_warehouse_id IN (231, 465, 466, 467)
        AND s.owning_warehouse_id != s.warehouse_id
    GROUP BY owning_warehouse_id, brand, date_added
    ) AS s
WHERE _orders.owning_warehouse_id = s.owning_warehouse_id
  AND _orders.brand = s.brand
  AND _orders.date_added = s.date_added;

CREATE OR REPLACE TEMPORARY TABLE _units AS
SELECT *
FROM (
    SELECT owning_warehouse_id, fc, brand, brands, date_added, data_type, unit_count
    FROM _fc_order_analysis
    ) AS a
PIVOT (SUM(unit_count)
FOR data_type IN ('East Coast Total', 'EU Total', 'Is Split', 'Multi',
    'Out of Region', 'Singles', 'Total', 'West Coast Total')
    ) AS b
    (owning_warehouse_id, fc, brand, brands, date_added, east_coast_total_units, eu_total_units, is_split_units,
    multi_units,
    out_of_region_units, singles_units, total_units, west_coast_total_units)
;

INSERT INTO gfc.fc_order_analysis
SELECT
    o.*,
    u.east_coast_total_units,
    u.eu_total_units,
    u.is_split_units,
    u.multi_units,
    u.out_of_region_units,
    u.singles_units,
    u.total_units,
    u.west_coast_total_units
FROM _orders AS o
JOIN _units AS u
    ON o.owning_warehouse_id = u.owning_warehouse_id
    AND o.brand = u.brand
    AND o.date_added = u.date_added;
