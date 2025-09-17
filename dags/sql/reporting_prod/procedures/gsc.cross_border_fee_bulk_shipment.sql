SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

-- base table
CREATE OR REPLACE TEMP TABLE _bulk_shipment__base AS
SELECT order_source,
       shipment_date,
       po_number,
       sku,
       hts_code,
       SUM(quantity) AS total_quantity
FROM reporting_base_prod.gfc.bulk_transfer_shipments b
         JOIN lake.ultra_warehouse.warehouse w
              ON b.from_warehouse_id = w.warehouse_id
                  AND w.region_id = 11
         JOIN lake.ultra_warehouse.warehouse wh
              ON b.to_warehouse_id = wh.warehouse_id AND
                 wh.region_id IN (1, 2, 3, 4, 8, 9)
GROUP BY order_source, shipment_date, po_number, sku, hts_code;


CREATE OR REPLACE TEMP TABLE _bulk_shipment__product_attribute AS
SELECT DISTINCT a.sku,
                a.po_number,
                p.cost,
                IFNULL(p.cmt, 0) AS cmt,
                actual_weight,
                avg_weight,
                NULL             AS weight,
                p.country_origin
FROM _bulk_shipment__base a
         LEFT JOIN gsc.po_detail_dataset p
                   ON a.sku = p.sku AND a.po_number = p.po_number;

-- if a sku is missing avg weight, get it from a same sku with different po from the _bulk_shipment__product_attribute
CREATE OR REPLACE TEMP TABLE _missing_avg_weight_from_pa AS
SELECT sku, avg_weight, actual_weight
FROM _bulk_shipment__product_attribute
WHERE sku IN (SELECT sku
              FROM _bulk_shipment__product_attribute
              WHERE avg_weight IS NULL)
  AND avg_weight IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY sku ORDER BY avg_weight DESC, actual_weight DESC) = 1;

UPDATE _bulk_shipment__product_attribute pa
SET weight = COALESCE(pa.actual_weight, pa.avg_weight, mw.avg_weight, mw.actual_weight)
FROM _missing_avg_weight_from_pa mw
WHERE pa.sku = mw.sku(+);


-- if a sku is missing avg weight, get it from a same sku with different po from the po_detail_dataset
CREATE OR REPLACE TEMP TABLE _missing_avg_weight_from_po AS
SELECT pa.sku, po.avg_weight, po.actual_weight
FROM gsc.po_detail_dataset po
         JOIN _bulk_shipment__product_attribute pa
              ON po.sku = pa.sku
WHERE pa.avg_weight IS NULL
  AND (po.avg_weight IS NOT NULL OR pa.actual_weight IS NOT NULL)
QUALIFY ROW_NUMBER() OVER (PARTITION BY po.sku ORDER BY po.avg_weight DESC, po.actual_weight DESC) = 1;

UPDATE _bulk_shipment__product_attribute pa
SET weight = COALESCE(pa.actual_weight, pa.avg_weight, mw.avg_weight, mw.actual_weight)
FROM _missing_avg_weight_from_po mw
WHERE pa.sku = mw.sku(+);

--  if a cost is missing, get it from a same sku with different po from the _bulk_shipment__product_attribute
CREATE OR REPLACE TEMP TABLE _missing_product_cost_from_pa AS
SELECT sku, cost
FROM _bulk_shipment__product_attribute
WHERE sku IN (SELECT DISTINCT sku
              FROM _bulk_shipment__product_attribute
              WHERE cost IS NULL)
  AND cost IS NOT NULL
  AND cost <> 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY sku ORDER BY cost DESC) = 1;

UPDATE _bulk_shipment__product_attribute pa
SET pa.cost = COALESCE(pa.cost, mp.cost)
FROM _missing_product_cost_from_pa mp
WHERE pa.sku = mp.sku
  AND pa.cost IS NULL;

--  if a cost is missing, get the highest cost from the same sku in  po_detail_dataset
CREATE OR REPLACE TEMP TABLE _missing_product_cost_from_po AS
SELECT sku, cost
FROM gsc.po_detail_dataset
WHERE sku IN (SELECT sku FROM _bulk_shipment__product_attribute WHERE cost IS NULL)
QUALIFY ROW_NUMBER() OVER (PARTITION BY sku ORDER BY cost DESC) = 1;

UPDATE _bulk_shipment__product_attribute pa
SET pa.cost = COALESCE(pa.cost, mp.cost)
FROM _missing_product_cost_from_po mp
WHERE pa.sku = mp.sku
  AND pa.cost IS NULL;


-- start hts detail
CREATE OR REPLACE TEMP TABLE _bulk_shipment__hts_detail AS
SELECT DISTINCT a.po_number,
                a.sku,
                COALESCE(a.hts_code, po.hts_code) AS hts_code,
                r.hts_us                          AS hts_code_alternative,
                NULL                              AS hts_code_ref -- hts_code_ref holds the matched hts_code with the gsc.hts_reference
FROM _bulk_shipment__base a
         LEFT JOIN gsc.sku_hts r
                   ON a.sku = r.sku
         LEFT JOIN (SELECT REPLACE(hts_code, '.', '') AS hts_code, sku
                    FROM gsc.po_detail_dataset
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY sku ORDER BY LEN(hts_code) DESC, hts_code DESC) = 1) AS po
                   ON po.sku = a.sku;


UPDATE _bulk_shipment__hts_detail a
SET a.hts_code_ref = h.hts_code
FROM reporting_base_prod.gsc.hts_reference h
WHERE a.hts_code = h.hts_code;


-- get missing hts codes from a same sku in _bulk_shipment__hts_detail
CREATE OR REPLACE TEMP TABLE _missing_hts_code_from_hd AS
SELECT sku, hts_code_ref
FROM _bulk_shipment__hts_detail
WHERE sku IN (SELECT sku
              FROM _bulk_shipment__hts_detail
              WHERE hts_code_ref IS NULL)
  AND hts_code_ref IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY sku ORDER BY hts_code_ref DESC) = 1;

UPDATE _bulk_shipment__hts_detail a
SET a.hts_code_ref = h.hts_code_ref
FROM _missing_hts_code_from_hd h
WHERE a.sku = h.sku
  AND a.hts_code_ref IS NULL;

-- hts codes usually joins better when the last 2 digits are removed
UPDATE _bulk_shipment__hts_detail a
SET a.hts_code_ref = h.hts_code
FROM reporting_base_prod.gsc.hts_reference h
WHERE LEFT(a.hts_code, LEN(a.hts_code) - 2) = h.hts_code
  AND a.hts_code_ref IS NULL;


CREATE OR REPLACE TEMP TABLE _missing_hts_code_from_hd AS
SELECT sku, hts_code_ref
FROM _bulk_shipment__hts_detail
WHERE sku IN (SELECT sku
              FROM _bulk_shipment__hts_detail
              WHERE hts_code_ref IS NULL)
  AND hts_code_ref IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY sku ORDER BY hts_code_ref DESC) = 1;

UPDATE _bulk_shipment__hts_detail a
SET a.hts_code_ref = h.hts_code_ref
FROM _missing_hts_code_from_hd h
WHERE a.sku = h.sku
  AND a.hts_code_ref IS NULL;


-- try alternative hts code
UPDATE _bulk_shipment__hts_detail a
SET a.hts_code_ref = h.hts_code
FROM reporting_base_prod.gsc.hts_reference h
WHERE a.hts_code_alternative = h.hts_code
  AND a.hts_code_ref IS NULL;

-- try alternative hts code -2
UPDATE _bulk_shipment__hts_detail a
SET a.hts_code_ref = h.hts_code
FROM reporting_base_prod.gsc.hts_reference h
WHERE LEFT(a.hts_code_alternative, LEN(a.hts_code_alternative) - 2) = h.hts_code
  AND a.hts_code_ref IS NULL;


-- get missing hts code from the po detail dataset
CREATE OR REPLACE TEMP TABLE _missing_hts_code_from_po AS
SELECT DISTINCT REPLACE(p.hts_code, '.', '') AS hts_code, p.sku
FROM gsc.po_detail_dataset p
         JOIN (SELECT DISTINCT sku, hts_code, hts_code_alternative
               FROM _bulk_shipment__hts_detail
               WHERE hts_code_ref IS NULL) h
              ON p.sku = h.sku
                  AND REPLACE(p.hts_code, '.', '') <> h.hts_code
                  AND REPLACE(p.hts_code, '.', '') <> h.hts_code_alternative
                  AND p.hts_code IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY p.sku ORDER BY LEN(p.hts_code) DESC, p.hts_code DESC) = 1;

UPDATE _bulk_shipment__hts_detail h
SET h.hts_code = p.hts_code
FROM _missing_hts_code_from_po p
WHERE p.sku = h.sku
  AND h.hts_code_ref IS NULL;

UPDATE _bulk_shipment__hts_detail a
SET a.hts_code_ref = h.hts_code
FROM reporting_base_prod.gsc.hts_reference h
WHERE a.hts_code = h.hts_code
  AND a.hts_code_ref IS NULL;

UPDATE _bulk_shipment__hts_detail a
SET a.hts_code_ref = h.hts_code
FROM reporting_base_prod.gsc.hts_reference h
WHERE LEFT(a.hts_code, LEN(a.hts_code) - 2) = h.hts_code
  AND a.hts_code_ref IS NULL;


-- get missing hts code from blue cherry style master
CREATE OR REPLACE TEMP TABLE _missing_hts_code_from_bc_style_master AS
SELECT DISTINCT REPLACE(hts_code, '.', '') AS hts_code, sku
FROM lake.bluecherry.tfg_style_master
WHERE sku IN (SELECT sku FROM _bulk_shipment__hts_detail WHERE hts_code_ref IS NULL)
  AND hts_code IS NOT NULL;

UPDATE _bulk_shipment__hts_detail h
SET h.hts_code = p.hts_code
FROM _missing_hts_code_from_bc_style_master p
WHERE p.sku = h.sku
  AND h.hts_code_ref IS NULL;

UPDATE _bulk_shipment__hts_detail a
SET a.hts_code_ref = h.hts_code
FROM reporting_base_prod.gsc.hts_reference h
WHERE a.hts_code = h.hts_code
  AND a.hts_code_ref IS NULL;

UPDATE _bulk_shipment__hts_detail a
SET a.hts_code_ref = h.hts_code
FROM reporting_base_prod.gsc.hts_reference h
WHERE LEFT(a.hts_code, LEN(a.hts_code) - 2) = h.hts_code
  AND a.hts_code_ref IS NULL;


-- clean up duty rate fields
CREATE OR REPLACE TEMP TABLE _bulk_shipment__duty_rate AS
SELECT d.po_number,
       d.sku,
       d.hts_code_ref,
       CAST((CASE
                 WHEN h.duty_rate = 'Free' THEN 0.00
                 WHEN h.duty_rate ILIKE '%+%'
                     THEN REPLACE(
                         LTRIM(SUBSTRING(h.duty_rate, CHARINDEX('+', h.duty_rate) + 1,
                                         LEN(h.duty_rate))), '%', '')
                 ELSE REPLACE(h.duty_rate, '%', '') END) AS NUMERIC(18, 5)) /
       100.00                                                                        AS duty_rate,             -- divide 100 is for %
       IFF(h.duty_rate ILIKE '%kg%',
           LEFT(h.duty_rate, CHARINDEX('¢', h.duty_rate) - 1) / 100.00,
           0)                                                                        AS duty_amount_per_kg,    -- divide 100 is for cents
       IFF(h.duty_rate ILIKE '%pr%',
           LEFT(h.duty_rate, CHARINDEX('¢', h.duty_rate) - 1) / 100.00,
           0)                                                                        AS duty_amount_per_piece, -- divide 100 is for cent
       IFF(h.china_rate IS NULL, 0.00,
           CAST(REPLACE(h.china_rate, '%', '') AS NUMERIC(18, 5))) / 100.00          AS china_rate,
       cotton_rate,
       mpf_rate / 100.00                                                             AS mpf_rate
FROM _bulk_shipment__hts_detail d
         LEFT JOIN reporting_base_prod.gsc.hts_reference h
                   ON d.hts_code_ref = h.hts_code;


-- final table
CREATE OR REPLACE TRANSIENT TABLE gsc.cross_border_fee_bulk_shipment AS
SELECT ab.order_source,
       ab.shipment_date,
       CASE
           WHEN LOWER(pdd.division_id) IN ('shoedazzle', 'justfab', 'fabkids') THEN 'JustFab'
           WHEN LOWER(pdd.division_id) IN ('lingerie', 'sxf', 'savage x', 'savage x fenty') THEN 'Savage X'
           WHEN LOWER(pdd.division_id) IN ('fabletics') THEN 'Fabletics'
           WHEN LOWER(pdd.division_id) IN ('yitty', 'yittyactive') THEN 'Yitty' END AS brand,
       ab.po_number,
       ab.sku,
       hd.hts_code_ref                                                              AS hts_code,
       ab.total_quantity,
       hd.duty_rate,
       hd.duty_amount_per_kg,
       hd.duty_amount_per_piece,
       p.weight,
       p.cost + p.cmt                                                               AS product_value,
       (product_value * duty_rate) + (p.weight * hd.duty_amount_per_kg) +
       hd.duty_amount_per_piece                                                     AS total_duty_amount_per_unit,
       ROUND(product_value * ab.total_quantity) * hd.mpf_rate /
       ab.total_quantity                                                            AS merchant_processing_fee_amount_per_unit,
       china_rate,
       p.country_origin,
       IFF(country_origin = 'CN', china_rate * product_value, 0)                    AS china_cost_per_unit,
       hd.cotton_rate,
       hd.cotton_rate * p.weight                                                    AS cotton_cost_per_unit,
       $execution_start_time                                                        AS meta_create_datetime,
       $execution_start_time                                                        AS meta_update_datetime
FROM _bulk_shipment__base ab
         LEFT JOIN _bulk_shipment__product_attribute p
                   ON ab.sku = p.sku AND ab.po_number = p.po_number
         LEFT JOIN _bulk_shipment__duty_rate hd ON ab.sku = hd.sku AND ab.po_number = hd.po_number
         LEFT JOIN (SELECT DISTINCT po_number, sku, division_id FROM gsc.po_detail_dataset) pdd
                   ON pdd.po_number = ab.po_number
                       AND pdd.sku = ab.sku;

-- get brand from another PO from gsc.po_detail_dataset
UPDATE gsc.cross_border_fee_bulk_shipment b
SET b.brand = CASE
                  WHEN LOWER(pdd.division_id) IN ('shoedazzle', 'justfab', 'fabkids') THEN 'JustFab'
                  WHEN LOWER(pdd.division_id) IN ('lingerie', 'sxf', 'savage x', 'savage x fenty') THEN 'Savage X'
                  WHEN LOWER(pdd.division_id) IN ('fabletics') THEN 'Fabletics'
                  WHEN LOWER(pdd.division_id) IN ('yitty', 'yittyactive') THEN 'Yitty' END
FROM gsc.po_detail_dataset pdd
WHERE pdd.sku = b.sku
  AND b.brand IS NULL;
