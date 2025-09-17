SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

-- base table
CREATE OR REPLACE TEMP TABLE _ecomm_shipment__orders_over_800 AS
SELECT foreign_order_id AS meta_original_order_id
FROM (SELECT *
      FROM lake.ultra_warehouse.transload_manifest_file_for_mx
      QUALIFY ROW_NUMBER() OVER (PARTITION BY foreign_order_id, sku ORDER BY transload_manifest_file_id DESC, product_value DESC) =
              1)
GROUP BY foreign_order_id
HAVING SUM(product_value) > 800;

-- order by lpn_id by ASC to prioritize not null values over null
CREATE OR REPLACE TEMP TABLE _ecomm_shipment__base AS
SELECT sku, lpn_id, transload_manifest_file_id, foreign_order_id AS meta_original_order_id, quantity
FROM (SELECT sku,
             lpn_id,
             transload_manifest_file_id,
             foreign_order_id,
             SUM(quantity) AS quantity
      FROM lake.ultra_warehouse.transload_manifest_file_for_mx
      WHERE foreign_order_id IN (SELECT meta_original_order_id FROM _ecomm_shipment__orders_over_800)
      GROUP BY sku, transload_manifest_file_id, foreign_order_id, lpn_id)
QUALIFY ROW_NUMBER() OVER (PARTITION BY foreign_order_id, sku ORDER BY transload_manifest_file_id DESC, lpn_id) = 1;

-- get the PO number from FOL, dim_lpn
CREATE OR REPLACE TEMP TABLE _ecomm_shipment__po_number AS
SELECT DISTINCT e.meta_original_order_id, e.sku, dl.po_number
FROM _ecomm_shipment__base e
         JOIN edw_prod.data_model.dim_lpn dl
              ON e.lpn_id = dl.lpn_id
WHERE e.lpn_id IS NOT NULL

UNION ALL

SELECT DISTINCT edw_prod.stg.udf_unconcat_brand(order_id) AS meta_original_order_id,
       dp.sku,
       dl.po_number
FROM edw_prod.stg.fact_order_line fol
         JOIN edw_prod.stg.dim_lpn dl
              ON dl.lpn_code = fol.lpn_code
         JOIN edw_prod.stg.dim_product dp
              ON dp.product_id = fol.product_id
         JOIN _ecomm_shipment__base e
              ON edw_prod.stg.udf_unconcat_brand(order_id) = e.meta_original_order_id
                  AND e.sku = dp.sku
WHERE e.lpn_id IS NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_prod.stg.udf_unconcat_brand(order_id), dp.sku ORDER BY po_number DESC ) = 1;

-- get product costs
CREATE OR REPLACE TEMP TABLE _ecomm_shipment__product_cost AS
    SELECT pn.*, pdd.cost, pdd.cmt FROM _ecomm_shipment__po_number pn
        JOIN reporting_prod.gsc.po_detail_dataset pdd
ON pn.po_number = pdd.po_number and pn.sku = pdd.sku
QUALIFY ROW_NUMBER() OVER(PARTITION BY pn.meta_original_order_id, pn.sku, pn.po_number ORDER BY pdd.cost DESC, pdd.cmt DESC) = 1;

-- get hts codes from different sources and prioritize accordingly
CREATE OR REPLACE TEMP TABLE _ecomm_shipment__sku_hts AS
SELECT DISTINCT d.meta_original_order_id, d.sku, REPLACE(a.hts_code, '.', '') AS hts_code, 1 AS logic_rank
FROM _ecomm_shipment__base d
         JOIN _ecomm_shipment__po_number p
              ON d.sku = p.sku
         JOIN gsc.po_detail_dataset a
              ON p.sku = d.sku AND p.po_number = a.po_number
WHERE a.hts_code IS NOT NULL
UNION ALL
SELECT DISTINCT d.meta_original_order_id, d.sku, REPLACE(a.hts_code, '.', '') AS hts_code, 2 AS logic_rank
FROM _ecomm_shipment__base d
         JOIN gsc.po_detail_dataset a
              ON d.sku = a.sku
WHERE a.hts_code IS NOT NULL
UNION ALL
SELECT DISTINCT d.meta_original_order_id, d.sku, REPLACE(s.hts_us, '.', ''), 3 AS logic_rank
FROM _ecomm_shipment__base d
         JOIN reporting_prod.gsc.sku_hts s
              ON d.sku = s.sku
WHERE s.hts_us IS NOT NULL
UNION ALL
SELECT DISTINCT d.meta_original_order_id, d.sku, REPLACE(t.hts_code, '.', ''), 4 AS logic_rank
FROM _ecomm_shipment__base d
         JOIN lake.bluecherry.tfg_style_master t
              ON d.sku = t.sku
WHERE t.hts_code IS NOT NULL
UNION ALL
SELECT DISTINCT d.meta_original_order_id, d.sku, REPLACE(t.hts_code, '.', ''), 5 AS logic_rank
FROM _ecomm_shipment__base d
         JOIN lake_view.bluecherry.tfg_po_dtl t
              ON d.sku = t.sku
WHERE t.hts_code IS NOT NULL;


-- try getting duty rate without making any changes on the hts codes
CREATE OR REPLACE TEMP TABLE _ecomm_shipment__duty_rate AS
SELECT s.meta_original_order_id,
       s.sku,
       h.duty_rate,
       h.china_rate,
       h.cotton_rate,
       h.mpf_rate,
       h.hts_code
FROM _ecomm_shipment__sku_hts s
         JOIN reporting_base_prod.gsc.hts_reference h
              ON s.hts_code = h.hts_code
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.meta_original_order_id, s.sku ORDER BY s.logic_rank, h.duty_rate DESC, h.cotton_rate DESC, h.china_rate DESC ,h.hts_code DESC) = 1;


-- try getting duty rate by removing the last 2 digits of the hts codes. Exclude the SKUs with a HTS code we found above.
CREATE OR REPLACE TEMP TABLE _ecomm_shipment__duty_rate_2nd AS
SELECT s.meta_original_order_id,
       s.sku,
       h.duty_rate,
       h.china_rate,
       h.cotton_rate,
       h.mpf_rate,
       h.hts_code,
       s.logic_rank
FROM _ecomm_shipment__sku_hts s
         JOIN reporting_base_prod.gsc.hts_reference h
              ON LEFT(s.hts_code, LEN(s.hts_code) - 2) = h.hts_code
WHERE s.sku NOT IN (SELECT sku FROM _ecomm_shipment__duty_rate)
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.meta_original_order_id, s.sku ORDER BY s.logic_rank, h.duty_rate DESC, h.cotton_rate DESC, h.china_rate DESC ,h.hts_code DESC) = 1;

-- combine duty rates
CREATE OR REPLACE TEMP TABLE _ecomm_shipment__duty_rate_combined AS
SELECT meta_original_order_id, sku, duty_rate, china_rate, cotton_rate, mpf_rate, hts_code
FROM _ecomm_shipment__duty_rate
UNION ALL
SELECT meta_original_order_id, sku, duty_rate, china_rate, cotton_rate, mpf_rate, hts_code
FROM _ecomm_shipment__duty_rate_2nd;

-- product attributes
-- get product attributes from po_detail_dataset by using PO/SKU combination
CREATE OR REPLACE TEMP TABLE _ecomm_shipment__product_attribute AS
SELECT pn.meta_original_order_id,
       pn.sku,
       p.actual_weight,
       p.avg_weight,
       p.country_origin
FROM _ecomm_shipment__po_number pn
         JOIN gsc.po_detail_dataset p
              ON pn.sku = p.sku
                  AND pn.po_number = p.po_number
QUALIFY ROW_NUMBER() OVER (PARTITION BY pn.meta_original_order_id, pn.sku ORDER BY p.actual_weight DESC, p.avg_weight DESC) =
        1;

-- get product attributes from po_detail_dataset by using SKU. Exclude the records we found with PO/SKU combination
CREATE OR REPLACE TEMP TABLE _ecomm_shipment__product_attribute_2nd AS
SELECT e.meta_original_order_id,
       e.sku,
       p.actual_weight,
       p.avg_weight,
       NULL AS country_origin
FROM _ecomm_shipment__base e
         JOIN gsc.po_detail_dataset p
              ON p.sku = e.sku
         LEFT JOIN _ecomm_shipment__product_attribute pa
                   ON e.sku = pa.sku
                       AND e.meta_original_order_id = pa.meta_original_order_id
WHERE pa.sku IS NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY e.meta_original_order_id, e.sku ORDER BY p.actual_weight DESC, p.avg_weight DESC) = 1;

-- combine product attributes
CREATE OR REPLACE TEMP TABLE _ecomm_shipment__product_attribute_combined AS
SELECT meta_original_order_id                                       AS order_id,
       sku,
       IFF(IFNULL(actual_weight, 0) = 0, avg_weight, actual_weight) AS weight,
       country_origin
FROM _ecomm_shipment__product_attribute
UNION ALL
SELECT meta_original_order_id                                       AS order_id,
       sku,
       IFF(IFNULL(actual_weight, 0) = 0, avg_weight, actual_weight) AS weight,
       country_origin
FROM _ecomm_shipment__product_attribute_2nd;


-- create stg table with cleaned up duty rates
CREATE OR REPLACE TEMP TABLE _ecomm_shipment__stg AS
SELECT e.meta_original_order_id,
       e.sku,
       esd.hts_code,
       pac.weight,
       pac.country_origin,
       pc.cost + pc.cmt                                                                                        AS product_value,
       e.quantity                                                                                              AS total_quantity,
       CAST((CASE
                 WHEN esd.duty_rate = 'Free' THEN 0.00
                 WHEN esd.duty_rate ILIKE '%+%'
                     THEN CAST(REPLACE(
                         LTRIM(SUBSTRING(esd.duty_rate, CHARINDEX('+', esd.duty_rate) + 1,
                                         LEN(esd.duty_rate))), '%', '') AS NUMERIC(18, 5))
                 ELSE CAST(REPLACE(esd.duty_rate, '%', '') AS NUMERIC(18, 5)) END) AS NUMERIC(18, 5)) /
       100.00                                                                                                  AS duty_rate,             -- divide 100 is for %

       IFF(esd.duty_rate ILIKE '%kg%',
           LEFT(esd.duty_rate, CHARINDEX('¢', esd.duty_rate) - 1) / 100.00,
           0)                                                                                                  AS duty_amount_per_kg,    -- divide 100 is for cents

       IFF(esd.duty_rate ILIKE '%pr%',
           LEFT(esd.duty_rate, CHARINDEX('¢', esd.duty_rate) - 1) / 100.00,
           0)                                                                                                  AS duty_amount_per_piece, -- divide 100 is for cents

       IFF(esd.china_rate IS NULL, 0.00,
           CAST(REPLACE(esd.china_rate, '%', '') AS NUMERIC(18, 5))) /
       100.00                                                                                                  AS china_rate,
       cotton_rate,
       mpf_rate / 100.00                                                                                       AS mpf_rate
FROM _ecomm_shipment__base e
         LEFT JOIN _ecomm_shipment__product_cost pc
                   ON e.meta_original_order_id = pc.meta_original_order_id AND e.sku  = pc.sku
         LEFT JOIN _ecomm_shipment__duty_rate_combined esd
                   ON e.meta_original_order_id = esd.meta_original_order_id AND e.sku = esd.sku
         LEFT JOIN _ecomm_shipment__product_attribute_combined pac
                   ON e.meta_original_order_id = pac.order_id AND e.sku = pac.sku;


CREATE OR REPLACE TRANSIENT TABLE gsc.cross_border_fee_ecomm AS
SELECT 'ecomm shipment'                                                                       AS order_source,
       CAST(fo.shipped_local_datetime AS DATE)                                                AS shipment_date,
       IFF(ds.store_brand IN ('FabKids', 'ShoeDazzle', 'JustFab'), 'JustFab', ds.store_brand) AS brand,
       c.meta_original_order_id                                                               AS order_id,
       c.sku,
       c.hts_code,
       c.total_quantity,
       c.duty_rate,
       c.duty_amount_per_kg,
       c.duty_amount_per_piece,
       c.weight,
       c.product_value,
       (c.product_value * c.duty_rate) + (c.weight * c.duty_amount_per_kg) +
       c.duty_amount_per_piece                                                                AS total_duty_amount_per_unit,
       ROUND(product_value * c.total_quantity) * mpf_rate / total_quantity                    AS merchant_processing_fee_amount_per_unit,
       c.china_rate,
       c.country_origin,
       IFF(country_origin = 'CN', china_rate * product_value, 0)                              AS china_cost_per_unit,
       cotton_rate,
       cotton_rate * weight                                                                   AS cotton_cost_per_unit,
       $execution_start_time                                                                  AS meta_create_datetime,
       $execution_start_time                                                                  AS meta_update_datetime
FROM _ecomm_shipment__stg c
         LEFT JOIN edw_prod.stg.fact_order fo
                   ON fo.meta_original_order_id = c.meta_original_order_id
         JOIN edw_prod.stg.dim_store ds
              ON ds.store_id = fo.store_id;
