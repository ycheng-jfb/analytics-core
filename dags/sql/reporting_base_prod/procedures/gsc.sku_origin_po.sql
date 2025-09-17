CREATE
OR REPLACE TEMPORARY TABLE _sku
AS
SELECT p.sku,
       p.region_id,
       MIN(p.date_create) AS date_create
FROM gsc.po_skus_data AS p
         JOIN edw_prod.data_model.dim_sku as s --lake_view.merlin.skumaster AS s
              ON p.sku = s.sku
WHERE 1 = 1
  AND p.qty > 0
  AND nvl(p.is_cancelled, 'FALSE') = 'FALSE'
  AND s.meta_create_datetime >= DATEADD(MONTH, -2, CURRENT_TIMESTAMP())
GROUP BY p.sku,
         p.region_id;

CREATE
OR REPLACE TEMPORARY TABLE _sku_ed
AS
SELECT s.*,
       p.po_number,
       ROW_NUMBER() OVER (PARTITION BY s.sku, s.region_id ORDER BY p.po_number ASC) AS rownumber
FROM _sku AS s
         JOIN gsc.po_skus_data AS p
              ON s.sku = p.sku
                  AND s.region_id = p.region_id
                  AND s.date_create = p.date_create
WHERE p.qty > 0
  and nvl(p.is_cancelled, 'FALSE') = 'FALSE'
;

UPDATE gsc.po_skus_data AS p
SET sku_origin_po = 1 FROM _sku_ed AS s
WHERE p.sku = s.sku
  AND p.po_number = s.po_number
  AND p.region_id = s.region_id
  AND p.date_create = s.date_create
  and p.qty
    >0
  and nvl(p.is_cancelled
    , 'FALSE') = 'FALSE'
  AND s.rownumber = 1;
