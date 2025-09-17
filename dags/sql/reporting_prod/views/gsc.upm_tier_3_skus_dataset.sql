CREATE OR replace VIEW REPORTING_PROD.GSC.UPM_TIER_3_SKUS(
	"SKU",
    "BRAND"
) AS
WITH _fbl_skus AS(
SELECT
    DISTINCT a.item_number
FROM
    lake_view.ultra_warehouse.item a JOIN lake.excel.fl_items_ubt b
        ON LEFT(item_number, charindex('-', item_number, charindex('-', item_number)+1)-1) = b.sku
ORDER BY
    item_number ASC
),
_gfb_skus AS(
SELECT
    DISTINCT a.item_number
FROM
    lake_view.ultra_warehouse.item a JOIN reporting.gfb.merch_dim_product b
        ON LEFT(item_number, charindex('-', item_number, charindex('-', item_number)+1)-1) = b.product_sku
ORDER BY
    item_number ASC
),
_sxf_skus AS(
SELECT
    DISTINCT a.item_number
FROM
    lake_view.ultra_warehouse.item a JOIN reporting.sxf.style_master b
        ON LEFT(item_number, charindex('-', item_number, charindex('-', item_number)+1)-1) = b.color_sku_po
ORDER BY
    item_number ASC
),
_any_ubt AS(
SELECT
DISTINCT item_number FROM _fbl_skus
UNION
SELECT DISTINCT item_number FROM _sxf_skus
UNION
SELECT DISTINCT item_number FROM _gfb_skus
),
_centric_or_ubt AS(
SELECT
    DISTINCT item_number
FROM lake_view.ultra_warehouse.item
WHERE (
    item_number IN (SELECT DISTINCT node_name FROM lake_view.centric.ed_sku)
        OR
    item_number IN (SELECT DISTINCT item_number FROM _any_ubt))
),
_tier_3 AS(
SELECT
    DISTINCT item_number
FROM
    lake_view.ultra_warehouse.item
WHERE
    item_number NOT IN (SELECT item_number FROM _centric_or_ubt)
)
SELECT t.item_number,
       uwc.company_code
FROM _tier_3 AS t
         LEFT JOIN lake_view.ultra_warehouse.item uwi
                   ON t.item_number = uwi.item_number
         LEFT JOIN lake_view.ultra_warehouse.company uwc ON uwc.company_id = uwi.company_id;