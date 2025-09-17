CREATE OR replace VIEW REPORTING_PROD.GSC.UPM_TIER_2_SKUS(
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
in_sxf_not_centric AS (
SELECT DISTINCT item_number FROM lake_view.ultra_warehouse.item WHERE (
    item_number NOT IN (SELECT node_name FROM lake_view.centric.ed_sku)
        AND item_number IN (SELECT item_number FROM _sxf_skus))
),
in_fbl_not_centric AS(
SELECT DISTINCT item_number FROM lake_view.ultra_warehouse.item WHERE (
    item_number NOT IN (SELECT node_name FROM lake_view.centric.ed_sku)
        AND item_number IN (SELECT item_number FROM _fbl_skus))
),
in_gfb_not_centric AS(
SELECT DISTINCT item_number FROM lake_view.ultra_warehouse.item WHERE (
    item_number NOT IN (SELECT node_name FROM lake_view.centric.ed_sku)
        AND item_number IN (SELECT item_number FROM _gfb_skus))
),
_tier_2 AS(
SELECT DISTINCT item_number FROM(
    SELECT DISTINCT item_number FROM in_sxf_not_centric
    UNION
    SELECT DISTINCT item_number FROM in_fbl_not_centric
    UNION
    SELECT DISTINCT item_number FROM in_gfb_not_centric
                       )
)
SELECT t.item_number,
       uwc.company_code
FROM _tier_2 AS t
         LEFT JOIN lake_view.ultra_warehouse.item uwi
                   ON t.item_number = uwi.item_number
         LEFT JOIN lake_view.ultra_warehouse.company uwc ON uwc.company_id = uwi.company_id;