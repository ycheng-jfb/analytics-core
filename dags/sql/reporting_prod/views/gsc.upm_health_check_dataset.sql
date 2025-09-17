CREATE OR replace VIEW REPORTING_PROD.GSC.UPM_HEALTH_CHECK_DATASET(
	"SKU",
	"Latest_Showroom",
	"In_Centric",
	"In_UBT",
	"In_PO_DETAIL_DATASET",
	"In_Ultra_Warehouse"
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
_tier_3_in_po AS(
SELECT a.sku AS item_number, MAX(b.show_room) AS latest_showroom,
       0 AS in_centric,
       0 AS in_ubt,
       1 AS in_po_detail_dataset,
       1 AS in_ultra_warehouse
FROM REPORTING_PROD.GSC.UPM_TIER_3_SKUS a JOIN reporting.gsc.po_detail_dataset b ON a.sku = b.sku GROUP BY 1
),
_tier_3_no_po AS(
SELECT a.sku AS item_number, MAX(concat(DATE_PART(YEAR , datetime_added), '-', DATE_PART(MM, datetime_added))) AS latest_showroom,
       0 AS in_centric,
       0 AS in_ubt,
       0 AS in_po_detail_dataset,
       1 AS in_ultra_warehouse
FROM REPORTING_PROD.GSC.UPM_TIER_3_SKUS a JOIN lake_view.ultra_warehouse.item b ON a.sku = b.item_number
WHERE a.sku NOT IN (SELECT DISTINCT item_number FROM _tier_3_po) GROUP BY 1
),
_tier_1_all AS(
SELECT
    sku AS item_number,
    last_showroom AS latest_showroom,
    1 AS in_centric,
    1 AS in_ubt,
    1 AS in_po_detail_dataset,
    1 AS in_ultra_warehouse
FROM
    edw_prod.data_model.dim_universal_product
),
 _tier_2_in_po AS(
SELECT
    a.item_number,
    MAX(b.show_room) AS latest_showroom,
    0 AS in_centric,
    1 AS in_ubt,
    1 AS in_po_detail_dataset,
    1 AS in_ultra_warehouse
FROM
    _any_ubt a LEFT JOIN reporting.gsc.po_detail_dataset b ON a.item_number = b.sku
WHERE item_number NOT IN (SELECT DISTINCT sku FROM edw_prod.data_model.dim_universal_product)
GROUP BY
    1
HAVING MAX(b.show_room) IS NOT NULL
),
_tier_2_no_po AS(
SELECT
    a.item_number,
    MAX(concat(DATE_PART(YEAR , datetime_added), '-', DATE_PART(MM, datetime_added))) AS latest_showroom,
    0 AS in_centric,
    1 AS in_ubt,
    0 AS in_po_detail_dataset,
    1 AS in_ultra_warehouse
FROM
    _any_ubt a LEFT JOIN lake_view.ultra_warehouse.item b ON a.item_number = b.item_number
WHERE
    a.item_number NOT IN (SELECT DISTINCT sku FROM edw_prod.data_model.dim_universal_product)
    AND a.item_number NOT IN (SELECT DISTINCT item_number FROM _tier_2_in_po)
GROUP BY
    1
HAVING MAX(concat(DATE_PART(YEAR , datetime_added), '-', DATE_PART(MM, datetime_added))) IS NOT NULL
)
SELECT item_number,
       lastest_showroom,
       in_centric,
       in_ubt,
       in_po_detail_dataset,
       in_ultra_warehouse
FROM _tier_1_all
UNION
SELECT item_number,
       lastest_showroom,
       in_centric,
       in_ubt,
       in_po_detail_dataset,
       in_ultra_warehouse
FROM _tier_2_in_po
UNION
SELECT item_number,
       lastest_showroom,
       in_centric,
       in_ubt,
       in_po_detail_dataset,
       in_ultra_warehouse
FROM _tier_2_no_po
UNION
SELECT item_number,
       lastest_showroom,
       in_centric,
       in_ubt,
       in_po_detail_dataset,
       in_ultra_warehouse
FROM _tier_3_in_po
UNION
SELECT item_number,
       lastest_showroom,
       in_centric,
       in_ubt,
       in_po_detail_dataset,
       in_ultra_warehouse
FROM _tier_3_no_po;