BEGIN TRANSACTION NAME GSC_LPN_DETAIL_DATASET;

TRUNCATE TABLE REPORTING_PROD.GSC.LPN_DETAIL_DATASET;

INSERT INTO REPORTING_PROD.GSC.LPN_DETAIL_DATASET
(LPN, SKU, PO)
select
	l.lpn_code as LPN,
    i.item_number as SKU,
    coalesce(r.po_number,las.po_number) as PO
from
	lake_view.ultra_warehouse.lpn l
    JOIN lake_view.ultra_warehouse.item i
    on l.item_id = i.item_id
	left join lake_view.ultra_warehouse.receipt r
	on l.receipt_id = r.receipt_id
	left join lake_view.ultra_warehouse.las_po las
	on l.las_po_id = las.las_po_id;

COMMIT;
