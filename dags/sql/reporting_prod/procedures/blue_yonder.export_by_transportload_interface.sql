use reporting_prod;
TRUNCATE TABLE reporting_prod.blue_yonder.export_by_transportload_interface_stg;

INSERT INTO reporting_prod.blue_yonder.export_by_transportload_interface_stg
(
LOADID,
ARRIVDATE,
SHIPDATE,
TRANSMODE,
SOURCE,
DEST,
ITEM,
SOURCING,
UNITS,
U_STATUS
)
WITH
RI_LPNS AS (
SELECT LPN_CODE, inv.qty_onhand, inv.min_datetime_added, DI.ITEM, DL.warehouse_id, DL.LOCATION AS DEST FROM lake_view.ultra_warehouse.vw_inventory inv
inner join reporting_base_prod.blue_yonder.dim_item di
on inv.item_number = di.item
inner join reporting_base_prod.blue_yonder.dim_location dl
on inv.warehouse_id = dl.warehouse_id
WHERE
    inv.zone_label = 'RI'
    and inv.qty_onhand > 0 --with intransit units
    and left(dl.location,1) = 'R'
    and dl.ff_flag = 1
    and dl.type != '3'
    and di.is_ladder = TRUE
),
LPN_SOURCE AS (
SELECT LPN_CODE, ITEM, QTY_ONHAND, MIN_DATETIME_ADDED, SOURCE, DEST
FROM
(
SELECT RI.LPN_CODE, RI.ITEM, RI.QTY_ONHAND, RI.MIN_DATETIME_ADDED, RI.DEST, DL.FF_FLAG, DL.LOCATION AS SOURCE, ROW_NUMBER() OVER (PARTITION BY RI.LPN_CODE ORDER BY RI.min_datetime_added DESC) AS R
from RI_LPNS ri
inner join reporting_base_prod.ultra_warehouse.vw_inventory_log inv
on inv.lpn_code = ri.lpn_code and inv.datetime_added < ri.min_datetime_added and inv.warehouse_id != ri.warehouse_id
inner join reporting_base_prod.blue_yonder.dim_location dl
on inv.warehouse_id = dl.warehouse_id and left(dl.location,1) = 'R')
WHERE R = 1
and FF_FLAG = 1
)

SELECT
    'IT_' || sl.location || '_' || dl.location || '_' || REPLACE(cast(i.min_datetime_added as date),'-','') as loadid,
    cast(i.min_datetime_added as date) as arrivdate,
    cast(i.min_datetime_added as date) as shipdate,
    'TRUCK' as transmode,
    sl.location as source,
    dl.location as dest,
    i.item_number as item,
    i.item_number || '_' || sl.location || '_' || dl.location || '_TRUCK' as sourcing,
    SUM(qty_onhand) as units,
    'ACTUAL'
FROM
    reporting_base_prod.blue_yonder.dim_item di
    --LAKE_VIEW.ULTRA_WAREHOUSE.VW_INVENTORY i
    inner join lake_view.ultra_warehouse.vw_inventory i
    on di.item = i.item_number
    inner join REPORTING_BASE_PROD.BLUE_YONDER.dim_location sl
    on i.warehouse_id = sl.warehouse_id
    inner join lake_view.ultra_warehouse.location l
    on i.location_id = l.location_id
    inner join REPORTING_BASE_PROD.BLUE_YONDER.dim_location dl
    on l.data_2 = cast(dl.warehouse_id as varchar(50))
WHERE
    i.zone_label = 'In Transit' --intransit location
    and i.qty_onhand > 0 --with intransit units
    and left(sl.location,1) in ('E','R')
    and left(dl.location,1) in ('E','R')
    and dl.ff_flag = 1
    and sl.ff_flag = 1
    and di.is_ladder = TRUE
    and sl.location || dl.location != 'S231E466'
    and sl.type || dl.type != '33'
    and left(sl.location,1) || left(dl.location,1) != 'RE'
    and sl.location not in ('E154','E231')
GROUP BY
    'IT_' || sl.location || '_' || dl.location || '_' || REPLACE(cast(i.min_datetime_added as date),'-',''),
    cast(i.min_datetime_added as date),
    cast(i.min_datetime_added as date),
    'TRUCK',
    sl.location,
    dl.location,
    i.item_number,
    i.item_number || '_' || sl.location || '_' || dl.location || '_TRUCK'

UNION

SELECT
    'OO_' || sl.location || '_' || dl.location || '_' || CASE WHEN REPLACE(cast(f.datetime_estimated_shipment as date),'-','') IS NULL THEN REPLACE(dateadd(day,2,cast(getdate() as date)),'-','') ELSE REPLACE(cast(f.datetime_estimated_shipment as date),'-','') END || '_' || cast(f.foreign_order_id as varchar(15)) as loadid,
    CASE WHEN f.datetime_estimated_shipment IS NULL THEN dateadd(day,2,cast(getdate() as date)) ELSE cast(f.datetime_estimated_shipment as date) END as arrivdate,
    CASE WHEN f.datetime_estimated_shipment IS NULL THEN dateadd(day,2,cast(getdate() as date)) ELSE cast(f.datetime_estimated_shipment as date) END as shipdate,
    'TRUCK' as transmode,
    sl.location as source,
    dl.location as dest,
    i.item_number as item,
    i.item_number || '_' || sl.location || '_' || dl.location || '_TRUCK' as sourcing,
    SUM(quantity - quantity_cancelled) as units,
    'PENDING'
FROM
    lake_view.ultra_warehouse.fulfillment f
    inner join lake_view.ultra_warehouse.invoice inv
    on f.warehouse_id = inv.warehouse_id and f.foreign_order_id = inv.foreign_order_id
    inner join lake_view.ultra_warehouse.fulfillment_item fi
    on f.fulfillment_id = fi.fulfillment_id
    inner join lake_view.ultra_warehouse.item i
    on fi.item_id = i.item_id
    inner join reporting_base_prod.blue_yonder.dim_item di
    on i.item_number = di.item
    inner join REPORTING_BASE_PROD.BLUE_YONDER.dim_location sl
    on f.warehouse_id = sl.warehouse_id
    inner join REPORTING_BASE_PROD.BLUE_YONDER.dim_location dl
    on f.foreign_customer_id = dl.warehouse_id
WHERE
    f.status_code_id not in (143,144,151)
    and inv.status_code_id not in (41,42)
    and source in ('OMS-RETAIL','BY-RETAIL')
    and (quantity - quantity_cancelled > 0)
    and left(sl.location,1) in ('E','R')
    and left(dl.location,1) in ('E','R')
    and dl.ff_flag = 1
    and sl.ff_flag = 1
    and di.is_ladder = TRUE
    and sl.location || dl.location != 'S231E466'
    and sl.type || dl.type != '33'
    and left(sl.location,1) || left(dl.location,1) != 'RE'
    and sl.location not in ('E154','E231')
GROUP BY
'OO_' || sl.location || '_' || dl.location || '_' || CASE WHEN REPLACE(cast(f.datetime_estimated_shipment as date),'-','') IS NULL THEN REPLACE(dateadd(day,2,cast(getdate() as date)),'-','') ELSE REPLACE(cast(f.datetime_estimated_shipment as date),'-','') END || '_' || cast(f.foreign_order_id as varchar(15)),
    CASE WHEN f.datetime_estimated_shipment IS NULL THEN dateadd(day,2,cast(getdate() as date)) ELSE cast(f.datetime_estimated_shipment as date) END,
    CASE WHEN f.datetime_estimated_shipment IS NULL THEN dateadd(day,2,cast(getdate() as date)) ELSE cast(f.datetime_estimated_shipment as date) END,
    'TRUCK',
    sl.location,
    dl.location,
    i.item_number,
    i.item_number || '_' || sl.location || '_' || dl.location || '_TRUCK'

UNION

SELECT
    'RI_' || ls.source || '_' || ls.dest || '_' || REPLACE(cast(ls.min_datetime_added as date),'-','') as loadid,
    cast(ls.min_datetime_added as date) as arrivdate,
    cast(ls.min_datetime_added as date) as shipdate,
    'TRUCK' as transmode,
    ls.source,
    ls.dest,
    ls.item,
    ls.item || '_' || ls.source || '_' || ls.dest || '_TRUCK' as sourcing,
    SUM(qty_onhand) as units,
    'INSPECTION'
FROM
    lpn_source ls
WHERE
    left(source,1) || left(dest,1) != 'RE'
GROUP BY
    'RI_' || ls.source || '_' || ls.dest || '_' || REPLACE(cast(ls.min_datetime_added as date),'-',''),
    cast(ls.min_datetime_added as date),
    ls.source,
    ls.dest,
    ls.item,
    ls.item || '_' || ls.source || '_' || ls.dest || '_TRUCK'
;

update reporting_prod.blue_yonder.export_by_transportLoad_interface_stg
set
LOADID = reporting_prod.blue_yonder.udf_cleanup_field(LOADID),
TRANSMODE = reporting_prod.blue_yonder.udf_cleanup_field(TRANSMODE),
SOURCE = reporting_prod.blue_yonder.udf_cleanup_field(SOURCE),
DEST = reporting_prod.blue_yonder.udf_cleanup_field(DEST),
ITEM = reporting_prod.blue_yonder.udf_cleanup_field(ITEM),
SOURCING = reporting_prod.blue_yonder.udf_cleanup_field(SOURCING)
;

