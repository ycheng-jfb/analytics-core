use reporting_dev;
truncate table reporting_dev.blue_yonder.export_by_INVENTORY_interface_stg;

insert into reporting_dev.blue_yonder.export_by_INVENTORY_interface_stg(
	ITEM,
	LOC,
	AVAILDATE,
	UNITS,
	OTB_UNITS,
	OTB_COST,
	INTRANSIT_UNITS,
    ORDER_UNITS
)
select
    item,
    loc,
    availdate,
    sum(units),
    sum(otb_units),
    sum(otb_cost),
    sum(intransit_units),
    sum(order_units)
from
(
select
    fi.sku as item,
    CASE WHEN l.location in ('E154','E231') THEN 'E466' ELSE l.location END as loc,
    dateadd("day",-1,fi.local_date) as availdate,
    CASE WHEN loc in ('R154','R231') THEN
        CASE WHEN rr.reserve_qty < fi.manual_stock_reserve_quantity THEN rr.reserve_qty
        ELSE fi.manual_stock_reserve_quantity END
    ELSE iff(fi.available_to_sell_quantity < 0,0,fi.available_to_sell_quantity) END as units,
    CASE WHEN loc in ('R154','R231') THEN 0 ELSE fi.open_to_buy_quantity + fi.intransit_quantity END as otb_units,
    CASE WHEN loc in ('R154','R231') THEN 0 ELSE (fi.open_to_buy_quantity + fi.intransit_quantity) * fi.landed_cost_per_unit END as otb_cost,
    fi.intransit_quantity + fi.receipt_inspection_quantity as intransit_units,
    0 as order_units
from
    edw_prod.data_model.fact_inventory_history fi
    left join reporting_prod.gfc.vw_retail_replen_reserve rr
    on fi.warehouse_id = rr.warehouse_id and fi.sku = rr.item_number
    left join reporting_base_prod.blue_yonder.dim_location l
    on fi.warehouse_id = l.warehouse_id
    inner join reporting_base_prod.blue_yonder.dim_item i
    on fi.sku = i.item
where
    l.location is not null
    and fi.local_date = cast(getdate() as date)
    and (
    (fi.open_to_buy_quantity + fi.intransit_quantity) > 0 or
    fi.available_to_sell_quantity > 0 or
    (fi.intransit_quantity + fi.receipt_inspection_quantity) > 0
    )
    and fi.brand in ('FABLETICS','YITTY')

UNION ALL

    SELECT
    	di.ITEM,
        CASE WHEN l.location in ('E154','E231') THEN 'E466' ELSE l.location END as loc,
    	cast(dateadd("day",-1,getdate()) as date),
    	0,
    	0,
    	0,
        0,
        SUM(quantity - quantity_cancelled)
    FROM
        lake_view.ultra_warehouse.fulfillment f
        inner join lake_view.ultra_warehouse.invoice inv
        on f.warehouse_id = inv.warehouse_id and f.foreign_order_id = inv.foreign_order_id
        inner join lake_view.ultra_warehouse.fulfillment_item fi
        on f.fulfillment_id = fi.fulfillment_id
        inner join lake_view.ultra_warehouse.item i
        on fi.item_id = i.item_id
        left join reporting_base_prod.blue_yonder.dim_location l
        on f.foreign_customer_id = l.warehouse_id and left(l.location,1) = 'R'
        inner join reporting_base_prod.blue_yonder.dim_item di
        on i.item_number = di.item
    WHERE
        l.location is not null
        and f.status_code_id not in (143,144,151)
        and inv.status_code_id not in (41,42)
        and f.source in ('OMS-RETAIL','BY-RETAIL')
        and (quantity - quantity_cancelled > 0)
    GROUP BY
        di.item,
        CASE WHEN l.location in ('E154','E231') THEN 'E466' ELSE l.location END
)
group by
    item,
    loc,
    availdate
;

update reporting_dev.blue_yonder.export_by_INVENTORY_interface_stg
set
ITEM = reporting_prod.blue_yonder.udf_cleanup_field(ITEM),
LOC = reporting_prod.blue_yonder.udf_cleanup_field(LOC)
;
