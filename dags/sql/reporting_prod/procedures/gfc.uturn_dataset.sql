create or replace temporary table _inventory (
fc varchar(25),
case_id bigint,
case_code varchar(50),
lpn_id bigint,
lpn_code varchar(50),
item_number varchar(50),
location_label varchar(25)
);

insert into _inventory
select
    'TIJ',
    case_id,
	case_code,
    lpn_id,
	lpn_code,
	item_number,
    location_label
from
	reporting_base_prod.ultra_warehouse.vw_inventory i
where
    i.warehouse_id = 466 --Only select TIJ2 inventory
	and i.qty_onhand > 0;

create or replace temporary table _putaway(
case_id int,
lpn_id int,
putaway_datetime datetime
);

insert into _putaway
select
i.case_id,
i.lpn_id,
min(coalesce(vilL.datetime_added,vilC.datetime_added))
from
    _inventory i
	left join reporting_base_prod.ultra_warehouse.vw_inventory_log vilC
        on vilC.warehouse_id in (465,466)
        and vilC.object = 'putaway_detail'
        and i.case_id = vilC.case_id
	left join reporting_base_prod.ultra_warehouse.vw_inventory_log vilL
        on vilL.warehouse_id in (465,466)
        and vilL.object in ('lpn','putaway_detail')
        and i.lpn_id = vilL.lpn_id
group by
    i.case_id, i.lpn_id;

create or replace temporary table _uturn(
case_id bigint,
lpn_id bigint,
uturn_datetime datetime
);

insert into _uturn
select
i.case_id,
i.lpn_id,
max(coalesce(vil1L.datetime_added,vil1C.datetime_added))
from
    _inventory i
	left join reporting_base_prod.ultra_warehouse.vw_inventory_log vil1C
        on i.case_id = vil1C.case_id
        and vil1C.warehouse_id = 563
    JOIN reporting_base_prod.ultra_warehouse.vw_inventory_log vil2C
        on vil1C.inventory_log_id = vil2C.parent_inventory_log_id
        and vil2C.warehouse_id = 466
	left join reporting_base_prod.ultra_warehouse.vw_inventory_log vil1L
        on i.lpn_id = vil1L.lpn_id
        and vil1L.warehouse_id = 563
    JOIN reporting_base_prod.ultra_warehouse.vw_inventory_log vil2L
        on vil1L.inventory_log_id = vil2L.parent_inventory_log_id
        and vil2L.warehouse_id = 466
group by
    i.case_id, i.lpn_id;


create or replace transient table reporting_prod.gfc.uturn_dataset
as
select
    i.fc,
    i.case_code,
    i.lpn_code,
	i.item_number,
    i.location_label,
    coalesce(uturn_datetime,putaway_datetime) as fc_putaway_datetime,
	current_timestamp() as refresh_datetime
from
	_inventory i
	left join _putaway p
	    on NVL(i.case_id,1) = NVL(p.case_id,1)
        and NVL(i.lpn_id,1) = NVL(p.lpn_id,1)
	left join _uturn u
	    on NVL(i.case_id,1) = NVL(u.case_id,1)
        and NVL(i.lpn_id,1) = NVL(u.lpn_id,1);
