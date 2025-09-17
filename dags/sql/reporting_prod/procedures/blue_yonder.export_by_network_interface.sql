use reporting_prod;
truncate table reporting_prod.blue_yonder.export_by_network_interface_stg;

insert into reporting_prod.blue_yonder.export_by_network_interface_stg
(
	SOURCE,
	DEST,
	TRANSMODE,
	ITEM,
	SOURCING,
	TRANSLEADTIME
)
select
    dls.location as source,
    dld.location as dest,
    'TRUCK' as transmode,
    i.item,
    i.item || '_' || dls.location || '_' || dld.location || '_TRUCK' as sourcing,
    tts.transit_time_sla * 1440 as transit_time_sla
from
    reporting_base_prod.blue_yonder.dim_item i
    left join reporting_base_prod.blue_yonder.dim_location dls
    on dls.warehouse_id in (154,231) and left(dls.location,1) = 'R'
    left join lake_view.ultra_warehouse.warehouse w
    on dls.warehouse_id = w.warehouse_id
    left join reporting_base_prod.blue_yonder.dim_location dld
    on dld.channel like 'RETAIL%'
        and dld.location like 'R%'
        and dld.store_id is not null
    join reporting_base_prod.fabletics.dim_store_extended ds
        on dld.store_id = ds.store_id
    join (
        select tts.zipcode, tts.airport_code, transit_time_sla
        , row_number() over(partition by tts.zipcode order by tts.transit_time_sla asc, airport_code desc, service_type desc) as sla
        from reporting_base_prod.reference.transit_time_sla tts
        where tts.service_type in ('FedEx Home Delivery','Retail Replen 2 Day')
    ) tts
        on ds.store_retail_zip_code = tts.zipcode
        and case
            when w.airport_code = 'ONT'
            then 'TIJ2'
            when w.airport_code = 'SDF1'
            then 'SDF2'
            else w.airport_code
        end = tts.airport_code
        and tts.sla = 1
where
    i.is_ladder = TRUE
    and dld.ff_flag = 1

UNION

select
    dls.location as source,
    dld.location as dest,
    'TRUCK' as transmode,
    i.item,
    i.item || '_' || dls.location || '_' || dld.location || '_TRUCK' as sourcing,
    0
from
    reporting_base_prod.blue_yonder.dim_item i
    left join reporting_base_prod.blue_yonder.dim_location dls
    on dls.warehouse_id in (466,221,366) and dls.location like 'E%'
    left join reporting_base_prod.blue_yonder.dim_location dld
    on dld.channel like 'ECOM%'
        and dld.location like 'S%'
        and dld.store_id is not null
        and (
        (dls.warehouse_id = 466 and dls.region = dld.region) or
        (dls.warehouse_id = 221 and dld.store_id not in (67,153) and dls.region = dld.region) or
        (dls.warehouse_id = 366 and dld.store_id in (67,153))
        )
    join reporting_base_prod.fabletics.dim_store_extended ds
        on dld.store_id = ds.store_id
where
    i.is_ladder = TRUE
    and dld.ff_flag = 1
;

update reporting_prod.blue_yonder.export_by_NETWORK_interface_stg
set
SOURCE = reporting_prod.blue_yonder.udf_cleanup_field(SOURCE),
DEST = reporting_prod.blue_yonder.udf_cleanup_field(DEST),
TRANSMODE = reporting_prod.blue_yonder.udf_cleanup_field(TRANSMODE),
ITEM = reporting_prod.blue_yonder.udf_cleanup_field(ITEM),
SOURCING = reporting_prod.blue_yonder.udf_cleanup_field(SOURCING)
;
