truncate table EDW_PROD.NEW_STG.dim_lpn;

insert into EDW_PROD.NEW_STG.dim_lpn
select
    t1.id lpn_id,
    t1.logistics_code lpn_code,
    t1.objective_warehouse_code warehouse_id,
    t2.sku,
    t2.pairs,
    null,
    null po_number,
    null receipt_received_datetime
from lake.mmt.ods_tms_plus_tms_logistics_track_df t1
left join lake.mmt.ods_tms_plus_tms_logistics_detail_df t2 on t1.logistics_code = t2.logistics_code
and t1.PT_DAY = t2.PT_DAY
and t1.PT_DAY = current_date
where t2.SKU is not null;
















