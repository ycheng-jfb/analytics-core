use reporting_prod;
truncate table reporting_prod.blue_yonder.export_by_INVENTORY_TRANSACTION_interface_stg;

insert into reporting_prod.blue_yonder.export_by_INVENTORY_TRANSACTION_interface_stg(
	ITEM,
	LOC,
	STARTDATE,
	TRANSACTIONCODE,
	UNITS,
	COST,
    RETAIL
)
select
    item,
    location,
    receipt_date,
    transactioncode,
    sum(received),
    sum(cost),
    sum(retail)
from (
select distinct
    po.po_number,
    i.item,
    i.class,
    CASE WHEN po.is_retail = True THEN 'RETAIL_' ELSE 'ECOM_' END || l.region as location,
    cast(r.receipt_datetime as date) as receipt_date,
    31 as transactioncode,
    received,
    po.weighted_lc_rpt * received as cost,
    CASE
           WHEN po.is_retail = True THEN first_value(ubt.US_VIP_DOLLAR) over (partition by ubt.sku order by current_showroom DESC)
           WHEN po.region_id = 'EU' and po.warehouse_id in ('221','366') THEN first_value(ubt.EU_VIP_DOLLAR) over (partition by ubt.sku order by current_showroom DESC)
           WHEN po.region_id in ('US', 'CA') THEN first_value(ubt.US_VIP_DOLLAR) over (partition by ubt.sku order by current_showroom DESC)
    END * received as retail
from
    reporting_prod.gfc.receipt_detail_dataset r
    left join reporting_prod.gsc.po_detail_dataset_agg po
    on r.po_number = po.po_number and r.item_number = po.sku
    left join reporting_base_prod.blue_yonder.dim_location l
    on po.warehouse_id = l.warehouse_id and left(l.location,1) = CASE WHEN po.is_retail = TRUE THEN 'R' ELSE 'E' END
    inner join reporting_base_prod.blue_yonder.dim_item i
    on r.item_number = i.item
    left join lake_view.excel.fl_merch_items_ubt_hierarchy ubt
    ON CASE
        WHEN LENGTH(UBT.SKU) = 15
        then left(po.sku, 15) = ubt.sku
        WHEN LENGTH(UBT.SKU) = 14
        then left(po.sku, 14) = ubt.sku
    end
    and po.show_room >= left(ubt.current_showroom, 7)
where
    cast(r.receipt_datetime as date) = cast(dateadd("day",-1,getdate()) as date)
    and TRIM(UPPER(po.po_type)) in ('CORE','DIRECT','CHASE','CAPSULE','RE-ORDER','FASHION','TOP-UP','RETAIL')
    and po.division_id in ('FABLETICS','YITTY')
    and l.warehouse_id is not null
) q1
group by
    item,
    location,
    receipt_date,
    transactioncode

UNION

select
    class,
    channel,
    showroom,
    transactioncode,
    sum(units_total),
    sum(cost),
    sum(retail)
from
(
select
    coalesce(i.class,x5.dream_state_class,x4.dream_state_class,x3.dream_state_class,x2.dream_state_class,x1.dream_state_class,uh.class) as class,
    CASE WHEN bp.channel = 'Web' THEN 'ECOM_' WHEN bp.channel = 'Retail' THEN 'RETAIL_' END ||
    CASE WHEN bp.region in ('EU','UK') THEN 'EU' WHEN bp.region in ('US','MX') THEN 'NA' ELSE 'XX' END as channel,
    dateadd("month",-1,CASE WHEN len(bp.showroom)< 7 THEN '1900-01-01' ELSE bp.showroom||'-01' END) as showroom,
    99 as transactioncode,
    units_total,
    LDP_BLEND * units_total as cost,
    VIP * units_total as retail
from
    LAKE_VIEW.CENTRIC.TFG_BUY_PLAN bp
    left join (select distinct product_sku, class from reporting_base_prod.blue_yonder.dim_item) i
    on bp.base_sku = i.product_sku
    left join lake_view.excel.fl_merch_items_ubt_hierarchy ubt
    on bp.base_sku = ubt.sku and CASE WHEN len(bp.showroom)< 7 THEN '1900-01-01' ELSE bp.showroom||'-01' END = ubt.current_showroom
left join REPORTING_BASE_PROD.BLUE_YONDER.DREAM_STATE_XREF x1
    on  x1.department = UPPER(TRIM(bp.department)) and
        x1.sub_dept = UPPER(TRIM(bp.sub_department)) and
        x1.class = UPPER(TRIM(bp.class)) and
        x1.sub_class is null
        and x1.silhouette is null
        and x1.end_use is null
left join REPORTING_BASE_PROD.BLUE_YONDER.DREAM_STATE_XREF x2
    on  x2.department = UPPER(TRIM(bp.department)) and
        x2.sub_dept = UPPER(TRIM(bp.sub_department)) and
        x2.class = UPPER(TRIM(bp.class)) and
        x2.sub_class = UPPER(TRIM(bp.sub_class))
        and x2.silhouette is null
        and x2.end_use is null
left join REPORTING_BASE_PROD.BLUE_YONDER.DREAM_STATE_XREF x3
    on  x3.department = UPPER(TRIM(bp.department)) and
        x3.sub_dept = UPPER(TRIM(bp.sub_department)) and
        x3.class = UPPER(TRIM(bp.class)) and
        x3.sub_class is null
        and x3.silhouette = UPPER(TRIM(bp.silhouette))
        and x3.end_use is null
left join REPORTING_BASE_PROD.BLUE_YONDER.DREAM_STATE_XREF x4
    on  x4.department = UPPER(TRIM(bp.department)) and
        x4.sub_dept = UPPER(TRIM(bp.sub_department)) and
        x4.class = UPPER(TRIM(bp.class)) and
        x4.sub_class is null
        and x4.silhouette is null
        and x4.end_use = UPPER(TRIM(bp.end_use))
left join REPORTING_BASE_PROD.BLUE_YONDER.DREAM_STATE_XREF x5
    on  x5.department = UPPER(TRIM(bp.department)) and
        x5.sub_dept = UPPER(TRIM(bp.sub_department)) and
        x5.class = UPPER(TRIM(bp.class)) and
        x5.sub_class = UPPER(TRIM(bp.sub_class))
        and x5.silhouette = UPPER(TRIM(bp.silhouette))
        and x5.end_use is null
    left join LAKE_VIEW.sharepoint.DREAMSTATE_UBT_HIERARCHY uh
        on UPPER(TRIM(bp.class)) = uh.class
where
    units_total > 0
    and len(bp.showroom) > 5
    and dateadd("month",1,CASE WHEN len(bp.showroom)< 7 THEN '1900-01-01' ELSE bp.showroom||'-01' END) > getdate()
    and bp.department != 'YITTY ACTIVE'
    and (i.class is not null or x5.dream_state_class is not null or x4.dream_state_class is not null or x3.dream_state_class is not null or x2.dream_state_class is not null or x1.dream_state_class is not null or uh.class is not null)
) q1
group by
    class,
    channel,
    showroom,
    transactioncode;

update reporting_prod.blue_yonder.export_by_INVENTORY_TRANSACTION_interface_stg
set
ITEM = reporting_prod.blue_yonder.udf_cleanup_field(ITEM),
LOC = reporting_prod.blue_yonder.udf_cleanup_field(LOC)
;
