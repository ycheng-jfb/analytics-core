use reporting_dev;
truncate table reporting_dev.blue_yonder.export_by_schedrcpts_interface_stg;
insert into reporting_dev.blue_yonder.export_by_schedrcpts_interface_stg
(
ITEM,
LOC,
SCHEDDATE,
UNITS,
COST,
RETAIL,
PO_TYPE,
PO_NUMBER,
VENDOR,
ORIG_UNITS,
SEQNUM,
ORIG_SHOWROOM_DATE
)
select
    po.sku,
    CASE WHEN l.location in ('E154','E231') THEN 'E466' ELSE l.location END,
    coalesce(pd."fnd/eta",pd.showroom_date),
    CASE WHEN (zeroifnull(po.qty) - zeroifnull(po.recv_qty)) < 0 THEN 0 ELSE (zeroifnull(po.qty) - zeroifnull(po.recv_qty)) END,
    CASE WHEN (zeroifnull(po.qty) - zeroifnull(po.recv_qty)) < 0 THEN 0 ELSE (zeroifnull(po.qty) - zeroifnull(po.recv_qty)) END * po.reporting_landed_cost,
    null,
    CASE WHEN po.is_retail THEN 'RETAIL' ELSE 'ECOM' END,
    CAST(po.po_num_bc as varchar(50)),
    po.vend_name,
    ifnull(po.original_issue_qty,po.qty),
    row_number() over(order by 1) SEQNUM,
    po.original_showroom || '-01'
from
    REPORTING_PROD.GSC.PO_DETAIL_DATASET po
    left join REPORTING_PROD.GSC.PULSE_DATASET pd
    on pd.po = po.po_number and (pd.po_line_number = po.bc_po_line_number or pd.po_line_number = po.po_line_number)
    left join reporting_base_prod.blue_yonder.dim_item i
    on pd."Style-Color" = i.item
    left join reporting_base_prod.blue_yonder.dim_location l
    on po.warehouse_id = l.warehouse_id and left(l.location,1) = CASE WHEN po.is_retail = TRUE THEN 'R' ELSE 'E' END
where
    l.location is not null
    and (zeroifnull(po.qty) - zeroifnull(po.recv_qty)) > 0
    and po.fc_delivery > getdate()
    and coalesce(pd."fnd/eta",pd.showroom_date) is not null
    and TRIM(UPPER(po.po_type)) in ('CORE','DIRECT','CHASE','CAPSULE','RE-ORDER','FASHION','TOP-UP','RETAIL')
    and po.line_status not in ('PLAN','PNDAPR','REC')
    and po.division_id in ('FABLETICS','YITTY')
    and l.ff_flag = 1
    and i.is_ladder = TRUE
 ;

update reporting_dev.blue_yonder.export_by_SchedRcpts_interface_stg
set
ITEM = reporting_prod.blue_yonder.udf_cleanup_field(ITEM),
LOC = reporting_prod.blue_yonder.udf_cleanup_field(LOC),
PO_TYPE = reporting_prod.blue_yonder.udf_cleanup_field(PO_TYPE),
PO_NUMBER = reporting_prod.blue_yonder.udf_cleanup_field(PO_NUMBER),
VENDOR = reporting_prod.blue_yonder.udf_cleanup_field(VENDOR)
;
