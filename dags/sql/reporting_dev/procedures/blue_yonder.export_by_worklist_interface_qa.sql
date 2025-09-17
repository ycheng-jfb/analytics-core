use reporting_dev;
truncate table reporting_dev.blue_yonder.export_by_worklist_interface_stg;

insert into reporting_dev.blue_yonder.export_by_worklist_interface_stg(
	OPERATION_CODE,
	DEFAULT_WAREHOUSE,
	QUANTITY_PER_PACK,
	PACK_SKU,
	VENDOR_PACK,
	AVAILABLE_PACKS,
	ON_ORDER_PACKS,
	PO_DST_NUMBER,
	PRODUCT_NUMBER,
	MERCHANDISE_SOURCE,
	ASN_NUMBER,
	RECEIPT_DATE,
	EXP_RECEIPT_DATE,
	SKU,
	LINE_SEQUENCE,
	PRODUCT_IMAGE
)
--RESERVE STOCK
select distinct
    1,
    l.location,
    1,
    fi.sku,
    null,
    CASE WHEN rr.reserve_qty < fi.manual_stock_reserve_quantity THEN rr.reserve_qty ELSE fi.manual_stock_reserve_quantity END,--fi.available_to_sell_quantity,
    null,
    l.location,
    null,
    8,
    null,
    null,
    null,
    fi.sku,
    null,
    COALESCE(u1.image_url,u2.image_url)
from
    edw_prod.data_model.fact_inventory fi
    left join reporting_base_prod.blue_yonder.dim_location l
    on fi.warehouse_id = l.warehouse_id and l.channel like 'RETAIL%'
    left join reporting_base_prod.blue_yonder.dim_item i
    on fi.sku = i.item
    left join (select sku, image_url from REPORTING_BASE_PROD.FABLETICS.UBT_MIN_SHOWROOM_DH) u1
    on i.product_sku = u1.sku
    left join (select sku, image_url from REPORTING_BASE_PROD.FABLETICS.UBT_MAX_SHOWROOM_DH) u2
    on i.product_sku = u2.sku
    left join REPORTING_PROD.GFC.VW_RETAIL_REPLEN_RESERVE rr
    on fi.warehouse_id = rr.warehouse_id and fi.sku = rr.item_number
where
    l.location is not null
    and brand in ('FABLETICS','YITTY')
    and l.warehouse_id in (154,231)
    and i.is_retail_ladder = FALSE
    and CASE WHEN rr.reserve_qty < fi.manual_stock_reserve_quantity THEN rr.reserve_qty ELSE fi.manual_stock_reserve_quantity END > 0--and fi.available_to_sell_quantity > 0
    and rr.reserve_qty > 0
    and i.item is not null

union

--PO ON ORDER
select distinct
    1, --operation code
    l.location, --default whse
    1, --qty per pack
    pd."Style-Color", --pack_sku
    null, --vendor_pack
    po.qty, --available_packs
    CASE WHEN (zeroifnull(po.qty) - zeroifnull(po.recv_qty)) < 0 THEN 0 ELSE (zeroifnull(po.qty) - zeroifnull(po.recv_qty)) END, --on_order (for the PO) ordered - received
    CAST(po.po_num_bc as varchar()), --po_dst_number (PO)
    null, --alternative product desc
    5, --merch type (0ASN / 5PO / 8STOCK)
    null, --asn#
    to_varchar(pd."Date Received"::date,'YYYYMMDD'), --receipt_date
    to_varchar(coalesce(pd."fnd/eta",po.show_room||'-01')::date,'YYYYMMDD'), --exp receipt date
    pd."Style-Color",
    po.po_line_number,
    COALESCE(u1.image_url,u2.image_url)
from
    REPORTING_PROD.GSC.PO_DETAIL_DATASET po
    left join REPORTING_PROD.GSC.PULSE_DATASET pd
    on pd.po = po.po_number and (pd.po_line_number = po.bc_po_line_number or pd.po_line_number = po.po_line_number)
    left join reporting_base_prod.blue_yonder.dim_item i
    on pd."Style-Color" = i.item
    left join reporting_base_prod.blue_yonder.dim_location l
    on po.warehouse_id = l.warehouse_id and l.channel like 'RETAIL%'
    left join (select sku, image_url from REPORTING_BASE_PROD.FABLETICS.UBT_MIN_SHOWROOM_DH) u1
    on i.product_sku = u1.sku
    left join (select sku, image_url from REPORTING_BASE_PROD.FABLETICS.UBT_MAX_SHOWROOM_DH) u2
    on i.product_sku = u2.sku
where
    l.location is not null
    and l.warehouse_id in (154,231)
    and po.is_retail = TRUE
    and i.is_retail_ladder = FALSE
    and po.fc_delivery > getdate() --expire after passing FC_DELIVERY
    and TRIM(UPPER(po.po_type)) in ('CORE','DIRECT','CHASE','CAPSULE','RE-ORDER','FASHION','TOP-UP','RETAIL')
    and po.division_id in ('FABLETICS','YITTY')

union

--ASN
select distinct
    1, --operation code
    l.location, --default whse
    1, --qty per pack
    i.item_number, --pack_sku
    null, --vendor_pack
    po.qty, --available_packs
    null, --on_order
    CAST(po.po_num_bc as varchar()), --po_dst_number (PO)
    null, --alternative product desc
    0, --merch type (0ASN / 5PO / 8STOCK)
    it.label, --asn#
    to_varchar(cast(itccd.datetime_added as date),'YYYYMMDD'), --receipt_date
    to_varchar(coalesce(pd."fnd/eta",po.show_room||'-01')::date,'YYYYMMDD'), --exp receipt date
    i.item_number,
    po.po_line_number,
    COALESCE(u1.image_url,u2.image_url)
from
    --ASN section
    lake_view.ultra_warehouse.in_transit it
    inner join lake_view.ultra_warehouse.in_transit_container itc
    on it.in_transit_id = itc.in_transit_id
    inner join lake_view.ultra_warehouse.in_transit_container_case itcc
	on itc.in_transit_container_id = itcc.in_transit_container_id
    inner join lake_view.ultra_warehouse.in_transit_document itd
    on itcc.in_transit_document_id = itd.in_transit_document_id
    inner join lake_view.ultra_warehouse.in_transit_container_case_detail itccd
    on itcc.in_transit_container_case_id = itccd.in_transit_container_case_id
    inner join lake_view.ultra_warehouse.item i
    on itccd.item_id = i.item_id
    --PO+Pulse section
    left join REPORTING_PROD.GSC.PO_DETAIL_DATASET po
    on itd.label = po.po_number and itccd.foreign_po_line_number = po.bc_po_line_number
    left join REPORTING_PROD.GSC.PULSE_DATASET pd
    on po.po_number = pd.po and (pd.po_line_number = po.bc_po_line_number or pd.po_line_number = po.po_line_number)
    left join reporting_base_prod.blue_yonder.dim_location l
    on po.warehouse_id = l.warehouse_id and l.channel like 'RETAIL%'
    left join reporting_base_prod.blue_yonder.dim_item di
    on i.item_number = di.item
    left join (select sku, image_url from REPORTING_BASE_PROD.FABLETICS.UBT_MIN_SHOWROOM_DH) u1
    on di.product_sku = u1.sku
    left join (select sku, image_url from REPORTING_BASE_PROD.FABLETICS.UBT_MAX_SHOWROOM_DH) u2
    on di.product_sku = u2.sku
where
    l.location is not null
    and l.warehouse_id in (154,231)
    and po.is_retail = TRUE
    and di.is_retail_ladder = FALSE
    and TRIM(UPPER(po.po_type)) in ('CORE','DIRECT','CHASE','CAPSULE','RE-ORDER','FASHION','TOP-UP','RETAIL')
    and po.division_id in ('FABLETICS','YITTY')
    and itccd.datetime_added > dateadd("hour",-2,getdate()) --grab all ASN data sent within last 4 hours -- needs to be modified based on cadence of file -- might need to create a watermark for interday incremental rather than hardcode, and one-time "end of day" file should be looking at last 24 hours or full completed day's worth of data
;


update reporting_dev.blue_yonder.export_by_WORKLIST_interface_stg
set
DEFAULT_WAREHOUSE = reporting_prod.blue_yonder.udf_cleanup_field(DEFAULT_WAREHOUSE),
PACK_SKU = reporting_prod.blue_yonder.udf_cleanup_field(PACK_SKU),
VENDOR_PACK = reporting_prod.blue_yonder.udf_cleanup_field(VENDOR_PACK),
PO_DST_NUMBER = reporting_prod.blue_yonder.udf_cleanup_field(PO_DST_NUMBER),
PRODUCT_NUMBER = reporting_prod.blue_yonder.udf_cleanup_field(PRODUCT_NUMBER),
ASN_NUMBER = reporting_prod.blue_yonder.udf_cleanup_field(ASN_NUMBER),
SKU = reporting_prod.blue_yonder.udf_cleanup_field(SKU)
;
