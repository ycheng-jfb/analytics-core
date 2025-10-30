truncate table EDW_PROD.NEW_STG.fact_ebs_bulk_shipment;

insert into EDW_PROD.NEW_STG.fact_ebs_bulk_shipment
SELECT
    t1.id AS transaction_id,
    t2.sku AS item_number,
    t1.platform AS business_unit,
    t2.sku AS item_id,
    t2.platform_sku AS product_id,
    t1.from_warehouse_code AS facility,
    t1.order_code AS sales_order_id,
    t3.line_number, -- 假设按 transport_code 分组并按 id 排序生成行号
    t1.from_warehouse_name AS from_sub_inventory,
    t2.pairs AS shipped_quantity,
    t1.system_create_time_utc AS system_datetime,
    t1.delivery_time_utc AS shipment_local_datetime
FROM lake.mmt.ods_tms_plus_tms_toc_transport_df t1
JOIN lake.mmt.ods_tms_plus_tms_toc_transport_detail_df t2 ON t1.transport_code = t2.transport_code
and t1.PT_DAY = t2.PT_DAY
    left join lake.mmt.ods_oms_bs_order_item_df t3 on t1.platform_code = t3.order_number
and t1.PT_DAY = t3.PT_DAY
WHERE t1.PT_DAY = current_date;








