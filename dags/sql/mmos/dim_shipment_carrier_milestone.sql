truncate table EDW_PROD.NEW_STG.dim_shipment_carrier_milestone;

insert into EDW_PROD.NEW_STG.dim_shipment_carrier_milestone
SELECT
  d.logistics_code AS order_id,
  d.logistics_split_code AS tracking_number,
  d.CARGO_OWNER_CODE AS carrier_service_id, -- 可选：来自业务系统
  t.shipping_time AS order_shipped_local_datetime,
  'TMS' AS carrier_ingestion_source, -- 固定值或可配置字段
  'TMS' AS carrier_delivered_source, -- 固定值或可配置字段
  d.arrival_time AS carrier_delivered_local_datetime,
  d.create_time AS carrier_ingestion_local_datetime,
  t.free_time AS returned_local_datetime,
  COALESCE(t.first_shipping_time, t.shipping_time) AS en_route_local_datetime,
  CASE WHEN d.abnormal_quantity > 0 THEN d.create_time END AS shipment_damaged_local_datetime,
  t.expect_warehouse_time AS estimated_delivery_local_datetime,
  t.arrival_transit_time AS arrived_at_terminal_local_datetime,
  t.start_receive_time AS shipment_acknowledged_local_datetime,
  CASE WHEN t.is_intercept = 1 THEN d.create_time END AS shipment_cancelled_local_datetime,
  NULL AS shipment_delayed_local_datetime, -- 暂无对应字段，可留空或后续扩展
  d.create_time AS meta_create_datetime,
  d.modify_time AS meta_update_datetime
FROM lake.mmt.ods_tms_plus_tms_logistics_detail_df d
LEFT JOIN lake.mmt.ods_tms_plus_tms_logistics_track_df t
  ON d.logistics_code = t.logistics_code
 and d.PT_DAY = t.PT_DAY
where d.PT_DAY = current_date;




