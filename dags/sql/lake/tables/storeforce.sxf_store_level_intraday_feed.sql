CREATE TRANSIENT TABLE IF NOT EXISTS storeforce.sxf_store_level_intraday_feed_stg
(
  store_code VARCHAR,
  date VARCHAR,
  slot VARCHAR,
  order_count INT,
  product_gross_revenue NUMBER(19,2),
  unit_count INT,
  refund_orders INT,
  product_refund NUMBER(19,2),
  refund_units INT,
  activating_vip_order_count INT,
  guest_order_count INT,
  repeat_vip_order_count INT,
  metadata_filename VARCHAR,
  snapshot_datetime TIMESTAMP_NTZ(3)
);
