CREATE VIEW lake_view.storeforce.sxf_store_level_intraday_feed AS
SELECT
    store_code,
    date,
    slot,
    order_count,
    product_gross_revenue,
    unit_count,
    refund_orders,
    product_refund,
    refund_units,
    activating_vip_order_count,
    guest_order_count,
    repeat_vip_order_count,
    metadata_filename,
    snapshot_datetime,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
FROM lake.storeforce.sxf_store_level_intraday_feed;
