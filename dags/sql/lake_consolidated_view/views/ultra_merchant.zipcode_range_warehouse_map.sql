CREATE OR REPLACE VIEW LAKE_CONSOLIDATED_VIEW.ULTRA_MERCHANT.ZIPCODE_RANGE_WAREHOUSE_MAP  (
    zipcode_range_warehouse_map_id,
    zipcode_start,
    zipcode_end,
    warehouse_id,
    datetime_added,
    datetime_modified,
    store_group_id,
    is_rush_shipping,
    allocation,
    hvr_is_deleted,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    zipcode_range_warehouse_map_id,
    zipcode_start,
    zipcode_end,
    warehouse_id,
    datetime_added,
    datetime_modified,
    store_group_id,
    is_rush_shipping,
    allocation,
    hvr_is_deleted,
    meta_create_datetime,
    meta_update_datetime
FROM LAKE_CONSOLIDATED.ULTRA_MERCHANT.ZIPCODE_RANGE_WAREHOUSE_MAP
WHERE NVL(hvr_is_deleted, 0) = 0;
