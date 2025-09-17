CREATE OR REPLACE VIEW LAKE_FL_VIEW.ULTRA_MERCHANT.ZIPCODE_RANGE_WAREHOUSE_MAP  (
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
    hvr_change_time
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
    hvr_change_time
FROM LAKE_FL.ULTRA_MERCHANT.ZIPCODE_RANGE_WAREHOUSE_MAP
WHERE NVL(hvr_is_deleted, 0) = 0;
