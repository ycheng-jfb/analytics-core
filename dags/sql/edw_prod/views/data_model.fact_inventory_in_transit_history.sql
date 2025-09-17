
CREATE OR REPLACE VIEW data_model.fact_inventory_in_transit_history
(
    rollup_date,
    item_id,
    sku,
    from_warehouse_id,
    to_warehouse_id,
    units,
    is_current,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    rollup_date,
    item_id,
    sku,
    from_warehouse_id,
    to_warehouse_id,
    units,
    is_current,
    meta_create_datetime,
    meta_update_datetime
FROM stg.fact_inventory_in_transit_history
WHERE NOT is_deleted;
