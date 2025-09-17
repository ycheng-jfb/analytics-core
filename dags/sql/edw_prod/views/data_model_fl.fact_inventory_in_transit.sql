
CREATE OR REPLACE VIEW data_model_fl.fact_inventory_in_transit
(
    item_id,
    sku,
    from_warehouse_id,
    to_warehouse_id,
    units,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    item_id,
    sku,
    from_warehouse_id,
    to_warehouse_id,
    units,
    meta_create_datetime,
    meta_update_datetime
FROM stg.fact_inventory_in_transit
WHERE NOT is_deleted;
