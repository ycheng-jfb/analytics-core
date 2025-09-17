
CREATE OR REPLACE VIEW data_model_jfb.fact_inventory_in_transit_from (
    item_id,
    sku,
    from_warehouse_id,
    total_units
    ) AS
SELECT
    item_id,
    sku,
    from_warehouse_id,
    SUM(units) AS total_units
FROM stg.fact_inventory_in_transit
WHERE NOT is_deleted
GROUP BY
    item_id,
    sku,
    from_warehouse_id;
