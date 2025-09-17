
CREATE OR REPLACE VIEW data_model_jfb.fact_inventory_in_transit_from_history (
    rollup_date,
    item_id,
    sku,
    from_warehouse_id,
    total_units,
    effective_from_date,
    effective_to_date,
    is_current
    ) AS
SELECT
    rollup_date,
    item_id,
    sku,
    from_warehouse_id,
    SUM(units) AS total_units,
    rollup_date::TIMESTAMP_NTZ(3) AS effective_from_date,
    LEAD(DATEADD(MILLISECOND, -1, rollup_date), 1, '9999-12-31') OVER (PARTITION BY item_id, sku, from_warehouse_id ORDER BY rollup_date)::TIMESTAMP_NTZ(3) AS effective_to_date,
    is_current
FROM stg.fact_inventory_in_transit_history
WHERE NOT is_deleted and item_id = 870592
GROUP BY
    rollup_date,
    item_id,
    sku,
    from_warehouse_id,
    is_current;
