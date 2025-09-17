CREATE OR REPLACE VIEW data_model_sxf.fact_inventory_history
(
    item_id,
    warehouse_id,
    rollup_datetime_added,
    local_date,
    brand,
    region,
    is_retail,
    sku,
    product_sku,
    onhand_quantity,
    replen_quantity,
    ghost_quantity,
    reserve_quantity,
    special_pick_quantity,
    manual_stock_reserve_quantity,
    available_to_sell_quantity,
    ecom_available_to_sell_quantity,
    receipt_inspection_quantity,
    return_quantity,
    damaged_quantity,
    damaged_returns_quantity,
    allocated_quantity,
    intransit_quantity,
    staging_quantity,
    pick_staging_quantity,
    lost_quantity,
    open_to_buy_quantity,
    landed_cost_per_unit,
    total_onhand_landed_cost,
    dsw_dropship_quantity,
    meta_create_datetime,
    meta_update_datetime
) AS
SELECT
    item_id,
    warehouse_id,
    rollup_datetime_added,
    local_date,
    brand,
    CASE
        WHEN warehouse_id IN (107, 154, 421) THEN 'US-EC'
        WHEN warehouse_id = 231 THEN 'US-WC'
        WHEN warehouse_id = 109 THEN 'CA'
        WHEN warehouse_id IN (108, 221, 366) THEN 'EU'
        WHEN region_id = 8 THEN 'RTL'
        WHEN region_id = 11 THEN 'MX' END AS region,
    CASE WHEN region_id = 8 THEN TRUE ELSE FALSE END AS is_retail,
    sku,
    UPPER(TRIM(SUBSTRING(sku, 1, CHARINDEX('-', sku, CHARINDEX('-', sku) + 1) - 1))) AS product_sku,
    onhand_quantity,
    replen_quantity,
    ghost_quantity,
    reserve_quantity,
    special_pick_quantity,
    manual_stock_reserve_quantity,
    available_to_sell_quantity,
    CASE
        WHEN local_date < '2023-04-10' THEN available_to_sell_quantity
        WHEN local_date >= '2023-04-10' AND local_date < '2024-02-14' THEN
            CASE
                WHEN warehouse_id = 231 THEN 0 -- Perris
                ELSE available_to_sell_quantity
            END
        WHEN local_date >= '2024-02-14' AND local_date < '2025-01-03' THEN
            CASE
                WHEN warehouse_id IN (231, 154, 107) THEN 0 -- Kentucky and Perris
                ELSE available_to_sell_quantity
            END
        WHEN local_date >= '2025-01-03' THEN
            CASE
                WHEN warehouse_id = 231 THEN 0 -- Only Perris
                ELSE available_to_sell_quantity
            END
        ELSE available_to_sell_quantity
    END AS ecom_available_to_sell_quantity,
    receipt_inspection_quantity,
    return_quantity,
    damaged_quantity,
    damaged_returns_quantity,
    allocated_quantity,
    intransit_quantity,
    staging_quantity,
    pick_staging_quantity,
    lost_quantity,
    open_to_buy_quantity,
    landed_cost_per_unit,
    ROUND(landed_cost_per_unit * onhand_quantity, 2) AS total_onhand_landed_cost,
    dsw_dropship_quantity,
    meta_create_datetime,
    meta_update_datetime
FROM stg.fact_inventory_history
WHERE NOT COALESCE(is_deleted, FALSE);
