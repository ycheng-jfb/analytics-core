CREATE OR REPLACE VIEW data_model_jfb.fact_inventory
(
    item_id,
    warehouse_id,
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
        WHEN warehouse_id <> 231 THEN available_to_sell_quantity
        ELSE 0
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
FROM stg.fact_inventory
WHERE NOT COALESCE(is_deleted, FALSE);
