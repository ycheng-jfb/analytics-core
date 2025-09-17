SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMP TABLE _gfc_inventory_cost_stg AS
SELECT
    warehouse_id,
    region_id,
    sku,
    inventory_cost,
    FALSE AS is_deleted,
    hash(warehouse_id, region_id, sku, inventory_cost, FALSE) AS meta_row_hash,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM reporting_base_prod.gfc.inventory_cost_dataset;

BEGIN TRANSACTION;

UPDATE reference.gfc_inventory_cost t
    SET t.is_deleted = TRUE,
        t.meta_row_hash = hash(t.warehouse_id, t.region_id, t.sku, t.inventory_cost, TRUE),
        t.meta_update_datetime = $execution_start_time
WHERE NOT NVL(t.is_deleted, FALSE)
AND NOT EXISTS(SELECT 1 FROM _gfc_inventory_cost_stg AS s
    WHERE equal_null(t.warehouse_id, s.warehouse_id)
    AND equal_null(t.region_id, s.region_id)
    AND equal_null(t.sku, s.sku));

MERGE INTO reference.gfc_inventory_cost t
USING  _gfc_inventory_cost_stg s
    ON equal_null(t.warehouse_id, s.warehouse_id)
    AND equal_null(t.region_id, s.region_id)
    AND equal_null(t.sku, s.sku)
WHEN NOT MATCHED THEN INSERT (
    warehouse_id,
    region_id,
    sku,
    inventory_cost,
    is_deleted,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
)
VALUES (
    warehouse_id,
    region_id,
    sku,
    inventory_cost,
    is_deleted,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
    )
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash
THEN UPDATE
    SET
        t.inventory_cost = s.inventory_cost,
        t.is_deleted = s.is_deleted,
        t.meta_row_hash = s.meta_row_hash,
        t.meta_update_datetime = $execution_start_time;
COMMIT;
