SET target_table = 'stg.fact_inventory_in_transit';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

/*
-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
SET is_full_refresh = TRUE;
*/

-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_self = (SELECT stg.udf_get_watermark($target_table, NULL));
SET wm_lake_ultra_warehouse_inventory = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.inventory'));
SET wm_lake_ultra_warehouse_inventory_location = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.inventory_location'));
SET wm_lake_ultra_warehouse_location = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.location'));
SET wm_lake_ultra_warehouse_zone = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.zone'));
SET wm_lake_ultra_warehouse_code = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.code'));
SET wm_lake_ultra_warehouse_lpn = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.lpn'));
SET wm_lake_ultra_warehouse_case_item = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.case_item'));
SET wm_lake_ultra_warehouse_item = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.item'));

/*
SELECT
    $wm_self,
    $wm_lake_ultra_warehouse_inventory,
    $wm_lake_ultra_warehouse_inventory_location,
    $wm_lake_ultra_warehouse_location,
    $wm_lake_ultra_warehouse_zone,
    $wm_lake_ultra_warehouse_code,
    $wm_lake_ultra_warehouse_lpn,
    $wm_lake_ultra_warehouse_case_item,
    $wm_lake_ultra_warehouse_item;
*/

CREATE OR REPLACE TEMP TABLE _fact_inventory_in_transit__item_base (item_id INT, sku VARCHAR(30));

-- Full Refresh
INSERT INTO _fact_inventory_in_transit__item_base (item_id, sku)
SELECT DISTINCT it.item_id, it.item_number AS sku
FROM lake_view.ultra_warehouse.inventory AS i
    JOIN lake_view.ultra_warehouse.inventory_location AS il
        ON il.inventory_location_id = i.inventory_location_id
    JOIN lake_view.ultra_warehouse.location AS l
        ON l.location_id = il.location_id
    JOIN lake_view.ultra_warehouse.zone AS z
        ON z.zone_id = l.zone_id
    JOIN lake_view.ultra_warehouse.code AS c
        ON c.code_id = z.type_code_id
        AND UPPER(c.label) = 'IN TRANSIT'
    LEFT JOIN lake_view.ultra_warehouse.lpn AS lpn
        ON lpn.lpn_id = i.lpn_id
    LEFT JOIN lake_view.ultra_warehouse.case_item AS ci
        ON ci.case_id = i.case_id
    LEFT JOIN lake_view.ultra_warehouse.item AS it
        ON it.item_id = COALESCE(ci.item_id, lpn.item_id)
WHERE $is_full_refresh  /* SET is_full_refresh = TRUE; */
    AND it.item_id IS NOT NULL
ORDER BY it.item_id;
-- SELECT COUNT(1) FROM _fact_inventory_in_transit__item_base;

-- Incremental Refresh
INSERT INTO _fact_inventory_in_transit__item_base (item_id, sku)
SELECT DISTINCT incr.item_id, incr.sku
FROM (
    /* Self-check for manual updates */
    SELECT fit.item_id, fit.sku
    FROM stg.fact_inventory_in_transit AS fit
    WHERE fit.meta_update_datetime > $wm_self
    UNION ALL
    /* Check for dependency table updates */
    SELECT it.item_id, it.item_number AS sku
    FROM lake_view.ultra_warehouse.inventory AS i
        JOIN lake_view.ultra_warehouse.inventory_location AS il
            ON il.inventory_location_id = i.inventory_location_id
        JOIN lake_view.ultra_warehouse.location AS l
            ON l.location_id = il.location_id
        JOIN lake_view.ultra_warehouse.zone AS z
            ON z.zone_id = l.zone_id
        JOIN lake_view.ultra_warehouse.code AS c
            ON c.code_id = z.type_code_id
            AND UPPER(c.label) = 'IN TRANSIT'
        LEFT JOIN lake_view.ultra_warehouse.lpn AS lpn
            ON lpn.lpn_id = i.lpn_id
        LEFT JOIN lake_view.ultra_warehouse.case_item AS ci
            ON ci.case_id = i.case_id
        LEFT JOIN lake_view.ultra_warehouse.item AS it
            ON it.item_id = COALESCE(ci.item_id, lpn.item_id)
    WHERE (
        i.hvr_change_time > $wm_lake_ultra_warehouse_inventory
        OR il.hvr_change_time > $wm_lake_ultra_warehouse_inventory_location
        OR l.hvr_change_time > $wm_lake_ultra_warehouse_location
        OR z.hvr_change_time > $wm_lake_ultra_warehouse_zone
        OR c.hvr_change_time > $wm_lake_ultra_warehouse_code
        OR lpn.hvr_change_time > $wm_lake_ultra_warehouse_lpn
        OR ci.hvr_change_time > $wm_lake_ultra_warehouse_case_item
        OR it.hvr_change_time > $wm_lake_ultra_warehouse_item
        )
    UNION ALL
    /* Previously errored rows */
    SELECT item_id, sku
    FROM excp.fact_inventory_in_transit
    WHERE meta_is_current_excp
        AND meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh
    AND incr.item_id IS NOT NULL
ORDER BY incr.item_id;

CREATE OR REPLACE TEMP TABLE _fact_inventory_in_transit__pre_stg AS
SELECT
    base.item_id,
    base.sku,
    COALESCE(l.warehouse_id, -1) AS from_warehouse_id,
    COALESCE(TRY_TO_NUMBER(l.data_2), -1) AS to_warehouse_id,
    SUM(COALESCE(ci.quantity, i.qty_onhand)) AS units
FROM lake_view.ultra_warehouse.inventory AS i
    LEFT JOIN lake_view.ultra_warehouse.lpn AS lpn
        ON lpn.lpn_id = i.lpn_id
    LEFT JOIN lake_view.ultra_warehouse.case_item AS ci
        ON ci.case_id = i.case_id
    JOIN _fact_inventory_in_transit__item_base AS base
        ON base.item_id = COALESCE(ci.item_id, lpn.item_id)
    JOIN lake_view.ultra_warehouse.inventory_location AS il
        ON il.inventory_location_id = i.inventory_location_id
    JOIN lake_view.ultra_warehouse.location AS l
        ON l.location_id = il.location_id
    JOIN lake_view.ultra_warehouse.zone AS z
        ON z.zone_id = l.zone_id
    JOIN lake_view.ultra_warehouse.code AS c
        ON c.code_id = z.type_code_id
        AND UPPER(c.label) = 'IN TRANSIT'
GROUP BY
    base.item_id,
    base.sku,
    COALESCE(l.warehouse_id, -1),
    COALESCE(TRY_TO_NUMBER(l.data_2), -1);

CREATE OR REPLACE TEMP TABLE _fact_inventory_in_transit__stg AS
SELECT /* History with no matching values in underlying table */
    src.item_id,
    src.sku,
    src.from_warehouse_id,
    src.to_warehouse_id,
    src.units,
    TRUE AS is_deleted,
    src.meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM stg.fact_inventory_in_transit AS src
    JOIN _fact_inventory_in_transit__item_base AS base
        ON base.item_id = src.item_id
WHERE NOT src.is_deleted
    AND NOT EXISTS ( /* Compare using all the fields that make the table row unique */
        SELECT 1
        FROM _fact_inventory_in_transit__pre_stg AS stg
        WHERE stg.item_id = src.item_id
            AND stg.from_warehouse_id = src.from_warehouse_id
            AND stg.to_warehouse_id = src.to_warehouse_id
        )
UNION ALL
SELECT
    item_id,
    sku,
    from_warehouse_id,
    to_warehouse_id,
    units,
    FALSE AS is_deleted,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _fact_inventory_in_transit__pre_stg AS stg;
-- SELECT * FROM _fact_inventory_in_transit__stg;
-- SELECT COUNT(1) FROM _fact_inventory_in_transit__stg;
-- SELECT item_id, from_warehouse_id, to_warehouse_id, COUNT(1) FROM _fact_inventory_in_transit__stg GROUP BY 1, 2, 3 HAVING COUNT(1) > 1;

INSERT INTO stg.fact_inventory_in_transit_stg (
    item_id,
	sku,
    from_warehouse_id,
    to_warehouse_id,
    units,
    is_deleted,
    meta_create_datetime,
    meta_update_datetime
    )
SELECT
    item_id,
	sku,
    from_warehouse_id,
    to_warehouse_id,
    units,
    is_deleted,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _fact_inventory_in_transit__stg
ORDER BY item_id;
-- SELECT COUNT(1) FROM stg.fact_inventory_in_transit_stg;
