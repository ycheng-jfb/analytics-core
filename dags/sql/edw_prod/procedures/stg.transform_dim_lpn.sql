SET target_table = 'stg.dim_lpn';
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
SET wm_lake_ultra_warehouse_lpn = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.lpn'));
SET wm_lake_ultra_warehouse_receipt = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.receipt'));
SET wm_lake_ultra_warehouse_item = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.item'));

/*
SELECT
    $wm_self,
    $wm_lake_ultra_warehouse_lpn,
    $wm_lake_ultra_warehouse_receipt,
    $wm_lake_ultra_warehouse_item;
*/

CREATE OR REPLACE TEMP TABLE _dim_lpn__lpn_base(lpn_id int);

-- Full Refresh
INSERT INTO _dim_lpn__lpn_base (lpn_id)
SELECT DISTINCT l.lpn_id
FROM lake.ultra_warehouse.lpn AS l
WHERE $is_full_refresh  /* SET is_full_refresh = TRUE; */
ORDER BY l.lpn_id;

-- Incremental Refresh
INSERT INTO _dim_lpn__lpn_base (lpn_id)
SELECT DISTINCT incr.lpn_id
FROM (
    -- Self-check for manual updates
    SELECT lpn_id
    FROM stg.dim_lpn
    WHERE meta_update_datetime > $wm_self

    UNION ALL

    SELECT l.lpn_id
    FROM lake.ultra_warehouse.lpn AS l
        LEFT JOIN lake.ultra_warehouse.receipt AS r
            ON r.receipt_id = l.receipt_id
        LEFT JOIN lake.ultra_warehouse.item AS i
            ON i.item_id = l.item_id
    WHERE l.hvr_change_time > $wm_lake_ultra_warehouse_lpn
        OR r.hvr_change_time > $wm_lake_ultra_warehouse_receipt
        OR i.hvr_change_time > $wm_lake_ultra_warehouse_item

    UNION ALL /* previously errored rows */

    SELECT lpn_id
    FROM excp.dim_lpn
    WHERE meta_is_current_excp
        AND meta_data_quality = 'error'
    ) AS incr
WHERE NOT $is_full_refresh
    AND incr.lpn_id IS NOT NULL
ORDER BY incr.lpn_id;

CREATE OR REPLACE TEMP TABLE _dim_lpn__pre_stg AS
SELECT
    l.lpn_id,
    l.lpn_code,
    l.warehouse_id,
    l.item_id as item_id_uw,
    i.item_number,
    l.receipt_id,
    UPPER(r.po_number) AS po_number,
    r.datetime_received::TIMESTAMP_TZ(3) AS receipt_received_datetime
FROM _dim_lpn__lpn_base AS base
    JOIN lake.ultra_warehouse.lpn AS l
        ON l.lpn_id = base.lpn_id
    LEFT JOIN lake.ultra_warehouse.receipt AS r
        ON r.receipt_id = l.receipt_id
    LEFT JOIN lake.ultra_warehouse.item AS i
        ON i.item_id = l.item_id;

INSERT INTO stg.dim_lpn_stg (
	lpn_id,
    lpn_code,
    warehouse_id,
    item_id_uw,
    item_number,
    receipt_id,
    po_number,
    receipt_received_datetime,
    meta_create_datetime,
    meta_update_datetime
)
SELECT
    lpn_id,
    lpn_code,
    warehouse_id,
    item_id_uw,
    item_number,
    receipt_id,
    po_number,
    receipt_received_datetime,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _dim_lpn__pre_stg AS stg
ORDER BY lpn_id;
