SET target_table  = 'reference.gfc_po_sku_line_lpn_mapping';
SET execution_start_time = CURRENT_TIMESTAMP :: TIMESTAMP_LTZ(3);
SET is_full_refresh = (SELECT IFF(stg.udf_get_watermark($target_table, NULL) = '1900-01-01'::TIMESTAMP_LTZ(3), TRUE, FALSE));

MERGE INTO stg.meta_table_dependency_watermark AS w
    USING (
        SELECT $target_table                                               AS table_name,
               NULLIF(dependent_table_name, 'reference.gfc_po_sku_line_lpn_mapping') AS dependent_table_name,
               high_watermark_datetime::timestamp_ltz                                     AS new_high_watermark_datetime
        FROM (
                 SELECT -- For self table
                        'reference.gfc_po_sku_line_lpn_mapping' AS dependent_table_name,
                        '1900-01-01'                  AS high_watermark_datetime

                 UNION ALL

                 SELECT 'lake.ultra_warehouse.receipt'      AS dependent_table_name,
                        MAX(hvr_change_time) AS high_watermark_datetime
                 FROM lake.ultra_warehouse.receipt

                 UNION ALL

                 SELECT 'lake.ultra_warehouse.receipt_item'      AS dependent_table_name,
                        MAX(hvr_change_time) AS high_watermark_datetime
                 FROM lake.ultra_warehouse.receipt_item

                 UNION ALL

                 SELECT 'lake.ultra_warehouse.item'      AS dependent_table_name,
                        MAX(hvr_change_time) AS high_watermark_datetime
                 FROM lake.ultra_warehouse.item

                 UNION ALL

                 SELECT 'lake.ultra_warehouse.receipt_item_container'      AS dependent_table_name,
                        MAX(meta_update_datetime) AS high_watermark_datetime
                 FROM lake.ultra_warehouse.receipt_item_container

                 UNION ALL

                 SELECT 'lake.ultra_warehouse.lpn'      AS dependent_table_name,
                        MAX(hvr_change_time) AS high_watermark_datetime
                 FROM lake.ultra_warehouse.lpn


             ) AS t
        ORDER BY COALESCE(t.dependent_table_name, '')
    ) AS s
    ON w.table_name = s.table_name
        AND EQUAL_NULL(w.dependent_table_name, s.dependent_table_name)
    WHEN NOT MATCHED THEN
        INSERT (
                table_name,
                dependent_table_name,
                high_watermark_datetime,
                new_high_watermark_datetime,
                meta_create_datetime,
                meta_update_datetime
            )
            VALUES (s.table_name,
                    s.dependent_table_name,
                    '1900-01-01', -- current high_watermark_datetime
                    s.new_high_watermark_datetime,
                    $execution_start_time,
                    $execution_start_time)

    WHEN MATCHED AND NOT EQUAL_NULL(w.new_high_watermark_datetime, s.new_high_watermark_datetime) THEN
        UPDATE
            SET w.new_high_watermark_datetime = s.new_high_watermark_datetime,
                w.meta_update_datetime = $execution_start_time;

/*
-- Initial Load / Full Refresh
UPDATE stg.meta_table_dependency_watermark
SET high_watermark_datetime = '1900-01-01',
    meta_update_datetime = CURRENT_TIMESTAMP()
WHERE table_name = $target_table;
*/

-- Use watermark variables for each dependent table to allow pruning of micro-partitions which doesn't happen with UDFs.
SET wm_lake_ultra_warehouse_receipt = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.receipt'));
SET wm_lake_ultra_warehouse_receipt_item = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.receipt_item'));
SET wm_lake_ultra_warehouse_item = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.item'));
SET wm_lake_ultra_warehouse_receipt_item_container = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.receipt_item_container'));
SET wm_lake_ultra_warehouse_lpn = (SELECT stg.udf_get_watermark($target_table, 'lake.ultra_warehouse.lpn'));
SET wm_edw_reference_gfc_po_sku_line_lpn_mapping = (SELECT stg.udf_get_watermark($target_table, NULL));

/*

SELECT
     $wm_lake_ultra_warehouse_receipt,
     $wm_lake_ultra_warehouse_receipt_item,
     $wm_lake_ultra_warehouse_item,
     $wm_lake_ultra_warehouse_receipt_item_container,
     $wm_lake_ultra_warehouse_lpn,
     $wm_edw_reference_gfc_po_sku_line_lpn_mapping;
*/

CREATE OR REPLACE TEMP TABLE _gfc_po_sku_line_lpn_mapping__lpn_base (lpn_id INT) CLUSTER BY (lpn_id);

INSERT INTO _gfc_po_sku_line_lpn_mapping__lpn_base (lpn_id)
-- Full Refresh
SELECT l.lpn_id
FROM lake.ultra_warehouse.lpn AS l
WHERE $is_full_refresh = TRUE

UNION ALL
-- Incremental Refresh
SELECT DISTINCT incr.lpn_id
FROM (
        SELECT l.lpn_id
        FROM lake.ultra_warehouse.lpn l
        INNER JOIN lake.ultra_warehouse.receipt_item_container ric
            ON l.lpn_id = ric.lpn_id
        INNER JOIN lake.ultra_warehouse.receipt_item ri
            ON ric.receipt_item_id = ri.receipt_item_id
        INNER JOIN lake.ultra_warehouse.receipt r
            ON ri.receipt_id = r.receipt_id
        INNER JOIN lake.ultra_warehouse.item i
            ON l.item_id = i.item_id
        WHERE
            r.status_code_id != 199
            AND r.label not like '%-C'
            AND
            (
                l.hvr_change_time > $wm_lake_ultra_warehouse_lpn OR
                ric.meta_update_datetime > $wm_lake_ultra_warehouse_receipt_item_container OR
                ri.hvr_change_time > $wm_lake_ultra_warehouse_receipt_item OR
                r.hvr_change_time > $wm_lake_ultra_warehouse_receipt OR
                i.hvr_change_time > $wm_lake_ultra_warehouse_item
            )

        UNION ALL

        SELECT l.lpn_id
        FROM lake.ultra_warehouse.lpn l
        INNER JOIN lake.ultra_warehouse.receipt r
            ON l.receipt_id = r.receipt_id
        INNER JOIN lake.ultra_warehouse.receipt_item ri
            ON r.receipt_id = ri.receipt_id
            AND ri.item_id = l.item_id
        INNER JOIN lake.ultra_warehouse.item i
            ON l.item_id = i.item_id
        WHERE
            r.status_code_id != 199
            AND r.label NOT LIKE '%-C'
            AND
            (
                l.hvr_change_time > $wm_lake_ultra_warehouse_lpn OR
                r.hvr_change_time > $wm_lake_ultra_warehouse_receipt OR
                ri.hvr_change_time > $wm_lake_ultra_warehouse_receipt_item OR
                i.hvr_change_time > $wm_lake_ultra_warehouse_item
            )

        UNION ALL

        SELECT g.lpn_id
        FROM reference.gfc_po_sku_line_lpn_mapping g
        WHERE g.meta_update_datetime > $wm_edw_reference_gfc_po_sku_line_lpn_mapping
        AND g.lpn_id IS NOT NULL
    ) AS incr
WHERE NOT $is_full_refresh;

CREATE OR REPLACE TEMP TABLE _gfc_po_sku_line_lpn_mapping_stg
(
    lpn_id INT,
    lpn_code VARCHAR,
    item_id INT,
    sku VARCHAR,
    po_number VARCHAR,
    po_line_number INT,
    is_deleted BOOLEAN
);

INSERT INTO _gfc_po_sku_line_lpn_mapping_stg
(
    lpn_id,
    lpn_code,
    item_id,
    sku,
    po_number,
    po_line_number,
    is_deleted
)
SELECT DISTINCT
    lpn_id,
    lpn_code,
    item_id,
    sku,
    po_number,
    po_line_number,
    FALSE AS is_deleted
FROM
    (
        SELECT
            l.lpn_id,
            l.lpn_code,
            ri.item_id,
            i.item_number as sku,
            UPPER(TRIM(r.po_number)) as po_number,
            NULLIF(ri.foreign_po_line_number,'')::INT as po_line_number,
            i.item_id as org_item_id
        FROM _gfc_po_sku_line_lpn_mapping__lpn_base lb
        INNER JOIN lake.ultra_warehouse.lpn l
            ON l.lpn_id = lb.lpn_id
        INNER JOIN lake.ultra_warehouse.receipt_item_container ric
            ON l.lpn_id = ric.lpn_id
        INNER JOIN lake.ultra_warehouse.receipt_item ri
            ON ric.receipt_item_id = ri.receipt_item_id
        INNER JOIN lake.ultra_warehouse.receipt r
            ON ri.receipt_id = r.receipt_id
        INNER JOIN lake.ultra_warehouse.item i
            ON l.item_id = i.item_id
        WHERE
            r.status_code_id != 199
            AND r.label not like '%-C'

        UNION ALL

        SELECT
            l.lpn_id,
            l.lpn_code,
            ri.item_id,
            i.item_number as sku,
            UPPER(TRIM(r.po_number)) as po_number,
            NULLIF(ri.foreign_po_line_number,'')::INT as po_line_number,
            i.item_id as org_item_id
        FROM _gfc_po_sku_line_lpn_mapping__lpn_base lb
        INNER JOIN lake.ultra_warehouse.lpn l
            ON l.lpn_id = lb.lpn_id
        INNER JOIN lake.ultra_warehouse.receipt r
            ON l.receipt_id = r.receipt_id
        INNER JOIN lake.ultra_warehouse.receipt_item ri
            ON r.receipt_id = ri.receipt_id
            AND ri.item_id = l.item_id
        INNER JOIN lake.ultra_warehouse.item i
            ON l.item_id = i.item_id
        WHERE
            r.status_code_id != 199
            AND r.label NOT LIKE '%-C'
) as l
QUALIFY ROW_NUMBER() OVER (PARTITION BY lpn_id ORDER BY IFF(EQUAL_NULL(org_item_id, item_id), 1, 2)) = 1 ;

BEGIN TRANSACTION;

UPDATE reference.gfc_po_sku_line_lpn_mapping tgt
SET tgt.is_deleted = TRUE,
    tgt.meta_update_datetime = $execution_start_time
WHERE $is_full_refresh
AND NOT EXISTS (
    SELECT 1 FROM _gfc_po_sku_line_lpn_mapping_stg src
    WHERE tgt.lpn_id = src.lpn_id
    );

MERGE INTO reference.gfc_po_sku_line_lpn_mapping t
USING (
       SELECT
            lpn_id,
            lpn_code,
            sku,
            item_id,
            po_number,
            po_line_number,
            is_deleted,
            hash(*) AS meta_row_hash
       FROM _gfc_po_sku_line_lpn_mapping_stg
    ) s
ON t.lpn_id = s.lpn_id
WHEN NOT MATCHED THEN
    INSERT (
        lpn_id,
        lpn_code,
        sku,
        item_id,
        po_number,
        po_line_number,
        is_deleted,
        meta_row_hash,
        meta_create_datetime,
        meta_update_datetime
    )
VALUES (
        s.lpn_id,
        s.lpn_code,
        s.sku,
        s.item_id,
        s.po_number,
        s.po_line_number,
        s.is_deleted,
        s.meta_row_hash,
        $execution_start_time,
        $execution_start_time
    )
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash
THEN UPDATE SET
        t.lpn_code = s.lpn_code,
        t.sku = s.sku,
        t.po_number = s.po_number,
        t.po_line_number = s.po_line_number,
        t.is_deleted = s.is_deleted,
        t.meta_row_hash = s.meta_row_hash,
        t.meta_update_datetime = $execution_start_time;

COMMIT;

UPDATE stg.meta_table_dependency_watermark
SET
    high_watermark_datetime = IFF(
                                    dependent_table_name IS NOT NULL,
                                    new_high_watermark_datetime,
                                    (
                                        SELECT
                                            MAX(meta_update_datetime)
                                        FROM reference.gfc_po_sku_line_lpn_mapping
                                    )
                                ),
    meta_update_datetime = $execution_start_time
WHERE table_name = 'reference.gfc_po_sku_line_lpn_mapping';
