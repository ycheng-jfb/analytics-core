SET target_table = 'reference.gsc_po_detail_dataset_history';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

MERGE INTO stg.meta_table_dependency_watermark AS t
USING
(
    SELECT
        'reference.gsc_po_detail_dataset_history' AS table_name,
        NULLIF(dependent_table_name,'edw_prod.reference.gsc_po_detail_dataset_history') AS dependent_table_name,
        high_watermark_datetime AS new_high_watermark_datetime
    FROM(
            SELECT
                'edw_prod.reference.gsc_po_detail_dataset' AS dependent_table_name,
                max(meta_update_datetime) AS high_watermark_datetime
            FROM reference.gsc_po_detail_dataset
            UNION
            SELECT
                'edw_prod.reference.gsc_po_detail_dataset_history' AS dependent_table_name,
                '1900-01-01'::TIMESTAMP_LTZ AS high_watermark_datetime
        ) h
) AS s
ON t.table_name = s.table_name
    AND equal_null(t.dependent_table_name, s.dependent_table_name)
WHEN MATCHED
    AND not equal_null(t.new_high_watermark_datetime, s.new_high_watermark_datetime)
THEN
    UPDATE
        SET t.new_high_watermark_datetime = s.new_high_watermark_datetime,
            t.meta_update_datetime = $execution_start_time
WHEN NOT MATCHED
THEN
    INSERT
    (
        table_name,
        dependent_table_name,
        high_watermark_datetime,
        new_high_watermark_datetime
    )
    VALUES
    (
        s.table_name,
        s.dependent_table_name,
        '1900-01-01'::timestamp_ltz,
        s.new_high_watermark_datetime
    );

SET wm_reference_gsc_po_detail_dataset = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.reference.gsc_po_detail_dataset'));
/*
SELECT
    $wm_reference_gsc_po_detail_dataset
*/

CREATE OR REPLACE TEMP TABLE _gsc_po_detail_dataset_history_delta AS
SELECT
    s.po_id,
    s.po_dtl_id,
    s.po_number,
    s.sku,
    s.po_line_number,
    s.warehouse_id,
    s.brand,
    s.po_status_text,
    s.qty,
    s.freight,
    s.duty,
    s.cmt,
    s.cost,
    s.landed_cost_estimated,
    s.actual_landed_cost,
    s.reporting_landed_cost,
    s.fc_delivery,
    s.xfd,
    s.delivery,
    s.date_launch,
    s.date_create,
    s.po_status_id,
    s.line_status,
    s.show_room,
    s.is_deleted,
    s.meta_create_datetime,
    s.meta_update_datetime,
    hash(
             s.po_id,
             s.po_dtl_id,
             s.po_number,
             s.sku,
             s.po_line_number,
             s.warehouse_id,
             s.brand,
             s.po_status_text,
             s.qty,
             s.freight,
             s.duty,
             s.cmt,
             s.cost,
             s.landed_cost_estimated,
             s.actual_landed_cost,
             s.reporting_landed_cost,
             s.fc_delivery,
             s.xfd,
             s.delivery,
             s.date_launch,
             s.date_create,
             s.po_status_id,
             s.line_status,
             s.show_room,
             s.is_deleted
    ) as meta_row_hash_stg,
    IFF(h.po_dtl_id IS NULL, TRUE, FALSE) AS is_new,
    IFF( not equal_null(meta_row_hash_stg, h.meta_row_hash), TRUE, FALSE) AS is_data_changed
FROM reference.gsc_po_detail_dataset s
LEFT JOIN reference.gsc_po_detail_dataset_history h
    ON s.po_dtl_id = h.po_dtl_id
    AND s.po_id = h.po_id
    AND h.is_current = TRUE
WHERE s.meta_update_datetime > $wm_reference_gsc_po_detail_dataset;

BEGIN TRANSACTION;

-- Insert new records
INSERT INTO reference.gsc_po_detail_dataset_history
(
    po_id,
    po_dtl_id,
    po_number,
    sku,
    po_line_number,
    warehouse_id,
    brand,
    po_status_text,
    qty,
    freight,
    duty,
    cmt,
    cost,
    landed_cost_estimated,
    actual_landed_cost,
    reporting_landed_cost,
    fc_delivery,
    xfd,
    delivery,
    date_launch,
    date_create,
    po_status_id,
    line_status,
    show_room,
    is_deleted,
    effective_start_datetime,
    effective_end_datetime,
    is_current,
    meta_event_datetime,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
)
SELECT
    d.po_id,
    d.po_dtl_id,
    d.po_number,
    d.sku,
    d.po_line_number,
    d.warehouse_id,
    d.brand,
    d.po_status_text,
    d.qty,
    d.freight,
    d.duty,
    d.cmt,
    d.cost,
    d.landed_cost_estimated,
    d.actual_landed_cost,
    d.reporting_landed_cost,
    d.fc_delivery,
    d.xfd,
    d.delivery,
    d.date_launch,
    d.date_create,
    d.po_status_id,
    d.line_status,
    d.show_room,
    d.is_deleted,
    d.meta_create_datetime AS effective_start_datetime,
    '9999-12-31'::TIMESTAMP_LTZ AS effective_end_datetime,
    TRUE AS is_current,
    d.meta_update_datetime AS meta_event_datetime,
    d.meta_row_hash_stg,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _gsc_po_detail_dataset_history_delta d
WHERE is_new = TRUE;

-- close off existing records

UPDATE reference.gsc_po_detail_dataset_history h
SET h.is_current = FALSE,
    h.meta_update_datetime = $execution_start_time,
    h.effective_end_datetime = DATEADD('millisecond', -1, d.meta_update_datetime)
FROM _gsc_po_detail_dataset_history_delta d
WHERE d.is_new = FALSE
AND d.is_data_changed = TRUE
AND d.po_dtl_id = h.po_dtl_id
AND d.po_id = h.po_id
AND h.is_current = TRUE;

-- Insert updated values
INSERT INTO reference.gsc_po_detail_dataset_history
(
    po_id,
    po_dtl_id,
    po_number,
    sku,
    po_line_number,
    warehouse_id,
    brand,
    po_status_text,
    qty,
    freight,
    duty,
    cmt,
    cost,
    landed_cost_estimated,
    actual_landed_cost,
    reporting_landed_cost,
    fc_delivery,
    xfd,
    delivery,
    date_launch,
    date_create,
    po_status_id,
    line_status,
    show_room,
    is_deleted,
    effective_start_datetime,
    effective_end_datetime,
    is_current,
    meta_event_datetime,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
)
SELECT
    d.po_id,
    d.po_dtl_id,
    d.po_number,
    d.sku,
    d.po_line_number,
    d.warehouse_id,
    d.brand,
    d.po_status_text,
    d.qty,
    d.freight,
    d.duty,
    d.cmt,
    d.cost,
    d.landed_cost_estimated,
    d.actual_landed_cost,
    d.reporting_landed_cost,
    d.fc_delivery,
    d.xfd,
    d.delivery,
    d.date_launch,
    d.date_create,
    d.po_status_id,
    d.line_status,
    d.show_room,
    d.is_deleted,
    d.meta_update_datetime AS effective_start_datetime,
    '9999-12-31'::TIMESTAMP_LTZ AS effective_end_datetime,
    TRUE AS is_current,
    d.meta_update_datetime AS meta_event_datetime,
    d.meta_row_hash_stg,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _gsc_po_detail_dataset_history_delta d
WHERE d.is_new = FALSE
AND d.is_data_changed = TRUE;

COMMIT;

UPDATE stg.meta_table_dependency_watermark
SET
    high_watermark_datetime = IFF(
                                    dependent_table_name IS NOT NULL,
                                    new_high_watermark_datetime,
                                    (
                                        SELECT
                                            MAX(meta_update_datetime)
                                        FROM reference.gsc_po_detail_dataset_history
                                    )
                                ),
    meta_update_datetime = $execution_start_time
WHERE table_name = 'reference.gsc_po_detail_dataset_history';
