SET target_table = 'reference.gsc_landed_cost_history';
SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

MERGE INTO stg.meta_table_dependency_watermark AS t
USING
(
    SELECT
        'reference.gsc_landed_cost_history' AS table_name,
        NULLIF(dependent_table_name,'edw_prod.reference.gsc_landed_cost_history') AS dependent_table_name,
        high_watermark_datetime AS new_high_watermark_datetime
    FROM(
            SELECT
                'edw_prod.reference.gsc_landed_cost' AS dependent_table_name,
                max(meta_update_datetime) AS high_watermark_datetime
            FROM reference.gsc_landed_cost
            UNION
            SELECT
                'edw_prod.reference.gsc_landed_cost_history' AS dependent_table_name,
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

SET wm_reference_gsc_landed_cost = (SELECT stg.udf_get_watermark($target_table, 'edw_prod.reference.gsc_landed_cost'));
/*
SELECT
    $wm_reference_gsc_landed_cost
*/

CREATE OR REPLACE TEMP TABLE _gsc_landed_cost_history_delta AS
SELECT
    s.po_number,
    s.po_line_number,
    s.year_month_received,
    s.sku,
    s.plm_style,
    s.fully_landed,
    s.units_received,
    s.actual_landed_cost_per_unit,
    s.po_cost_without_commission,
    s.cmt_cost,
    s.actual_duty_cost_per_unit,
    s.actual_tariff_cost_per_unit,
    s.cpu_freight,
    s.cpu_ocean,
    s.cpu_air,
    s.cpu_transload,
    s.cpu_otr,
    s.cpu_domestic,
    s.cpu_pierpass,
    s.agency_cost_and_po_cost,
    s.history,
    s.total_actual_landed_cost,
    s.is_deleted,
    s.meta_create_datetime,
    s.meta_update_datetime,
    hash(
        s.po_number,
        s.po_line_number,
        s.year_month_received,
        s.sku,
        s.plm_style,
        s.fully_landed,
        s.units_received,
        s.actual_landed_cost_per_unit,
        s.po_cost_without_commission,
        s.cmt_cost,
        s.actual_duty_cost_per_unit,
        s.actual_tariff_cost_per_unit,
        s.cpu_freight,
        s.cpu_ocean,
        s.cpu_air,
        s.cpu_transload,
        s.cpu_otr,
        s.cpu_domestic,
        s.cpu_pierpass,
        s.agency_cost_and_po_cost,
        s.history,
        s.total_actual_landed_cost,
        s.is_deleted
    ) AS meta_row_hash_stg,
    IFF(h.po_number IS NULL, TRUE, FALSE) AS is_new,
    IFF( not equal_null(meta_row_hash_stg, h.meta_row_hash), TRUE, FALSE) AS is_data_changed
FROM reference.gsc_landed_cost s
LEFT JOIN reference.gsc_landed_cost_history h
    ON equal_null(s.po_number , h.po_number)
    AND equal_null(s.po_line_number , h.po_line_number)
    AND equal_null(s.sku , h.sku)
    AND equal_null(s.year_month_received , h.year_month_received)
    AND h.is_current = TRUE
WHERE s.meta_update_datetime > $wm_reference_gsc_landed_cost;

BEGIN TRANSACTION;

-- Insert new records
INSERT INTO reference.gsc_landed_cost_history
(
    po_number,
    po_line_number,
    year_month_received,
    sku,
    plm_style,
    fully_landed,
    units_received,
    actual_landed_cost_per_unit,
    po_cost_without_commission,
    cmt_cost,
    actual_duty_cost_per_unit,
    actual_tariff_cost_per_unit,
    cpu_freight,
    cpu_ocean,
    cpu_air,
    cpu_transload,
    cpu_otr,
    cpu_domestic,
    cpu_pierpass,
    agency_cost_and_po_cost,
    history,
    total_actual_landed_cost,
    effective_start_datetime,
    effective_end_datetime,
    is_current,
    meta_event_datetime,
    meta_row_hash,
    is_deleted,
    meta_create_datetime,
    meta_update_datetime
)
SELECT
    po_number,
    po_line_number,
    year_month_received,
    sku,
    plm_style,
    fully_landed,
    units_received,
    actual_landed_cost_per_unit,
    po_cost_without_commission,
    cmt_cost,
    actual_duty_cost_per_unit,
    actual_tariff_cost_per_unit,
    cpu_freight,
    cpu_ocean,
    cpu_air,
    cpu_transload,
    cpu_otr,
    cpu_domestic,
    cpu_pierpass,
    agency_cost_and_po_cost,
    history,
    total_actual_landed_cost,
    meta_create_datetime AS effective_start_datetime,
    '9999-12-31'::TIMESTAMP_LTZ AS effective_end_datetime,
    TRUE AS is_current,
    meta_update_datetime AS meta_event_datetime,
    meta_row_hash_stg,
    is_deleted,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _gsc_landed_cost_history_delta
WHERE is_new = TRUE;

-- close off existing records

UPDATE reference.gsc_landed_cost_history h
SET h.is_current = FALSE,
    h.meta_update_datetime = $execution_start_time,
    h.effective_end_datetime = DATEADD('millisecond', -1, d.meta_update_datetime)
FROM _gsc_landed_cost_history_delta d
WHERE d.is_new = FALSE
AND d.is_data_changed = TRUE
AND equal_null(d.po_number, h.po_number)
AND equal_null(d.po_line_number , h.po_line_number)
AND equal_null(d.sku , h.sku)
AND equal_null(d.year_month_received , h.year_month_received)
AND h.is_current = TRUE;

-- Insert updated values
INSERT INTO reference.gsc_landed_cost_history
(
    po_number,
    year_month_received,
    sku,
    po_line_number,
    plm_style,
    fully_landed,
    units_received,
    actual_landed_cost_per_unit,
    po_cost_without_commission,
    cmt_cost,
    actual_duty_cost_per_unit,
    actual_tariff_cost_per_unit,
    cpu_freight,
    cpu_ocean,
    cpu_air,
    cpu_transload,
    cpu_otr,
    cpu_domestic,
    cpu_pierpass,
    agency_cost_and_po_cost,
    history,
    total_actual_landed_cost,
    effective_start_datetime,
    effective_end_datetime,
    is_current,
    meta_event_datetime,
    meta_row_hash,
    is_deleted,
    meta_create_datetime,
    meta_update_datetime
)
SELECT
    po_number,
    year_month_received,
    sku,
    po_line_number,
    plm_style,
    fully_landed,
    units_received,
    actual_landed_cost_per_unit,
    po_cost_without_commission,
    cmt_cost,
    actual_duty_cost_per_unit,
    actual_tariff_cost_per_unit,
    cpu_freight,
    cpu_ocean,
    cpu_air,
    cpu_transload,
    cpu_otr,
    cpu_domestic,
    cpu_pierpass,
    agency_cost_and_po_cost,
    history,
    total_actual_landed_cost,
    meta_update_datetime AS effective_start_datetime,
    '9999-12-31'::TIMESTAMP_LTZ AS effective_end_datetime,
    TRUE AS is_current,
    meta_update_datetime AS meta_event_datetime,
    meta_row_hash_stg,
    is_deleted,
    $execution_start_time AS meta_create_datetime,
    $execution_start_time AS meta_update_datetime
FROM _gsc_landed_cost_history_delta
WHERE is_new = FALSE
AND is_data_changed = TRUE;

COMMIT;

UPDATE stg.meta_table_dependency_watermark
SET
    high_watermark_datetime = IFF(
                                    dependent_table_name IS NOT NULL,
                                    new_high_watermark_datetime,
                                    (
                                        SELECT
                                            MAX(meta_update_datetime)
                                        FROM reference.gsc_landed_cost_history
                                    )
                                ),
    meta_update_datetime = $execution_start_time
WHERE table_name = 'reference.gsc_landed_cost_history';
