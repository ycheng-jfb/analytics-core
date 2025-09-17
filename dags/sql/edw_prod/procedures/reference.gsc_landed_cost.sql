SET execution_start_time = CURRENT_TIMESTAMP::TIMESTAMP_LTZ(3);

CREATE OR REPLACE TEMP TABLE _gsc_landed_cost_dataset_stg AS
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
    FALSE AS is_deleted,
    hash(
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
        FALSE) AS meta_row_hash,
        $execution_start_time AS meta_create_datetime,
        $execution_start_time AS meta_update_datetime
    FROM reporting_prod.gsc.landed_cost_dataset
WHERE po_number IS NOT NULL AND sku IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY po_number, year_month_received, sku, po_line_number ORDER BY fully_landed DESC,units_shipped DESC,units_received DESC, actual_landed_cost_per_unit DESC, po_cost_without_commission DESC, IFNULL(actual_duty_cost_per_unit, 0) + IFNULL(actual_tariff_cost_per_unit, 0) DESC, IFNULL(cpu_freight, 0) + IFNULL(cpu_ocean, 0) + IFNULL(cpu_air, 0) DESC) = 1;

BEGIN TRANSACTION;

-- soft delete orphan records
UPDATE reference.gsc_landed_cost glc
SET
    glc.is_deleted = TRUE,
    glc.meta_row_hash =   hash(
                                    po_number,year_month_received,sku,po_line_number,plm_style,fully_landed,units_received,
                                    actual_landed_cost_per_unit,po_cost_without_commission,cmt_cost,
                                    actual_duty_cost_per_unit,actual_tariff_cost_per_unit,cpu_freight,cpu_ocean,
                                    cpu_air,cpu_transload,cpu_otr,cpu_domestic,cpu_pierpass,agency_cost_and_po_cost,
                                    history,total_actual_landed_cost,TRUE
                                ),
    glc.meta_update_datetime = $execution_start_time
WHERE NOT NVL(glc.is_deleted,FALSE)
    AND NOT EXISTS(
    SELECT 1 FROM _gsc_landed_cost_dataset_stg AS base
    WHERE  equal_null(glc.po_number, base.po_number)
    AND equal_null(glc.year_month_received, base.year_month_received)
    AND equal_null(glc.sku, base.sku)
    AND equal_null(glc.po_line_number, base.po_line_number)
);

MERGE INTO reference.gsc_landed_cost t
USING _gsc_landed_cost_dataset_stg AS s
    ON equal_null(t.po_number, s.po_number)
    AND equal_null(t.year_month_received, s.year_month_received)
    AND equal_null(t.sku, s.sku)
    AND equal_null(t.po_line_number, s.po_line_number)
WHEN NOT MATCHED THEN INSERT (
    po_number, year_month_received, sku, po_line_number, plm_style, fully_landed,units_received, actual_landed_cost_per_unit, po_cost_without_commission, cmt_cost, actual_duty_cost_per_unit, actual_tariff_cost_per_unit, cpu_freight, cpu_ocean, cpu_air, cpu_transload, cpu_otr, cpu_domestic, cpu_pierpass, agency_cost_and_po_cost,history, total_actual_landed_cost, meta_row_hash,is_deleted, meta_create_datetime, meta_update_datetime
)
VALUES (
    po_number, year_month_received, sku, po_line_number,  plm_style, fully_landed,units_received, actual_landed_cost_per_unit, po_cost_without_commission, cmt_cost, actual_duty_cost_per_unit, actual_tariff_cost_per_unit, cpu_freight, cpu_ocean, cpu_air, cpu_transload, cpu_otr, cpu_domestic, cpu_pierpass, agency_cost_and_po_cost,history, total_actual_landed_cost, meta_row_hash,is_deleted, meta_create_datetime, meta_update_datetime
)
WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash THEN UPDATE
SET
    t.plm_style = s.plm_style,
    t.fully_landed = s.fully_landed,
    t.units_received = s.units_received,
    t.actual_landed_cost_per_unit = s.actual_landed_cost_per_unit,
    t.po_cost_without_commission = s.po_cost_without_commission,
    t.cmt_cost = s.cmt_cost,
    t.actual_duty_cost_per_unit = s.actual_duty_cost_per_unit,
    t.actual_tariff_cost_per_unit = s.actual_tariff_cost_per_unit,
    t.cpu_freight = s.cpu_freight,
    t.cpu_ocean = s.cpu_ocean,
    t.cpu_air = s.cpu_air,
    t.cpu_transload = s.cpu_transload,
    t.cpu_otr = s.cpu_otr,
    t.cpu_domestic = s.cpu_domestic,
    t.cpu_pierpass = s.cpu_pierpass,
    t.agency_cost_and_po_cost = s.agency_cost_and_po_cost,
    t.history = s.history,
    t.total_actual_landed_cost = s.total_actual_landed_cost,
    t.is_deleted = s.is_deleted,
    t.meta_row_hash = s.meta_row_hash,
    t.meta_update_datetime = s.meta_update_datetime;

COMMIT;



