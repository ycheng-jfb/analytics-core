SET execution_date = CURRENT_TIMESTAMP::DATE;

CREATE OR REPLACE TEMP  TABLE _actual_landed_cost_today AS
SELECT
    po_number,
    year_month_received,
    sku,
    po_line_number,
    fully_landed,
    actual_landed_cost_per_unit
FROM reference.gsc_landed_cost
WHERE
    fully_landed = 'Y'
    AND NOT is_deleted
    AND meta_update_datetime::DATE = $execution_date;

CREATE OR REPLACE TEMP  TABLE _actual_landed_cost_yesterday AS
SELECT
    a.po_number,
    a.year_month_received,
    a.sku,
    a.po_line_number,
    a.fully_landed,
    a.actual_landed_cost_per_unit
FROM reference.gsc_landed_cost AT(OFFSET => -24*60*60) AS a
JOIN _actual_landed_cost_today AS b
    ON equal_null(a.po_number, b.po_number)
    AND equal_null(a.po_line_number, b.po_line_number)
    AND equal_null(a.year_month_received, b.year_month_received)
    AND equal_null(a.sku, b.sku)
WHERE
    a.fully_landed = 'Y'
    AND NOT a.is_deleted;

CREATE OR REPLACE TEMP  TABLE _actual_landed_cost_val AS
SELECT
        a.po_number,
        a.year_month_received,
        a.sku,
        a.po_line_number,
        a.fully_landed,
        a.actual_landed_cost_per_unit AS today_actual_landed_cost_per_unit,
        b.actual_landed_cost_per_unit AS yesterday_actual_landed_cost_per_unit,
        (today_actual_landed_cost_per_unit - yesterday_actual_landed_cost_per_unit) AS diff_actual_landed_cost_per_unit,
        ((today_actual_landed_cost_per_unit - yesterday_actual_landed_cost_per_unit)/yesterday_actual_landed_cost_per_unit)*100 AS percentage_change
FROM _actual_landed_cost_today a
JOIN _actual_landed_cost_yesterday b
    ON EQUAL_NULL(a.po_number, b.po_number)
    AND EQUAL_NULL(a.po_line_number, b.po_line_number)
    AND EQUAL_NULL(a.year_month_received, b.year_month_received)
    AND EQUAL_NULL(a.sku, b.sku)
WHERE diff_actual_landed_cost_per_unit <> 0;

INSERT INTO validation.actual_landed_cost_changes
(
    validation_date,
    po_number,
    year_month_received,
    sku,
    po_line_number,
    fully_landed,
    today_actual_landed_cost_per_unit,
    yesterday_actual_landed_cost_per_unit,
    diff_actual_landed_cost_per_unit,
    percentage_change
)
SELECT
    $execution_date,
    po_number,
    year_month_received,
    sku,
    po_line_number,
    fully_landed,
    today_actual_landed_cost_per_unit,
    yesterday_actual_landed_cost_per_unit,
    diff_actual_landed_cost_per_unit,
    percentage_change
FROM _actual_landed_cost_val;

DELETE FROM validation.actual_landed_cost_changes
WHERE validation_date < DATEADD(MONTH, -6, CURRENT_DATE);

SELECT * FROM validation.actual_landed_cost_changes
WHERE
    validation_date = $execution_date
    AND percentage_change > 5
ORDER BY percentage_change DESC;


