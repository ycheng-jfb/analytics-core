CREATE OR REPLACE TRANSIENT TABLE validation.actual_landed_cost_changes
(
    validation_date DATE,
    po_number VARCHAR,
    year_month_received DATE,
    sku VARCHAR,
    po_line_number NUMBER(38,0),
    fully_landed VARCHAR,
    today_actual_landed_cost_per_unit NUMBER(38,6),
    yesterday_actual_landed_cost_per_unit NUMBER(38,6),
    diff_actual_landed_cost_per_unit NUMBER(38,6)
    percentage_change NUMBER(38,2)
);
