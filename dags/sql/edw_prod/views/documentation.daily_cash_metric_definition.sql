CREATE OR REPLACE VIEW documentation.daily_cash_metric_definition AS
SELECT sequence,
    metric_group,
    metric,
    definition,
    is_available_in_ssrs,
    is_available_in_tableau,
    meta_row_hash,
    meta_create_datetime,
    meta_update_datetime
FROM lake_view.sharepoint.daily_cash_metric_definition
ORDER BY 1
