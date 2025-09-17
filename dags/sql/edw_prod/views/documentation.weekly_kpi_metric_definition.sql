CREATE OR REPLACE VIEW documentation.weekly_kpi_metric_definition AS
SELECT sequence,
    metric_group,
    metric,
    definition,
    meta_create_datetime,
    meta_update_datetime
FROM lake_view.sharepoint.weekly_kpi_metric_definition
ORDER BY 1;
