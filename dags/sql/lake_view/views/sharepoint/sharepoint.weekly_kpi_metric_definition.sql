CREATE OR REPLACE VIEW lake_view.sharepoint.weekly_kpi_metric_definition AS
SELECT sequence,
       metric_group,
       metric,
       definition,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_create_datetime,
       _fivetran_synced::TIMESTAMP_LTZ AS meta_update_datetime
FROM lake_fivetran.fpa_nonconfidential_sharepoint_v1.weekly_kpi_documentation_weekly_kpi;
