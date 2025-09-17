CREATE OR REPLACE VIEW lake_view.sharepoint.gms_analytics_metric_definitions AS
SELECT metrics,
       description,
       calculations,
       _source as source,
       Convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.gms_sharepoint_v1.gms_analytics_metric_definitions_sheet_1;
