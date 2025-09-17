CREATE OR REPLACE VIEW lake_view.jira.field AS
SELECT id,
       name,
       is_custom,
       is_array,
       dimension_table,
       _fivetran_deleted,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_jira_cloud_v1.field;
