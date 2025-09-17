CREATE OR REPLACE VIEW lake_view.jira.status_category AS
SELECT id,
       name,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_jira_cloud_v1.status_category;
