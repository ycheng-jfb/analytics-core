CREATE OR REPLACE VIEW lake_view.jira.version AS
SELECT id,
       project_id,
       name,
       description,
       archived,
       released,
       overdue,
       start_date,
       release_date,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_jira_cloud_v1.version;
