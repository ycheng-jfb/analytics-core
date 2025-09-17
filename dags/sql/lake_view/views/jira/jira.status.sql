CREATE OR REPLACE VIEW lake_view.jira.status AS
SELECT id,
       description,
       name,
       status_category_id,
       CONVERT_TIMEZONE('America/Los_Angeles', _fivetran_synced) AS meta_create_datetime
FROM lake_fivetran.central_jira_cloud_v1.status;
