CREATE OR REPLACE VIEW lake_view.jira.epic AS
SELECT id,
       key,
       name,
       summary,
       done,
       CONVERT_TIMEZONE('America/Los_Angeles', _fivetran_synced) AS meta_create_datetime
FROM lake_fivetran.central_jira_cloud_v1.epic;
