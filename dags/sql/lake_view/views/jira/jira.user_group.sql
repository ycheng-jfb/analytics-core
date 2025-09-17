CREATE OR REPLACE VIEW lake_view.jira.user_group AS
SELECT user_id,
       group_name,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_jira_cloud_v1.user_group;
