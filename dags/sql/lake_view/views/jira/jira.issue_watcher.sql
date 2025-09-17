CREATE OR REPLACE VIEW lake_view.jira.issue_watcher AS
SELECT issue_id,
       user_id,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_jira_cloud_v1.issue_watcher;
