CREATE OR REPLACE VIEW lake_view.jira.worklog AS
SELECT issue_id,
       id,
       updated,
       started,
       COMMENT,
       time_spent_seconds,
       update_author_id,
       author_id,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_jira_cloud_v1.worklog;
