CREATE OR REPLACE VIEW lake_view.jira.comment AS
SELECT id,
       issue_id,
       author_id,
       update_author_id,
       body,
       created,
       updated,
       is_public,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_jira_cloud_v1.comment;
