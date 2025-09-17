CREATE OR REPLACE VIEW lake_view.jira.issue_field_history AS
SELECT field_id,
       time,
       value,
       is_active,
       author_id,
       issue_id,
       CONVERT_TIMEZONE('America/Los_Angeles', _fivetran_synced) AS meta_create_datetime
FROM lake_fivetran.central_jira_cloud_v1.issue_field_history;
