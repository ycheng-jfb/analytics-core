CREATE OR REPLACE VIEW lake_view.jira.issue_multiselect_history AS
SELECT field_id,
       issue_id,
       time,
       _fivetran_id,
       value,
       is_active,
       author_id,
       CONVERT_TIMEZONE('America/Los_Angeles', _fivetran_synced) AS meta_create_datetime
FROM lake_fivetran.central_jira_cloud_v1.issue_multiselect_history;
