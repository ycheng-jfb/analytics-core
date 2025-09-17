CREATE OR REPLACE VIEW lake_view.jira.field_option AS
SELECT id,
       parent_id,
       name,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_jira_cloud_v1.field_option;
