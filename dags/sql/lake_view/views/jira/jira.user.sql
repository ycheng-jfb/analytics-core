CREATE OR REPLACE VIEW lake_view.jira.user AS
SELECT id,
       locale,
       time_zone,
       email,
       name,
       is_active,
       username,
       CONVERT_TIMEZONE('America/Los_Angeles', _fivetran_synced) AS meta_create_datetime
FROM lake_fivetran.central_jira_cloud_v1.user;
