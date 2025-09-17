CREATE OR REPLACE VIEW lake_view.jira.project AS
SELECT id,
       key,
       name,
       project_type_key,
       description,
       project_category_id,
       lead_id,
       permission_scheme_id,
       _fivetran_deleted,
       CONVERT_TIMEZONE('America/Los_Angeles', _fivetran_synced) AS meta_create_datetime
FROM lake_fivetran.central_jira_cloud_v1.project;
