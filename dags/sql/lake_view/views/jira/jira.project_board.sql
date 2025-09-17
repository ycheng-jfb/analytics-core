CREATE OR REPLACE VIEW lake_view.jira.project_board AS
SELECT board_id,
       project_id,
       _fivetran_deleted,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_jira_cloud_v1.project_board;
