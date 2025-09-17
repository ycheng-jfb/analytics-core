CREATE OR REPLACE VIEW lake_view.jira.sprint AS
SELECT id,
       state,
       name,
       start_date,
       end_date,
       complete_date,
       goal,
       _fivetran_deleted,
       board_id,
       convert_timezone('America/Los_Angeles', _fivetran_synced) AS meta_update_datetime
FROM lake_fivetran.central_jira_cloud_v1.sprint;
