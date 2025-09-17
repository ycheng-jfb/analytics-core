CREATE OR REPLACE VIEW reporting_prod.jira.jira_first_close_date_mv2 AS
SELECT issue_id,
       MIN(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP_NTZ(time))) AS first_closed_date
FROM lake_view.jira.issue_field_history
WHERE (field_id = 'status' AND value = '10001')
   OR (field_id = 'status' AND value = '6') ---- this will give us everything that is "DONE"
   OR (field_id = 'resolution' AND value = '10001') ---- this will give us everything that is "COMPLETED")
GROUP BY 1;
