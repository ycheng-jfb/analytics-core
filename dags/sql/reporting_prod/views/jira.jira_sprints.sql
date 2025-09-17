CREATE OR REPLACE VIEW reporting_prod.jira.jira_sprints AS
SELECT id AS sprint_id,
       name AS sprint_name,
       start_date,
       end_date,
       complete_date
FROM lake_view.jira.sprint ;