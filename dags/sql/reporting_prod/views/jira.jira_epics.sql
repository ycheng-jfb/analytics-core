CREATE OR REPLACE VIEW reporting_prod.jira.jira_epics AS
WITH ifh AS (
    SELECT field_id,
        value,
        issue_id
    FROM (SELECT *
       FROM lake_view.jira.issue_field_history
       WHERE field_id = 'created'
          OR field_id = 'updated'
          OR field_id = 'duedate'
          OR field_id = 'customfield_10339'
          OR field_id = 'resolutiondate'
          OR field_id = 'status'
          OR field_id = 'reporter'
          OR field_id = 'assignee'
          OR field_id = 'customfield_10294'
          OR field_id = 'description')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY field_id, issue_id ORDER BY time DESC) = 1
     AND is_active = TRUE),
    labels AS (SELECT value,
           issue_id,
           ROW_NUMBER() OVER (PARTITION BY issue_id ORDER BY value ASC) AS row_n
    FROM lake_view.jira.issue_multiselect_history
    WHERE field_id = 'labels'
      AND is_active = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY issue_id ORDER BY value ASC) = 1),
    component AS (SELECT issue_id,
              LISTAGG(name, ', ') AS component
       FROM lake_view.jira.issue_multiselect_history imh
                JOIN lake_view.jira.component c
                     ON imh.value = c.id::VARCHAR AND field_id = 'components' AND is_active = TRUE
       GROUP BY issue_id
)
SELECT e.id,
       e.key,
       component.component,
       created.value::TIMESTAMP_NTZ                 AS created_date,
       updated.value::TIMESTAMP_NTZ                 AS updated_date,
       duedate.value::TIMESTAMP_NTZ                 AS due_date,
       requested_delivery_date.value::TIMESTAMP_NTZ AS requested_delivery_date,
       resolutiondate.value::TIMESTAMP_NTZ          AS resolution_date,
       s.name                                       AS status,
       reporter.name                                AS reporter,
       assignee.name                                AS assignee,
       e.summary,
       project_description.value                    AS project_description,
       e.name                                       AS epic_name,
       l1.value                                     AS label1,
       description.value                            AS release_notes
FROM lake_view.jira.epic e
    LEFT JOIN component
           ON e.id = component.issue_id
    LEFT JOIN ifh created
           ON e.id = created.issue_id AND created.field_id = 'created'
    LEFT JOIN ifh updated
           ON e.id = updated.issue_id AND updated.field_id = 'updated'
    LEFT JOIN ifh duedate
           ON e.id = duedate.issue_id AND duedate.field_id = 'duedate'
    LEFT JOIN ifh requested_delivery_date
           ON e.id = requested_delivery_date.issue_id AND requested_delivery_date.field_id = 'customfield_10339'
    LEFT JOIN ifh resolutiondate
           ON e.id = resolutiondate.issue_id AND resolutiondate.field_id = 'resolutiondate'
    LEFT JOIN ifh status_id
           ON e.id = status_id.issue_id AND status_id.field_id = 'status'
    LEFT JOIN lake_view.jira.status s
           ON status_id.value = s.id
    LEFT JOIN ifh reporter_id
           ON e.id = reporter_id.issue_id AND reporter_id.field_id = 'reporter'
    LEFT JOIN lake_view.jira.user reporter
           ON reporter_id.value = reporter.id
    LEFT JOIN ifh assignee_id
           ON e.id = assignee_id.issue_id AND assignee_id.field_id = 'assignee'
    LEFT JOIN lake_view.jira.user assignee
           ON assignee_id.value = assignee.id
    LEFT JOIN ifh project_description
           ON e.id = project_description.issue_id AND project_description.field_id = 'customfield_10294'
    LEFT JOIN labels l1
           ON e.id = l1.issue_id AND l1.row_n = 1
    LEFT JOIN ifh description
           ON e.id = description.issue_id AND description.field_id = 'description';
