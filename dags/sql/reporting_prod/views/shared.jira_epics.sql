create view reporting_prod.shared.jira_epics as (
with ifh as (
  select
    field_id,
    value,
    issue_id
  from (
    select * from lake_view.jira.issue_field_history
    where field_id = 'created'
    or field_id = 'updated'
    or field_id = 'duedate'
    or field_id = 'customfield_17824'
    or field_id = 'resolutiondate'
    or field_id = 'status'
    or field_id = 'reporter'
    or field_id = 'assignee'
    or field_id = 'customfield_12901'
    or field_id = 'description'
  )
  qualify row_number() over (partition by field_id, issue_id order by TIME desc) = 1
  and is_active = true
),

labels as (
  select
    value,
    issue_id,
    row_number() over (partition by issue_id order by value asc) as row_n
  from lake_view.jira.issue_multiselect_history
  where field_id = 'labels' and is_active = true
  qualify row_number() over (partition by issue_id order by value asc) = 1
),

component as (
  select
    issue_id,
    listagg(NAME, ', ') as component
  from lake_view.jira.issue_multiselect_history imh
  join lake_view.jira.component c
    on imh.value = c.id::VARCHAR and field_id = 'components' and is_active = true
  group by issue_id
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
           ON e.id = requested_delivery_date.issue_id AND requested_delivery_date.field_id = 'customfield_17824'
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
           ON e.id = project_description.issue_id AND project_description.field_id = 'customfield_12901'
    LEFT JOIN labels l1
           ON e.id = l1.issue_id AND l1.row_n = 1
    LEFT JOIN ifh description
           ON e.id = description.issue_id AND description.field_id = 'description'
)
