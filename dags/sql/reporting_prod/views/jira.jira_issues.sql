CREATE OR REPLACE VIEW reporting_prod.jira.jira_issues AS
WITH ifh AS (SELECT field_id,
                    value,
                    issue_id
             FROM (SELECT *
                   FROM lake_view.jira.issue_field_history
                   WHERE field_id = 'customfield_10339'
                      OR field_id = 'customfield_10014'
                      OR field_id = 'customfield_10294'
                      OR field_id = 'customfield_10210'
                      OR field_id = 'customfield_10289'
                      OR field_id = 'customfield_10335'
                      OR field_id = 'customfield_10336'
                      OR field_id = 'customfield_10338'
                      OR field_id = 'customfield_10337')
             QUALIFY ROW_NUMBER() OVER (PARTITION BY field_id, issue_id ORDER BY time DESC) = 1
                 AND is_active = TRUE),

     labels AS (SELECT value,
                       issue_id,
                       ROW_NUMBER() OVER (PARTITION BY issue_id ORDER BY value ASC) AS row_n
                FROM lake_view.jira.issue_multiselect_history
                WHERE field_id = 'labels'
                  AND is_active = TRUE),

     component AS (SELECT issue_id,
                          LISTAGG(name, ', ') AS component
                   FROM lake_view.jira.issue_multiselect_history imh
                            JOIN lake_view.jira.component c
                                 ON imh.value = c.id::VARCHAR AND field_id = 'components' AND is_active = TRUE
                   GROUP BY issue_id)

SELECT i.id,
       i.key,
       component.component,
       i.created::TIMESTAMP_NTZ                     AS created_date,
       i.updated::TIMESTAMP_NTZ                     AS updated_date,
       i.due_date::TIMESTAMP_NTZ                    AS due_date,
       requested_delivery_date.value::TIMESTAMP_NTZ AS requested_delivery_date,
       i.resolved::TIMESTAMP_NTZ                    AS resolution_date,
       e.key                                        AS epic_link,
       --i.project                                    AS project,
       s.name                                       AS status,
       it.name                                      AS issue_type,
       reporter.name                                AS reporter,
       assignee.name                                AS assignee,
       i.summary,
       project_description.value                    AS project_description,
       story_pts.value::NUMBER(38, 2)               AS story_pts,
       CASE
           WHEN capex.value = '10183' THEN 'Capital'
           WHEN capex.value = '10184' THEN 'Expense'
           ELSE capex.value
           END                                      AS capex,
       shared_capex_pts.value::NUMBER(38, 2)        AS shared_capex_pts,
       fl_capex_points.value::NUMBER(38, 2)         AS fl_capex_points,
       sxf_capex_points.value::NUMBER(38, 2)        AS sxf_capex_points,
       jfb_capex_points.value::NUMBER(38, 2)        AS jfb_capex_points,
       l1.value                                     AS label1,
       l2.value                                     AS label2,
       l3.value                                     AS label3,
       l4.value                                     AS label4,
       l5.value                                     AS label5,
       p.name                                       AS priority,
       i.project                                    AS project_id
FROM lake_view.jira.issue i
         LEFT JOIN component
                   ON i.id = component.issue_id
         LEFT JOIN ifh requested_delivery_date
                   ON i.id = requested_delivery_date.issue_id AND requested_delivery_date.field_id = 'customfield_10339'
         LEFT JOIN ifh epic_id
                   ON i.id = epic_id.issue_id AND epic_id.field_id = 'customfield_10014'
         LEFT JOIN lake_view.jira.epic e
                   ON epic_id.value = e.id
         LEFT JOIN lake_view.jira.status s
                   ON i.status = s.id
         LEFT JOIN lake_view.jira.issue_type it
                   ON i.issue_type = it.id
         LEFT JOIN lake_view.jira.user reporter
                   ON i.reporter = reporter.id
         LEFT JOIN lake_view.jira.user assignee
                   ON i.assignee = assignee.id
         LEFT JOIN ifh project_description
                   ON i.id = project_description.issue_id AND project_description.field_id = 'customfield_10294'
         LEFT JOIN ifh story_pts
                   ON i.id = story_pts.issue_id AND story_pts.field_id = 'customfield_10210'
         LEFT JOIN ifh capex
                   ON i.id = capex.issue_id AND capex.field_id = 'customfield_10289'
         LEFT JOIN ifh shared_capex_pts
                   ON i.id = shared_capex_pts.issue_id AND shared_capex_pts.field_id = 'customfield_10335'
         LEFT JOIN ifh fl_capex_points
                   ON i.id = fl_capex_points.issue_id AND fl_capex_points.field_id = 'customfield_10336'
         LEFT JOIN ifh sxf_capex_points
                   ON i.id = sxf_capex_points.issue_id AND sxf_capex_points.field_id = 'customfield_10338'
         LEFT JOIN ifh jfb_capex_points
                   ON i.id = jfb_capex_points.issue_id AND jfb_capex_points.field_id = 'customfield_10337'
         LEFT JOIN lake_view.jira.priority p
                   ON i.priority = p.id
         LEFT JOIN labels l1
                   ON i.id = l1.issue_id AND l1.row_n = 1
         LEFT JOIN labels l2
                   ON i.id = l2.issue_id AND l2.row_n = 2
         LEFT JOIN labels l3
                   ON i.id = l3.issue_id AND l3.row_n = 3
         LEFT JOIN labels l4
                   ON i.id = l4.issue_id AND l4.row_n = 4
         LEFT JOIN labels l5
                   ON i.id = l5.issue_id AND l5.row_n = 5;
