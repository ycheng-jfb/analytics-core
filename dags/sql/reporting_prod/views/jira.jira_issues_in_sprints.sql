CREATE OR REPLACE VIEW reporting_prod.jira.jira_issues_in_sprints AS
WITH cte_sprints AS (SELECT i.id,
                            i.key,
                            s.id                                                                         AS sprint_id,
                            s.name                                                                          sprint_name,
                            imh.time                                                                     AS updated_date,
                            DENSE_RANK() OVER (PARTITION BY i.key,updated_date ORDER BY s.end_date DESC) AS rno,
                            DENSE_RANK() OVER (PARTITION BY i.key ORDER BY updated_date DESC)            AS is_latest
                     FROM lake_view.jira.issue i
                     LEFT JOIN lake_view.jira.issue_multiselect_history imh
                               ON i.id = imh.issue_id AND imh.field_id = 'customfield_10020'
                     LEFT JOIN lake_view.jira.sprint s
                               ON imh.value = s.id)
SELECT id,
       key,
       MAX(CASE WHEN rno = 1 THEN sprint_id END)   AS sprint_id_rank1_desc,
       MAX(CASE WHEN rno = 1 THEN sprint_name END) AS sprint_name_rank1_desc,
       MAX(CASE WHEN rno = 2 THEN sprint_id END)   AS sprint_id_rank2_desc,
       MAX(CASE WHEN rno = 2 THEN sprint_name END) AS sprint_name_rank2_desc,
       MAX(CASE WHEN rno = 3 THEN sprint_id END)   AS sprint_id_rank3_desc,
       MAX(CASE WHEN rno = 3 THEN sprint_name END) AS sprint_name_rank3_desc
FROM cte_sprints
WHERE is_latest = 1
GROUP BY id, key, updated_date;