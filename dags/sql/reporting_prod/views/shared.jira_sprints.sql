create view reporting_prod.shared.jira_sprints as (
    select
      id as sprint_id,
      name as sprint_name,
      start_date,
      end_date,
      complete_date
    from lake_view.jira.sprint
)
