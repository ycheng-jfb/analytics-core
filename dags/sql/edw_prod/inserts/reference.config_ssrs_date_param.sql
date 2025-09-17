/*
    Update the report_date to generate the historical reports for Daily Cash and Weekly KPI
    We are using the report_date from this table to generate the report for that date in daily_cash and weekly_kpi views
    If report_date is NULL, then the latest report will be generated
 */
TRUNCATE TABLE reference.config_ssrs_date_param;

INSERT INTO reference.config_ssrs_date_param (
    report,
    report_date
)
VALUES ('Daily Cash', NULL),
    ('Weekly KPI', NULL);
