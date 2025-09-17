CREATE VIEW OR REPLACE VIEW lake_view.gms.kpi_scorecard_etech AS
SELECT
    division,
    region,
    country,
    bu,
    language,
    campaign,
    date,
    internal_qc_score_voice,
    internal_alert_rate_voice,
    internal_qc_score_chat_email,
    internal_alert_rate_chat_email,
    meta_create_datetime,
    meta_update_datetime
FROM lake.excel.kpi_scorecard_etech;
