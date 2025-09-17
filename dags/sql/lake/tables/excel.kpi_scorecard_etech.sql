CREATE TABLE IF NOT EXISTS lake_stg.excel.kpi_scorecard_etech_stg (
    sheet_name VARCHAR,
    week varchar,
    internal_qc_score_voice NUMBER(38,17),
    internal_alert_rate_voice NUMBER(38,17),
    internal_qc_score_chat_email NUMBER(38,17),
    internal_alert_rate_chat_email NUMBER(38,17)
);


CREATE TABLE IF NOT EXISTS lake.excel.kpi_scorecard_etech (
    division VARCHAR,
    region VARCHAR,
    country VARCHAR,
    bu VARCHAR,
    language VARCHAR,
    campaign VARCHAR,
    date DATE,
    internal_qc_score_voice NUMBER(38,17),
    internal_alert_rate_voice NUMBER(38,17),
    internal_qc_score_chat_email NUMBER(38,17),
    internal_alert_rate_chat_email NUMBER(38,17),
    meta_row_hash INT,
    meta_create_datetime TIMESTAMP_LTZ(3),
    meta_update_datetime TIMESTAMP_LTZ(3),
    PRIMARY KEY (division, region, country, bu, language, campaign, date)
);
