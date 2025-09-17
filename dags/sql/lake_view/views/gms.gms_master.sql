CREATE VIEW IF NOT EXISTS lake_view.gms.gms_master AS
SELECT
    agent_id,
    genesys_id,
    agent_name,
    team_name,
    team_id,
    attrition_date,
    sheet_name,
    meta_create_datetime,
    meta_update_datetime
FROM lake.excel.gms_master;
