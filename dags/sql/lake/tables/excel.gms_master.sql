CREATE TABLE IF NOT EXISTS lake_stg.excel.gms_master_stg (
    agent_id NUMBER(10,0),
    genesys_id VARCHAR,
    agent_name VARCHAR,
    team_name VARCHAR,
    team_id VARCHAR,
    attrition_date DATE,
    sheet_name VARCHAR
);

CREATE TABLE IF NOT EXISTS lake.excel.gms_master (
    agent_id NUMBER(10,0),
    genesys_id VARCHAR,
    agent_name VARCHAR,
    team_name VARCHAR,
    team_id VARCHAR,
    attrition_date DATE,
    sheet_name VARCHAR,
    meta_row_hash INT,
    meta_create_datetime TIMESTAMP_LTZ(3),
    meta_update_datetime TIMESTAMP_LTZ(3)
);
