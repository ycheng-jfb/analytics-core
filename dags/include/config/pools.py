from enum import Enum


class Pool(str, Enum):
    sql_server_pull_dbp61 = "sql_server_pull_dbp61"
    sql_server_pull_dbd05 = "sql_server_pull_dbd05"
    sql_server_pull_bento_prod_reader = "sql_server_pull_bento_prod_reader"
    sql_server_pull_fabletics = "sql_server_pull_fabletics"
    sql_server_pull_savagex = "sql_server_pull_savagex"
    sql_server_pull_jfb = "sql_server_pull_jfb"
    sql_server_pull_edw01 = "sql_server_pull_edw01"
    sql_server_pull_evolve01 = "sql_server_pull_evolve01"
    sql_server_pull_jfgcdb01 = "sql_server_pull_jfgcdb01"
    sql_server_pull_giftco = "sql_server_pull_giftco"
    snowflake_lake_load = "snowflake_lake_load"
    gsheets = "gsheets"
    sprinklr = "sprinklr"


conn_id_pool_mapping = {
    "mssql_dbp61_app_airflow": Pool.sql_server_pull_dbp61,
    "mssql_dbd05_app_airflow": Pool.sql_server_pull_dbd05,
    "mssql_bento_prod_reader_app_airflow": Pool.sql_server_pull_bento_prod_reader,
    "mssql_fabletics_app_airflow": Pool.sql_server_pull_fabletics,
    "mssql_savagex_app_airflow": Pool.sql_server_pull_savagex,
    "mssql_justfab_app_airflow": Pool.sql_server_pull_jfb,
    "mssql_edw01_app_airflow": Pool.sql_server_pull_edw01,
    "mssql_evolve01_app_airflow": Pool.sql_server_pull_evolve01,
    "mssql_jfgcdb01_app_airflow": Pool.sql_server_pull_jfgcdb01,
    "mssql_giftco_app_airflow": Pool.sql_server_pull_giftco,
    "google_cloud_default": Pool.gsheets,
}


def get_pool(conn_id):
    pool = conn_id_pool_mapping.get(conn_id)
    if pool is None:
        raise Exception(f"no pool mapped for conn if {conn_id}")
    return pool
