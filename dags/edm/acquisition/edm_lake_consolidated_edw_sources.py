from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from edm.acquisition.configs import get_lake_consolidated_table_config
from edm.acquisition.configs.edw_lake_consolidated_source_tables import (
    edw_source_list as table_list,
)
from edm.acquisition.configs.lake_consolidated_exclusion_list import (
    lake_consolidated_exclusion_table_list as exclusion_list,
)
from edm.acquisition.configs.lake_consolidated_high_frequency_tables import (
    table_list as high_freq_table_list,
)
from include import SQL_DIR
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.hvr_sensor import hvr_sensor
from include.config import owners
from include.config.email_lists import engineering_support

default_args = {
    "start_date": pendulum.datetime(2023, 1, 1, tz="America/Los_Angeles"),
    "retries": 3,
    "owner": owners.data_integrations,
    "email": engineering_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_lake_consolidated_edw_sources",
    default_args=default_args,
    schedule="0 13 * * *",
    catchup=False,
    max_active_tasks=25,
    max_active_runs=1,
)

history_list = {
    "lake_consolidated.ultra_merchant.dm_gateway",
    "lake_consolidated.ultra_merchant.dm_gateway_test_site",
    "lake_consolidated.ultra_merchant.membership_token",
    "lake_consolidated.ultra_merchant.store_credit",
}

with dag:
    warehouse = "DA_WH_ETL_LIGHT"

    hvr_sensor_ultra_merchant = hvr_sensor(
        task_id="hvr_sensor_ultra_merchant",
        schema="ULTRA_MERCHANT",
        lookback_minutes=90,
    )

    hvr_sensor_ultra_merchant_history = hvr_sensor(
        task_id="hvr_sensor_ultra_merchant_history",
        schema="ULTRA_MERCHANT_HISTORY",
        lookback_minutes=90,
    )

    hvr_acquisition = EmptyOperator(task_id="hvr_acquisition")

    hvr_sensor_ultra_merchant >> hvr_acquisition
    hvr_sensor_ultra_merchant_history >> hvr_acquisition

    for table_name in table_list:
        if table_name.lower() not in map(
            str.lower, high_freq_table_list
        ) and table_name.lower() not in map(str.lower, exclusion_list):
            if table_name == 'lake_consolidated.ultra_merchant."ORDER"':
                table_name = "lake_consolidated.ultra_merchant.order"
            cfg = get_lake_consolidated_table_config(table_name)
            to_lake_consolidated = cfg.to_lake_consolidated_operator
            to_lake_consolidated.warehouse = warehouse
            if table_name in history_list:
                print(table_name)
                to_lake_consolidated_history = cfg.to_lake_consolidated_history_operator
                to_lake_consolidated_history.warehouse = warehouse
                hvr_acquisition >> to_lake_consolidated_history
            hvr_acquisition >> to_lake_consolidated

    task_fl = SnowflakeProcedureOperator(
        procedure=Path(SQL_DIR, "lake_consolidated", "ultra_merchant.media_code.sql"),
        database="lake_fl",
        parameters={
            "lake_placeholder": "lake_fl",
            "company_id": 20,
            "data_source_id": 20,
        },
        warehouse=warehouse,
    )

    task_sxf = SnowflakeProcedureOperator(
        procedure=Path(SQL_DIR, "lake_consolidated", "ultra_merchant.media_code.sql"),
        database="lake_sxf",
        parameters={
            "lake_placeholder": "lake_sxf",
            "company_id": 30,
            "data_source_id": 30,
        },
        warehouse=warehouse,
    )

    # Temporarily disabled the JFB task as its a full load everytime causing delay
    # but no incremental data due to the usage of session_media_data in the sql script
    # task_jfb = SnowflakeProcedureOperator(
    #     procedure=Path(SQL_DIR, "lake_consolidated", "ultra_merchant.media_code.sql"),
    #     database='lake_jfb',
    #     parameters={"lake_placeholder": "lake_jfb", "company_id": 10, "data_source_id": 10},
    # )

    hvr_acquisition >> task_fl >> task_sxf  # >> task_jfb
