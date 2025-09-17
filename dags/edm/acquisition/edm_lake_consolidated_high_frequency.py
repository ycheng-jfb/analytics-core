from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from edm.acquisition.configs import get_lake_consolidated_table_config
from edm.acquisition.configs.lake_consolidated_exclusion_list import (
    lake_consolidated_exclusion_table_list as exclusion_list,
)
from edm.acquisition.configs.lake_consolidated_high_frequency_tables import table_list
from edm.acquisition.configs.lake_consolidated_history_high_frequency_tables import (
    history_table_list,
)
from include.airflow.callbacks.slack import slack_failure_edm, slack_sla_miss_edm_p1
from include.config import owners
from include.config.email_lists import engineering_support

default_args = {
    "start_date": pendulum.datetime(2023, 1, 1, tz="America/Los_Angeles"),
    "retries": 0,
    "owner": owners.data_integrations,
    "email": engineering_support,
    "on_failure_callback": slack_failure_edm,
    "sla": timedelta(minutes=60),
}

dag = DAG(
    dag_id="edm_lake_consolidated_high_frequency",
    default_args=default_args,
    schedule="40 1-23/2 * * *",
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=slack_sla_miss_edm_p1,
)

priority_weight_value = 12

with dag:
    warehouse = "DA_WH_ETL_LIGHT"
    acquisition_complete = EmptyOperator(task_id="consolidation_completion")

    for table_name in table_list:
        if table_name.lower() not in map(str.lower, exclusion_list):
            if table_name == 'lake_consolidated.ultra_merchant."ORDER"':
                table_name = "lake_consolidated.ultra_merchant.order"
            cfg = get_lake_consolidated_table_config(table_name)
            to_lake_consolidated = cfg.to_lake_consolidated_operator
            to_lake_consolidated.warehouse = warehouse
            to_lake_consolidated >> acquisition_complete

    for table_name in history_table_list:
        if table_name.lower() not in map(str.lower, exclusion_list):
            if table_name == 'lake_consolidated.ultra_merchant."ORDER"':
                table_name = "lake_consolidated.ultra_merchant.order"
            cfg = get_lake_consolidated_table_config(table_name)
            to_lake_consolidated_history = cfg.to_lake_consolidated_history_operator
            to_lake_consolidated_history.warehouse = warehouse
            to_lake_consolidated_history >> acquisition_complete
