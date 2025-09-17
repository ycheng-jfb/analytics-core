"""
All the configs remaining after edw_source_list, ultra_warehouse, and exclusion list
"""

from datetime import timedelta

import pendulum
from airflow import DAG

from edm.acquisition.configs import get_all_configs
from edm.acquisition.configs.edw_source_tables import edw_source_list
from edm.acquisition.configs.exclusion_list import lake_exclusion_table_list
from edm.acquisition.configs.lake_low_frequency_tables import lake_low_frequency_tables
from include.airflow.callbacks.slack import slack_failure_edm, slack_sla_miss_edm
from include.airflow.dag_helpers import TFGControlOperator
from include.config import owners
from include.config.email_lists import data_integration_support

dag = DAG(
    dag_id="edm_acquisition_other",
    default_args={
        "start_date": pendulum.datetime(2019, 11, 19, tz="America/Los_Angeles"),
        "retries": 0,
        "owner": owners.data_integrations,
        "email": data_integration_support,
        "on_failure_callback": slack_failure_edm,
        "execution_timeout": timedelta(hours=3),
    },
    schedule="20 0-23/5 * * *",
    sla_miss_callback=slack_sla_miss_edm,
    catchup=False,
    max_active_tasks=1000,
    max_active_runs=1,
    doc_md=__doc__,
)

with dag:
    tfg_control = TFGControlOperator()
    table_config_list = list(get_all_configs())
    for cfg in table_config_list:
        if (
            cfg.full_target_table_name not in lake_exclusion_table_list
            and cfg.full_target_table_name not in edw_source_list
            and cfg.full_target_table_name not in lake_low_frequency_tables
            and cfg.target_schema
            not in [
                "bluecherry",
                "gdpr",
                "ultra_cart",
                "ultra_cms",
                "ultra_cms_cdc",
                "ultra_merchant",
                "ultra_merchant_cdc",
                "ultra_rollup",
                "ultra_warehouse",
                "ultra_warehouse_cdc",
            ]
        ):
            to_s3 = cfg.to_s3_operator
            to_snowflake = cfg.to_snowflake_operator
            priority_weight = 1
            tfg_control >> to_s3 >> to_snowflake
            for op in (to_s3, to_snowflake):
                op.priority_weight = priority_weight
                op.weight_rule = "absolute"
