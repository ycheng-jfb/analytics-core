"""
Exclusion list tables. These are not scheduled, if found that source data for these tables
need to manually trigger this dag.

"""

from datetime import timedelta

import pendulum
from airflow import DAG

from edm.acquisition.configs import get_all_configs
from edm.acquisition.configs.exclusion_list import lake_exclusion_table_list
from include.airflow.callbacks.slack import slack_failure_edm, slack_sla_miss_edm
from include.config import owners
from include.config.email_lists import data_integration_support

dag = DAG(
    dag_id='edm_acquisition_once',
    default_args={
        'start_date': pendulum.datetime(2019, 11, 19, tz='America/Los_Angeles'),
        'retries': 0,
        'owner': owners.data_integrations,
        'email': data_integration_support,
        "on_failure_callback": slack_failure_edm,
        'execution_timeout': timedelta(hours=3),
    },
    schedule=None,
    sla_miss_callback=slack_sla_miss_edm,
    catchup=False,
    max_active_tasks=1000,
    max_active_runs=1,
    doc_md=__doc__,
)

with dag:
    table_config_list = list(get_all_configs())
    for cfg in table_config_list:
        if cfg.full_target_table_name in lake_exclusion_table_list:
            to_s3 = cfg.to_s3_operator
            to_snowflake = cfg.to_snowflake_operator
            priority_weight = 1
            to_s3 >> to_snowflake
            for op in (to_s3, to_snowflake):
                op.priority_weight = priority_weight
                op.weight_rule = 'absolute'
