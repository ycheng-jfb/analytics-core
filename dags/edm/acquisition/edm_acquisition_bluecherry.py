"""
All the configs for bluecherry
"""

from datetime import timedelta

import pendulum
from airflow import DAG

from edm.acquisition.configs import get_all_configs
from include.airflow.callbacks.slack import slack_failure_edm, slack_sla_miss_edm
from include.airflow.dag_helpers import TFGControlOperator
from include.config import owners
from include.config.email_lists import data_integration_support

dag = DAG(
    dag_id="edm_acquisition_bluecherry",
    default_args={
        "start_date": pendulum.datetime(2019, 11, 19, tz="America/Los_Angeles"),
        "retries": 0,
        "owner": owners.data_integrations,
        "email": data_integration_support,
        "on_failure_callback": slack_failure_edm,
        "execution_timeout": timedelta(hours=3),
    },
    schedule="0 5-19/2 * * *",
    sla_miss_callback=slack_sla_miss_edm,
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
    doc_md=__doc__,
)

with dag:
    tfg_control = TFGControlOperator()
    table_config_list = list(get_all_configs())
    for cfg in table_config_list:
        if cfg.full_target_table_name in [
            "lake.jf_portal.bluecherry_po_dtl",
            "lake.bluecherry.tfg_prepack",
            "lake.bluecherry.tfg_style_master",
        ]:
            to_s3 = cfg.to_s3_operator
            to_snowflake = cfg.to_snowflake_operator
            priority_weight = 1
            tfg_control >> to_s3 >> to_snowflake
            for op in (to_s3, to_snowflake):
                op.priority_weight = priority_weight
                op.weight_rule = "absolute"
