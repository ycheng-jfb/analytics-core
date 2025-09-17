"""
All the non hvr tables
"""

import copy
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from edm.acquisition.configs import get_all_configs
from edm.acquisition.configs.lake_hvr_no_articles import hvr_no_articles_list
from include.airflow.callbacks.slack import slack_failure_edm, slack_sla_miss_edm
from include.config import owners
from include.config.email_lists import data_integration_support

dag = DAG(
    dag_id="edm_acquisition_no_articles",
    default_args={
        "start_date": pendulum.datetime(2019, 11, 19, tz="America/Los_Angeles"),
        "retries": 0,
        "owner": owners.data_integrations,
        "email": data_integration_support,
        "on_failure_callback": slack_failure_edm,
        "execution_timeout": timedelta(hours=3),
    },
    schedule="15 */2 * * *",
    sla_miss_callback=slack_sla_miss_edm,
    catchup=False,
    max_active_tasks=1000,
    max_active_runs=1,
    doc_md=__doc__,
)

dbsplit_dbs = ["gdpr", "ultracms", "ultramerchant", "ultrarollup", "ultracart"]

dbsplit_servers = ["fl", "jfb", "sxf"]

with dag:
    warehouse = "DA_WH_ETL_LIGHT"
    acquisition_complete = EmptyOperator(task_id="acquisition_completion")
    table_config_list = list(get_all_configs())
    full_table_list = []
    for cfg in table_config_list:
        if cfg.full_target_table_name in hvr_no_articles_list:
            cfg_insert_list = []
            if cfg.database in dbsplit_dbs:
                for i in dbsplit_servers:
                    cfg.server = i
                    cfg_by_server = copy.deepcopy(cfg)
                    cfg_insert_list.append(cfg_by_server)
            else:
                cfg.server = None
                cfg_insert_list = [cfg]
            full_table_list.extend(cfg_insert_list)

    for cfg in full_table_list:
        to_s3 = cfg.to_s3_operator
        to_snowflake = cfg.to_snowflake_operator
        priority_weight = 1
        to_s3 >> to_snowflake >> acquisition_complete
        for op in (to_s3, to_snowflake):
            op.priority_weight = priority_weight
            op.weight_rule = "absolute"
