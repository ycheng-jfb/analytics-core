from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from edm.acquisition.configs import (
    exclusion_list,
    get_all_configs,
    get_lake_consolidated_table_config,
    get_lake_history_tables,
)
from include.airflow.callbacks.slack import slack_failure_edm, slack_sla_miss_edm
from include.config import owners, conn_ids
from include.config.email_lists import engineering_support
from include.airflow.operators.snowflake_lake_hist import SnowflakeLakeHistoryOperator

default_args = {
    "start_date": pendulum.datetime(2023, 5, 1, tz="America/Los_Angeles"),
    "retries": 0,
    "owner": owners.data_integrations,
    "email": engineering_support,
    "max_active_tis_per_dag": 1,
    "on_failure_callback": slack_failure_edm,
    "sla": timedelta(hours=3),
}

dag = DAG(
    dag_id='edm_lake_hvr_migrated_objects',
    default_args=default_args,
    schedule="0 8 * * *",
    catchup=False,
    max_active_tasks=25,
    max_active_runs=1,
    sla_miss_callback=slack_sla_miss_edm,
)

lake_history_list = {
    'lake.ultra_merchant.product_category',
    'lake.ultra_merchant.promo',
    'lake.ultra_merchant.pricing_option',
    'lake.ultra_merchant.discount',
    'lake.ultra_merchant.product_bundle_component',
    'lake.ultra_merchant.customer',
    'lake.ultra_merchant.product',
    'lake.ultra_merchant.payment_transaction_creditcard',
    'lake.ultra_merchant.refund',
    'lake.ultra_merchant.membership_token_transaction',
    'lake.ultra_merchant.membership',
    'lake.ultra_merchant.order',
    'lake.ultra_merchant.payment_transaction_psp',
    'lake.ultra_merchant.payment_transaction_cash',
}

with dag:
    database_list = ["ultramerchant", "ultracms", "ultrarollup", "ultracart", "gdpr"]
    db_task_groups = {database: TaskGroup(group_id=database) for database in database_list}
    acquisition_complete = EmptyOperator(task_id='lake_completion')

    for cfg in get_all_configs():
        if (
            cfg.database.lower() in database_list
            and cfg.full_target_table_name not in exclusion_list.lake_exclusion_table_list
        ):
            with db_task_groups[cfg.database.lower()] as tg:
                to_s3 = cfg.to_s3_operator
                to_snowflake = cfg.to_snowflake_operator
                to_s3 >> to_snowflake >> acquisition_complete

    history_complete = EmptyOperator(task_id='lake_history_completion')

    for table_name in lake_history_list:
        to_lake_history = SnowflakeLakeHistoryOperator(
            snowflake_conn_id=conn_ids.Snowflake.edw,
            database='lake',
            table=table_name.replace('"', '').lower(),
        )

        acquisition_complete >> to_lake_history >> history_complete
