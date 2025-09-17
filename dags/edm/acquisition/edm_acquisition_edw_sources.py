from datetime import timedelta
import pendulum
from airflow import DAG

from edm.acquisition.configs import get_all_configs
from edm.acquisition.configs.edw_source_tables import edw_source_list
from edm.acquisition.configs.exclusion_list import lake_exclusion_table_list
from edm.acquisition.configs.lake_high_frequency_tables import lake_high_frequency_table_list
from include.airflow.callbacks.slack import slack_failure_p1_edw, slack_sla_miss_edw_p1
from include.airflow.operators.snowflake_lake_hist import SnowflakeLakeHistoryOperator
from include.config import owners, conn_ids
from include.config.email_lists import engineering_support

lake_history_list = {
    'lake.ultra_merchant.promo',
    'lake.ultra_merchant.discount',
    'lake.ultra_merchant.product_bundle_component',
    'lake.ultra_merchant.product',
    'lake.ultra_merchant.product_category',
    'lake.ultra_merchant.pricing_option',
    'lake.ultra_merchant.promo_data',
}

default_args = {
    'start_date': pendulum.datetime(2020, 4, 19, tz='America/Los_Angeles'),
    'retries': 3,
    'owner': owners.data_integrations,
    'email': engineering_support,
    'max_active_tis_per_dag': 1,
    "on_failure_callback": slack_failure_p1_edw,
    'execution_timeout': timedelta(hours=2),
}

dag = DAG(
    dag_id='edm_acquisition_edw_sources',
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=18,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
    sla_miss_callback=slack_sla_miss_edw_p1,
)

priority_weight_value = 10

with dag:
    table_list = list(get_all_configs())

    for table in table_list:
        if (
            table.full_target_table_name not in lake_exclusion_table_list
            and table.full_target_table_name in edw_source_list
            and table.full_target_table_name not in lake_high_frequency_table_list
            and table.database != 'ultrawarehouse'
        ):
            to_s3 = table.to_s3_operator
            to_snowflake = table.to_snowflake_operator

            to_snowflake.snowflake_conn_id = conn_ids.Snowflake.edw
            to_snowflake.warehouse = "DA_WH_EDW"

            for op in (to_s3, to_snowflake):
                op.priority_weight = priority_weight_value
                op.weight_rule = 'absolute'

            to_s3 >> to_snowflake

            if table.full_target_table_name.replace('"', '').lower() in lake_history_list:
                to_lake_history = SnowflakeLakeHistoryOperator(
                    snowflake_conn_id=conn_ids.Snowflake.edw,
                    warehouse="DA_WH_EDW",
                    table=table.full_target_table_name.replace('"', '').lower(),
                )
                to_snowflake >> to_lake_history
