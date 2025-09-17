from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from edm.acquisition.configs import get_table_config
from edm.acquisition.configs.lake_high_frequency_tables import lake_high_frequency_table_list
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.dagrun_operator import TFGTriggerDagRunOperator
from include.airflow.operators.snowflake_lake_hist import SnowflakeLakeHistoryOperator
from include.config import owners
from include.config.email_lists import engineering_support

default_args = {
    'start_date': pendulum.datetime(2021, 5, 10, tz='America/Los_Angeles'),
    'retries': 0,
    'owner': owners.data_integrations,
    'email': engineering_support,
    'max_active_tis_per_dag': 1,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id='edm_acquisition_high_frequency',
    default_args=default_args,
    schedule='40 */1 * * *',
    catchup=False,
    max_active_tasks=12,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
)

lake_history_list = {
    'lake.ultra_merchant.customer',
    'lake.ultra_merchant.order',
}

# membership table alone is required for med61
lake_history_list2 = {
    'lake.ultra_merchant.membership',
}

heavy_queries = {
    'lake.ultra_merchant.customer_detail',
    'lake.ultra_merchant.session',
}

priority_weight_value = 12

with dag:

    acquisition_complete = EmptyOperator(task_id='acquisition_completion')

    trigger_med61 = TFGTriggerDagRunOperator(
        task_id='trigger_edm_reporting_hourly_med61',
        trigger_dag_id='edm_reporting_hourly_med61',
        execution_date='{{ data_interval_end }}',
    )

    for table_name in lake_high_frequency_table_list:
        if table_name == 'lake.ultra_merchant."ORDER"':
            table_name = 'lake.ultra_merchant.order'
        cfg = get_table_config(table_name)
        to_s3 = cfg.to_s3_operator
        to_s3.weight_rule = 'absolute'
        to_s3.priority_weight = priority_weight_value
        to_snowflake = cfg.to_snowflake_operator
        to_snowflake.weight_rule = 'absolute'
        to_snowflake.priority_weight = priority_weight_value
        to_s3 >> to_snowflake >> acquisition_complete
        if table_name in heavy_queries:
            to_snowflake.warehouse = 'DA_WH_ETL'
        if table_name in lake_history_list:
            to_lake_history = SnowflakeLakeHistoryOperator(
                table=table_name,
                warehouse='DA_WH_ETL',
            )
            to_snowflake >> to_lake_history
        if table_name in lake_history_list2:
            to_lake_history2 = SnowflakeLakeHistoryOperator(
                table=table_name,
                warehouse='DA_WH_ETL',
            )
            to_snowflake >> to_lake_history2 >> trigger_med61
