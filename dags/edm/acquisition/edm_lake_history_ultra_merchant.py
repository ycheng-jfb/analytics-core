import pendulum
from airflow import DAG

from edm.acquisition.configs import get_lake_history_tables
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.dag_helpers import TFGControlOperator
from include.airflow.operators.snowflake_lake_hist import SnowflakeLakeHistoryOperator
from include.config import owners
from include.config.email_lists import data_integration_support

default_args = {
    'start_date': pendulum.datetime(2021, 4, 13, tz='America/Los_Angeles'),
    'retries': 0,
    'owner': owners.data_integrations,
    'email': data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id='edm_lake_history_ultra_merchant',
    default_args=default_args,
    schedule='0 18 * * *',
    catchup=False,
    max_active_tasks=15,
    max_active_runs=1,
)


exclusion_list = [
    'lake.ultra_merchant.promo_promo_classfication',  # no date fields
    'lake.ultra_merchant.rma_refund',  # no date fields
    'lake.ultra_merchant.gift_certificate_transaction_reason',  # no data in lake
    'lake.ultra_merchant.promo',  # added in edw load
    'lake.ultra_merchant.discount',  # added in edw load
    'lake.ultra_merchant.product_bundle_component',  # added in edw load
    'lake.ultra_merchant.product',  # added in edw load
    'lake.ultra_merchant.product_category',  # added in edw load
    'lake.ultra_merchant.pricing_option',  # added in edw load
    'lake.ultra_merchant.customer',  # added in edw high frequency
    'lake.ultra_merchant.membership',  # added in edw high frequency
    'lake.ultra_merchant.promo',  # added in edw load
    'lake.ultra_merchant.order',  # added in edw high frequency
    'lake.ultra_merchant.membership_profile',  # duplicate is_current column
]

table_name_list = set(get_lake_history_tables(schema='ultra_merchant')) - set(exclusion_list)

with dag:
    tfg_control = TFGControlOperator()
    for table_name in table_name_list:
        table = SnowflakeLakeHistoryOperator(table=table_name)
        tfg_control >> table
