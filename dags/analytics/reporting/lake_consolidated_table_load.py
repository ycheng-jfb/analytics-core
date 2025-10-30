from pathlib import Path

import pendulum
from airflow.models import DAG

from include.airflow.operators.snowflake import SnowflakeProcedureOperator, SnowflakeSqlOperator
from include.config import owners
from airflow.operators.empty import EmptyOperator

default_args = {
    'depends_on_past': False,
    'start_date': pendulum.datetime(2019, 1, 1, 7, tz='America/Los_Angeles'),
    'retries': 1,
    'owner': owners.jfb_analytics,
}

dag = DAG(
    dag_id="analytics_lake_consolidated_table_load",
    default_args=default_args,
    schedule="0 0 * * *",
    catchup=False,
)

with (dag):
    lake_consolidated__ultra_merchant_history__customer = SnowflakeProcedureOperator(
        procedure='ultra_merchant_history.customer.sql',
        database='lake_consolidated',
        autocommit=False,
    )

    lake_consolidated__ultra_merchant_history__discount = SnowflakeProcedureOperator(
        procedure='ultra_merchant_history.discount.sql',
        database='lake_consolidated',
        autocommit=False,
    )

    lake_consolidated__ultra_merchant_history__membership = SnowflakeProcedureOperator(
        procedure='ultra_merchant_history.membership.sql',
        database='lake_consolidated',
        autocommit=False,
    )

    lake_consolidated__ultra_merchant_history__membership_reward_tier = SnowflakeProcedureOperator(
        procedure='ultra_merchant_history.membership_reward_tier.sql',
        database='lake_consolidated',
        autocommit=False,
    )

    lake_consolidated__ultra_merchant_history__pricing_option = SnowflakeProcedureOperator(
        procedure='ultra_merchant_history.pricing_option.sql',
        database='lake_consolidated',
        autocommit=False,
    )

    lake_consolidated__ultra_merchant_history__product = SnowflakeProcedureOperator(
        procedure='ultra_merchant_history.product.sql',
        database='lake_consolidated',
        autocommit=False,
    )

    lake_consolidated__ultra_merchant_history__product_bundle_component = SnowflakeProcedureOperator(
        procedure='ultra_merchant_history.product_bundle_component.sql',
        database='lake_consolidated',
        autocommit=False,
    )

    lake_consolidated__ultra_merchant_history__product_category = SnowflakeProcedureOperator(
        procedure='ultra_merchant_history.product_category.sql',
        database='lake_consolidated',
        autocommit=False,
    )

    lake_consolidated__ultra_merchant_history__promo = SnowflakeProcedureOperator(
        procedure='ultra_merchant_history.promo.sql',
        database='lake_consolidated',
        autocommit=False,
    )
