import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeEdwProcedureOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2022, 12, 13, tz="America/Los_Angeles"),
    'owner': owners.data_integrations,
    "email": email_lists.engineering_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id='edm_analytics_base_customer_lifetime_value_monthly_eu',
    default_args=default_args,
    schedule="0 6 1,2 * *",
    catchup=False,
    max_active_tasks=2,
    max_active_runs=1,
)

with dag:
    ltv_monthly_eu = SnowflakeEdwProcedureOperator(
        procedure='analytics_base.customer_lifetime_value_monthly_eu.sql',
        database='edw_prod',
        watermark_tables=[
            'edw_prod.analytics_base.finance_sales_ops',
            'edw_prod.stg.fact_activation',
        ],
        warehouse='DA_WH_ETL_HEAVY',
    )
