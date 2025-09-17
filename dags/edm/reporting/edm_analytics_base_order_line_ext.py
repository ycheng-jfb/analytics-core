import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2021, 2, 16, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.engineering_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_analytics_base_order_line_ext",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=2,
    max_active_runs=1,
)

with dag:
    ltv_ltd = SnowflakeProcedureOperator(
        procedure="analytics_base.order_line_ext_stg.sql",
        database="edw_prod",
        warehouse="DA_WH_ETL_HEAVY",
    )
