from datetime import timedelta


import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import email_lists, owners, snowflake_roles, conn_ids

default_args = {
    'depends_on_past': False,
    'start_date': pendulum.datetime(2019, 1, 1, 7, tz='America/Los_Angeles'),
    'retries': 1,
    'owner': owners.analytics_engineering,
    'email': email_lists.edw_engineering,
    'on_failure_callback': slack_failure_edm,
    'execution_timeout': timedelta(hours=3),
}


dag = DAG(
    dag_id='global_apps_gsc_reporting_ecom_returns',
    default_args=default_args,
    schedule='0 4 * * *',
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
    doc_md=__doc__,
)


with dag:
    gsc_ecom_returns = SnowflakeProcedureOperator(
        procedure='gsc.ecom_returns.sql',
        database='reporting_prod',
        autocommit=False,
    )
