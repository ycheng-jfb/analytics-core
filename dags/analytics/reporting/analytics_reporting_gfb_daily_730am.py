import pendulum
from airflow.models import DAG

from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import owners
from include.config.email_lists import analytics_support

default_args = {
    'depends_on_past': False,
    'start_date': pendulum.datetime(2019, 1, 1, 7, tz='America/Los_Angeles'),
    'retries': 1,
    'owner': owners.jfb_analytics,
    'email': analytics_support,
}


dag = DAG(
    dag_id="analytics_reporting_gfb_daily_730am",
    default_args=default_args,
    schedule="30 7 * * *",
    catchup=False,
)


with dag:
    dos_112_gfb_lead_performance = SnowflakeProcedureOperator(
        procedure='gfb.dos_112_gfb_lead_performance.sql',
        database='reporting_prod',
        autocommit=False,
    )

    gfb013_wide_fit_purchase = chain_tasks(
        SnowflakeProcedureOperator(
            procedure='gfb.gfb013_wide_fit_purchase.sql',
            database='reporting_prod',
            autocommit=False,
        ),
        TableauRefreshOperator(
            task_id='tableau_refresh_gfb013_wide_fit_purchase',
            data_source_name='GFB013_WIDE_FIT_PURCHASE',
        ),
    )
