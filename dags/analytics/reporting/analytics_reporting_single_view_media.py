import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.dagrun_operator import TFGTriggerDagRunOperator
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import owners
from include.config.email_lists import analytics_support

default_args = {
    'depends_on_past': False,
    'start_date': pendulum.datetime(2019, 1, 1, 7, tz='America/Los_Angeles'),
    'retries': 1,
    'owner': owners.analytics_engineering,
    'max_active_tis_per_dag': 2,
    'email': analytics_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="analytics_reporting_single_view_media",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=2,
    max_active_runs=1,
)

with dag:
    svm_session_ids_stg = SnowflakeProcedureOperator(
        procedure='shared.svm_session_ids_stg.sql',
        database='reporting_base_prod',
        warehouse='DA_WH_ETL_HEAVY',
    )
    session_refresh_base = SnowflakeProcedureOperator(
        procedure='shared.session_refresh_base.sql',
        database='reporting_base_prod',
        warehouse='DA_WH_ETL_HEAVY',
    )
    session_single_view_media = SnowflakeProcedureOperator(
        procedure='shared.session_single_view_media.sql',
        database='reporting_base_prod',
        warehouse='DA_WH_ETL_HEAVY',
    )
    single_view_media = SnowflakeProcedureOperator(
        procedure='shared.single_view_media.sql',
        database='reporting_prod',
        warehouse='DA_WH_ETL_HEAVY',
    )

    tableau_refresh_single_view_media = TableauRefreshOperator(
        task_id='tableau_refresh_single_view_media',
        data_source_name='SINGLE_VIEW_MEDIA_V2',
        wait_till_complete=True,
    )

    trigger_media_tff = TFGTriggerDagRunOperator(
        task_id='trigger_media_transform_friction_finder',
        trigger_dag_id='media_transform_friction_finder',
        execution_date='{{ data_interval_end }}',
    )
    chain_tasks(
        svm_session_ids_stg,
        session_refresh_base,
        session_single_view_media,
        [single_view_media, trigger_media_tff],
    )
    single_view_media >> tableau_refresh_single_view_media
