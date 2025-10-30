import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import email_lists, owners

default_args = {
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 4, 10, 7, tz='America/Los_Angeles'),
    'owner': owners.gms_analytics,
    'email': email_lists.data_integration_support,
    'on_failure_callback': slack_failure_gsc,
}

dag = DAG(
    dag_id='global_apps_gms_reporting_monthly',
    default_args=default_args,
    schedule='00 5 1 * *',
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
)

with dag:
    gms_001_tableau_refresh = TableauRefreshOperator(
        task_id='trigger_tableau_extract_refresh_gms_001',
        data_source_id='b503e13f-cfa0-4446-b0bf-48e3569778c5',
    )

    gms_operational_billing_history = SnowflakeProcedureOperator(
        procedure='gms.gms_operational_billing_history.sql', database='reporting_prod'
    )

    trust_pilot_and_consumeraffairs_list = SnowflakeProcedureOperator(
        procedure='gms.gms_trustpilot_and_consumeraffairs_list.sql', database='reporting_prod'
    )

    trust_pilot_and_consumeraffairs_list >> gms_001_tableau_refresh
