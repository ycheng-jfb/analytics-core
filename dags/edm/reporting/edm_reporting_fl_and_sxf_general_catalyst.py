import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2025, 1, 15, 7, tz="America/Los_Angeles"),
    "retries": 3,
    "owner": owners.analytics_engineering,
    "email": email_lists.edw_support + email_lists.central_analytics_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_reporting_fl_and_sxf_general_catalyst",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=2,
    max_active_runs=1,
)


def check_weekday(data_interval_end: pendulum.datetime):
    run_time = data_interval_end.in_timezone('America/Los_Angeles')
    if run_time.day_of_week == 0:
        return 'weekly_tasks_sunday_branch_out'
    else:
        return []


with dag:
    general_catalyst_historical_customer_request = SnowflakeProcedureOperator(
        procedure="fabletics.gc_historical_customer_request.sql",
        database="reporting_prod",
    )
    general_catalyst_historical_customer_request_snapshot = SnowflakeProcedureOperator(
        procedure="fabletics.gc_historical_customer_request_snapshot.sql",
        database="reporting_prod",
    )
    general_catalyst_historical_customer_request_sxf = SnowflakeProcedureOperator(
        procedure="sxf.gc_historical_customer_request_sxf.sql",
        database="reporting_prod",
    )
    general_catalyst_historical_customer_request_sxf_snapshot = SnowflakeProcedureOperator(
        procedure="sxf.gc_historical_customer_request_sxf_snapshot.sql",
        database="reporting_prod",
    )
    general_catalyst_historical_customer_request_validation_data = SnowflakeProcedureOperator(
        procedure="fabletics.gc_historical_customer_request_validation_data.sql",
        database="reporting_prod",
    )
    general_catalyst_historical_customer_request_sxf_validation_data = SnowflakeProcedureOperator(
        procedure="sxf.gc_historical_customer_request_sxf_validation_data.sql",
        database="reporting_prod",
    )

    check_weekday_br = BranchPythonOperator(
        task_id="check_weekday",
        python_callable=check_weekday,
    )

    weekly_tasks_sunday_branch_out = EmptyOperator(task_id='weekly_tasks_sunday_branch_out')

    chain_tasks(check_weekday_br, weekly_tasks_sunday_branch_out)

    (
        general_catalyst_historical_customer_request
        >> general_catalyst_historical_customer_request_snapshot
    )

    (
        general_catalyst_historical_customer_request_sxf
        >> general_catalyst_historical_customer_request_sxf_snapshot
    )

    chain_tasks(
        [
            general_catalyst_historical_customer_request_snapshot,
            weekly_tasks_sunday_branch_out,
        ],
        general_catalyst_historical_customer_request_validation_data,
    )
    chain_tasks(
        [
            general_catalyst_historical_customer_request_sxf_snapshot,
            weekly_tasks_sunday_branch_out,
        ],
        general_catalyst_historical_customer_request_sxf_validation_data,
    )
