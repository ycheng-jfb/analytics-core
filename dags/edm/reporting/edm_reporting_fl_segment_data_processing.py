import pendulum
from airflow import DAG


from include.config import email_lists, owners
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.tableau import TableauRefreshOperator
from include.airflow.operators.snowflake import SnowflakeProcedureOperator

default_args = {
    "start_date": pendulum.datetime(2020, 5, 19, 7, tz="America/Los_Angeles"),
    "owner": owners.fl_analytics,
    "email": email_lists.fabletics_analytics_support,
    "on_failure_callback": slack_failure_edm,
}


def check_time(**context):
    execution_time = context["data_interval_end"].in_timezone("America/Los_Angeles")

    if (execution_time.hour == 0 and execution_time.weekday() == 0) or (
        execution_time.hour == 5
    ):
        return [pcvr_session_historical.task_id]
    return []


dag = DAG(
    dag_id="edm_reporting_fl_segment_data_processing",
    default_args=default_args,
    schedule="0 0,5 * * *",
    catchup=False,
    max_active_runs=1,
)
with dag:
    pcvr_session_historical = SnowflakeProcedureOperator(
        procedure="fabletics.pcvr_session_historical.sql",
        database="reporting_prod",
    )
    pcvr_product_psource = SnowflakeProcedureOperator(
        procedure="fabletics.pcvr_product_psource.sql",
        database="reporting_prod",
    )
    FL073_pcvr_psource = TableauRefreshOperator(
        task_id="trigger_FL073_pcvr_psource_tableau_extract",
        data_source_name="FL073 PCVR PSource",
    )

    check_desired_schedule = BranchPythonOperator(
        task_id="check_time", python_callable=check_time
    )

    (
        check_desired_schedule
        >> pcvr_session_historical
        >> pcvr_product_psource
        >> FL073_pcvr_psource
    )
