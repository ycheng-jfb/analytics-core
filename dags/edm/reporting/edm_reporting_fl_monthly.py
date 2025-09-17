from datetime import timedelta

import pendulum
from airflow.models import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2023, 5, 17, tz="America/Los_Angeles"),
    "owner": owners.fl_analytics,
    "email": email_lists.fabletics_analytics_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_reporting_fl_monthly",
    default_args=default_args,
    schedule="0 5 * * 1-6",
    catchup=False,
    max_active_runs=1,
)


with dag:
    fl_ltv_dashboard = SnowflakeProcedureOperator(
        procedure="fabletics.fl_ltv_dashboard.sql", database="reporting_prod"
    )
    check_cltv_ltd_run = ExternalTaskSensor(
        task_id="check_cltv_ltd_run_completion",
        external_dag_id="edm_analytics_base_customer_lifetime_value_monthly",
        external_task_id="edw_prod.analytics_base.customer_lifetime_value_ltd.sql",
        execution_delta=timedelta(minutes=-1215),
        timeout=7200,
        poke_interval=60 * 10,
        mode="reschedule",
        allowed_states=["success"],
    )
    tableau_fl013_fl_ltv = TableauRefreshOperator(
        task_id="trigger_tableau_fl013_fl_ltv",
        data_source_id="c0221465-ef14-4b80-8bb2-b09a014e5bc3",
    )
    check_cltv_ltd_run >> fl_ltv_dashboard >> tableau_fl013_fl_ltv
