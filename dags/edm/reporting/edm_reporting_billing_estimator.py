import pendulum
from airflow.models import DAG
from airflow.operators.python import BranchPythonOperator

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2021, 4, 26, tz="America/Los_Angeles"),
    "owner": owners.central_analytics,
    "email": email_lists.edw_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_reporting_billing_estimator",
    default_args=default_args,
    schedule="0 11,19 7-14 * *",
    catchup=False,
    max_active_runs=1,
)


def check_date_and_time(**kwargs):
    execution_time = kwargs["data_interval_end"].in_timezone("America/Los_Angeles")
    if execution_time.hour == 19 and execution_time.day == 7:
        return [billing_estimator_estimates.task_id]
    elif execution_time.hour == 11 and execution_time.day != 7:
        return [billing_estimator_estimates.task_id]
    else:
        return []


with dag:
    billing_estimator_estimates = SnowflakeProcedureOperator(
        procedure="shared.billing_estimator_estimates.sql", database="reporting_prod"
    )
    billing_estimator_output = SnowflakeProcedureOperator(
        procedure="shared.billing_estimator_output.sql", database="reporting_prod"
    )
    billing_estimator_final_output_base = SnowflakeProcedureOperator(
        procedure="shared.billing_estimator_final_output_base.sql",
        database="reporting_prod",
    )
    billing_estimator_final_output = TableauRefreshOperator(
        task_id="trigger_billing_estimator_final_output_tableau_extract",
        data_source_name="TFG037a - Billing Estimator Final Output",
    )
    billing_estimator_rates = TableauRefreshOperator(
        task_id="trigger_billing_estimator_rates_tableau_extract",
        data_source_name="TFG037b - Billing Estimator Rates",
    )
    budget_forecast = TableauRefreshOperator(
        task_id="trigger_budget_forecast_tableau_extract",
        data_source_name="TFG037c - Budget-Forecast",
    )
    footer_notes = TableauRefreshOperator(
        task_id="trigger_footer_notes_tableau_extract",
        data_source_name="TFG037d - Footer Notes",
    )
    check_run = BranchPythonOperator(
        python_callable=check_date_and_time, task_id="check_run_date_and_time"
    )
    chain_tasks(
        check_run,
        billing_estimator_estimates,
        billing_estimator_output,
        billing_estimator_final_output_base,
        [
            billing_estimator_final_output,
            billing_estimator_rates,
            budget_forecast,
            footer_notes,
        ],
    )
