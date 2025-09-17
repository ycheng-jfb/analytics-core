import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import email_lists, owners

default_args = {
    "start_date": pendulum.datetime(2021, 6, 28, 7, tz="America/Los_Angeles"),
    "retries": 3,
    "owner": owners.analytics_engineering,
    "email": email_lists.edw_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_reporting_credits_daily",
    default_args=default_args,
    schedule="30 3 * * *",
    catchup=False,
    max_active_tasks=2,
    max_active_runs=1,
)


with dag:
    dim_credit = SnowflakeProcedureOperator(
        procedure="shared.dim_credit.sql",
        database="reporting_base_prod",
        warehouse="DA_WH_EDW",
    )
    fact_credit_event = SnowflakeProcedureOperator(
        procedure="shared.fact_credit_event.sql",
        database="reporting_base_prod",
        warehouse="DA_WH_EDW",
    )
    fact_order_credit = SnowflakeProcedureOperator(
        procedure="shared.fact_order_credit.sql",
        database="reporting_base_prod",
    )
    cohort_waterfall = SnowflakeProcedureOperator(
        procedure="shared.credit_activity_waterfalls_original_cohort.sql",
        database="reporting_prod",
    )
    cohort_waterfall_tableau_refresh = TableauRefreshOperator(
        task_id="trigger_cohort_waterfall_tableau_refresh",
        data_source_id="318f36ee-3e18-4009-9fa5-eaaddaa61972",
    )
    endowment_reporting = SnowflakeProcedureOperator(
        procedure="shared.bounceback_endowment_detail.sql",
        database="reporting_base_prod",
    )

    (
        dim_credit
        >> [fact_credit_event, fact_order_credit]
        >> cohort_waterfall
        >> cohort_waterfall_tableau_refresh
        >> endowment_reporting
    )
