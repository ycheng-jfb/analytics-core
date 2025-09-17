import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.dagrun_operator import TFGTriggerDagRunOperator
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.tableau import TableauRefreshOperator
from include.config import owners
from include.config.email_lists import edw_support

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2022, 1, 11, 7, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.analytics_engineering,
    "email": edw_support,
    "on_failure_callback": slack_failure_edm,
}
dag = DAG(
    dag_id="edm_reporting_psource_payments_by_day",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
)

with dag:
    order_line_psource_payment = SnowflakeProcedureOperator(
        procedure="shared.order_line_psource_payment.sql",
        warehouse="DA_WH_ETL_HEAVY",
        database="reporting_base_prod",
    )

    payment_method_by_day = SnowflakeProcedureOperator(
        procedure="shared.payment_method_by_day.sql",
        database="reporting_prod",
    )

    psource_revenue_by_day = SnowflakeProcedureOperator(
        procedure="shared.psource_revenue_by_day.sql",
        database="reporting_prod",
    )

    psource_scaffold_revenue_by_day = SnowflakeProcedureOperator(
        procedure="shared.psource_scaffold_revenue_by_day.sql",
        database="reporting_prod",
    )

    tableau_psource_revenue_by_day = TableauRefreshOperator(
        task_id="tableau_psource_revenue_by_day",
        data_source_name="TFG018 - Psource Revenue by Day",
        wait_till_complete=True,
    )

    tableau_payment_method_by_day = TableauRefreshOperator(
        task_id="tableau_payment_method_by_day",
        data_source_name="TFG017 - Payment Methods by Day",
        wait_till_complete=True,
    )

    order_line_psource_payment >> [payment_method_by_day, psource_revenue_by_day]
    psource_revenue_by_day >> psource_scaffold_revenue_by_day
    payment_method_by_day >> tableau_payment_method_by_day
    psource_scaffold_revenue_by_day >> tableau_psource_revenue_by_day

    analytics_reporting_ab_testing = TFGTriggerDagRunOperator(
        task_id="trigger_analytics_reporting_ab_testing",
        trigger_dag_id="analytics_reporting_ab_testing",
        execution_date="{{ data_interval_end }}",
    )
    [
        payment_method_by_day,
        psource_scaffold_revenue_by_day,
    ] >> analytics_reporting_ab_testing
