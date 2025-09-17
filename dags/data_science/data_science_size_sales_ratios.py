import pendulum
from airflow.models import DAG
from include.airflow.dag_helpers import chain_tasks
from include.airflow.callbacks.slack import SlackFailureCallback
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import owners

default_args = {
    "start_date": pendulum.datetime(2024, 1, 1, 7, tz="America/Los_Angeles"),
    "owner": owners.data_science,
    "email": "datascience@techstyle.com",
    "on_failure_callback": SlackFailureCallback("slack_alert_data_science"),
}

dag = DAG(
    dag_id="data_science_size_sales_ratios",
    default_args=default_args,
    schedule="40 22 * * *",
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
)

with dag:
    get_sales_inventory_by_day = SnowflakeProcedureOperator(
        procedure="data_science.streamlit_size_sales_inventory_by_day.sql",
        database="reporting_prod",
    )
