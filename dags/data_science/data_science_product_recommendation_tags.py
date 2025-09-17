import pendulum
from airflow.models import DAG
from include.airflow.dag_helpers import chain_tasks
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from include.airflow.callbacks.slack import SlackFailureCallback
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import conn_ids, owners

default_args = {
    "start_date": pendulum.datetime(2020, 11, 1, tz="America/Los_Angeles"),
    "owner": owners.data_science,
    "email": "datascience@techstyle.com",
    "on_failure_callback": SlackFailureCallback("slack_alert_data_science"),
}

# start early evening to be finished before nightly algorithm runs
dag = DAG(
    dag_id="data_science_product_recommendation_tags",
    default_args=default_args,
    schedule="15 16 * * *",
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
)

with dag:
    chain_tasks(
        SnowflakeProcedureOperator(
            procedure="data_science.product_recommendation_tags.sql",
            database="reporting_base_prod",
        ),
        SnowflakeProcedureOperator(
            procedure="data_science.product_recommendation_tags_pivoted.sql",
            database="reporting_base_prod",
        ),
        SnowflakeProcedureOperator(
            procedure="data_science.ad_id_source_data.sql",
            database="reporting_base_prod",
        ),
        DatabricksRunNowOperator(
            task_id="ad_id",
            databricks_conn_id=conn_ids.Databricks.duplo,
            job_id=165244648727689,
        ),
        SnowflakeProcedureOperator(
            procedure="data_science.ad_id_tableau.sql",
            database="reporting_base_prod",
        ),
    )
