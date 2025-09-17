import pendulum
from airflow.models import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

from include.airflow.callbacks.slack import slack_failure_data_science
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_load import (
    CopyConfigCsv,
    SnowflakeIncrementalLoadOperator,
)
from include.config import conn_ids, owners, snowflake_roles, stages
from include.config.email_lists import airflow_media_support
from include.utils.snowflake import Column

file_path = f"{stages.tsos_da_int_databricks}/snowflake/reporting_prod.data_science/tableau_reviews_topic_model.update.csv.gz"

default_args = {
    "start_date": pendulum.datetime(2019, 1, 1, 7, tz="America/Los_Angeles"),
    "owner": owners.data_science,
    "email": airflow_media_support,
    "on_failure_callback": slack_failure_data_science,
}


dag = DAG(
    dag_id="data_science_reviews_topic_model",
    default_args=default_args,
    schedule="0 6 * * *",
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
)

with dag:
    transform_reviews = SnowflakeProcedureOperator(
        procedure="data_science.tableau_customer_reviews.sql",
        database="reporting_prod",
    )

    databricks_run_topic_model = DatabricksRunNowOperator(
        databricks_conn_id=conn_ids.Databricks.duplo,
        task_id="databricks_run_topic_model",
        job_id="910810365578499",
        json={
            "notebook_params": {
                "run_mode": "daily_update",
                "number_of_products": "0",
                "file_path": file_path,
            }
        },
    )

    update_file_to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="snowflake_load_topic_model",
        files_path=file_path,
        role=snowflake_roles.etl_service_account,
        database="reporting_prod",
        schema="data_science",
        table="tableau_reviews_topic_model",
        column_list=[
            Column("group_code", "VARCHAR(20)"),
            Column("review_id", "INT", uniqueness=True),
            Column("topic", "VARCHAR(50)", uniqueness=True),
            Column("score", "DOUBLE"),
            Column("updated_at", "TIMESTAMP_LTZ", delta_column=True),
        ],
        pre_merge_command="""
            DELETE FROM reporting_prod.data_science.tableau_reviews_topic_model
            WHERE group_code IN (
                SELECT DISTINCT group_code
                FROM reporting_prod.data_science.tableau_reviews_topic_model_stg
            );
        """,
        copy_config=CopyConfigCsv(field_delimiter=",", header_rows=1, skip_pct=1),
    )

    databricks_run_vector_semantic_score = DatabricksRunNowOperator(
        databricks_conn_id=conn_ids.Databricks.duplo,
        task_id="databricks_run_vector_semantic_score",
        job_id="476387114396320",
    )

    update_embedding_snowflake = SnowflakeProcedureOperator(
        procedure="data_science.tableau_customer_reviews_embedding_snowflake.sql",
        database="reporting_prod",
    )

    chain_tasks(
        transform_reviews,
        databricks_run_topic_model,
        update_file_to_snowflake,
        databricks_run_vector_semantic_score,
        update_embedding_snowflake,
    )
