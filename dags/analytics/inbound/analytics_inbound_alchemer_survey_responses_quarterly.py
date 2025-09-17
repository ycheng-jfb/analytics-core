import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.alchemer import AlchemerGetSurveyResponses
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.config import conn_ids, email_lists, owners, s3_buckets
from task_configs.dag_config.alchemer_config import column_list

default_args = {
    "start_date": pendulum.datetime(2023, 4, 1, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "retries": 0,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="analytics_inbound_alchemer_survey_responses_quarterly",
    default_args=default_args,
    schedule="0 2 1 1,4,7,10 *",
    catchup=False,
    max_active_runs=1,
)

database = "lake"
schema = "alchemer"
table = "survey_responses_quarterly"
# Don't forget to update version in alchemer.quarterly_delete_and_load too
s3_key = f"lake/{database}.{schema}.{table}/v1"

with dag:
    to_s3 = AlchemerGetSurveyResponses(
        task_id="to_s3",
        bucket=s3_buckets.tsos_da_int_inbound,
        key=f"{s3_key}/{schema}_{table}_{{{{ ts_nodash }}}}.csv.gz",
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        column_list=[col.source_name for col in column_list],
        write_header=True,
        process_name="alchemer_survey_responses_quarterly",
        namespace="alchemer",
        full_survey_pull=False,
    )

    snowflake_proc = SnowflakeProcedureOperator(
        procedure="alchemer.survey_responses.sql", database="lake"
    )

    to_s3 >> snowflake_proc
