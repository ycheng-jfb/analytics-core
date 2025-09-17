import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.genesys import GenesysFlowOutcomesToS3Operator
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import conn_ids, email_lists, owners, s3_buckets, stages
from include.utils.snowflake import Column, CopyConfigJson

column_list = [
    Column("conversationId", "STRING", uniqueness=True),
    Column("flowOutcomeId", "STRING", uniqueness=True),
    Column("flowOutcome", "STRING"),
    Column("flowOutcomeValue", "STRING"),
    Column("flowName", "STRING"),
    Column("mediaType", "STRING"),
    Column("updated_at", "TIMESTAMP_LTZ(9)", delta_column=True),
]


default_args = {
    "start_date": pendulum.datetime(2020, 9, 30, tz="America/Los_Angeles"),
    "retries": 3,
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_gsc,
}


dag = DAG(
    dag_id="global_apps_inbound_genesys_flow_outcomes",
    default_args=default_args,
    schedule="1 1,15 * * *",
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)

s3_prefix = "lake/lake.genesys.flow_outcomes/v2"

with dag:
    genesys_to_s3 = GenesysFlowOutcomesToS3Operator(
        task_id="genesys_to_s3",
        key=f"{s3_prefix}/{{macros.datetime.utcnow().strftime('%Y%m%dT%H%M%S')}}.ndjson.gz",
        bucket=s3_buckets.tsos_da_int_inbound,
        genesys_conn_id=conn_ids.Genesys.genesys_bond,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        fail_on_no_rows=False,
    )
    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="genesys_s3_to_snowflake",
        database="lake",
        schema="genesys",
        table="flow_outcomes",
        staging_database="lake_stg",
        view_database="lake_view",
        snowflake_conn_id=conn_ids.Snowflake.default,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}/",
        copy_config=CopyConfigJson(),
    )
    genesys_to_s3 >> to_snowflake
