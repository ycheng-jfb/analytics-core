import pendulum
from airflow.models import DAG
from dags.include.config import conn_ids, email_lists, owners, s3_buckets, stages
from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.genesys import GenesysRoutingQueuesToS3
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.utils.snowflake import Column, CopyConfigCsv

default_args = {
    "start_date": pendulum.datetime(2019, 7, 14, tz="America/Los_Angeles"),
    "retries": 3,
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_gsc,
}

dag = DAG(
    dag_id="global_apps_inbound_genesys_queues",
    default_args=default_args,
    schedule="0 6 * * *",
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)

column_list = [
    Column("id", "VARCHAR", uniqueness=True),
    Column("name", "VARCHAR"),
    Column("division", "VARIANT"),
    Column("dateCreated", "TIMESTAMP_NTZ(3)", source_name="date_created"),
    Column(
        "dateModified", "TIMESTAMP_NTZ(3)", source_name="date_modified", delta_column=0
    ),
    Column("modifiedBy", "VARCHAR", source_name="modified_by"),
    Column("memberCount", "NUMBER", source_name="member_count"),
    Column("userMemberCount", "NUMBER", source_name="user_member_count"),
    Column("joinedMemberCount", "NUMBER", source_name="joined_member_count"),
    Column("mediaSettings", "VARIANT", source_name="media_settings"),
    Column("routingRules", "VARIANT", source_name="routing_rules"),
    Column("acwSettings", "VARIANT", source_name="acw_settings"),
    Column("skillEvaluationMethod", "VARCHAR", source_name="skill_evaluation_method"),
    Column("queueFlow", "VARIANT", source_name="queue_flow"),
    Column("autoAnswerOnly", "Boolean", source_name="auto_answer_only"),
    Column("enableTranscription", "Boolean", source_name="enable_transcription"),
    Column("enableManualAssignment", "Boolean", source_name="enable_manual_assignment"),
    Column("defaultScripts", "VARIANT", source_name="default_scripts"),
    Column(
        "suppressInQueueCallRecording",
        "Boolean",
        source_name="suppress_inqueue_call_recording",
    ),
    Column("selfUri", "VARCHAR", source_name="self_uri"),
]

s3_prefix = "lake/lake.genesys.routing_queues/v2"
date_param = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%f')[0:-3] }}"

with dag:
    to_s3 = GenesysRoutingQueuesToS3(
        task_id="genesys_routing_queues_to_s3",
        genesys_conn_id=conn_ids.Genesys.genesys_bond,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        column_list=[col.name for col in column_list],
        api_version="v2",
        bucket=s3_buckets.tsos_da_int_inbound,
        key=f"{s3_prefix}/{date_param}.gz",
    )

    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="genesys_routing_queues_s3_to_snowflake",
        database="lake",
        staging_database="lake_stg",
        schema="genesys",
        table="routing_queues",
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}/",
        copy_config=CopyConfigCsv(field_delimiter="\t", header_rows=0, skip_pct=3),
    )

    to_s3 >> to_snowflake
