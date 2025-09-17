import pendulum

from airflow.models import DAG
from collections import namedtuple
from dataclasses import dataclass
from datetime import timedelta

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.cognigy import CognigyToS3Operator
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_load import (
    Column,
    CopyConfigCsv,
    SnowflakeIncrementalLoadOperator,
)
from include.config import email_lists, owners, s3_buckets, stages, conn_ids


@dataclass
class Config:
    endpoint: str
    table_name: str
    date_column: str or None
    column_list: list


configs = [
    Config(
        endpoint="Analytics",
        table_name="analytics",
        date_column="timestamp",
        column_list=[
            Column("odata_id", "VARCHAR", source_name="@odata.id"),
            Column("id", "VARCHAR", source_name="_id", uniqueness=True),
            Column("organisation", "VARCHAR", source_name="organisation"),
            Column("project_id", "VARCHAR", source_name="projectId"),
            Column("flow_reference_id", "VARCHAR", source_name="flowReferenceId"),
            Column("entrypoint", "VARCHAR", source_name="entrypoint"),
            Column("ip", "VARCHAR", source_name="ip"),
            Column("contact_id", "VARCHAR", source_name="contactId"),
            Column("session_id", "VARCHAR", source_name="sessionId"),
            Column("input_id", "VARCHAR", source_name="inputId"),
            Column("input_text", "VARCHAR", source_name="inputText"),
            Column("input_data", "VARIANT", source_name="inputData"),
            Column("state", "VARCHAR", source_name="state"),
            Column("mode", "VARCHAR", source_name="mode"),
            Column("user_type", "VARCHAR", source_name="userType"),
            Column("channel", "VARCHAR", source_name="channel"),
            Column("flow_language", "VARCHAR", source_name="flowLanguage"),
            Column("intent", "VARCHAR", source_name="intent"),
            Column("intent_flow", "VARCHAR", source_name="intentFlow"),
            Column("intent_score", "NUMBER(19, 4)", source_name="intentScore"),
            Column("completed_goals_list", "VARCHAR", source_name="completedGoalsList"),
            Column("found_slots", "VARCHAR", source_name="foundSlots"),
            Column("found_slot_details", "VARCHAR", source_name="foundSlotDetails"),
            Column("understood", "BOOLEAN", source_name="understood"),
            Column("timestamp", "TIMESTAMP_TZ", source_name="timestamp"),
            Column("execution_time", "INT", source_name="executionTime"),
            Column("execution", "INT", source_name="execution"),
            Column("custom1", "VARCHAR", source_name="custom1"),
            Column("custom2", "VARCHAR", source_name="custom2"),
            Column("custom3", "VARCHAR", source_name="custom3"),
            Column("custom4", "VARCHAR", source_name="custom4"),
            Column("custom5", "VARCHAR", source_name="custom5"),
            Column("custom6", "VARCHAR", source_name="custom6"),
            Column("custom7", "VARCHAR", source_name="custom7"),
            Column("custom8", "VARCHAR", source_name="custom8"),
            Column("custom9", "VARCHAR", source_name="custom9"),
            Column("custom10", "VARCHAR", source_name="custom10"),
            Column("locale_reference_id", "VARCHAR", source_name="localeReferenceId"),
            Column("locale_name", "VARCHAR", source_name="localeName"),
            Column("endpoint_url_token", "VARCHAR", source_name="endpointUrlToken"),
            Column("endpoint_name", "VARCHAR", source_name="endpointName"),
            Column("rating", "VARCHAR", source_name="rating"),
            Column("rating_comment", "VARCHAR", source_name="ratingComment"),
            Column("snapshot_name", "VARCHAR", source_name="snapshotName"),
            Column("updated_at", "TIMESTAMP_LTZ", delta_column=True),
        ],
    ),
    Config(
        endpoint="Sessions",
        table_name="sessions",
        date_column="startedAt",
        column_list=[
            Column("odata_id", "VARCHAR", source_name="@odata.id"),
            Column("id", "VARCHAR", source_name="_id", uniqueness=True),
            Column("project_id", "VARCHAR", source_name="projectId"),
            Column("organisation_id", "VARCHAR", source_name="organisationId"),
            Column("goals", "VARCHAR", source_name="goals"),
            Column("step_path", "VARCHAR", source_name="stepPath"),
            Column("steps_count", "INT", source_name="stepsCount"),
            Column("handover_escalations", "INT", source_name="handoverEscalations"),
            Column("started_at", "TIMESTAMP_TZ", source_name="startedAt"),
            Column("user_id", "VARCHAR", source_name="userId"),
            Column("session_id", "VARCHAR", source_name="sessionId"),
            Column("locale_reference_id", "VARCHAR", source_name="localeReferenceId"),
            Column("locale_name", "VARCHAR", source_name="localeName"),
            Column(
                "endpoint_reference_id", "VARCHAR", source_name="endpointReferenceId"
            ),
            Column("endpoint_name", "VARCHAR", source_name="endpointName"),
            Column("project_name", "VARCHAR", source_name="projectName"),
            Column("snapshot_id", "VARCHAR", source_name="snapshotId"),
            Column("snapshot_name", "VARCHAR", source_name="snapshotName"),
            Column("rating", "VARCHAR", source_name="rating"),
            Column("rating_comment", "VARCHAR", source_name="ratingComment"),
            Column("updated_at", "TIMESTAMP_LTZ", delta_column=True),
        ],
    ),
    Config(
        endpoint="Conversations",
        table_name="conversations",
        date_column="timestamp",
        column_list=[
            Column("odata_id", "VARCHAR", source_name="@odata.id"),
            Column("id", "VARCHAR", source_name="_id", uniqueness=True),
            Column("project_id", "VARCHAR", source_name="projectId"),
            Column("project_name", "VARCHAR", source_name="projectName"),
            Column("input_id", "VARCHAR", source_name="inputId"),
            Column("session_id", "VARCHAR", source_name="sessionId"),
            Column("contact_id", "VARCHAR", source_name="contactId"),
            Column("organisation", "VARCHAR", source_name="organisation"),
            Column("input_text", "VARCHAR", source_name="inputText"),
            Column("input_data", "VARIANT", source_name="inputData"),
            Column("type", "VARCHAR", source_name="type"),
            Column("source", "VARCHAR", source_name="source"),
            Column("reference", "VARCHAR", source_name="reference"),
            Column("timestamp", "TIMESTAMP_TZ", source_name="timestamp"),
            Column("flow_name", "VARCHAR", source_name="flowName"),
            Column("flow_parent_id", "VARCHAR", source_name="flowParentId"),
            Column("channel", "VARCHAR", source_name="channel"),
            Column("in_handover_request", "BOOLEAN", source_name="inHandoverRequest"),
            Column(
                "in_handover_conversation",
                "BOOLEAN",
                source_name="inHandoverConversation",
            ),
            Column("output_id", "VARCHAR", source_name="outputId"),
            Column("locale_reference_id", "VARCHAR", source_name="localeReferenceId"),
            Column("locale_name", "VARCHAR", source_name="localeName"),
            Column("endpoint_url_token", "VARCHAR", source_name="endpointUrlToken"),
            Column("endpoint_name", "VARCHAR", source_name="endpointName"),
            Column("snapshot_id", "VARCHAR", source_name="snapshotId"),
            Column("snapshot_name", "VARCHAR", source_name="snapshotName"),
            Column("rating", "VARCHAR", source_name="rating"),
            Column("rating_comment", "VARCHAR", source_name="ratingComment"),
            Column("updated_at", "TIMESTAMP_LTZ", delta_column=True),
        ],
    ),
    Config(
        endpoint="Steps",
        table_name="steps",
        date_column=None,
        column_list=[
            Column("odata_id", "VARCHAR", source_name="@odata.id"),
            Column("id", "VARCHAR", source_name="_id", uniqueness=True),
            Column("label", "VARCHAR", source_name="label"),
            Column("type", "VARCHAR", source_name="type"),
            Column("entity_reference_id", "VARCHAR", source_name="entityReferenceId"),
            Column("flowReference_id", "VARCHAR", source_name="flowReferenceId"),
            Column("flow_name", "VARCHAR", source_name="flowName"),
            Column("project_name", "VARCHAR", source_name="projectName"),
            Column("snapshot_id", "VARCHAR", source_name="snapshotId"),
            Column("snapshot_name", "VARCHAR", source_name="snapshotName"),
            Column("updated_at", "TIMESTAMP_LTZ", delta_column=True),
        ],
    ),
    Config(
        endpoint="ExecutedSteps",
        table_name="executed_steps",
        date_column="timestamp",
        column_list=[
            Column("odata_id", "VARCHAR", source_name="@odata.id"),
            Column("id", "VARCHAR", source_name="_id", uniqueness=True),
            Column("project_id", "VARCHAR", source_name="projectId"),
            Column("organisation_id", "VARCHAR", source_name="organisationId"),
            Column("user_id", "VARCHAR", source_name="userId"),
            Column("session_id", "VARCHAR", source_name="sessionId"),
            Column("input_id", "VARCHAR", source_name="inputId"),
            Column("step_label", "VARCHAR", source_name="stepLabel"),
            Column("parent_step", "VARCHAR", source_name="parentStep"),
            Column("type", "VARCHAR", source_name="type"),
            Column("entity_reference_id", "VARCHAR", source_name="entityReferenceId"),
            Column("flow_reference_id", "VARCHAR", source_name="flowReferenceId"),
            Column("flow_name", "VARCHAR", source_name="flowName"),
            Column("timestamp", "TIMESTAMP_TZ", source_name="timestamp"),
            Column("project_name", "VARCHAR", source_name="projectName"),
            Column("snapshot_id", "VARCHAR", source_name="snapshotId"),
            Column("snapshot_name", "VARCHAR", source_name="snapshotName"),
            Column("locale_reference_id", "VARCHAR", source_name="localeReferenceId"),
            Column("locale_name", "VARCHAR", source_name="localeName"),
            Column("endpoint_url_token", "VARCHAR", source_name="endpointUrlToken"),
            Column("endpoint_name", "VARCHAR", source_name="endpointName"),
            Column("updated_at", "TIMESTAMP_LTZ", delta_column=True),
        ],
    ),
    # ToDo: Uncomment the below config when we start getting the data from API
    # Config(
    #     endpoint="LiveAgentEscalations",
    #     table_name="live_agent_escalations",
    #     date_column="timestamp",
    #     column_list=[
    #         Column("odata_id", "VARCHAR", source_name="@odata.id"),
    #         Column("id", "VARCHAR", source_name="_id", uniqueness=True),
    #         Column("organisation_id", "VARCHAR", source_name="organisationId"),
    #         Column("project_id", "VARCHAR", source_name="projectId"),
    #         Column("session_id", "VARCHAR", source_name="sessionId"),
    #         Column("timestamp", "TIMESTAMP_TZ", source_name="timestamp"),
    #         Column("locale_name", "VARCHAR", source_name="localeName"),
    #         Column("status", "VARCHAR", source_name="status"),
    #         Column("inbox_id", "VARCHAR", source_name="inboxId"),
    #         Column("inbox_name", "VARCHAR", source_name="inboxName"),
    #         Column("team_id", "VARCHAR", source_name="teamId"),
    #         Column("team_name", "VARCHAR", source_name="teamName"),
    #         Column("labels", "VARIANT", source_name="labels"),
    #         Column("agent_id", "VARCHAR", source_name="agentId"),
    #         Column("agent_name", "VARCHAR", source_name="agentName"),
    #         Column("contact_id", "VARCHAR", source_name="contactId"),
    #         Column("endpoint_name", "VARCHAR", source_name="endpointName"),
    #         Column("endpoint_type", "VARCHAR", source_name="endpointType"),
    #         Column("endpoint_url_token", "VARCHAR", source_name="endpointUrlToken"),
    #         Column("channel", "VARCHAR", source_name="channel"),
    #         Column("locale_reference_id", "VARCHAR", source_name="localeReferenceId"),
    #         Column("snapshot_id", "VARCHAR", source_name="snapshotId"),
    #         Column("endpsnapshot_name", "VARCHAR", source_name="endpsnapshotName"),
    #         Column("updated_at", "TIMESTAMP_LTZ", delta_column=True),
    #     ],
    # ),
]

Project = namedtuple("Project", ["name", "id"])
projects = [
    Project("web", "667048bc82226d430fab54c4"),
    Project("voice", "6601ae4c3d686631b813765d"),
]

default_args = {
    "start_date": pendulum.datetime(2024, 4, 1, tz="America/Los_Angeles"),
    "retries": 1,
    "owner": owners.gms_analytics,
    "email": email_lists.data_integration_support + email_lists.gms_support,
    "on_failure_callback": slack_failure_edm,
    "execution_timeout": timedelta(hours=3),
}


def from_ds(data_interval_start):
    look_back_time = data_interval_start.in_timezone("America/Los_Angeles")
    if look_back_time.hour == 2:
        look_back_time = look_back_time + timedelta(days=-3)
    return look_back_time.strftime("%Y-%m-%dT%H:%M:%S%z")[0:-5]


dag = DAG(
    dag_id=f"global_apps_gms_inbound_cognigy",
    default_args=default_args,
    schedule="0 2-23/6 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    user_defined_macros={"from_ds": from_ds},
)

with dag:
    schema = "cognigy"

    date_param = "{{ data_interval_start.strftime('%Y%m%dT%H%M%S%z')[0:-5] }}"
    start_time = "{{ (prev_data_interval_end_success or data_interval_start).strftime('%Y-%m-%dT%H:%M:%S%z')[0:-5]}}"
    start_time_sessions = (
        "{{ from_ds(prev_data_interval_end_success or data_interval_start) }}"
    )
    end_time = "{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S%z')[0:-5] }}"

    flatten_conversations = SnowflakeProcedureOperator(
        database="reporting_prod",
        procedure="gms.cognigy_conversations.sql",
        watermark_tables=["lake.cognigy.conversations"],
    )
    for config in configs:
        s3_prefix = f"lake/{schema}.{config.table_name}/daily"
        to_snowflake = SnowflakeIncrementalLoadOperator(
            task_id=f"cognigy_{config.table_name}_to_snowflake",
            database="lake",
            schema=schema,
            table=config.table_name,
            column_list=config.column_list,
            files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}",
            copy_config=CopyConfigCsv(field_delimiter="\t", header_rows=0, skip_pct=3),
        )
        for project in projects:
            get_data = CognigyToS3Operator(
                task_id=f"cognigy_{project.name}_{config.table_name}_to_s3",
                bucket=s3_buckets.tsos_da_int_inbound,
                key=f"{s3_prefix}/cognigy_{project.name}_{config.table_name}_{date_param}.tsv.gz",
                column_list=[x.source_name for x in config.column_list],
                s3_conn_id=conn_ids.S3.tsos_da_int_prod,
                req_params={
                    "endpoint": config.endpoint,
                    "date_column": config.date_column,
                    "start_time": (
                        start_time_sessions
                        if config.table_name == "sessions"
                        else start_time
                    ),
                    "end_time": end_time,
                    "project_id": project.id,
                    "interval": 6,
                },
            )

            (
                get_data >> to_snowflake >> flatten_conversations
                if config.table_name == "conversations"
                else get_data >> to_snowflake
            )
