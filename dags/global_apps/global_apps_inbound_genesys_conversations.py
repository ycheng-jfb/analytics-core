import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.genesys import (
    GenesysConversationAnalyticsToS3,
    GenesysConversationsToS3Operator,
)
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.config import (
    conn_ids,
    email_lists,
    owners,
    s3_buckets,
    snowflake_roles,
    stages,
)
from include.utils.snowflake import Column, CopyConfigCsv, CopyConfigJson
from task_configs.dag_config.genesys_conversations_config import analytics_config as cfg
from typing import List, Optional

# from include.config import email_lists, owners
# from include.utils.snowflake import Column


column_list = [
    Column("conversationId", "STRING", uniqueness=True),
    Column("conversationEnd", "TIMESTAMP_LTZ(3)", delta_column=True),
    Column("conversationStart", "TIMESTAMP_LTZ(3)"),
    Column("mediaStatsMinConversationMos", "NUMBER(38,16)"),
    Column("mediaStatsMinConversationRFactor", "NUMBER(38,16)"),
    Column("originatingDirection", "STRING"),
    Column("participants", "VARIANT"),
    Column("externalTag", "STRING"),
]

column_list_new = [
    Column("conversationId", "STRING", uniqueness=True),
    Column("conversationEnd", "TIMESTAMP_LTZ(3)", delta_column=True),
    Column("conversationStart", "TIMESTAMP_LTZ(3)"),
    Column("mediaStatsMinConversationMos", "NUMBER(38,16)"),
    Column("mediaStatsMinConversationRFactor", "NUMBER(38,16)"),
    Column("originatingDirection", "STRING"),
    Column("participants", "VARIANT"),
    Column("externalTag", "STRING"),
    Column("source_filename", "STRING"),
    Column("last_modified", "TIMESTAMP_LTZ(3)", delta_column=True),
]

default_args = {
    "start_date": pendulum.datetime(2019, 7, 14, tz="America/Los_Angeles"),
    "retries": 3,
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_gsc,
}

dag = DAG(
    dag_id="global_apps_inbound_genesys_conversations",
    default_args=default_args,
    schedule="0 1,5,9,13,17 * * *",
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)

s3_prefix = "lake/lake.genesys.conversations/v3"


class ExtendedCopyConfigJson(CopyConfigJson):
    def __init__(self, include_metadata: Optional[dict] = None, **kwargs):
        self.include_metadata = include_metadata
        super().__init__(**kwargs)

    @property
    def copy_params_list(self) -> List[str]:
        params = {}
        if self.skip_pct:
            params.update({"ON_ERROR": f"'SKIP_FILE_{self.skip_pct}%'"})
        if self.force_copy:
            params["FORCE"] = "TRUE"
        if self.match_by_column_name is True:
            params["MATCH_BY_COLUMN_NAME"] = "CASE_INSENSITIVE"
        if self.include_metadata:
            formatted_string = ", ".join(
                f"{key}={value}" for key, value in self.include_metadata.items()
            )
            formatted_string = f"({formatted_string})"
            params["INCLUDE_METADATA"] = formatted_string
        ret = [f"{k} = {v}" for k, v in params.items()]

        return ret


with dag:
    genesys_to_s3_list = []
    for i in range(3):
        genesys_to_s3 = GenesysConversationsToS3Operator(
            task_id=f"genesys_to_s3_{i}",
            from_date=f"{{{{ macros.tfgdt.utcnow().add(days=-3 + {i}, seconds=-5) }}}}",
            to_date=f"{{{{ macros.tfgdt.utcnow().add(days=-2 + {i}) }}}}",
            key=s3_prefix + f"/{i}_" + "{{ ts_nodash }}.ndjson.gz",
            bucket=s3_buckets.tsos_da_int_inbound,
            genesys_conn_id=conn_ids.Genesys.genesys_bond,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            fail_on_no_rows=False,
        )
        genesys_to_s3_list.append(genesys_to_s3)
    to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="genesys_s3_to_snowflake",
        database="lake",
        schema="genesys",
        table="conversations",
        staging_database="lake_stg",
        snowflake_conn_id=conn_ids.Snowflake.default,
        role=snowflake_roles.etl_service_account,
        column_list=column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}/",
        copy_config=CopyConfigJson(),
    )
    to_snowflake_new = SnowflakeIncrementalLoadOperator(
        task_id="genesys_s3_to_snowflake_new",
        database="lake",
        schema="genesys",
        table="conversations_new",
        staging_database="lake_stg",
        snowflake_conn_id=conn_ids.Snowflake.default,
        role=snowflake_roles.etl_service_account,
        column_list=column_list_new,
        files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}/",
        copy_config=ExtendedCopyConfigJson(
            include_metadata={
                "SOURCE_FILENAME": "METADATA$FILENAME",
                "LAST_MODIFIED": "METADATA$FILE_LAST_MODIFIED",
            }
        ),
    )
    genesys_conversation_history = SnowflakeProcedureOperator(
        database="lake",
        procedure="genesys.conversations_history.sql",
    )
    genesys_conversation = SnowflakeProcedureOperator(
        database="reporting_prod",
        procedure="gms.genesys_conversation.sql",
        watermark_tables=["lake.genesys.conversations"],
        warehouse="DA_WH_ETL_LIGHT",
    )
    genesys_conversation_new = SnowflakeProcedureOperator(
        database="reporting_prod",
        procedure="gms.genesys_conversation_new.sql",
        watermark_tables=["lake.genesys.conversations_new"],
        warehouse="DA_WH_ETL_LIGHT",
    )
    genesys_segment = SnowflakeProcedureOperator(
        database="reporting_prod",
        procedure="gms.genesys_segment.sql",
        watermark_tables=["lake.genesys.conversations"],
        warehouse="DA_WH_ETL_LIGHT",
    )

    yr_mth = "{{macros.datetime.now().strftime('%Y%m')}}"
    analytics_s3_key = f"lake/{cfg.database}.{cfg.schema}.{cfg.table}/v1"
    analytics_to_s3 = GenesysConversationAnalyticsToS3(
        task_id="analytics_to_s3",
        bucket=s3_buckets.tsos_da_int_inbound,
        key=f"{analytics_s3_key}/{yr_mth}/{cfg.schema}_{cfg.table}_{{{{ ts_nodash }}}}.csv.gz",
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        column_list=[col.source_name for col in cfg.column_list],
        write_header=True,
        hook_conn_id=conn_ids.Genesys.genesys_bond,
        process_name="conversation_analytics",
        namespace="genesys",
    )
    analytics_to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="analytics_to_snowflake",
        database=cfg.database,
        schema=cfg.schema,
        table=cfg.table,
        staging_database="lake_stg",
        view_database="lake_view",
        snowflake_conn_id=conn_ids.Snowflake.default,
        role=snowflake_roles.etl_service_account,
        column_list=cfg.column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{analytics_s3_key}/",
        copy_config=CopyConfigCsv(field_delimiter="\t", header_rows=1),
    )

    (
        genesys_to_s3_list
        >> to_snowflake
        >> [
            genesys_conversation,
            genesys_segment,
        ]
        >> analytics_to_s3
        >> analytics_to_snowflake
    )
    (
        genesys_to_s3_list
        >> to_snowflake_new
        >> [
            genesys_conversation_history,
            genesys_conversation_new,
        ]
    )
