from dataclasses import dataclass
from datetime import timedelta
from typing import List

import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.airflow.operators.sprinklr import SprinklrToS3Operator
from include.config import conn_ids, email_lists, owners, s3_buckets, stages
from include.config.pools import Pool
from include.utils.snowflake import Column, CopyConfigJson


yr_mth = "{{macros.datetime.now().strftime('%Y%m')}}"


@dataclass
class SprinklrConfig:
    endpoint: str
    column_list: List[Column]
    extract_case_id: bool = False
    look_back_period: int = 0
    time_increment: int = 24 * 60 * 60 * 1000


sprinklr_configs = [
    SprinklrConfig(
        endpoint="retention",
        column_list=[
            Column(
                "case_creation_time",
                "TIMESTAMP_LTZ(7)",
                source_name="DATE_TYPE_CASE_CREATION_TIME",
            ),
            Column("case_id", "int", uniqueness=True, source_name="CASE_ID"),
            Column(
                "handled_by", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY"
            ),
            Column(
                "initial_routing_team",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY1",
            ),
            Column("brand", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY2"),
            Column("account_id", "string", source_name="ACCOUNT_ID"),
            Column("language", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY3"),
            Column(
                "is_dark_post", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY4"
            ),
            Column("case_count", "float", source_name="CASE_COUNT"),
            Column(
                "retention_case_count", "float", source_name="RETENTION_CASE_COUNT2063"
            ),
            Column("case_count_saved", "float", source_name="CASE_COUNT_SAVED1634"),
            Column("load_time", "TIMESTAMP_LTZ(7)", delta_column=True),
        ],
        look_back_period=14 * 24 * 60 * 60 * 1000,
    ),
    SprinklrConfig(
        endpoint="closed_case",
        column_list=[
            Column(
                "case_marco_apply_time",
                "TIMESTAMP_LTZ(7)",
                source_name="CASE_MACRO_APPLY_TIME",
            ),
            Column("case_id", "int", uniqueness=True, source_name="CASE_ID"),
            Column("brand", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY"),
            Column(
                "initial_routing_team",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY1",
            ),
            Column("account_id", "string", source_name="ACCOUNT_ID"),
            Column("language", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY2"),
            Column(
                "social_network",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY3",
            ),
            Column(
                "initial_message_type",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY4",
            ),
            Column("status", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY5"),
            Column(
                "handled_by", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY6"
            ),
            Column(
                "is_dark_post", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY7"
            ),
            Column(
                "case_macro_usage_count", "float", source_name="CASE_MACRO_USAGE_COUNT"
            ),
            Column("load_time", "TIMESTAMP_LTZ(7)", delta_column=True),
        ],
    ),
    SprinklrConfig(
        endpoint="new_case",
        column_list=[
            Column(
                "case_creation_time",
                "TIMESTAMP_LTZ(7)",
                source_name="DATE_TYPE_CASE_CREATION_HISTOGRAM",
            ),
            Column("case_id", "int", uniqueness=True, source_name="CASE_ID"),
            Column("brand", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY"),
            Column(
                "initial_routing_team",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY1",
            ),
            Column("account_id", "string", source_name="ACCOUNT_ID"),
            Column("language", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY2"),
            Column(
                "social_network",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY3",
            ),
            Column(
                "initial_message_type",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY4",
            ),
            Column("status", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY5"),
            Column(
                "handled_by", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY6"
            ),
            Column(
                "is_dark_post", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY7"
            ),
            Column("has_brand_responded", "boolean", source_name="HAS_BRAND_RESPONDED"),
            Column("high_priority_case", "int", source_name="HIGH_PRIORITY_CASE1183"),
            Column("case_count", "float", source_name="CASE_COUNT"),
            Column("load_time", "TIMESTAMP_LTZ(7)", delta_column=True),
            Column(
                "last_engaged_user",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY8",
            ),
        ],
        look_back_period=14 * 24 * 60 * 60 * 1000,
    ),
    SprinklrConfig(
        endpoint="smm_first_response",
        column_list=[
            Column(
                "first_brand_response_msg_created_time",
                "TIMESTAMP_LTZ(7)",
                source_name="FIRST_BRAND_RESPONSE_SN_CREATED_TIME",
            ),
            Column("case_id", "int", uniqueness=True, source_name="CASE_ID"),
            Column("case_text", "string", source_name="CASE_TEXT"),
            Column("brand", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY"),
            Column("account_id", "string", source_name="ACCOUNT_ID"),
            Column("language", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY1"),
            Column(
                "social_network",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY2",
            ),
            Column(
                "initial_message_type",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY3",
            ),
            Column(
                "avg_case_first_user_response_sla",
                "float",
                source_name="CASE_FIRST_USER_RESPONSE_SLA",
            ),
            Column(
                "avg_case_first_user_response_sla_custom",
                "float",
                source_name="CASE_FIRST_RESPONSE_SLA_CUSTOMMETRIC_4446",
            ),
            Column("load_time", "TIMESTAMP_LTZ(7)", delta_column=True),
        ],
        extract_case_id=True,
    ),
    SprinklrConfig(
        endpoint="smm_handle_time",
        column_list=[
            Column(
                "case_creation_time",
                "TIMESTAMP_LTZ(7)",
                source_name="DATE_TYPE_CASE_CREATION_TIME",
            ),
            Column("case_id", "int", uniqueness=True, source_name="CASE_ID"),
            Column("case_text", "string", source_name="CASE_TEXT"),
            Column("brand", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY"),
            Column("account_id", "string", source_name="ACCOUNT_ID"),
            Column("language", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY1"),
            Column(
                "social_network",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY2",
            ),
            Column(
                "initial_message_type",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY3",
            ),
            Column("case_processing_sla", "float", source_name="CaseProcessingSLA"),
            Column("load_time", "TIMESTAMP_LTZ(7)", delta_column=True),
        ],
        extract_case_id=True,
        look_back_period=14 * 24 * 60 * 60 * 1000,
    ),
    SprinklrConfig(
        endpoint="sam_first_response",
        column_list=[
            Column(
                "first_brand_response_msg_created_time",
                "TIMESTAMP_LTZ(7)",
                source_name="FIRST_BRAND_RESPONSE_SN_CREATED_TIME",
            ),
            Column("case_id", "int", uniqueness=True, source_name="CASE_ID"),
            Column("case_text", "string", source_name="CASE_TEXT"),
            Column("brand", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY"),
            Column("account_id", "string", source_name="ACCOUNT_ID"),
            Column("language", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY1"),
            Column(
                "social_network",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY2",
            ),
            Column(
                "initial_message_type",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY3",
            ),
            Column(
                "avg_case_first_user_response_sla",
                "float",
                source_name="CASE_FIRST_USER_RESPONSE_SLA",
            ),
            Column(
                "avg_case_first_user_response_sla_custom",
                "float",
                source_name="CASE_FIRST_RESPONSE_SLA_CUSTOMMETRIC_4446",
            ),
            Column("load_time", "TIMESTAMP_LTZ(7)", delta_column=True),
        ],
        extract_case_id=True,
    ),
    SprinklrConfig(
        endpoint="sam_handle_time",
        column_list=[
            Column(
                "case_creation_time",
                "TIMESTAMP_LTZ(7)",
                source_name="DATE_TYPE_CASE_CREATION_TIME",
            ),
            Column("case_id", "int", uniqueness=True, source_name="CASE_ID"),
            Column("case_text", "string", source_name="CASE_TEXT"),
            Column("brand", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY"),
            Column("account_id", "string", source_name="ACCOUNT_ID"),
            Column("language", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY1"),
            Column(
                "social_network",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY2",
            ),
            Column(
                "initial_message_type",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY3",
            ),
            Column("case_processing_sla", "float", source_name="CaseProcessingSLA"),
            Column("load_time", "TIMESTAMP_LTZ(7)", delta_column=True),
        ],
        extract_case_id=True,
        look_back_period=14 * 24 * 60 * 60 * 1000,
    ),
    SprinklrConfig(
        endpoint="case_properties",
        column_list=[
            Column("case_id", "int", uniqueness=True, source_name="CASE_ID"),
            Column("priority", "string", source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY"),
            Column("sentiment", "string", source_name="SENTIMENT"),
            Column("case_count", "float", source_name="CASE_COUNT"),
            Column("load_time", "TIMESTAMP_LTZ(7)", delta_column=0),
        ],
        look_back_period=14 * 24 * 60 * 60 * 1000,
    ),
    SprinklrConfig(
        endpoint="agent_case_assignment",
        column_list=[
            Column("date", "TIMESTAMP_LTZ(7)"),
            Column(
                "affected_user_id",
                "string",
                source_name="AFFECTED_USER_ID",
                uniqueness=True,
            ),
            Column(
                "universal_case_custom_property",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY",
                uniqueness=True,
            ),
            Column(
                "universal_case_custom_property1",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY1",
                uniqueness=True,
            ),
            Column(
                "unique_cases_assigned", "float", source_name="UNIQUE_CASES_ASSIGNED"
            ),
            Column("load_time", "TIMESTAMP_LTZ(7)", delta_column=0),
        ],
    ),
    SprinklrConfig(
        endpoint="agent_response_volume",
        column_list=[
            Column("date", "TIMESTAMP_LTZ(7)", uniqueness=True),
            Column(
                "account_custom_property",
                "string",
                source_name="ACCOUNT_CUSTOM_PROPERTY",
                uniqueness=True,
            ),
            Column("user_id", "string", source_name="USER_ID", uniqueness=True),
            Column(
                "published_message_count",
                "float",
                source_name="PUBLISHED_MESSAGE_COUNT",
            ),
            Column("load_time", "TIMESTAMP_LTZ(7)", delta_column=0),
        ],
        extract_case_id=False,
    ),
    SprinklrConfig(
        endpoint="case_disposition_survey",
        column_list=[
            Column("case_id", "int", uniqueness=True, source_name="CASE_ID"),
            Column(
                "category",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY",
            ),
            Column(
                "sub_category",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY1",
            ),
            Column(
                "sub_sub_category",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY2",
            ),
            Column(
                "customer_id",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY3",
            ),
            Column("case_count", "float", source_name="CASE_COUNT"),
            Column("load_time", "TIMESTAMP_LTZ(7)", delta_column=0),
        ],
        extract_case_id=False,
        look_back_period=14 * 24 * 60 * 60 * 1000,
    ),
    SprinklrConfig(
        endpoint="agent_response_sla",
        column_list=[
            Column("date", "timestamp_ltz(7)", uniqueness=True),
            Column(
                "brand_response_by_user",
                "string",
                source_name="BRAND_RESPONSE_BY_USER",
                uniqueness=True,
            ),
            Column(
                "avg_case_first_response_sla",
                "float",
                source_name="CASE_FIRST_RESPONSE_SLA_CUSTOMMETRIC_4446",
            ),
            Column(
                "avg_case_response_sla",
                "float",
                source_name="CASE_RESPONSE_SLA_CUSTOMMETRIC_7664",
            ),
            Column("load_time", "TIMESTAMP_LTZ(7)", delta_column=0),
        ],
        extract_case_id=False,
        look_back_period=30 * 24 * 60 * 60 * 1000,
        time_increment=30 * 24 * 60 * 60 * 1000,
    ),
    SprinklrConfig(
        endpoint="agent_scorecard_process",
        column_list=[
            Column("date", "timestamp_ltz(7)", source_name="date_0", uniqueness=True),
            Column("user_id", "string", source_name="USER_ID_1", uniqueness=True),
            Column("account", "string", source_name="ACCOUNT_ID_2", uniqueness=True),
            Column(
                "social_network",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY_3",
                uniqueness=True,
            ),
            Column(
                "country",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY_4",
                uniqueness=True,
            ),
            Column(
                "processing_case_count",
                "float",
                source_name="M_CaseProcessingSLAReport_CASE_COUNT_0",
            ),
            Column(
                "unique_processing_case_count",
                "float",
                source_name="M_CaseProcessingSLAReport_PROCESSING_SLA_UNIQUE_CASES_1",
            ),
            Column(
                "case_processing_sla",
                "float",
                source_name="M_CaseProcessingSLAReport_CASEPROCESSINGSLA_2",
            ),
            Column(
                "avg_case_processing_sla",
                "float",
                source_name="M_CaseProcessingSLAReport_CASEPROCESSINGSLA_3",
            ),
            Column(
                "load_time", "TIMESTAMP_LTZ(7)", source_name="load_time", delta_column=0
            ),
        ],
        extract_case_id=False,
        look_back_period=30 * 24 * 60 * 60 * 1000,
        time_increment=30 * 24 * 60 * 60 * 1000,
    ),
    SprinklrConfig(
        endpoint="agent_scorecard_response",
        column_list=[
            Column("date", "timestamp_ltz(7)", uniqueness=True),
            Column("user_id", "string", source_name="USER_ID", uniqueness=True),
            Column("avg_response_time", "float", source_name="MESSAGE_RESPONSE_TIME"),
            Column("load_time", "TIMESTAMP_LTZ(7)", delta_column=0),
        ],
        extract_case_id=False,
        look_back_period=30 * 24 * 60 * 60 * 1000,
        time_increment=30 * 24 * 60 * 60 * 1000,
    ),
    SprinklrConfig(
        endpoint="case_queue_sla",
        column_list=[
            Column('"CASE"', "string", source_name="CASE_DATA", uniqueness=True),
            Column(
                "avg_case_queue_sla_awaiting_response",
                "float",
                source_name="AVG_CASE_QUEUE_SLA_AWAITING_RESPONSE_CUSTOMMETRIC_8344",
            ),
            Column(
                "avg_case_queue_sla_assigned",
                "float",
                source_name="AVG_CASE_QUEUE_SLA_ASSIGNED_CUSTOMMETRIC_7516",
            ),
            Column(
                "avg_case_queue_sla_awaiting_assignment",
                "float",
                source_name="AVG_CASE_QUEUE_SLA_AWAITING_ASSIGNMENT_CUSTOMMETRIC_4364",
            ),
            Column(
                "avg_case_queue_sla",
                "float",
                source_name="AVG_CASE_QUEUE_SLA_NEW_CUSTOMMETRIC_575",
            ),
            Column(
                "avg_case_queue_sla_total_worked_time",
                "float",
                source_name="AVG_CASE_QUEUE_SLA_TOTAL_WORKED_TIME_CUSTOMMETRIC_6269",
            ),
            Column(
                "case_queue_sla_awaiting_response",
                "float",
                source_name="CASE_QUEUE_SLA_AWAITING_RESPONSE_CUSTOMMETRIC_8344",
            ),
            Column(
                "case_queue_sla_assigned",
                "float",
                source_name="CASE_QUEUE_SLA_ASSIGNED_CUSTOMMETRIC_7516",
            ),
            Column(
                "case_queue_sla_awaiting_assignment",
                "float",
                source_name="CASE_QUEUE_SLA_AWAITING_ASSIGNMENT_CUSTOMMETRIC_4364",
            ),
            Column(
                "case_queue_sla",
                "float",
                source_name="CASE_QUEUE_SLA_NEW_CUSTOMMETRIC_575",
            ),
            Column(
                "case_queue_sla_total_worked_time",
                "float",
                source_name="CASE_QUEUE_SLA_TOTAL_WORKED_TIME_CUSTOMMETRIC_6269",
            ),
            Column("load_time", "TIMESTAMP_LTZ(7)", delta_column=0),
        ],
        extract_case_id=False,
    ),
    SprinklrConfig(
        endpoint="all_first_response",
        column_list=[
            Column(
                "case_creation_time",
                "string",
                source_name="DATE_TYPE_CASE_CREATION_TIME",
            ),
            Column("case_id", "int", uniqueness=True, source_name="CASE_ID"),
            Column("case_text", "string", source_name="CASE_TEXT"),
            Column(
                "brand",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY",
            ),
            Column("account_id", "string", source_name="ACCOUNT_ID"),
            Column(
                "social_network",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY1",
            ),
            Column(
                "initial_message_type",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY2",
            ),
            Column(
                "avg_case_first_user_response_sla",
                "float",
                source_name="CASE_FIRST_USER_RESPONSE_SLA",
            ),
            Column(
                "avg_case_first_response_sla",
                "float",
                source_name="CASE_FIRST_RESPONSE_SLA_CUSTOMMETRIC_4446",
            ),
            Column(
                "case_user_response_sla", "float", source_name="CASE_USER_RESPONSE_SLA"
            ),
            Column(
                "avg_case_user_response_sla",
                "float",
                source_name="AVG_CASE_USER_RESPONSE_SLA",
            ),
            Column("load_time", "TIMESTAMP_LTZ(7)", delta_column=True),
        ],
        extract_case_id=True,
        look_back_period=14 * 24 * 60 * 60 * 1000,
    ),
    SprinklrConfig(
        endpoint="all_handle_time",
        column_list=[
            Column(
                "case_creation_time",
                "TIMESTAMP_LTZ(7)",
                source_name="DATE_TYPE_CASE_CREATION_TIME",
            ),
            Column("case_id", "int", uniqueness=True, source_name="CASE_ID"),
            Column("case_text", "string", source_name="CASE_TEXT"),
            Column(
                "brand",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY",
            ),
            Column("account_id", "string", source_name="ACCOUNT_ID"),
            Column(
                "social_network",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY1",
            ),
            Column(
                "initial_message_type",
                "string",
                source_name="UNIVERSAL_CASE_CUSTOM_PROPERTY2",
            ),
            Column("case_processing_sla", "float", source_name="CaseProcessingSLA"),
            Column(
                "avg_case_processing_sla", "float", source_name="AvgCaseProcessingSLA"
            ),
            Column("load_time", "TIMESTAMP_LTZ(7)", delta_column=True),
        ],
        extract_case_id=True,
        look_back_period=14 * 24 * 60 * 60 * 1000,
    ),
]


dag = DAG(
    dag_id="global_apps_inbound_sprinklr",
    default_args={
        "start_date": pendulum.datetime(2020, 10, 22, tz="America/Los_Angeles"),
        "retries": 6,
        "owner": owners.data_integrations,
        "email": email_lists.data_integration_support,
        "on_failure_callback": slack_failure_gsc,
    },
    catchup=False,
    max_active_tasks=1000,
    max_active_runs=1,
    schedule="1 1 * * *",
)

with dag:
    for cfg in sprinklr_configs:
        lake_table = f"lake.sprinklr.{cfg.endpoint}"
        s3_prefix = f"lake/{lake_table}/v4"

        to_s3 = SprinklrToS3Operator(
            task_id=f"sprinklr_{cfg.endpoint}_to_s3",
            pool=Pool.sprinklr,
            retry_delay=timedelta(minutes=30),
            key=f"{s3_prefix}/{yr_mth}/{cfg.endpoint}_{{{{ ts_nodash }}}}.ndjson.gz",
            bucket=s3_buckets.tsos_da_int_inbound,
            endpoint=cfg.endpoint,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            extract_case_id=cfg.extract_case_id,
            look_back_period=cfg.look_back_period,
            time_increment=cfg.time_increment,
        )

        to_snowflake = SnowflakeIncrementalLoadOperator(
            task_id=f"sprinklr_{cfg.endpoint}_s3_to_snowflake",
            database="lake",
            schema="sprinklr",
            table=cfg.endpoint,
            staging_database="lake_stg",
            view_database="lake_view",
            snowflake_conn_id=conn_ids.Snowflake.default,
            stg_column_naming="source",
            column_list=cfg.column_list,
            files_path=f"{stages.tsos_da_int_inbound}/{s3_prefix}/",
            copy_config=CopyConfigJson(),
            priority_weight=999,
        )

        to_s3 >> to_snowflake
