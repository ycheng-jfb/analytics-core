from datetime import datetime, timedelta

import pendulum
from airflow.decorators import task
from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.models.process_state import ProcessState
from include.airflow.operators.callminer import CallMinerContacts
from include.airflow.operators.callminer_bulk import CallMinerBulkConversation
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
from include.utils.snowflake import CopyConfigCsv
from task_configs.dag_config.callminer_config import contacts_config as cfg
from task_configs.dag_config.callminer_config import transcripts_config as t_cfg

from airflow import DAG

default_args = {
    "start_date": pendulum.datetime(2022, 10, 31, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_inbound_callminer_contacts",
    default_args=default_args,
    schedule="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    # dagrun_timeout=timedelta(hours=6),
)

yr_mth = "{{macros.datetime.now().strftime('%Y%m')}}"
with dag:
    contacts_to_s3 = CallMinerContacts(
        task_id="contacts_to_s3",
        key=f"{cfg.s3_prefix}/{yr_mth}/{cfg.schema}_{cfg.table}_{{{{ ts_nodash }}}}.csv.gz",
        bucket=s3_buckets.tsos_da_int_inbound,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        column_list=[x.source_name for x in cfg.column_list],
        hook_conn_id=conn_ids.Callminer.default,
        namespace=cfg.schema,
        process_name=cfg.table,
        write_header=True,
    )

    contacts_to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="contacts_to_snowflake",
        database=cfg.database,
        schema=cfg.schema,
        table=cfg.table,
        staging_database="lake_stg",
        view_database="lake_view",
        snowflake_conn_id=conn_ids.Snowflake.default,
        role=snowflake_roles.etl_service_account,
        column_list=cfg.column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{cfg.s3_prefix}/",
        copy_config=CopyConfigCsv(field_delimiter="\t", header_rows=1),
    )

    contact_categories = SnowflakeProcedureOperator(
        procedure="callminer.contact_categories.sql",
        database="lake",
        warehouse="DA_WH_ETL_LIGHT",
    )

    contact_scores = SnowflakeProcedureOperator(
        procedure="callminer.contact_scores.sql",
        database="lake",
        warehouse="DA_WH_ETL_LIGHT",
    )

    contact_score_components = SnowflakeProcedureOperator(
        procedure="callminer.contact_score_components.sql",
        database="lake",
        warehouse="DA_WH_ETL_LIGHT",
    )

    @task
    def check_pending_transcripts_jobs(namespace, process_name, initial_load_value):
        job_status = CallMinerBulkConversation.check_job_status(
            "callminer_bulk_default"
        )

        task_state_val = ProcessState.get_value(
            namespace=namespace, process_name=process_name
        )
        low_watermark = task_state_val or initial_load_value
        new_data = list(
            filter(
                lambda x: x["JobCompletionTime"] > low_watermark
                and x["Status"] == "Completed",
                job_status.json(),
            )
        )

        return [
            {
                "s3_key": (
                    f'{t_cfg.s3_prefix}/'
                    f'{job_completion_time.strftime("%Y%m")}/'
                    f'{t_cfg.schema}_{t_cfg.table}_'
                    f'{job_completion_time.strftime("%Y%m%dT%H%M%S")}.csv.gz'
                )
            }
            for job in new_data
            if (
                job_completion_time := datetime.strptime(
                    job["JobCompletionTime"], "%Y-%m-%dT%H:%M:%S.%fZ"
                )
            )
        ]

    transcripts_to_s3 = CallMinerBulkConversation.partial(
        task_id="transcripts_to_s3",
        s3_bucket=s3_buckets.tsos_da_int_inbound,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        callminer_bulk_conn_id="callminer_bulk_default",
        namespace=t_cfg.schema,
        process_name=t_cfg.table,
        initial_load_value="1900-01-01T00:00:00.000Z",
        max_active_tis_per_dagrun=1,
    ).expand_kwargs(
        check_pending_transcripts_jobs(
            t_cfg.schema, t_cfg.table, "1900-01-01T00:00:00.000Z"
        )
    )

    transcripts_to_snowflake = SnowflakeIncrementalLoadOperator(
        task_id="transcripts_to_snowflake",
        database=t_cfg.database,
        schema=t_cfg.schema,
        table=t_cfg.table,
        staging_database="lake_stg",
        view_database="lake_view",
        snowflake_conn_id=conn_ids.Snowflake.default,
        role=snowflake_roles.etl_service_account,
        column_list=t_cfg.column_list,
        files_path=f"{stages.tsos_da_int_inbound}/{t_cfg.s3_prefix}/",
        copy_config=CopyConfigCsv(header_rows=1, null_if="'null'"),
    )

    (
        contacts_to_s3
        >> contacts_to_snowflake
        >> [contact_categories, contact_scores, contact_score_components]
    )
    transcripts_to_s3 >> transcripts_to_snowflake
