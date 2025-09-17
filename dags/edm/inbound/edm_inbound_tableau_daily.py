import importlib
from datetime import timedelta

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_edm
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
from task_configs.dag_config.tableau_config import config_list

default_args = {
    "start_date": pendulum.datetime(2022, 4, 27, tz="America/Los_Angeles"),
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_inbound_tableau_daily",
    default_args=default_args,
    schedule="0 3 * * *",
    catchup=False,
    max_active_tasks=2,
)


with dag:
    for cfg in config_list:
        module = importlib.import_module("include.airflow.operators.tableau")
        to_s3_operator = getattr(module, cfg.operator)

        to_s3 = to_s3_operator(
            task_id=f"to_s3_{cfg.table}",
            key=f"{cfg.s3_prefix}/{cfg.yr_mth}/{cfg.schema}_{cfg.table}_{{{{ ts_nodash }}}}.csv.gz",
            bucket=s3_buckets.tsos_da_int_inbound,
            column_list=[x.source_name for x in cfg.column_list],
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            write_header=True,
            tableau_api_version=cfg.api_version,
        )

        to_snowflake = SnowflakeIncrementalLoadOperator(
            task_id=f"to_snowflake_{cfg.table}",
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

        to_s3 >> to_snowflake
