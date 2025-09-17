import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_edm
from include.airflow.operators.snowflake_load import SnowflakeIncrementalLoadOperator
from include.airflow.operators.test_rail import TestRailCases
from include.config import (
    conn_ids,
    email_lists,
    owners,
    s3_buckets,
    snowflake_roles,
    stages,
)
from include.utils.snowflake import CopyConfigCsv
from task_configs.dag_config.test_rail_config import cases_config
from task_configs.dag_config.test_rail_config import test_rail_configs as configs

default_args = {
    "start_date": pendulum.datetime(2022, 10, 4, tz="America/Los_Angeles"),
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_edm,
}

dag = DAG(
    dag_id="edm_inbound_test_rail",
    default_args=default_args,
    schedule="0 2 * * 1-5",
    catchup=False,
    max_active_tasks=1,
)

yr_mth = "{{macros.datetime.now().strftime('%Y%m')}}"
with dag:
    for cfg in configs:
        to_s3 = TestRailCases(
            task_id="to_s3",
            key=f"{cfg.s3_prefix}/{yr_mth}/{cfg.schema}_{cfg.table}_{{{{ ts_nodash }}}}.csv.gz",
            bucket=s3_buckets.tsos_da_int_inbound,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            column_list=[x.source_name for x in cfg.column_list],
            hook_conn_id=conn_ids.TestRail.default,
            write_header=True,
            cases_config=cases_config,
        )

        to_snowflake = SnowflakeIncrementalLoadOperator(
            task_id="to_snowflake",
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
