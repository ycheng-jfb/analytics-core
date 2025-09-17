from dataclasses import dataclass
from typing import Any

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.excel_smb import ExcelSMBToS3BatchOperator, SheetConfig
from include.airflow.operators.snowflake_load import SnowflakeTruncateAndLoadOperator
from include.config import (
    conn_ids,
    email_lists,
    owners,
    s3_buckets,
    snowflake_roles,
    stages,
)
from include.utils.snowflake import Column, CopyConfigCsv

sheets = {
    "all": SheetConfig(
        sheet_name=0,
        schema="excel",
        table="operational_carrier_forecast",
        s3_replace=True,
        header_rows=1,
        column_list=[
            Column("date", "DATE", uniqueness=True),
            Column("bu", "VARCHAR", uniqueness=True),
            Column("fc", "VARCHAR", uniqueness=True),
            Column("carrier", "VARCHAR", uniqueness=True),
            Column("service", "VARCHAR", uniqueness=True),
            Column("forecast_volume", "NUMBER(38,12)"),
        ],
    ),
}
email_list = email_lists.data_integration_support
default_args = {
    "start_date": pendulum.datetime(2021, 8, 24, tz="America/Los_Angeles"),
    "retries": 2,
    "owner": owners.data_integrations,
    "email": email_list,
    "on_failure_callback": slack_failure_gsc,
}


dag = DAG(
    dag_id="global_apps_inbound_operational_carrier_forecast",
    default_args=default_args,
    schedule="1 7,14 * * *",
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)


@dataclass
class ExcelConfig:
    task_id: str
    smb_dir: str
    sheet_config: Any

    @property
    def to_s3(self):
        return ExcelSMBToS3BatchOperator(
            task_id=f"{self.task_id}_excel_to_s3",
            smb_dir=self.smb_dir,
            share_name="BI",
            file_pattern_list=["*.xls*", "*.XLS*"],
            bucket=s3_buckets.tsos_da_int_inbound,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            smb_conn_id=conn_ids.SMB.nas01,
            is_archive_file=False,
            sheet_configs=[self.sheet_config],
            remove_header_new_lines=True,
            default_schema_version="v2",
        )

    @property
    def to_snowflake(self):
        return SnowflakeTruncateAndLoadOperator(
            task_id=f"{self.task_id}_s3_to_snowflake",
            snowflake_conn_id=conn_ids.Snowflake.default,
            database="lake",
            staging_database="lake_stg",
            schema=self.sheet_config.schema,
            table=self.sheet_config.table,
            column_list=self.sheet_config.column_list,
            files_path=f"{stages.tsos_da_int_inbound}/lake/{self.sheet_config.schema}.{self.sheet_config.table}/v2/",
            copy_config=CopyConfigCsv(header_rows=1, field_delimiter="|"),
            role=snowflake_roles.etl_service_account,
        )


smb_path = "Inbound/airflow.operational_carrier_forecasts"


landed_cost_config = [
    ExcelConfig(
        task_id="operational_carrier_forecast",
        smb_dir=f"{smb_path}",
        sheet_config=sheets["all"],
    ),
]

with dag:
    for cfg in landed_cost_config:
        to_s3 = cfg.to_s3
        to_snowflake = cfg.to_snowflake
        to_s3 >> to_snowflake
