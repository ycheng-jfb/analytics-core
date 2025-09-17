from dataclasses import dataclass
from typing import Any, Type, Union

import pendulum
from airflow.models import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.dag_helpers import chain_tasks
from include.airflow.operators.email_to_smb import EmailToSMBOperator
from include.airflow.operators.excel_smb import ExcelSMBToS3BatchOperator, SheetConfig
from include.airflow.operators.snowflake_load import (
    SnowflakeCopyTableOperator,
    SnowflakeInsertOperator,
)
from include.config import conn_ids, email_lists, owners, s3_buckets, stages
from include.utils.snowflake import Column, CopyConfigCsv

sheets = {
    "manual_audit": SheetConfig(
        sheet_name=0,
        schema="excel",
        table="gps_scorecard",
        s3_replace=False,
        header_rows=1,
        column_list=[
            Column("bu", "VARCHAR", source_name="BU"),
            Column("process", "VARCHAR", source_name="Process"),
            Column("location", "VARCHAR", source_name="Location"),
            Column("date_of_contact", "DATE", source_name="Date of Contact"),
            Column("genesys_id", "VARCHAR", source_name="Genesys ID"),
            Column("fashion_consultant", "VARCHAR", source_name="Fashion Consultant"),
            Column("conversation_id", "VARCHAR", source_name="Conversation ID"),
            Column("num_of_contacts_scored", "INT", source_name="# of Contacts Scored"),
            Column(
                "overall_max_score", "NUMBER(38,4)", source_name="Overall MAX Score"
            ),
            Column(
                "total_points_earned", "NUMBER(38,2)", source_name="Total Points Earned"
            ),
            Column("num_of_max_alerts", "VARCHAR", source_name="# of MAX Alerts"),
        ],
    ),
    "automated_audit": SheetConfig(
        sheet_name=1,
        schema="excel",
        table="gps_scorecard",
        s3_replace=False,
        header_rows=2,
        column_list=[
            Column("bu", "VARCHAR", source_name="BU"),
            Column("process", "VARCHAR", source_name="Process"),
            Column("call_date", "DATE", source_name="Call Date"),
            Column("location", "VARCHAR", source_name="Location"),
            Column("type_of_fc", "VARCHAR", source_name="Type of FC"),
            Column("genesys_id", "VARCHAR", source_name="Genesys ID"),
            Column("fashion_consultant", "VARCHAR", source_name="Fashion Consultant"),
            Column("me_possible_points", "INT", source_name="Possible Points"),
            Column("me_obtained_points", "INT", source_name="Obtained Points"),
            Column("me_score", "NUMBER(38,4)", source_name="ME Score"),
            Column("bond_possible_points", "INT", source_name="Possible Points"),
            Column("bond_obtained_points", "INT", source_name="Obtained Points"),
            Column("bond_score", "NUMBER(38,4)", source_name="Bond Score"),
            Column(
                "overall_max_score", "NUMBER(38,4)", source_name="Overall MAX Score"
            ),
        ],
    ),
}

default_args = {
    "start_date": pendulum.datetime(2021, 2, 1, tz="America/Los_Angeles"),
    "retries": 2,
    "owner": owners.data_integrations,
    "email": email_lists.data_integration_support,
    "on_failure_callback": slack_failure_gsc,
}

dag = DAG(
    dag_id="global_apps_inbound_etech",
    default_args=default_args,
    schedule="0 9,12,15,18 * * 1",
    catchup=False,
    max_active_tasks=100,
    max_active_runs=1,
)

smb_path = "Inbound/airflow.etech_audits"
share_name = "BI"


@dataclass
class ExcelConfig:
    task_id: str
    smb_dir: str
    sheet_config: Any
    file_pattern_list: list
    load_operator: Type[Union[SnowflakeInsertOperator, SnowflakeCopyTableOperator]] = (
        SnowflakeInsertOperator
    )
    is_archive_file: bool = True

    @property
    def to_s3(self):
        return ExcelSMBToS3BatchOperator(
            task_id=f"{self.task_id}_excel_to_s3",
            smb_dir=self.smb_dir,
            share_name=share_name,
            file_pattern_list=self.file_pattern_list,
            bucket=s3_buckets.tsos_da_int_inbound,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            smb_conn_id=conn_ids.SMB.nas01,
            is_archive_file=self.is_archive_file,
            sheet_configs=[self.sheet_config],
            remove_header_new_lines=True,
            default_schema_version="v2",
        )

    @property
    def file_path(self):
        return (
            f"{stages.tsos_da_int_inbound}/lake/{self.sheet_config.schema}."
            f"{self.sheet_config.table}/v2/gps"
        )

    @property
    def to_snowflake(self):
        return self.load_operator(
            task_id=f"{self.task_id}_s3_to_snowflake",
            database="lake",
            schema=self.sheet_config.schema,
            table=self.sheet_config.table,
            staging_database="lake_stg",
            view_database="lake_view",
            snowflake_conn_id=conn_ids.Snowflake.default,
            column_list=self.sheet_config.column_list,
            files_path=self.file_path,
            copy_config=CopyConfigCsv(header_rows=1, field_delimiter="|"),
        )


manual_audits_config = ExcelConfig(
    task_id="manual_audit",
    smb_dir=f"{smb_path}",
    sheet_config=sheets["manual_audit"],
    file_pattern_list=["*.xls*"],
)

with dag:
    email_to_smb = EmailToSMBOperator(
        task_id="email_to_smb",
        remote_path=smb_path,
        smb_conn_id=conn_ids.SMB.nas01,
        from_address="@etech",
        resource_address="svc_tfg_etech@techstyle.com",
        subjects=["GPS Scorecard"],
        share_name=share_name,
    )
    manual_to_s3 = manual_audits_config.to_s3
    manual_to_snowflake = manual_audits_config.to_snowflake

    chain_tasks(email_to_smb, manual_to_s3, manual_to_snowflake)
