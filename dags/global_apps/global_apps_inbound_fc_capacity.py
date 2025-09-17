from dataclasses import dataclass
from typing import List, Optional

import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.excel_smb import ExcelSMBToS3BatchOperator, SheetConfig
from include.airflow.operators.snowflake_load import CopyConfigCsv, SnowflakeTruncateAndLoadOperator
from include.airflow.operators.snowflake_to_mssql import SnowflakeSqlToMSSqlOperatorTruncateAndLoad
from include.config import conn_ids, email_lists, owners, s3_buckets, stages
from include.utils.snowflake import Column
from include.utils.string import unindent_auto

default_args = {
    "start_date": pendulum.datetime(2022, 6, 15, tz="America/Los_Angeles"),
    'owner': owners.data_integrations,
    'email': email_lists.global_applications,
    'on_failure_callback': slack_failure_gsc,
    'retries': 0,
}

dag = DAG(
    dag_id='global_apps_inbound_fc_capacity',
    default_args=default_args,
    schedule='50 1,7,13,19 * * *',
    catchup=False,
)


@dataclass
class ExcelConfig:
    task_id: str
    smb_conn_id: str
    smb_dir: str
    share_name: str
    file_pattern_list: List[str]
    sheet_config: SheetConfig
    s3_conn_id: str
    bucket: str
    load_to_mssql: bool = False
    mssql_conn_id: Optional[str] = None
    mssql_database: Optional[str] = None
    mssql_schema: Optional[str] = None
    mssql_table: Optional[str] = None

    @property
    def file_path(self) -> str:
        return (
            f'{stages.tsos_da_int_inbound}/lake/{self.sheet_config.schema}'
            f'.{self.sheet_config.table}/{self.sheet_config.default_schema_version}'
        )

    @property
    def to_s3(self) -> ExcelSMBToS3BatchOperator:
        return ExcelSMBToS3BatchOperator(
            task_id=f"{self.task_id}.to_s3",
            smb_dir=self.smb_dir,
            share_name=self.share_name,
            file_pattern_list=self.file_pattern_list,
            bucket=self.bucket,
            s3_conn_id=self.s3_conn_id,
            smb_conn_id=self.smb_conn_id,
            sheet_configs=[self.sheet_config],
            remove_header_new_lines=True,
        )

    @property
    def to_snowflake(self) -> SnowflakeTruncateAndLoadOperator:
        return SnowflakeTruncateAndLoadOperator(
            task_id=f"{self.task_id}.to_snowflake",
            snowflake_conn_id=conn_ids.Snowflake.default,
            database="lake",
            staging_database="lake_stg",
            schema=self.sheet_config.schema,
            table=self.sheet_config.table,
            column_list=self.sheet_config.column_list,
            files_path=self.file_path,
            copy_config=CopyConfigCsv(header_rows=1, field_delimiter='|'),
        )

    @property
    def to_mssql(self) -> SnowflakeSqlToMSSqlOperatorTruncateAndLoad:
        assert (
            self.load_to_mssql
            and self.mssql_conn_id
            and self.mssql_database
            and self.mssql_schema
            and self.mssql_table
        )
        sql_cmd = unindent_auto(
            f"""
            SELECT
                {",".join(i.name for i in self.sheet_config.column_list)}
            FROM
                lake.{self.sheet_config.schema}.{self.sheet_config.table}
        """
        )
        return SnowflakeSqlToMSSqlOperatorTruncateAndLoad(
            task_id=f"{self.task_id}.to_mssql",
            sql_or_path=sql_cmd,
            tgt_table=self.mssql_table,
            tgt_database=self.mssql_database,
            tgt_schema=self.mssql_schema,
            mssql_conn_id=self.mssql_conn_id,
            if_exists='append',
        )


excel_configs = [
    ExcelConfig(
        task_id="fc_capacity",
        smb_conn_id=conn_ids.SMB.nas01,
        share_name="BI",
        smb_dir="Inbound/airflow.fc_capacity",
        file_pattern_list=["*.xls*"],
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        bucket=s3_buckets.tsos_da_int_inbound,
        sheet_config=SheetConfig(
            sheet_name='Sheet1',
            schema='excel',
            table='fc_capacity',
            s3_replace=False,
            header_rows=1,
            column_list=[
                Column('bu', 'VARCHAR', source_name='BU', uniqueness=True),
                Column('building', 'VARCHAR', source_name='Building', uniqueness=True),
                Column('year', 'NUMBER', source_name='Year', uniqueness=True),
                Column('month', 'NUMBER', source_name='Month', uniqueness=True),
                Column('budget', 'NUMBER', source_name='Budget'),
                Column('budget_update', 'NUMBER', source_name='Update'),
                Column('act_on_hand', 'NUMBER', source_name='Act On Hand'),
                Column('lpn_rack_by_building', 'NUMBER', source_name='LPN Rack by Building'),
                Column('case_rack_by_building', 'NUMBER', source_name='Case Rack by Building'),
                Column(
                    'lpn_rack_by_building_capacity',
                    'VARCHAR',
                    source_name='Capacity - LPN Rack by Building',
                ),
                Column(
                    'case_rack_by_building_capacity',
                    'VARCHAR',
                    source_name='Capacity - Case Rack by Building',
                ),
            ],
        ),
    ),
    ExcelConfig(
        task_id="fc_location_capacity",
        smb_conn_id=conn_ids.SMB.nas01,
        share_name="BI",
        smb_dir="Inbound/airflow.fc_location_capacity",
        file_pattern_list=["*.xls*"],
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        bucket=s3_buckets.tsos_da_int_inbound,
        sheet_config=SheetConfig(
            sheet_name="Sheet1",
            schema="excel",
            table="fc_location_capacity",
            header_rows=1,
            column_list=[
                Column("fc", "VARCHAR", source_name="FC"),
                Column("zone", "VARCHAR", source_name="Zone"),
                Column("aisle", "VARCHAR", source_name="Aisle"),
                Column("capacity", "NUMBER", source_name="Capacity"),
            ],
        ),
        load_to_mssql=True,
        mssql_conn_id=conn_ids.MsSql.dbp40_app_airflow_rw,
        mssql_database="ultrawarehouse",
        mssql_schema="rpt",
        mssql_table="fc_location_capacity",
    ),
]

with dag:
    for excel_config in excel_configs:
        to_s3 = excel_config.to_s3
        to_snowflake = excel_config.to_snowflake
        to_s3 >> to_snowflake
        if excel_config.load_to_mssql:
            to_snowflake >> excel_config.to_mssql
