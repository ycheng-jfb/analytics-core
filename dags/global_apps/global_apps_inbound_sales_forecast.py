from dataclasses import dataclass

import pendulum
from airflow import DAG

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.excel_smb import ExcelSMBToS3BatchOperator, SheetConfig
from include.airflow.operators.snowflake_load import SnowflakeTruncateAndLoadOperator
from include.airflow.operators.snowflake_to_mssql import SnowflakeSqlToMSSqlOperatorTruncateAndLoad
from include.config import conn_ids, email_lists, owners, s3_buckets, snowflake_roles, stages
from include.utils.snowflake import Column, CopyConfigCsv

sheets = {
    'data_current': SheetConfig(
        sheet_name='Data - Current',
        schema='excel',
        table='sales_forecast_by_fc_bu',
        s3_replace=False,
        header_rows=1,
        column_list=[
            Column('year', 'VARCHAR'),
            Column('month', 'VARCHAR'),
            Column('day', 'VARCHAR'),
            Column('date', 'DATE'),
            Column('city', 'VARCHAR'),
            Column('bu', 'VARCHAR'),
            Column('orders', 'INTEGER'),
            Column('units', 'INTEGER'),
            Column('promos_and_notes', 'VARCHAR'),
        ],
    ),
}

smb_path = 'Inbound/airflow.sales_forecast'
share_name = 'BI'


@dataclass
class ExcelConfig:
    task_id: str
    smb_dir: str
    sheet_config: SheetConfig
    file_pattern_list: list
    is_archive_file: bool = False

    @property
    def to_s3(self):
        return ExcelSMBToS3BatchOperator(
            task_id=f'{self.task_id}_excel_to_s3',
            smb_dir=self.smb_dir,
            share_name=share_name,
            file_pattern_list=self.file_pattern_list,
            bucket=s3_buckets.tsos_da_int_inbound,
            s3_conn_id=conn_ids.S3.tsos_da_int_prod,
            smb_conn_id=conn_ids.SMB.nas01,
            is_archive_file=self.is_archive_file,
            sheet_configs=[self.sheet_config],
            remove_header_new_lines=True,
            default_schema_version='v3',
        )

    @property
    def file_path(self):
        return (
            f'{stages.tsos_da_int_inbound}/lake/{self.sheet_config.schema}.'
            f'{self.sheet_config.table}/v3/'
        )

    @property
    def to_snowflake(self):
        return SnowflakeTruncateAndLoadOperator(
            task_id='sales_forecast_to_snowflake',
            snowflake_conn_id=conn_ids.Snowflake.default,
            database='lake',
            staging_database='lake_stg',
            schema=self.sheet_config.schema,
            table=self.sheet_config.table,
            column_list=self.sheet_config.column_list,
            files_path=self.file_path,
            copy_config=CopyConfigCsv(header_rows=1, field_delimiter='|'),
            role=snowflake_roles.etl_service_account,
        )

    @property
    def to_evolve(self):
        sql_cmd = """select year ,month ,day ,date ,city ,bu ,orders ,units ,promos_and_notes ,meta_row_hash
            ,to_varchar(meta_create_datetime,'yyyy-mm-dd hh:mi:ss') as meta_create_datetime
            ,to_varchar(meta_update_datetime,'yyyy-mm-dd hh:mi:ss') as meta_update_datetime
            from lake.excel.sales_forecast_by_fc_bu;"""
        # print(sql_cmd)
        return SnowflakeSqlToMSSqlOperatorTruncateAndLoad(
            task_id='sales_forecast_to_evolve',
            sql_or_path=sql_cmd,
            tgt_table=self.sheet_config.table,
            tgt_database='ultrawarehouse',
            tgt_schema='rpt',
            mssql_conn_id=conn_ids.MsSql.dbp40_app_airflow_rw,
            if_exists='append',
        )


default_args = {
    'owner': owners.data_integrations,
    'email': email_lists.data_integration_support,
    'on_failure_callback': slack_failure_gsc,
}

dag = DAG(
    dag_id='global_apps_inbound_sales_forecast',
    default_args=default_args,
    start_date=pendulum.datetime(2021, 6, 10, tz='America/Los_Angeles'),
    catchup=False,
    schedule='30 5,13 * * *',
)

data_current_config = ExcelConfig(
    task_id='sales_forecast_data_current',
    smb_dir=f'{smb_path}',
    sheet_config=sheets['data_current'],
    file_pattern_list=['*.xls*'],
)

with dag:
    to_s3 = data_current_config.to_s3
    to_snowflake = data_current_config.to_snowflake
    to_evolve = data_current_config.to_evolve

    to_s3 >> to_snowflake >> to_evolve
