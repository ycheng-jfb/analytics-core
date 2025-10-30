from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pendulum
from airflow.models import DAG
from include import SQL_DIR
from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.dag_helpers import chain_tasks
from include.airflow.hooks.mssql import MsSqlOdbcHook
from include.airflow.operators.sftp_to_s3 import SFTPToS3BatchOperator
from include.airflow.operators.snowflake import (
    SnowflakeProcedureOperator,
    SnowflakeWatermarkSqlOperator,
    TableDependencyTzLtz,
)
from include.airflow.operators.snowflake_load import SnowflakeCopyOperator
from include.config import conn_ids, email_lists, owners, s3_buckets, snowflake_roles, stages
from include.utils.context_managers import ConnClosing
from include.utils.snowflake import generate_query_tag_cmd
from include.utils.string import unindent

default_args = {
    'start_date': pendulum.datetime(2020, 1, 1, tz='America/Los_Angeles'),
    'retries': 1,
    'owner': owners.data_integrations,
    'email': email_lists.data_integration_support,
    'on_failure_callback': slack_failure_gsc,
}


dag = DAG(
    dag_id='global_apps_inbound_sps_asn_processor',
    default_args=default_args,
    schedule='*/60 * * * *',
    catchup=False,
    max_active_tasks=6,
    max_active_runs=1,
)


class SnowflakeToMsSql(SnowflakeWatermarkSqlOperator):
    def __init__(
        self,
        mssql_conn_id: str,
        mssql_db_name: str,
        mssql_schema_name: str,
        mssql_table_name: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.mssql_db_name = mssql_db_name
        self.mssql_schema_name = mssql_schema_name
        self.mssql_table_name = mssql_table_name

    @staticmethod
    def read_sql_from_file(sql_path):
        with open(sql_path.as_posix(), "rt") as f:
            return f.read()

    def watermark_execute(self, context=None):
        ms_hook = MsSqlOdbcHook(mssql_conn_id=self.mssql_conn_id, database=self.mssql_db_name)
        export_cmd = self.read_sql_from_file(self.sql_or_path)
        with ConnClosing(self.snowflake_hook.get_conn()) as conn, conn.cursor() as cur:
            query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
            cur.execute(query_tag)
            df = pd.read_sql(export_cmd, con=conn, params=self.parameters)
        with ms_hook.get_sqlalchemy_connection() as conn_mssql:
            print(df)
            df.to_sql(
                name=self.mssql_table_name,
                con=conn_mssql,
                if_exists='append',
                schema=self.mssql_schema_name,
                index=False,
            )


@dataclass
class ExportConfig:
    """
    Config for push-to-mssql component
    """

    export_sql: str
    target_table_name: str
    dependency_table: str
    database = 'export'

    @property
    def to_mssql_operator(self):
        return SnowflakeToMsSql(
            task_id=f'{self.database}_{self.export_sql}',
            mssql_conn_id=conn_ids.MsSql.dbp40_app_airflow_rw,
            mssql_db_name='ultrawarehouse',
            mssql_schema_name='dbo',
            mssql_table_name=self.target_table_name,
            sql_or_path=Path(SQL_DIR, self.database, "procedures", self.export_sql),
            initial_load_value='1900-01-01',
            watermark_tables=[
                TableDependencyTzLtz(
                    table_name=self.dependency_table,
                    column_name='meta_create_datetime',
                )
            ],
        )


@dataclass
class ProcessConfig:
    """
    Config for snowflake load and reporting table ETL
    """

    s3_bucket: str
    s3_conn_id: str
    s3_location: str
    table_name: str
    remote_file_pattern: str
    reporting_procedure: str

    def __post_init__(self):
        self.s3_prefix = self.s3_location.split('/', 1)[1]
        self.reporting_dependency_table = self.table_name

    @property
    def sftp_get_operator(self):
        return SFTPToS3BatchOperator(
            task_id=f'to_s3_{self.table_name.replace("lake.","")}',
            s3_prefix=self.s3_prefix,
            s3_bucket=self.s3_bucket,
            sftp_conn_id=conn_ids.SFTP.sftp_sps_spscommerce,
            email_on_retry=False,
            s3_conn_id=self.s3_conn_id,
            files_per_batch=100,
            remote_dir='/out',
            file_pattern=self.remote_file_pattern,
            remove_remote_files=True,
        )

    @staticmethod
    def generate_copy_command(table_name: str, s3_location: str):
        copy_command = f"""
        COPY INTO {table_name} (XML_DATA, FILE_NAME,META_CREATE_DATETIME)
            FROM (
                SELECT
                    $1,
                    metadata$filename AS FILE_NAME, current_timestamp AS meta_create_datetime
                FROM {s3_location})
                    FILE_FORMAT = (TYPE ='XML')
                ON_ERROR ='CONTINUE'

        ;
        """
        return unindent(copy_command)

    @property
    def snowflake_copy_operator(self):
        return SnowflakeCopyOperator(
            task_id=f'to_{self.table_name}',
            snowflake_conn_id=conn_ids.Snowflake.default,
            role=snowflake_roles.etl_service_account,
            sql_or_path=self.generate_copy_command(
                table_name=self.table_name, s3_location=self.s3_location
            ),
            database="lake",
            schema="sps",
        )

    @property
    def reporting_operator(self):
        return SnowflakeProcedureOperator(
            database='reporting_prod',
            procedure=self.reporting_procedure,
            watermark_tables=[
                TableDependencyTzLtz(
                    table_name=self.reporting_dependency_table,
                    column_name='meta_create_datetime',
                )
            ],
            warehouse='DA_WH_ETL_LIGHT',
        )

    @property
    def operators(self):
        return (
            self.sftp_get_operator,
            self.snowflake_copy_operator,
            self.reporting_operator,
        )


with dag:
    cfg_in_asn = ProcessConfig(
        s3_bucket=s3_buckets.tsos_da_int_inbound,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        s3_location=f'{stages.tsos_da_int_inbound}/lake/sps.asn/v2/',
        table_name='lake.sps.asn',
        remote_file_pattern='SH_*.xml',
        reporting_procedure='sps.asn.sql',
    )
    cfg_out_asn = ExportConfig('sps.asn.sql', 'asn_processor', 'reporting_prod.sps.asn')
    cfg_out_asn_milestone = ExportConfig(
        'sps.asn_milestone.sql', 'milestone_processor', 'reporting_prod.sps.asn'
    )
    chain_tasks(
        *cfg_in_asn.operators,
        [cfg_out_asn.to_mssql_operator, cfg_out_asn_milestone.to_mssql_operator],
    )

    cfg_inbound_milestone = ProcessConfig(
        s3_bucket=s3_buckets.tsos_da_int_inbound,
        s3_conn_id=conn_ids.S3.tsos_da_int_prod,
        s3_location=f'{stages.tsos_da_int_inbound}/lake/sps.milestone/v2/',
        table_name='lake.sps.milestone',
        remote_file_pattern='QO_*.xml',
        reporting_procedure='sps.milestone_processor.sql',
    )
    cfg_out_milestone_processor = ExportConfig(
        'sps.milestone_processor.sql',
        'milestone_processor',
        'reporting_prod.sps.milestone_processor',
    )
    chain_tasks(
        *cfg_inbound_milestone.operators,
        cfg_out_milestone_processor.to_mssql_operator,
    )
