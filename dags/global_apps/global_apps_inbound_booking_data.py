import fnmatch
from functools import cached_property
from pathlib import Path
from tempfile import TemporaryDirectory

import pendulum
from airflow.models import DAG, BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.ftp.hooks.ftp import FTPHook

from include.airflow.callbacks.slack import slack_failure_gsc
from include.airflow.operators.snowflake import SnowflakeProcedureOperator
from include.airflow.operators.snowflake_to_mssql import SnowflakeToMsSqlBCPOperator
from include.config import conn_ids, email_lists, owners, s3_buckets

default_args = {
    'start_date': pendulum.datetime(2022, 8, 6, tz='America/Los_Angeles'),
    'retries': 1,
    'owner': owners.data_integrations,
    'email': email_lists.data_integration_support,
    'on_failure_callback': slack_failure_gsc,
}

dag = DAG(
    dag_id='global_apps_inbound_booking_data',
    default_args=default_args,
    schedule='0 */6 * * *',
    catchup=False,
)


class BookingDataToS3Operator(BaseOperator):
    def __init__(
        self,
        s3_bucket: str,
        s3_prefix: str,
        parent_dir: str,
        s3_schema_version: str = "v1",
        file_pattern: str = "*",
        ftp_conn_id: str = conn_ids.SFTP.ftp_booking_data,
        s3_conn_id: str = conn_ids.S3.tsos_da_int_prod,
        archive: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.ftp_conn_id = ftp_conn_id
        self.file_pattern = file_pattern
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix.strip('/')
        self.s3_schema_version = s3_schema_version
        self.s3_conn_id = s3_conn_id
        self.parent_dir = parent_dir.rstrip('/')
        self.archive = archive

    @cached_property
    def ftp_hook(self) -> FTPHook:
        ftp_hook = FTPHook(self.ftp_conn_id)
        return ftp_hook

    @cached_property
    def s3_hook(self) -> S3Hook:
        return S3Hook(self.s3_conn_id)

    def execute(self, context=None):
        with TemporaryDirectory() as td:
            temp_dir = Path(td)
            self.log.info(f"pull from ftp: {self.parent_dir}")
            for file in fnmatch.filter(
                names=self.ftp_hook.list_directory(path=self.parent_dir),
                pat=self.file_pattern,
            ):
                tmp_file_path = temp_dir / file
                self.log.info(tmp_file_path)
                self.ftp_hook.retrieve_file(
                    remote_full_path=f"{self.parent_dir}/{file}",
                    local_full_path_or_buffer=tmp_file_path.as_posix(),
                )
                s3_key = f"{self.s3_prefix}/{self.s3_schema_version}/{file}"
                self.log.info(f"push to s3: s3://{self.s3_bucket}/{s3_key}")
                self.s3_hook.load_file(
                    filename=tmp_file_path.as_posix(),
                    key=s3_key,
                    bucket_name=self.s3_bucket,
                    replace=True,
                )
                if self.archive:
                    self.log.info(f"archive file: {self.parent_dir}/archive/{file}")
                    self.ftp_hook.store_file(
                        remote_full_path=f"{self.parent_dir}/archive/{file}",
                        local_full_path_or_buffer=tmp_file_path.as_posix(),
                    )
                    self.ftp_hook.delete_file(path=f"{self.parent_dir}/{file}")


with dag:
    ftp_to_s3 = BookingDataToS3Operator(
        task_id='ftp_to_s3',
        s3_prefix="lake/gsc.booking_data/",
        s3_bucket=s3_buckets.tsos_da_int_inbound,
        s3_schema_version="v3",
        file_pattern='JF_bookings_*.csv',
        parent_dir='/ftp_justfab/out/bookings',
        archive=True,
    )

    to_snowflake = SnowflakeProcedureOperator(procedure='gsc.booking_data.sql', database='lake')

    booking_data_to_mssql = SnowflakeToMsSqlBCPOperator(
        task_id='booking_dataset_to_mssql',
        snowflake_database="reporting_prod",
        snowflake_schema="gsc",
        snowflake_table="booking_dataset",
        mssql_target_table='booking_dataset',
        mssql_target_database='ssrs_reports',
        mssql_target_schema='gsc',
        mssql_conn_id=conn_ids.MsSql.evolve01_app_airflow_rw,
        unique_columns=['so', 'po', 'style_color'],
        watermark_column='meta_update_datetime',
    )

    ftp_to_s3 >> to_snowflake >> booking_data_to_mssql
