import csv
import gzip
import tempfile
from pathlib import Path
from tempfile import TemporaryDirectory

from airflow.models import BaseOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from include.airflow.hooks.gcs import GCSHook
from include.airflow.hooks.s3 import S3Hook
from include.airflow.hooks.smb import SMBHook
from include.airflow.hooks.snowflake import SnowflakeHook
from include.airflow.operators.snowflake import SnowflakeWatermarkSqlOperator
from include.config import conn_ids
from include.utils.s3 import get_filename
from include.utils.snowflake import (
    generate_query_tag_cmd,
    get_result_column_names,
    split_statements,
)


class BaseSnowflakeExportOperator(SnowflakeWatermarkSqlOperator):
    def __init__(
        self,
        field_delimiter,
        batch_size=100000,
        compression=None,
        header=False,
        dialect="unix",
        lowercase_column_names=True,
        warehouse='DA_WH_ETL_LIGHT',
        quoting=0,
        quotechar=None,
        *args,
        **kwargs,
    ):
        super().__init__(warehouse=warehouse, *args, **kwargs)
        self.field_delimiter = field_delimiter
        self.batch_size = batch_size
        self.compression = compression
        self.header = header
        self.dialect = dialect
        self.lowercase_column_names = lowercase_column_names
        self.quoting = quoting
        self.quotechar = quotechar

    @staticmethod
    def get_effective_column_list(lowercase_column_names, col_lst):
        return [x.lower() if lowercase_column_names else x for x in col_lst]

    def export_to_file(self, sql, local_path) -> int:
        batch_count = 1
        rows_exported = 0
        with self.snowflake_hook.get_conn() as cnx:
            for statement in split_statements(sql):
                self.log.info("Executing: %s", statement)
                cur = cnx.cursor()
                query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
                cur.execute(query_tag)
                cur.execute(statement, params=self.parameters)
            open_func = gzip.open if self.compression == "gzip" else open
            with open_func(local_path, "wt") as f:  # type: ignore
                if self.quotechar:
                    writer = csv.writer(
                        f,
                        dialect=self.dialect,
                        delimiter=self.field_delimiter,
                        quoting=self.quoting,
                        escapechar='\\',
                        quotechar=self.quotechar,
                    )
                else:
                    writer = csv.writer(
                        f,
                        dialect=self.dialect,
                        delimiter=self.field_delimiter,
                        quoting=self.quoting,
                        escapechar='\\',
                    )
                rows = cur.fetchmany(self.batch_size)
                if rows and self.header:
                    column_list = self.get_effective_column_list(
                        self.lowercase_column_names, get_result_column_names(cur)
                    )
                    writer.writerow(column_list)
                while rows:
                    writer.writerows(rows)
                    rows_exported += len(rows)
                    batch_count += 1
                    self.log.info(f"fetching more rows: batch {batch_count}")
                    rows = cur.fetchmany(self.batch_size)
        return rows_exported


class SnowflakeToSFTPOperator(BaseSnowflakeExportOperator):
    template_fields = ["sql_or_path", "parameters", "filename", "sftp_dir"]

    def __init__(self, filename, sftp_dir, sftp_conn_id=conn_ids.SFTP.default, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.filename = filename
        self.sftp_dir = sftp_dir

    def upload_to_sftp(self, local_path, remote_path):
        sftp_hook = SFTPHook(ftp_conn_id=self.sftp_conn_id)
        print(f"Transfering local file {local_path} to {remote_path}")

        class Callback:
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.last_print_percent = 0

            def __call__(self, transferred, total):
                new_percent = 100 * float(transferred) / float(total)
                if new_percent - self.last_print_percent >= 5:
                    print(f"{new_percent:.1f}% transferred")
                    self.last_print_percent = new_percent

        progress_callback = Callback()

        with sftp_hook.get_conn() as sftp_client:
            sftp_client.put(
                localpath=local_path, remotepath=remote_path, callback=progress_callback
            )

    def watermark_execute(self, context=None):
        with tempfile.TemporaryDirectory() as td:
            local_path = Path(td, self.filename)
            sql = self.get_sql_cmd(self.sql_or_path)
            rows_exported = self.export_to_file(sql=sql, local_path=local_path)
            if rows_exported > 0:
                print(f"{rows_exported}: rows exported")
                remote_path = Path(self.sftp_dir, self.filename).as_posix()
                self.upload_to_sftp(local_path=local_path, remote_path=remote_path)
            else:
                print("no rows written.  skipping upload.")


class SnowflakeToSFTPBatchOperator(BaseSnowflakeExportOperator):
    """Execute a Snowflake query and upload the results to SFTP in batches

    Args:
        sftp_conn_id: SFTP connection id
        filename: name of the file to be uploaded, this will be suffixed with part number in SFTP
        sftp_dir: SFTP target directory
    """

    template_fields = ["sql_or_path", "parameters", "filename", "sftp_dir"]

    def __init__(self, filename, sftp_dir, sftp_conn_id=conn_ids.SFTP.default, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.filename = filename
        self.sftp_dir = sftp_dir

    def upload_to_sftp(self, local_path, remote_path):
        sftp_hook = SFTPHook(ftp_conn_id=self.sftp_conn_id)
        print(f"Transfering local file {local_path} to {remote_path}")

        class Callback:
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.last_print_percent = 0

            def __call__(self, transferred, total):
                new_percent = 100 * float(transferred) / float(total)
                if new_percent - self.last_print_percent >= 5:
                    print(f"{new_percent:.1f}% transferred")
                    self.last_print_percent = new_percent

        progress_callback = Callback()

        with sftp_hook.get_conn() as sftp_client:
            sftp_client.put(
                localpath=local_path, remotepath=remote_path, callback=progress_callback
            )

    def export_batch_file(self, sql):
        with self.snowflake_hook.get_conn() as cnx:
            for statement in split_statements(sql):
                self.log.info("Executing: %s", statement)
                cur = cnx.cursor()
                cur.execute(statement, params=self.parameters)
            i = 0
            while True:
                i += 1
                # Fetch the next batch of results from the cursor
                rows = cur.fetchmany(self.batch_size)
                if not rows:
                    break
                # Generate a file name based on the batch index
                with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='') as temp_file:
                    csv_writer = csv.writer(temp_file)
                    csv_writer.writerows(rows)
                    remote_path = Path(self.sftp_dir, self.filename.format(part=i)).as_posix()
                    self.upload_to_sftp(local_path=temp_file.name, remote_path=remote_path)

    def watermark_execute(self, context=None):
        sql = self.get_sql_cmd(self.sql_or_path)
        self.export_batch_file(sql=sql)


class SnowflakeToWindowsShareOperator(BaseSnowflakeExportOperator):
    template_fields = ["sql_or_path", "filename", "share_name", "shared_dir"]

    def __init__(self, filename, shared_dir, share_name, smb_conn_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filename = filename
        self.shared_dir = shared_dir
        self.share_name = share_name
        self.smb_conn_id = smb_conn_id

    def upload_to_share(self, remote_path, local_path):

        smb_hook = SMBHook(smb_conn_id=self.smb_conn_id)
        smb_hook.upload(share_name=self.share_name, local_path=local_path, remote_path=remote_path)

    def watermark_execute(self, context=None):
        with tempfile.TemporaryDirectory() as td:
            local_path = Path(td, self.filename)
            sql = self.get_sql_cmd(self.sql_or_path)
            rows_exported = self.export_to_file(sql=sql, local_path=local_path)
            if rows_exported > 0:
                print(f"{rows_exported}: rows exported")
                remote_path = Path(self.shared_dir, self.filename).as_posix()
                print(f"Transfering local file {local_path} to {remote_path}")
                self.upload_to_share(remote_path, local_path)
                print(f"Transfered local file {local_path} to {self.shared_dir}")
            else:
                print("no rows written.  skipping upload.")


class SnowflakeToS3Operator(BaseSnowflakeExportOperator):
    template_fields = ["sql_or_path", "parameters", "key", "bucket"]

    def __init__(
        self,
        bucket,
        key,
        s3_conn_id=conn_ids.AWS.tfg_default,
        acl='bucket-owner-full-control',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.s3_conn_id = s3_conn_id
        self.bucket = bucket
        self.key = key
        self.acl = acl

    def upload_to_s3(self, local_path):
        print(f"load to key {self.key}")
        s3_hook = S3Hook(self.s3_conn_id)
        print(f"uploading report to {self.key}")
        s3_hook.load_file(
            filename=local_path.as_posix(),
            key=self.key,
            bucket_name=self.bucket,
            replace=True,
            acl=self.acl,
        )

    def watermark_execute(self, context=None):
        with tempfile.TemporaryDirectory() as td:
            filename = get_filename(self.key)
            local_path = Path(td, filename)
            sql = self.get_sql_cmd(self.sql_or_path)
            rows_exported = self.export_to_file(sql=sql, local_path=local_path)
            if rows_exported > 0:
                print(f"{rows_exported}: rows exported")
                self.upload_to_s3(local_path=local_path)
            else:
                print("no rows written.  skipping upload.")


class SnowflakeToSMBOperator(BaseSnowflakeExportOperator):
    """Sends output of snowflake sql script or query to SMB

    Args:
        remote_path: SMB path
        share_name: SMB share name
        smb_conn_id: airflow SMB connection id
        header: boolean, indicates if header row should be included
        file_format: supports xlsx, csv
    """

    template_fields = ["sql_or_path", "remote_path"]

    def __init__(
        self,
        remote_path,
        share_name,
        smb_conn_id=conn_ids.SMB.default,
        header=True,
        file_format='xlsx',
        **kwargs,
    ):
        self.remote_path = remote_path
        self.share_name = share_name
        self.smb_conn_id = smb_conn_id
        self.header = header
        self.file_format = file_format
        super().__init__(**kwargs)

    def write_to_file(self, query, local_path):
        session_parameters = {'QUERY_TAG': f'{self.dag_id},{self.task_id}'}
        hook = SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id, session_parameters=session_parameters
        )
        query_result = hook.get_pandas_df(sql=query)
        if self.file_format == 'xlsx':
            query_result.to_excel(local_path, index=None, header=True)
        else:
            query_result.to_csv(local_path, index=None, header=True)

    def upload_to_smb(self, local_path):
        smb_hook = SMBHook(smb_conn_id=self.smb_conn_id)
        smb_hook.upload(
            share_name=self.share_name, remote_path=self.remote_path, local_path=local_path
        )

    def watermark_execute(self, context=None):
        with tempfile.TemporaryDirectory() as td:
            filename = Path(td) / Path(self.remote_path).name
            local_path = Path(td, filename)
            sql = self.get_sql_cmd(self.sql_or_path)
            self.write_to_file(query=sql, local_path=local_path)
            self.upload_to_smb(local_path=filename)


class SnowflakeToSMBExcelOperator(BaseOperator):
    """
    Sends output of given snowflake query to the provided excel file

    Note:
        - Throws error when output row size is > 1048575 as Excel cannot handle it.
        - Throws error when trying to store timezone-aware data as excel cannot store it.
        - Overwrites an existing file if there is a file with same name in target location

    Args:
        sql_cmd: Snowflake command to which output is needed
        remote_path: location in the shared drive to which excel file need to be saved
        share_name: name of the shared drive
        snowflake_conn_id: connection_id to connect to snowflake
        smb_conn_id: connection_id to connect to shared SMB server
        header: if you want the excel file to have header or just data
    """

    template_fields = ['remote_path']

    def __init__(
        self,
        sql_cmd,
        remote_path,
        share_name,
        snowflake_conn_id=conn_ids.Snowflake.default,
        warehouse='DA_WH_ETL_LIGHT',
        smb_conn_id=conn_ids.SMB.default,
        header=True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sql_cmd = sql_cmd
        self.remote_path = remote_path
        self.share_name = share_name
        self.snowflake_conn_id = snowflake_conn_id
        self.smb_conn_id = smb_conn_id
        self.header = header
        self.warehouse = warehouse

    def write_to_file(self, query, local_path):
        """
        Sends snowflake output to temporary excel file.

        Note:
            - Throws error when output row size is > 1048575 as Excel cannot handle it.
            - Throws error when trying to store timezone-aware data as excel cannot store it

        Args:
            query: snowflake query
            local_path: temporary path
        """
        session_parameters = {'QUERY_TAG': f'{self.dag_id},{self.task_id}'}
        hook = SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id, session_parameters=session_parameters
        )
        query_result = hook.get_pandas_df(sql=query)

        if len(query_result) > 1048575:
            raise Exception("ResultSet > 1048575 rows. Excel cannot handle!")

        query_result.to_excel(local_path, index=None, header=True)

    def upload_to_smb(self, local_path):
        """
        Uploads the excel file from temporary location to desired path

        Note:
            Overwrites an existing file if there is a file with same name in target location

        Args:
            local_path: path at which the output excel file is expected
        """
        smb_hook = SMBHook(smb_conn_id=self.smb_conn_id)
        smb_hook.upload(
            share_name=self.share_name, remote_path=self.remote_path, local_path=local_path
        )

    def execute(self, context):
        with TemporaryDirectory() as td:
            filename = Path(td) / Path(self.remote_path).name
            self.write_to_file(query=self.sql_cmd, local_path=filename)
            self.upload_to_smb(local_path=filename)


class SnowflakeToGCSOperator(BaseSnowflakeExportOperator):
    """Sends output of snowflake sql script or query to GCS

    Args:
        bucket_name: bucket name of GCS to upload to
        remote_path: path of GCS to upload to
        gcp_conn_id: GCP connection id
        filename: filename to be saved as
        header: boolean, indicates if header row should be included
    """

    template_fields = ["filename"]

    def __init__(
        self,
        remote_dir,
        bucket_name,
        filename,
        gcp_conn_id=conn_ids.Google.cloud_default,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.filename = filename
        self.bucket_name = bucket_name
        self.remote_dir = remote_dir

    def upload_to_gcs(self, local_path):
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        gcs_hook.upload(
            bucket_name=self.bucket_name,
            object_name=f"{self.remote_dir}/{self.filename}",
            filename=local_path,
        )

    def watermark_execute(self, context=None):
        with tempfile.TemporaryDirectory() as td:
            local_path = Path(td, self.filename)
            sql = self.get_sql_cmd(self.sql_or_path)
            rows_exported = self.export_to_file(sql=sql, local_path=local_path)
            if rows_exported > 0:
                print(f"{rows_exported}: rows exported")
                self.upload_to_gcs(local_path=local_path)
            else:
                print("no rows.  skipping upload.")
