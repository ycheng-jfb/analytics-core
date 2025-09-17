import logging
import tempfile
from pathlib import Path
import json
from airflow.models import BaseOperator
from include.airflow.hooks.gcs import GCSHook
from include.config import conn_ids

from include.airflow.hooks.mssql import MsSqlOdbcHook
from include.utils.context_managers import ConnClosing


class MssqlToGCSOperator(BaseOperator):
    template_fields = ["filename"]

    def __init__(
        self,
        sql_or_path,
        remote_dir,
        bucket_name,
        filename,
        mssql_conn_id=conn_ids.MsSql.default,
        gcp_conn_id=conn_ids.Google.cloud_default,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.filename = filename
        self.bucket_name = bucket_name
        self.remote_dir = remote_dir
        self.sql_or_path = sql_or_path

    def get_sql_cmd(self):
        try:
            if Path(self.sql_or_path).exists() or isinstance(self.sql_or_path, Path):
                with open(self.sql_or_path.as_posix(), "rt") as f:
                    sql = f.read()
            else:
                sql = self.sql_or_path
        except OSError as e:
            if e.strerror == "File name too long":
                sql = self.sql_or_path
            else:
                raise e
        return sql

    def mssql_to_file(self, query, local_path):
        hook = MsSqlOdbcHook(mssql_conn_id=self.mssql_conn_id)
        with ConnClosing(hook.get_conn()) as con:
            cursor = con.cursor()
            cursor.execute(query)
            records = cursor.fetchall()
            json_str = ""
            for record in records:
                json_str += record[0] + "\n"
            with open(local_path, 'w') as file:
                file.write(json_str)

    def upload_to_gcs(self, local_path):
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        gcs_hook.upload(
            bucket_name=self.bucket_name,
            object_name=f"{self.remote_dir}/{self.filename}",
            filename=local_path,
        )

    def execute(self, context):
        with tempfile.TemporaryDirectory() as td:
            local_path = Path(td, self.filename)
            query = self.get_sql_cmd()
            self.mssql_to_file(query=query, local_path=local_path)
            self.upload_to_gcs(local_path=local_path)
