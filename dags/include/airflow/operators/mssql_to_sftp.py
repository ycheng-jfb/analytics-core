import tempfile
from pathlib import Path

import pandas as pd
from airflow.models import BaseOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook

from include.airflow.hooks.mssql import MsSqlOdbcHook
from include.utils.context_managers import ConnClosing
from plugins.operator_links import SqlScriptLink
from include.config import conn_ids


class MssqlToSFTPOperator(BaseOperator):
    operator_extra_links = (SqlScriptLink,)

    def __init__(
        self,
        sql_or_path,
        remote_dir,
        filename,
        mssql_conn_id=conn_ids.MsSql.default,
        sftp_conn_id=conn_ids.SFTP.default,
        header=True,
        compression=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.sftp_conn_id = sftp_conn_id
        self.filename = filename
        self.remote_dir = remote_dir
        self.sql_or_path = sql_or_path
        self.header = header
        self.compression = compression

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
            query_result = pd.read_sql_query(query, con)
            query_result.to_csv(
                local_path,
                header=self.header,
                index=False,
                compression=self.compression,
            )

    def upload_to_sftp(self, local_path):
        sftp_hook = SFTPHook(ftp_conn_id=self.sftp_conn_id)

        with sftp_hook.get_conn() as sftp_client:
            sftp_client.put(
                localpath=local_path,
                remotepath=Path(self.remote_dir, self.filename).as_posix(),
            )

    def execute(self, context):
        with tempfile.TemporaryDirectory() as td:
            local_path = Path(td, self.filename)
            query = self.get_sql_cmd()

            self.mssql_to_file(query=query, local_path=local_path)

            self.upload_to_sftp(local_path=local_path)
