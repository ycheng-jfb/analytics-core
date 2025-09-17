import os

from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from include.config import conn_ids

from include.utils.sql import build_bcp_to_s3_bash_command


class MsSqlBcpToS3Operator(BashOperator):
    def __init__(
        self,
        query,
        bucket,
        key,
        record_delimiter="0x02",
        field_delimiter="0x01",
        mssql_conn_id=conn_ids.MsSql.default,
        *args,
        **kwargs,
    ):
        self.query = query
        self.bucket = bucket
        self.key = key
        self.record_delimiter = record_delimiter
        self.field_delimiter = field_delimiter
        self.mssql_conn_id = mssql_conn_id
        super().__init__(bash_command="", *args, **kwargs)

    def prepare_bash_command(self):
        password_env_var = "BCP_PASS"
        conn = BaseHook.get_connection(self.mssql_conn_id)
        self.bash_command = build_bcp_to_s3_bash_command(
            login=conn.login,
            server=conn.host,
            query=self.query,
            bucket=self.bucket,
            key=self.key,
            record_delimiter=self.record_delimiter,
            field_delimiter=self.field_delimiter,
            password=f'"${password_env_var}"',
        )
        self.env = os.environ.copy()
        self.env.update({password_env_var: conn.password})

    def execute(self, context):
        self.prepare_bash_command()
        super().execute(context)
