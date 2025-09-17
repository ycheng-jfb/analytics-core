from airflow.providers.mysql.hooks.mysql import MySqlHook as BaseMySqlHook

from include.airflow.hooks.base_db_hook import BaseDbHook
from include.config import conn_ids


class MySqlHook(BaseMySqlHook, BaseDbHook):
    def __init__(
        self,
        mysql_conn_id=conn_ids.MySQL.default,
        **kwargs,
    ):
        super().__init__(
            mysql_conn_id=mysql_conn_id,
            **kwargs,
        )

    def hook_copy_to_file(self, sql, conn, file_obj):
        self.pandas_copy_to_file(sql.replace("\n", ""), conn, file_obj)
