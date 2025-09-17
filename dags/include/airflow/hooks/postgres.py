from airflow.providers.postgres.hooks.postgres import PostgresHook as BasePostgresHook

from include.airflow.hooks.base_db_hook import BaseDbHook
from include.config import conn_ids


class PostgresHook(BasePostgresHook, BaseDbHook):
    def __init__(
        self,
        postgres_conn_id='postgres_default',
        **kwargs,
    ):
        super().__init__(postgres_conn_id=postgres_conn_id, **kwargs)

    def hook_copy_to_file(self, sql, conn, file_obj):
        self.pandas_copy_to_file(sql.replace('\n', ''), conn, file_obj)
