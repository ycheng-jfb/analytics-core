from functools import cached_property

from include.airflow.hooks.mysql import MySqlHook
from include.airflow.operators.db_table_to_s3 import BaseDbTableToS3WatermarkOperator


class MySqlTableToS3WatermarkOperator(BaseDbTableToS3WatermarkOperator):
    @cached_property
    def db_hook(self):
        return MySqlHook(mysql_conn_id=self.db_conn_id)
