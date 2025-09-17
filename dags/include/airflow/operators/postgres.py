from functools import cached_property

from include.airflow.hooks.postgres import PostgresHook
from include.airflow.operators.db_table_to_s3 import (
    BaseDbQueryToS3Operator,
    BaseDbTableToS3Operator,
    BaseDbTableToS3WatermarkOperator,
)


class PostgresToS3WatermarkOperator(BaseDbTableToS3WatermarkOperator):
    @cached_property
    def db_hook(self):
        return PostgresHook(postgres_conn_id=self.db_conn_id)


class PostgresQueryToS3Operator(BaseDbQueryToS3Operator):
    @cached_property
    def db_hook(self):
        return PostgresHook(postgres_conn_id=self.db_conn_id)


class PostgresToS3Operator(BaseDbTableToS3Operator):
    @cached_property
    def db_hook(self):
        return PostgresHook(postgres_conn_id=self.db_conn_id)
