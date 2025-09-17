import gzip
import tempfile
from abc import abstractmethod
from pathlib import Path

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from functools import cached_property

from include.airflow.operators.watermark import BaseProcessWatermarkOperator
from include.utils.context_managers import ConnClosing
from include.utils.string import unindent_auto


class BaseDbToS3Operator(BaseOperator):
    """
    Base operator for saving results from DB to a file in S3.

    Override db_hook method for specific DB instance being hit. DB hook used should
    inherit from BaseDbHook.

    Override build_sql_statement method to construct sql statement to query DB instance.
    """

    def __init__(
        self, bucket, key, db_conn_id, s3_conn_id=None, s3_replace=True, **kwargs
    ):
        super().__init__(**kwargs)
        self.bucket = bucket
        self.key = key
        self.db_conn_id = db_conn_id
        self.s3_conn_id = s3_conn_id
        self.s3_replace = s3_replace

    @cached_property
    @abstractmethod
    def db_hook(self):
        raise NotImplementedError

    @cached_property
    def s3_hook(self):
        return S3Hook(self.s3_conn_id)

    @abstractmethod
    def build_sql_statement(self):
        raise NotImplementedError

    def DbTableToS3Operator_execute(self):
        with tempfile.TemporaryDirectory() as td:
            temp_file = Path(td, "tmpfile").as_posix()
            with gzip.open(temp_file, "wb") as f:
                sql = self.build_sql_statement()

                self.db_hook.copy_to_file(sql, f)

                print(f"uploaded to {self.key}")
                self.s3_hook.load_file(
                    filename=f.filename,
                    key=self.key,
                    bucket_name=self.bucket,
                    replace=self.s3_replace,
                )

    def execute(self, context=None):
        self.DbTableToS3Operator_execute()


class BaseDbTableToS3Operator(BaseDbToS3Operator):
    """
    Base operator to copy full DB table, or list of columns from table, to a file in S3.

    Override db_hook method for specific DB instance being hit. DB hook used should
    inherit from BaseDbHook.
    """

    template_fields = ["key"]

    def __init__(
        self,
        table,
        database=None,
        schema=None,
        column_list=None,
        additional_sql=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.database = database
        self.schema = schema
        self.table = table
        self.column_list = column_list
        self.additional_sql = additional_sql

    def build_table_string(self):
        table_str = self.table
        table_str = f"{self.schema}.{table_str}" if self.schema else table_str
        table_str = f"{self.database}.{table_str}" if self.database else table_str
        return table_str

    def build_sql_statement(self):
        if self.column_list:
            cnt = ",\n\t"
            column_names = [x.source_name for x in self.column_list]
            select_columns = cnt.join(column_names)
        else:
            select_columns = "*"

        sql = f"""SELECT {select_columns}
                FROM {self.build_table_string()}
                """
        if self.additional_sql:
            sql = sql + self.additional_sql

        return sql


class BaseDbQueryToS3Operator(BaseDbToS3Operator):
    """
    Base operator for saving the results returned from querying a DB to a file in S3.

    Override db_hook method for specific DB instance being hit. DB hook used should
    inherit from BaseDbHook.
    """

    template_fields = ["key"]

    def __init__(self, sql_query, **kwargs):
        super().__init__(**kwargs)
        self.sql_query = sql_query

    def build_sql_statement(self):
        return self.sql_query


class BaseDbTableToS3WatermarkOperator(
    BaseProcessWatermarkOperator, BaseDbTableToS3Operator
):
    """
    Base operator for saving the results from querying a DB to a file in S3, using standard
    watermark functionality.

    Override db_hook method for specific DB instance being hit. DB hook used should
    inherit from BaseDbHook.
    """

    def __init__(
        self,
        bucket,
        key,
        table,
        db_conn_id,
        watermark_column,
        database=None,
        schema=None,
        column_list=None,
        additional_sql=None,
        last_high_watermark=None,
        s3_conn_id=None,
        s3_replace=True,
        **kwargs,
    ):
        super().__init__(
            bucket=bucket,
            key=key,
            table=table,
            db_conn_id=db_conn_id,
            **kwargs,
        )

        self.database = database
        self.schema = schema
        self.watermark_column = watermark_column
        self.column_list = column_list
        self.additional_sql = additional_sql
        self.last_high_watermark = last_high_watermark
        self.s3_conn_id = s3_conn_id
        self.s3_replace = s3_replace

    def build_high_watermark_command(self):
        if self.last_high_watermark:
            cmd = f"""
                SELECT coalesce(max({self.watermark_column}), '{self.last_high_watermark}')
                FROM {self.build_table_string()}
                WHERE {self.watermark_column} >= '{self.last_high_watermark}'
            """
        else:
            cmd = f"""
                SELECT max({self.watermark_column})
                FROM {self.build_table_string()}
            """

        return unindent_auto(cmd)

    def get_high_watermark(self):
        cmd = self.build_high_watermark_command()
        with ConnClosing(self.db_hook.get_conn()) as cnx:
            cur = cnx.cursor()
            cur.execute(cmd)
            k = cur.fetchone()
            val = k[0]
            if hasattr(val, "isoformat"):
                return val.isoformat()
            else:
                return val

    def build_sql_statement(self):
        if self.column_list:
            cnt = ",\n\t"
            column_names = [x.source_name for x in self.column_list]
            select_columns = cnt.join(column_names)
        else:
            select_columns = "*"

        sql = f"""SELECT {select_columns}
                FROM {self.build_table_string()}
                WHERE {self.watermark_column} > '{self.low_watermark}'
                AND {self.watermark_column} <= '{self.new_high_watermark}'
                """
        if self.additional_sql:
            sql = sql + self.additional_sql

        return sql

    def watermark_execute(self, context=None):
        self.DbTableToS3Operator_execute()
