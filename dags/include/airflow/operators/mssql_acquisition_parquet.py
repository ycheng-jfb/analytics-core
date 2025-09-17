import tempfile
from binascii import hexlify
from pathlib import Path

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pyodbc import SQL_BINARY, SQL_VARBINARY

from include.airflow.operators.mssql_acquisition import BaseTableAcquisitionOperator
from include.utils.context_managers import ConnClosing
from include.utils.parquet import BatchedParquetWriter, get_parquet_schema_from_table
from include.config import conn_ids


class MsSqlTableToS3ParquetOperator(BaseTableAcquisitionOperator):
    template_fields = ["key"]

    def __init__(
        self,
        bucket,
        key,
        s3_conn_id=conn_ids.AWS.tfg_default,
        s3_replace=True,
        batch_size=500000,
        *args,
        **kwargs,
    ):
        self._mssql_hook = None
        self.bucket = bucket
        self.key = key
        self.s3_conn_id = s3_conn_id
        self.s3_replace = s3_replace
        self.batch_size = batch_size
        super().__init__(*args, **kwargs)

    def copy_to_s3(self, filename, key, bucket, replace):
        s3_hook = S3Hook(self.s3_conn_id)
        if Path(filename).exists():
            self.log.info(f"uploading to {self.key}")
            s3_hook.load_file(
                filename=filename, key=key, bucket_name=bucket, replace=replace
            )
        else:
            self.log.info(f"filename {filename} does not exist; nothing to do.")

    def get_arrow_schema(self, cur):
        return get_parquet_schema_from_table(
            cur=cur,
            database=self.src_database,
            schema=self.src_schema,
            table=self.src_table,
            column_subset=self.column_list,
        )

    def get_sql_to_s3_local(self, sql):
        with tempfile.TemporaryDirectory() as td, ConnClosing(
            self.mssql_hook.get_conn()
        ) as cnx:
            cnx.add_output_converter(SQL_VARBINARY, hexlify)
            cnx.add_output_converter(SQL_BINARY, hexlify)
            filename = Path(td, "temp").as_posix()
            cur = cnx.cursor()
            table_schema = self.get_arrow_schema(cur)
            cur.execute(sql)
            with BatchedParquetWriter(
                where=filename, schema=table_schema, flavor="spark", version="2.0"
            ) as writer:
                writer.write_all(cur=cur, batch_size=30000)

            self.copy_to_s3(
                filename=filename,
                key=self.key,
                bucket=self.bucket,
                replace=self.s3_replace,
            )

    def watermark_execute(self, context=None):
        sql = self.build_query(
            lower_bound=self.low_watermark, strict_inequality=self.strict_inequality
        )
        self.get_sql_to_s3_local(sql)
