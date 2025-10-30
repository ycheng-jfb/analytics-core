import tempfile
from pathlib import Path

from airflow.models import TaskInstance
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from include.airflow.hooks.mssql import MsSqlOdbcHook
from include.airflow.operators.watermark import BaseTaskWatermarkOperator
from include.utils.data_structures import greatest
from include.utils.string import unindent_auto
from include.config import conn_ids


class MsSqlCDCToS3Operator(BaseTaskWatermarkOperator):
    source_meta_cols = ["__$start_lsn_datetime", "__$start_lsn", "__$operation"]
    template_fields = ["key"]

    def __init__(
        self,
        column_list: list,
        database: str,
        schema: str,
        cdc_instance_name: str,
        bucket: str,
        key: str,
        row_filter_option: str = "all",
        initial_load_value: str = '1900-01-01T00:00:00+00:00',
        mssql_conn_id: str = conn_ids.MsSql.default,
        s3_conn_id: str = conn_ids.AWS.tfg_default,
        s3_replace: bool = True,
        **kwargs,
    ):
        super().__init__(initial_load_value=initial_load_value, **kwargs)
        self.column_list = column_list
        self.database = database
        self.schema = schema
        self.cdc_instance_name = cdc_instance_name
        self.row_filter_option = row_filter_option
        self.bucket = bucket
        self.key = key
        self.mssql_conn_id = mssql_conn_id
        self.s3_conn_id = s3_conn_id
        self.s3_replace = s3_replace
        self._mssql_hook = None
        self._snowflake_hook = None

    def test_run(self, data_interval_start):
        ti = TaskInstance(task=self, execution_date=data_interval_start)
        ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True)

    def build_sql_cmd(self, from_lsn, to_lsn):
        repl_map = {
            "__$start_lsn_datetime": "sys.fn_cdc_map_lsn_to_time(l.__$start_lsn) AS __$start_lsn_datetime",
            "__$start_lsn": "convert(varchar, l.__$start_lsn, 2) AS __$start_lsn",
        }
        meta_cols = [repl_map.get(x, x) for x in self.source_meta_cols]
        col_list_str = ",\n\t\t\t\t".join(meta_cols + self.column_list)
        fqfn = f"{self.database}.{self.schema}.fn_cdc_get_net_changes_{self.cdc_instance_name}"
        cmd = f"""
            SELECT
                {col_list_str}
            FROM {fqfn}({from_lsn}, {to_lsn}, '{self.row_filter_option}') l
            ORDER BY l.__$start_lsn
        """
        return unindent_auto(cmd)

    def get_mssql_hook(self):
        if not self._mssql_hook:
            self._mssql_hook = MsSqlOdbcHook(
                mssql_conn_id=self.mssql_conn_id, database=self.database, schema=self.schema
            )
        return self._mssql_hook

    def get_sql_to_local_file(self, cnx, sql, filename):
        # imported locally so turbodbc can be optional
        from include.utils.deprecated_parquet import write_sql_to_parquet_pandas

        return write_sql_to_parquet_pandas(cnx=cnx, sql=sql, filename=filename, chunk_size=100000)

    def copy_to_s3(self, filename, key, bucket, replace):
        s3_hook = S3Hook(self.s3_conn_id)
        if Path(filename).exists():
            self.log.info(f"uploading to {self.key}")
            s3_hook.load_file(filename=filename, key=key, bucket_name=bucket, replace=replace)
        else:
            self.log.info(f"filename {filename} does not exist; nothing to do.")

    def watermark_execute(self, context=None):
        from_lsn = greatest(self.get_min_lsn(), self.low_watermark)
        cmd = self.build_sql_cmd(from_lsn=from_lsn, to_lsn=self.new_high_watermark)
        hook = self.get_mssql_hook()
        with tempfile.TemporaryDirectory() as td, hook.get_conn() as cnx:
            filename = Path(td, "tempfile").as_posix()
            self.get_sql_to_local_file(cnx=cnx, sql=cmd, filename=filename)
            self.copy_to_s3(
                filename=filename, key=self.key, bucket=self.bucket, replace=self.s3_replace
            )

    def get_high_watermark(self) -> str:
        cmd = "select convert(varchar, sys.fn_cdc_get_max_lsn(), 1) as max_lsn"
        hook = self.get_mssql_hook()
        rows = hook.get_records(cmd)
        high_watermark = str(rows[0][0])
        return high_watermark

    def get_min_lsn(self) -> str:
        cmd = f"select convert(varchar, sys.fn_cdc_get_min_lsn('{self.cdc_instance_name}'), 1) as min_lsn"
        hook = self.get_mssql_hook()
        rows = hook.get_records(cmd)
        min_lsn = str(rows[0][0])
        return min_lsn
