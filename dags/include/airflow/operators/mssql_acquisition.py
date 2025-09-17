import os
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import List, Optional, Type, Union

from dateutil.parser import parse
from include.airflow.hooks.bash import BashHook
from include.airflow.hooks.mssql import MsSqlOdbcHook
from include.airflow.operators.mssql import MsSqlToS3Operator
from include.airflow.operators.watermark import BaseProcessWatermarkOperator
from include.utils.context_managers import ConnClosing
from include.utils.sql import build_bcp_to_s3_bash_command
from include.utils.string import unindent_auto
from include.config import conn_ids


def is_hex(val: str):
    try:
        int(val, 16)
        return True
    except (ValueError, TypeError):
        return False


def quote_if_not_hex(val: str):
    if is_hex(val):
        return val
    else:
        return f"'{val}'"


class BaseHighWatermarkQuery(ABC):
    def __init__(self, table_name, column_name, last_high_watermark=None):
        self.table_name = table_name
        self.column_name = column_name
        self.last_high_watermark = last_high_watermark

    def __call__(self, *args, **kwargs):
        return self.high_watermark_command

    @property
    @abstractmethod
    def high_watermark_command(self):
        pass


class HighWatermarkMax(BaseHighWatermarkQuery):
    @property
    def high_watermark_command(self):
        if self.last_high_watermark:
            cmd = f"""
                SELECT coalesce(max({self.column_name}), '{self.last_high_watermark}')
                FROM {self.table_name} WITH (nolock)
                WHERE {self.column_name} >= '{self.last_high_watermark}'
            """
        else:
            cmd = f"""
                SELECT max({self.column_name})
                FROM {self.table_name} WITH (nolock)
            """
        return unindent_auto(cmd)


class HighWatermarkMaxRowVersion(BaseHighWatermarkQuery):
    @property
    def high_watermark_command(self):
        cmd = f"""
            SELECT convert(varchar, convert(binary(8), max({self.column_name})), 1)
            FROM {self.table_name} WITH (nolock)
        """
        return unindent_auto(cmd)


class HighWatermarkMaxVarcharSS(BaseHighWatermarkQuery):
    @property
    def high_watermark_command(self):
        if self.last_high_watermark:
            cmd = f"""
                SELECT coalesce(convert(VARCHAR, max({self.column_name}), 21), '{self.last_high_watermark}')
                FROM {self.table_name} WITH (nolock)
                WHERE {self.column_name} >= '{self.last_high_watermark}'
            """
        else:
            cmd = f"""
                SELECT convert(VARCHAR, max({self.column_name}), 21)
                FROM {self.table_name} WITH (nolock)
            """
        return unindent_auto(cmd)


class HighWatermarkMaxVarcharDatetimeOffset(BaseHighWatermarkQuery):
    @property
    def high_watermark_command(self):
        if self.last_high_watermark:
            cmd = f"""
                SELECT coalesce(convert(VARCHAR, max({self.column_name}), 127), '{self.last_high_watermark}')
                FROM {self.table_name} WITH (nolock)
                WHERE {self.column_name} >= '{self.last_high_watermark}'
            """
        else:
            cmd = f"""
                SELECT convert(VARCHAR, max({self.column_name}), 127)
                FROM {self.table_name} WITH (nolock)
            """
        return unindent_auto(cmd)


class BaseSourceLookback:
    def __call__(self, expression) -> str:
        raise NotImplementedError


class SourceLookbackDatepart(BaseSourceLookback):
    def __init__(self, datepart, value):
        self.datepart = datepart
        self.value = value

    def __call__(self, expression) -> str:
        return f"dateadd({self.datepart}, {self.value}, '{expression}')"

    def __repr__(self):
        return f"SourceLookbackDatepart({self.datepart.__repr__()}, {self.value.__repr__()})"


class BaseTableAcquisitionOperator(BaseProcessWatermarkOperator):
    def __init__(
        self,
        src_table,
        src_schema,
        src_database,
        watermark_namespace,
        watermark_process_name,
        column_list: List[str] = None,
        table_hints: List[str] = None,
        linked_server: Optional[str] = None,
        watermark_column=None,
        mssql_conn_id=conn_ids.MsSql.default,
        initial_load_value="1900-01-01 00:00:00+00:00",
        strict_inequality=False,
        high_watermark_cls: Type[BaseHighWatermarkQuery] = HighWatermarkMax,
        lookback_func: Optional[BaseSourceLookback] = None,
        partition_cols: List[str] = None,
        *args,
        **kwargs,
    ):
        super().__init__(
            initial_load_value=initial_load_value,
            namespace=watermark_namespace,
            process_name=watermark_process_name,
            **kwargs,
        )
        self.src_table = src_table
        self.src_schema = src_schema
        self.src_database = src_database
        self.linked_server = linked_server
        self.watermark_column = watermark_column
        self.column_list = column_list
        self._table_hints = table_hints
        self.strict_inequality = strict_inequality
        self.mssql_conn_id = mssql_conn_id
        self.high_watermark_cls = high_watermark_cls or HighWatermarkMax
        self._mssql_hook = None
        self.lookback_func = lookback_func
        self.partition_cols = partition_cols

    def build_high_watermark_command(self):
        table_name = (
            f"{self.linked_server}.{self.src_database}.{self.src_schema}.{self.src_table}"
            if self.linked_server
            else f"{self.src_database}.{self.src_schema}.{self.src_table}"
        )
        hwm_getter = self.high_watermark_cls(
            table_name=table_name,
            column_name=self.watermark_column,
            last_high_watermark=self.low_watermark,
        )
        return unindent_auto(hwm_getter.high_watermark_command)

    @property
    def mssql_hook(self):
        if not self._mssql_hook:
            self._mssql_hook = MsSqlOdbcHook(self.mssql_conn_id)
        return self._mssql_hook

    def get_high_watermark(self):
        cmd = self.build_high_watermark_command()
        with ConnClosing(self.mssql_hook.get_conn()) as cnx:
            cur = cnx.cursor()
            cur.execute(cmd)
            k = cur.fetchone()
            val = k[0]
            if hasattr(val, "isoformat"):
                return val.isoformat()
            else:
                return val

    @property
    def table_hints(self):
        default_hints = ["nolock"]
        extra_hints = [
            x for x in self._table_hints or [] if x.lower() not in default_hints
        ]
        return ", ".join(default_hints + extra_hints)

    def build_query(
        self,
        lower_bound,
        upper_bound=None,
        strict_inequality=False,
        column_override=None,
    ):
        inequality = ">" if strict_inequality else ">="
        select_list = (
            ", ".join([f"{x}" for x in self.column_list]) if self.column_list else "*"
        )
        where_clause = ""
        watermark_column = column_override or self.watermark_column
        if watermark_column:
            if self.lookback_func and lower_bound != self.initial_load_value:
                eff_lower_bound = self.lookback_func(lower_bound)
            else:
                eff_lower_bound = quote_if_not_hex(lower_bound)

            where_clause = f"WHERE s.{watermark_column} {inequality} {eff_lower_bound}"
            if upper_bound:
                where_clause += f"\n            AND s.{watermark_column} < {quote_if_not_hex(upper_bound)}"
        full_table_name = (
            f"{self.linked_server}.{self.src_database}.{self.src_schema}.{self.src_table}"
            if self.linked_server
            else f"{self.src_database}.{self.src_schema}.{self.src_table}"
        )
        if self.partition_cols:
            par_col_select_list = ", ".join(
                [f"{x} AS __t_{x}" for x in self.partition_cols]
            )
            par_col_join = "\n                AND ".join(
                [f"s.{x} = i.__t_{x}" for x in self.partition_cols]
            )
            query = f"""
            SELECT {par_col_select_list}
            INTO #tmp
            FROM {full_table_name} s WITH ({self.table_hints})
            {where_clause};

            SELECT {select_list}
            FROM #tmp i
            JOIN {full_table_name} s WITH ({self.table_hints}) ON {par_col_join};"""
        else:
            query = f"""
            SELECT {select_list}
            FROM {full_table_name} s WITH ({self.table_hints})
            {where_clause}"""
        return unindent_auto(query)

    def execute(self, context=None):
        if self.watermark_column:
            self.watermark_pre_execute()

        self.watermark_execute(context=context)

        if self.watermark_column:
            self.watermark_post_execute()


class MsSqlTableToS3CsvOperator(BaseTableAcquisitionOperator, MsSqlToS3Operator):
    template_fields = ["key"]

    def __init__(
        self,
        bucket,
        key,
        s3_conn_id=conn_ids.AWS.tfg_default,
        s3_replace=True,
        batch_size=100000,
        file_split_size=5000000,
        *args,
        **kwargs,
    ):
        self._mssql_hook = None
        super().__init__(
            sql="",
            bucket=bucket,
            key=key,
            s3_conn_id=s3_conn_id,
            s3_replace=s3_replace,
            batch_size=batch_size,
            file_split_size=file_split_size,
            *args,
            **kwargs,
        )

    def set_sql(
        self, lower_bound_override=None, upper_bound_override=None, column_override=None
    ):
        self.sql = self.build_query(
            lower_bound=lower_bound_override or self.low_watermark,
            upper_bound=upper_bound_override,
            strict_inequality=self.strict_inequality,
            column_override=column_override,
        )
        print(self.sql)

    def watermark_execute(self, context=None):
        self.set_sql()
        MsSqlToS3Operator.execute(self, context)

    def backfill(self, lower_bound, upper_bound, column_override, dry_run=False):
        self.set_sql(
            lower_bound_override=lower_bound,
            upper_bound_override=upper_bound,
            column_override=column_override,
        )
        if dry_run:
            MsSqlToS3Operator.dry_run(self)
        else:
            MsSqlToS3Operator.execute(self, context=None)

    def dry_run(self):
        self.set_sql()
        print(self.sql)


class BaseChunker(ABC):
    def __init__(self, chunk_size):
        self.chunk_size = chunk_size
        self.value = None
        self.high_watermark = None

    def initialize(self, initial_value, high_watermark):
        self.value = initial_value
        self.high_watermark = high_watermark

    @property
    @abstractmethod
    def curr_lower_bound(self):
        pass

    @property
    def is_complete(self):
        return self.curr_lower_bound >= self.high_watermark

    @property
    @abstractmethod
    def curr_upper_bound(self):
        pass

    @property
    @abstractmethod
    def batch_id(self):
        pass

    @abstractmethod
    def increment(self):
        pass


class DatetimeChunker(BaseChunker):
    def __init__(self, chunk_size=timedelta(days=30)):
        super().__init__(chunk_size=chunk_size)

    def initialize(self, initial_value, high_watermark):
        self.value = parse(initial_value)
        self.high_watermark = high_watermark
        print(f"chunker initialized with value {self.value}")

    @property
    def curr_lower_bound(self) -> str:
        return self.dt_to_str_sql(self.value)

    @property
    def curr_upper_bound(self) -> str:
        return self.dt_to_str_sql(self.value + self.chunk_size)

    @property
    def batch_id(self):
        return self.dt_to_str_id(self.value)

    def increment(self):
        self.value += self.chunk_size

    def dt_to_str_sql(self, val) -> str:
        out_val = val.isoformat()  # type: str
        if out_val[-3:] == "000":
            return out_val[0:-3]
        else:
            return out_val

    def dt_to_str_id(self, val):
        out_val = self.dt_to_str_sql(val)
        return out_val.replace("-", "").replace(":", "")


class IntegerChunker(BaseChunker):
    def __init__(self, chunk_size=10000000):
        super().__init__(chunk_size=chunk_size)

    @property
    def curr_lower_bound(self):
        return self.value

    @property
    def curr_upper_bound(self):
        return self.value + self.chunk_size

    @staticmethod
    def least(*args):
        val = None
        for v in args:
            if not val:
                val = int(v)
            elif int(v) < val:
                val = v
        return val

    @property
    def batch_id(self):
        upper_bound = self.least(self.high_watermark, self.curr_upper_bound)
        return f"{self.curr_lower_bound}_{upper_bound}"

    def increment(self):
        self.value += self.chunk_size


class MsSqlTableToS3CsvChunkOperator(BaseTableAcquisitionOperator, MsSqlToS3Operator):
    template_fields = ["key"]

    def __init__(
        self,
        bucket,
        key,
        s3_conn_id=conn_ids.AWS.tfg_default,
        s3_replace=True,
        batch_size=100000,
        initial_load_value: Union[str, int] = "1900-01-01 00:00:00.000",
        chunker: BaseChunker = DatetimeChunker(),
        **kwargs,
    ):
        self._mssql_hook = None
        self.chunker = chunker
        if "*" not in key:
            raise Exception(
                "key must have '*' character, which will be replaced with batch id"
            )
        super().__init__(
            sql="",
            bucket=bucket,
            key=key,
            s3_conn_id=s3_conn_id,
            s3_replace=s3_replace,
            batch_size=batch_size,
            initial_load_value=initial_load_value,
            **kwargs,
        )

    def watermark_execute(self, context=None):
        key_mask = self.key
        self.chunker.initialize(
            initial_value=self.low_watermark, high_watermark=self.new_high_watermark
        )
        while not self.chunker.is_complete:
            print(self.chunker.curr_lower_bound)
            self.key = key_mask.replace("*", self.chunker.batch_id)
            self.sql = self.build_query(
                lower_bound=self.chunker.curr_lower_bound,
                upper_bound=self.chunker.curr_upper_bound,
                strict_inequality=self.strict_inequality,
            )
            MsSqlToS3Operator.execute(self, context)
            self.chunker.increment()
            if self.chunker.curr_upper_bound < self.new_high_watermark:
                self.update_progress(self.chunker.curr_upper_bound)
        else:
            print("all caught up.")


class MsSqlBcpTableToS3Operator(BaseTableAcquisitionOperator):
    PASSWORD_ENV_VAR = "BCP_PASS"

    def __init__(
        self,
        bucket,
        key,
        record_delimiter="0x02",
        field_delimiter="0x01",
        chunker: Union[BaseChunker, IntegerChunker] = None,
        *args,
        **kwargs,
    ):
        self._mssql_hook = None
        self._bash_hook = None
        self.bucket = bucket
        self.key = key
        self.record_delimiter = record_delimiter
        self.field_delimiter = field_delimiter
        self.chunker = chunker
        super().__init__(*args, **kwargs)
        if self.chunker and "*" not in key:
            raise Exception(
                "key must have '*' character, which will be replaced with batch id"
            )

    def get_bash_command(self, sql):
        bash_command = build_bcp_to_s3_bash_command(
            login=self.mssql_hook.conn.login,
            server=self.mssql_hook.conn.host,
            query=sql,
            bucket=self.bucket,
            key=self.key,
            record_delimiter=self.record_delimiter,
            field_delimiter=self.field_delimiter,
            password=f'"${self.PASSWORD_ENV_VAR}"',
        )
        return bash_command

    @property
    def bash_hook(self):
        if not self._bash_hook:
            self._bash_hook = BashHook()
        return self._bash_hook

    @property
    def env(self):
        env = os.environ.copy()
        env.update({self.PASSWORD_ENV_VAR: self.mssql_hook.conn.password})
        return env

    def single_batch_execute(self):
        sql = self.build_query(
            lower_bound=self.low_watermark, strict_inequality=self.strict_inequality
        )
        bash_command = self.get_bash_command(sql=sql)
        self.bash_hook.run_command(
            bash_command=bash_command,
            env=self.env,
        )

    def chunk_execute(self):
        key_mask = self.key
        self.chunker.initialize(
            initial_value=self.low_watermark, high_watermark=self.new_high_watermark
        )
        while not self.chunker.is_complete:
            print(self.chunker.curr_lower_bound)
            self.key = key_mask.replace("*", self.chunker.batch_id)
            sql = self.build_query(
                lower_bound=self.chunker.curr_lower_bound,
                upper_bound=self.chunker.curr_upper_bound,
                strict_inequality=self.strict_inequality,
            )
            bash_command = self.get_bash_command(sql=sql)
            self.bash_hook.run_command(
                bash_command=bash_command,
                env=self.env,
            )
            self.chunker.increment()
            if self.chunker.curr_upper_bound < self.new_high_watermark:
                self.update_progress(self.chunker.curr_upper_bound)
        else:
            print("all caught up.")

    def watermark_execute(self, context=None):
        if self.chunker:
            self.chunk_execute()
        else:
            self.single_batch_execute()

    def dry_run(self):
        sql = self.build_query(
            lower_bound=self.low_watermark, strict_inequality=self.strict_inequality
        )
        bash_command = self.get_bash_command(sql=sql)
        print(bash_command)

    def on_kill(self):
        self.bash_hook.send_sigterm()
