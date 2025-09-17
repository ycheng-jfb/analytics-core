import inspect
from typing import List, Optional, Type

from include.airflow.operators.mssql_acquisition import (
    BaseHighWatermarkQuery,
    BaseSourceLookback,
    HighWatermarkMaxVarcharSS,
)

from include.utils.functions import get_default_args
from include.utils.snowflake import Column
from include.utils.string import unindent_auto


class BaseTargetLookback:
    def __call__(self, expression) -> str:
        raise NotImplementedError


class TargetLookbackDatepart(BaseTargetLookback):
    def __init__(self, datepart, value):
        self.datepart = datepart
        self.value = value

    def __call__(self, expression) -> str:
        return f"dateadd({self.datepart}, {self.value}, max({expression}))"

    def __repr__(self):
        return f"TargetLookbackDatepart({self.datepart.__repr__()}, {self.value.__repr__()})"


class ColumnList(list):
    @property
    def has_uniqueness(self):
        for col in self:
            if col.uniqueness is True:
                return True
        return False

    @property
    def has_delta_column(self):
        for col in self:
            if col.delta_column is not False:
                return True
        return False


class TableConfig:
    UTCNOW_TEMPLATE = '{{ macros.tfgdt.utcnow_nodash() }}'

    def __init__(
        self,
        database: str,
        schema: str,
        table: str,
        target_database: str,
        target_schema: str,
        column_list: List[Column],
        warehouse: str = 'DA_WH_ETL_LIGHT',
        source_column_mapping: Optional[dict] = None,
        watermark_column: Optional[str] = None,
        initial_load_value: Optional[str] = '1900-01-01',
        use_select_star: bool = False,
        high_watermark_cls: Type[BaseHighWatermarkQuery] = HighWatermarkMaxVarcharSS,
        strict_inequality=False,
        source_lookback_func: Optional[BaseSourceLookback] = None,
        target_lookback_func: Optional[BaseTargetLookback] = None,
        fetch_batch_size: int = 50000,
        split_files: bool = False,
        schema_version_prefix: str = 'v1',
    ):
        """
        :param database: source database
        :param schema: source schema
        :param table: source table
        :param target_database: target database
        :param target_schema: target schema
        :param column_list: List of Column objects.  The names given here are for the target.
        :param source_column_mapping: optional dictionary to rename target column names when building
        source pull query
        :param watermark_column: column used for incremental load watermarking
        :param initial_load_value: on first run, what will be lower bound
        :param use_select_star: if True, source pull query will be select *. Otherwise, select list
        is built explicitly.
        :param high_watermark_cls: subclass of BaseHighWatermarkQuery that takes watermark_col
            (e.g. 'datetime_added') and returns e.g. ``max(datetime_added)`` or
            ``max(convert(varchar, datetime_added, 2))``
        :param stage_name: in copy command, what stage do we use?
        :param strict_inequality: controls whether where clause is ``>`` or ``>=``.
        :param source_lookback_func: use if you wand to subtract time from low watermark
        :param target_lookback_func: this is used to delete rows from target in pre-merge command
        :param fetch_batch_size: when pulling data we do cur.fetch_many. this controls rows per fetch.
        :param split_files: if True, files will be split after 10,000,000 rows to reduce local storage
        :param schema_version_prefix: because we are writing to csv, if schema changes, we want to write to
            a different directory.
        """
        self.database = database
        self.schema = schema
        self.table = table
        self.target_database = target_database
        self.target_schema = target_schema
        self.column_list = column_list
        self.warehouse = warehouse
        self.source_column_mapping = source_column_mapping
        self.watermark_column = watermark_column
        self.initial_load_value = initial_load_value
        self.use_select_star = use_select_star
        self.high_watermark_cls = high_watermark_cls
        self.strict_inequality = strict_inequality
        self.target_lookback_func = target_lookback_func
        self.source_lookback_func = source_lookback_func
        self.fetch_batch_size = fetch_batch_size
        self.mssql_conn_id = 'mssql_edw01_app_airflow'
        self.split_files = split_files
        self.validate_column_list()
        self.validate_params()
        self.schema_version_prefix = schema_version_prefix

    def __repr__(self):
        default_args = get_default_args(self.__init__)
        cmd = "TableConfig(\n"
        signature = inspect.signature(self.__init__)

        param_names = [k for k, v in signature.parameters.items()]
        sort_map = {name: idx for idx, name in enumerate(signature.parameters)}
        sort_map['column_list'] = 10000

        for p in sorted(param_names, key=lambda x: sort_map.get(x)):
            actual_attr = getattr(self, p)
            if p not in default_args or actual_attr != default_args[p]:
                cmd += f"    {p}={actual_attr.__repr__()},\n"
        cmd += ")"
        return cmd

    @property
    def full_table_name(self):
        return f"{self.database}.{self.schema}.{self.table}".lower()

    @property
    def filename(self):
        file_part = '_*' if self.split_files else ''
        suffix = f"_{self.UTCNOW_TEMPLATE}" if self.watermark_column else ''
        return f"{self.full_table_name}{suffix}{file_part}.csv.gz".lower()

    @property
    def target_table_name(self):
        return f"{self.table}".lower()

    @property
    def full_target_table_name(self):
        return f"{self.target_database}.{self.target_schema}.{self.table}".lower()

    @property
    def source_column_name_list(self):
        mapping = self.source_column_mapping or {}
        if self.use_select_star:
            return ['*']
        else:
            return [self.rename_metas(mapping.get(x.name, x.name)) for x in self.column_list]

    @staticmethod
    def rename_metas(column_name):
        if column_name[0:6] == '__meta':
            return column_name[2:]
        else:
            return column_name

    def validate_column_list(self):
        cl = ColumnList(self.column_list)
        if self.watermark_column:
            if not cl.has_uniqueness:
                raise Exception("Column list must have uniqueness cols if using watermark column")
            if not cl.has_delta_column:
                raise Exception("Column list must have delta col if using watermark column")

    @property
    def pre_merge_command(self):
        if not self.target_lookback_func:
            return None

        inequality = '>' if self.strict_inequality else '>='
        cmd = f"""
            DELETE FROM {self.full_target_table_name}
            WHERE {self.watermark_column} {inequality} (
                SELECT {self.target_lookback_func(self.watermark_column)}
                FROM {self.full_target_table_name}
            );
            """
        return unindent_auto(cmd)

    def validate_params(self):
        if self.use_select_star and self.source_column_mapping:
            raise Exception("source_column_mapping is not used when use_select_star=True")
