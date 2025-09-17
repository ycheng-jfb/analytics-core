import logging
import os
import re
from datetime import datetime
from functools import cached_property
from io import StringIO
from typing import Any, List, Optional

import pytz
from include.utils.functions import get_default_args
from include.utils.string import unindent_auto
from snowflake.connector import DictCursor, SnowflakeConnection
from snowflake.connector.util_text import split_statements as _split_statements


def get_result_column_names(cur):
    if cur.description is None:
        return None
    return tuple(col[0] for col in cur.description)


def split_statements(cmd):
    for x in list(_split_statements(StringIO(cmd), remove_comments=True)):
        next_val = x[0].rstrip(';')
        if len(next_val) > 0:
            yield next_val


def generate_query_tag_cmd(dag_id, task_id):
    query_tag_sql = f"ALTER SESSION SET QUERY_TAG='{dag_id},{task_id}';"
    return query_tag_sql


def set_database(database):
    if database is not None:
        set_db_query = f"Use {database};"
        return set_db_query
    else:
        return None


def set_query_tag(hook_obj, dag_id, task_id):
    query_tag = generate_query_tag_cmd(dag_id, task_id)
    print(query_tag)
    with hook_obj.get_conn() as cnx:
        cur = cnx.cursor(cursor_class=DictCursor)
        cur.execute(query_tag)


def get_snowflake_warehouse(context):
    if 'production' in os.environ.get('ENVIRONMENT_STAGE', '').lower():
        execution_time = context['ti'].execution_date.astimezone(pytz.timezone('US/Pacific'))
        if execution_time.hour in (1, 17):
            warehouse = 'DA_WH_EDW'
        else:
            warehouse = 'DA_WH_EDW'
    else:
        warehouse = 'DA_WH_EDW'
    return warehouse


class ForeignKey:
    def __init__(self, table, database=None, schema=None, field=None):
        self.database = database
        self.schema = schema
        self.table = table
        self.field = field

    def __repr__(self):
        attrs = ['database', 'schema', 'field']
        additional_params = [f"{a}={getattr(self, a)}" for a in attrs if getattr(self, a)]
        return f"ForeignKey('{self.table}', {', '.join(additional_params)})"

    def __eq__(self, other) -> bool:
        if not isinstance(other, ForeignKey):
            return NotImplemented
        return (self.database, self.schema, self.table, self.field) == (
            other.database,
            other.schema,
            other.table,
            other.field,
        )


class Column:
    def __init__(
        self,
        name: str,
        type: str,
        uniqueness: bool = False,
        key: bool = False,
        keep_original: bool = False,
        type_1: bool = False,
        type_2: bool = False,
        delta_column: Any = False,
        source_name: str = None,
        foreign_key: ForeignKey = None,
        default_value: Any = None,
    ):
        self.name = name
        self.type = type
        self.uniqueness = uniqueness
        self.key = key
        self.keep_original = keep_original
        self.type_1 = type_1
        self.type_2 = type_2
        self.delta_column = delta_column
        self._source_name = source_name
        self.foreign_key = foreign_key
        self._default_value = default_value

    def __repr__(self):
        default_args = get_default_args(self.__init__)
        default_args['source_name'] = self.name
        optional_params = ''
        attrs = (
            'uniqueness',
            'key',
            'type_1',
            'type_2',
            'delta_column',
            'source_name',
            'foreign_key',
        )
        for attr in attrs:
            value = getattr(self, attr)
            default_value = default_args.get(attr)
            is_diff_class = default_value.__class__ != value.__class__
            if value != default_args.get(attr) or is_diff_class:
                value = f"'{value}'" if isinstance(value, str) else value
                optional_params += f", {attr}={value}"
        return f"Column('{self.name}', '{self.type}'{optional_params})"

    @property
    def default_value(self):
        """If we need a defalut value for a column"""
        return f" DEFAULT {self._default_value}" if self._default_value else ""

    @property
    def source_name(self):
        """
        The attribute ``name`` represents the snowflake column name as it will be defined in the table.
        Commonly the column name in the source system is the same as we want it to be in snowflake.
        In these cases we may use the ``name`` attribute to generate a source system query, for example.
        But when source system column name is different, we can use ``source_name`` to override.
        When ``source_name`` parameter is not given at init, ``source_name`` will have a default of ``name``,
        so that it can always be used whether an override is provided or not.
        """
        return self._source_name or self.name

    @property
    def type_base_name(self):
        m = re.match(r'([a-zA-Z_]+).*?', self.type)
        return m.group(1)

    @property
    def column_size(self):
        m = re.match(r'[a-zA-Z_]+\((.*?)\)', self.type)
        if m:
            match = m.group(1)
            if ',' in match:
                return int(match.split(',')[0].strip())
            else:
                return int(match.strip())

    @property
    def decimal_digits(self):
        m = re.match(r'[a-zA-Z_]+\((.*?)\)', self.type)
        if m:
            match = m.group(1)
            if ',' in match:
                return int(match.split(',')[1].strip())


class BaseCopyConfig:
    def __init__(self, custom_select=None):
        self.custom_select = custom_select

    SPECIFY_COLUMNS = True

    @property
    def format_params_list(self) -> List[str]:
        raise NotImplementedError

    @property
    def copy_params_list(self) -> List[str]:
        raise NotImplementedError

    def dml_copy_into_table(self, table, files_path, pattern=None, column_names=None) -> str:
        format_params_str = ',\n            '.join(self.format_params_list)
        copy_params_str = ',\n        '.join(self.copy_params_list)
        if self.custom_select:
            from_line = f"FROM ({self.custom_select})"
        else:
            from_line = f"FROM '{files_path}'"
        if pattern:
            from_line += f"\n        PATTERN = '{pattern}'"
        insert_names_str = ', '.join(column_names)
        col_names = f"({insert_names_str})" if self.SPECIFY_COLUMNS else ''
        cmd = rf"""
        COPY INTO {table} {col_names}
        {from_line}
        FILE_FORMAT=(
            {format_params_str}
        )
        {copy_params_str}
        ;
        """
        return unindent_auto(cmd)


class CopyConfigCsv(BaseCopyConfig):
    SPECIFY_COLUMNS = True
    TRANS_DELIM = str.maketrans({'\t': r'\t', '\n': r'\n', '\r': r'\r'})

    def __init__(
        self,
        field_delimiter: str = ',',
        force_copy=False,
        header_rows=0,
        record_delimiter: str = '\n',
        skip_pct: int = 1,
        timestamp_format=None,
        date_format=None,
        null_if="''",
        custom_select=None,
    ):
        self.field_delimiter = field_delimiter.translate(self.TRANS_DELIM)
        self.record_delimiter = record_delimiter.translate(self.TRANS_DELIM)
        self.timestamp_format = timestamp_format
        self.header_rows = header_rows
        self.skip_pct = skip_pct
        self.force_copy = force_copy
        self.date_format = date_format
        self.null_if = null_if
        super().__init__(custom_select=custom_select)

    @property
    def format_params_list(self) -> List[str]:
        params = {
            "TYPE": "CSV",
            "FIELD_DELIMITER": f"'{self.field_delimiter}'",
            "RECORD_DELIMITER": f"'{self.record_delimiter}'",
            "FIELD_OPTIONALLY_ENCLOSED_BY": "'\"'",
            "SKIP_HEADER": f"{self.header_rows}",
            "ESCAPE_UNENCLOSED_FIELD": "NONE",
        }
        if self.null_if:
            params["NULL_IF"] = f"({self.null_if})"
        if self.timestamp_format:
            params["TIMESTAMP_FORMAT"] = f"'{self.timestamp_format}'"
        if self.date_format:
            params["DATE_FORMAT"] = f"'{self.date_format}'"

        ret = [f"{k} = {v}" for k, v in params.items()]

        return ret

    @property
    def copy_params_list(self) -> List[str]:
        params = {}
        if self.skip_pct:
            params.update({"ON_ERROR": f"'SKIP_FILE_{self.skip_pct}%'"})
        if self.force_copy:
            params["FORCE"] = "TRUE"
        ret = [f"{k} = {v}" for k, v in params.items()]

        return ret


class CopyConfigJson(BaseCopyConfig):
    SPECIFY_COLUMNS = False

    def __init__(
        self,
        force_copy=False,
        match_by_column_name=True,
        skip_pct: int = 1,
        timestamp_format=None,
        strip_null_values=True,
        custom_select=None,
    ):
        self.timestamp_format = timestamp_format
        self.skip_pct = skip_pct
        self.force_copy = force_copy
        self.match_by_column_name = match_by_column_name
        self.strip_null_values = strip_null_values
        super().__init__(custom_select=custom_select)

    @property
    def format_params_list(self) -> List[str]:
        params = {
            "TYPE": "JSON",
        }
        if self.timestamp_format:
            params["TIMESTAMP_FORMAT"] = f"'{self.timestamp_format}'"
        if self.strip_null_values is True:
            params["STRIP_NULL_VALUES"] = f"{self.strip_null_values}"
        ret = [f"{k} = {v}" for k, v in params.items()]
        return ret

    @property
    def copy_params_list(self) -> List[str]:
        params = {}
        if self.skip_pct:
            params.update({"ON_ERROR": f"'SKIP_FILE_{self.skip_pct}%'"})
        if self.force_copy:
            params["FORCE"] = "TRUE"
        if self.match_by_column_name is True:
            params["MATCH_BY_COLUMN_NAME"] = "CASE_INSENSITIVE"
        ret = [f"{k} = {v}" for k, v in params.items()]

        return ret


class BaseLoadJob:
    def __init__(
        self,
        cnx: SnowflakeConnection,
        database: str,
        schema: str,
        table: str,
        files_path: str,
        column_list: List[Column],
        copy_config: BaseCopyConfig,
        warehouse: str = None,
        stg_column_naming: str = 'target',
        pattern: str = None,
        fail_on_partially_loaded=False,
        fail_on_load_failed=True,
        staging_database=None,
        staging_schema=None,
        archive_database=None,
        view_database=None,
        cluster_by: str = None,
    ):
        self.table = table
        if '.' in table:
            raise ValueError("table cannot have a '.'")
        self.schema = schema
        self.database = database
        self.staging_database = staging_database
        self.staging_schema = staging_schema
        self.view_database = view_database
        self.warehouse = warehouse
        self.cnx = cnx
        self.column_list = column_list
        self.column_names = [x.name for x in column_list]
        if stg_column_naming.lower() not in ['source', 'target']:
            raise ValueError("stg_column_naming must be one of ['source', 'target']")
        self.stg_column_naming = stg_column_naming
        self.stg_cols_as_source = self.stg_column_naming.lower() == 'source'
        self.stg_column_names = (
            [x.source_name for x in column_list] if self.stg_cols_as_source else self.column_names
        )
        self.files_path = files_path
        self.pattern = pattern
        self.copy_config = copy_config
        self.start_datetime = datetime.now()
        self.end_datetime = None
        self.fail_on_load_failed = fail_on_load_failed
        self.fail_on_partially_loaded = fail_on_partially_loaded
        self.has_partial_load = False
        self.has_failed_load = False
        self.cluster_by = cluster_by
        self.archive_database = archive_database
        self._full_cmd = ''

    @property
    def fk_cols(self):
        fk_cols = []
        for col in self.column_list:
            if col.foreign_key:
                # If database, schema or field were not provided then use the same val as the column
                col.foreign_key.database = col.foreign_key.database or self.database
                col.foreign_key.schema = col.foreign_key.schema or self.schema
                col.foreign_key.field = col.foreign_key.field or col.name
                fk_cols.append(col)
        return fk_cols

    @property
    def pk_col_names(self):
        pk_col_names = [x.name for x in self.column_list if x.uniqueness is True]
        if not pk_col_names:
            pk_col_names = [x.name for x in self.column_list]
        return pk_col_names

    @property
    def pk_stg_col_names(self):
        if not self.stg_cols_as_source:
            return self.pk_col_names
        pk_stg_col_names = [x.source_name for x in self.column_list if x.uniqueness is True]
        if not pk_stg_col_names:
            pk_stg_col_names = [x.source_name for x in self.column_list]
        return pk_stg_col_names

    def print(self, val):
        self._full_cmd += val + '\n'
        print(val)

    @property
    def base_table(self):
        return self.database + '.' + self.schema + '.' + self.table

    @staticmethod
    def unquote_and_lower(val: str) -> str:
        return val.replace('"', '').lower()

    @property
    def stg_table(self):
        stg_database = self.staging_database or self.database
        stg_schema = self.staging_schema or self.schema
        return stg_database + '.' + stg_schema + '.' + self.unquote_and_lower(self.table) + '_stg'

    @property
    def archive_table(self):
        if self.archive_database:
            return self.archive_database + '.' + self.schema + '.' + self.table

    @property
    def full_cmd(self):
        return self._full_cmd[:-1]

    @property
    def base_table_meta_cols(self) -> List[Column]:
        base_table_meta_cols: List[Column] = []
        return base_table_meta_cols

    @property
    def stg_table_meta_cols(self) -> List[Column]:
        stg_table_meta_cols: List[Column] = []
        return stg_table_meta_cols

    @property
    def insert_names_str(self) -> str:
        insert_names_str = ', '.join(self.column_names)
        return insert_names_str

    @property
    def insert_stg_names_str(self) -> str:
        insert_stg_names_str = ', '.join(self.stg_column_names)
        return insert_stg_names_str

    @property
    def pk_names_str(self) -> str:
        pk_names_str = ', '.join(self.pk_col_names)
        return pk_names_str

    @property
    def pk_stg_names_str(self) -> str:
        pk_stg_names_str = ', '.join(self.pk_stg_col_names)
        return pk_stg_names_str

    def run_cmd(self, cmd: Optional[str], dryrun=False):
        if cmd is None:
            return
        elif dryrun:
            self.print(cmd)
            return
        else:
            logging.info(cmd)
            with self.cnx.cursor(cursor_class=DictCursor) as cur:
                cur.execute(cmd)
                result_list = cur.fetchall()
                logging.info(result_list)
                messages = cur.messages
                logging.info(messages)

    def run_copy_cmd(self, cmd: str, dryrun: bool):
        if dryrun:
            self.print(cmd)
            return
        else:
            logging.info(cmd)
            with self.cnx.cursor(cursor_class=DictCursor) as cur:
                cur.execute(cmd)
                result_list = cur.fetchall()
                logging.info('\n' + '\n'.join(map(str, result_list)))
                for row in result_list:
                    if row['status'] == 'Copy executed with 0 files processed.':
                        logging.info(row['status'])
                    else:
                        filename = row['file']
                        status = row['status']
                        if status == 'PARTIALLY_LOADED':
                            self.has_partial_load = True
                        if 'LOADED' not in status:
                            self.has_failed_load = True
                            logging.error(f"file {filename} failed with status {status}")

    @staticmethod
    def _is_timestamp_col(data_type) -> bool:
        return data_type.lower()[0:4] in ('date', 'time')

    @staticmethod
    def _get_col_ddls(column_list) -> str:
        return f',\n{" " * 12}'.join([f"{x.name} {x.type}" for x in column_list])

    @staticmethod
    def _get_src_col_ddls(column_list) -> str:
        return f',\n{" " * 12}'.join([f"{x.source_name} {x.type}" for x in column_list])

    @property
    def source_col_ddls(self):
        return self._get_col_ddls(self.column_list)

    @property
    def stg_col_ddls(self):
        if self.stg_cols_as_source:
            return self._get_src_col_ddls(self.column_list)
        else:
            return self._get_col_ddls(self.column_list)

    @staticmethod
    def _unindent(cmd, char_count=8) -> str:
        ret_val = re.sub(rf"^[ ]{{{char_count}}}", '', cmd, flags=re.MULTILINE)  # type: str
        return ret_val

    @property
    def primary_key_col_ddl(self):
        return f",\n    PRIMARY KEY ({self.pk_names_str})"

    @property
    def foreign_key_ref_ddl(self):
        fk_str = ""
        for col in self.fk_cols:
            fk = col.foreign_key
            fk_str += (
                f",\n    FOREIGN KEY ({col.name}) "
                f"references {fk.database}.{fk.schema}.{fk.table} ({fk.field})"
            )
        return fk_str

    @property
    def base_table_meta_col_ddls(self) -> str:
        base_table_meta_col_ddls = self._get_col_ddls(self.base_table_meta_cols)
        if base_table_meta_col_ddls:
            base_table_meta_col_ddls = ',\n            ' + base_table_meta_col_ddls
        return base_table_meta_col_ddls or ''

    @property
    def ddl_base_table(self) -> str:
        cluster_by = f"\nCLUSTER BY ({self.cluster_by})\n" if self.cluster_by else ''
        surr_key_name = (
            hasattr(self, 'surrogate_key_name') and getattr(self, 'surrogate_key_name') or ''
        )
        surr_key_ddl = f"{surr_key_name} INT IDENTITY,\n    " if surr_key_name else ''
        cmd = f"""
        CREATE TABLE IF NOT EXISTS {self.base_table} (
            {surr_key_ddl}{self.source_col_ddls}{self.base_table_meta_col_ddls}{self.primary_key_col_ddl}{self.foreign_key_ref_ddl}
        ){cluster_by};
        """
        return self._unindent(cmd)

    @property
    def stg_table_meta_col_ddls(self):
        stg_table_meta_col_ddls = self._get_col_ddls(self.stg_table_meta_cols)
        if stg_table_meta_col_ddls:
            stg_table_meta_col_ddls = ',\n\t\t\t' + stg_table_meta_col_ddls
        return stg_table_meta_col_ddls or ''

    @property
    def ddl_stg_table(self) -> str:
        create_command = 'CREATE TABLE IF NOT EXISTS'
        cmd = f"""
        {create_command} {self.stg_table} (
            {self.stg_col_ddls}{self.stg_table_meta_col_ddls}
        );
        """
        return self._unindent(cmd)

    @property
    def ddl_archive_table(self) -> Optional[str]:
        if not self.archive_table:
            return None
        create_command = 'CREATE TABLE IF NOT EXISTS'
        cmd = f"""
        {create_command} {self.archive_table} (
            {self.source_col_ddls}{self.stg_table_meta_col_ddls}
        );
        """
        return self._unindent(cmd)

    @property
    def view_name(self):
        if not self.view_database:
            return None
        return f"{self.view_database}.{self.schema}.{self.table}"

    @property
    def ddl_view(self):
        if not self.view_name:
            return None
        meta_list = ['meta_create_datetime', 'meta_update_datetime']
        select_list = ',\n    '.join([x.name for x in self.column_list] + meta_list)
        select = f"SELECT\n    {select_list}\n"
        select += f'FROM {self.base_table}'
        cmd = f"CREATE VIEW IF NOT EXISTS {self.view_name} AS\n{select};\n"
        return cmd

    @property
    def dml_clear_stage(self) -> str:
        cmd = f"DELETE FROM {self.stg_table};\n"
        return cmd

    @property
    def dml_truncate_base_table(self) -> str:
        cmd = f"DELETE FROM {self.base_table};"
        return cmd

    @property
    def dml_copy_into_stg(self) -> str:
        return self.copy_config.dml_copy_into_table(
            table=self.stg_table,
            files_path=self.files_path,
            pattern=self.pattern,
            column_names=self.column_names,
        )

    def copy_into_archive_table(self) -> str:
        return self.copy_config.dml_copy_into_table(
            table=self.archive_table,
            files_path=self.files_path,
            pattern=self.pattern,
            column_names=self.column_names,
        )

    def raise_for_status(self):
        failure = (self.fail_on_partially_loaded and self.has_partial_load) or (
            self.fail_on_load_failed and self.has_failed_load
        )
        if failure:
            raise Exception('Load failed')

    def execute(self, *, initial_load: bool = False, dryrun: bool = False):
        raise NotImplementedError

    @property
    def dml_begin(self):
        return 'BEGIN;\n'

    @property
    def dml_commit(self):
        return 'COMMIT;\n'

    def run_ddls(self, dryrun=True):
        self.run_cmd(self.ddl_stg_table, dryrun=dryrun)
        self.run_cmd(self.ddl_base_table, dryrun=dryrun)
        self.run_cmd(self.ddl_archive_table, dryrun=dryrun)
        self.run_cmd(self.ddl_view, dryrun=dryrun)


class LoadJobInsertUpdate(BaseLoadJob):
    def __init__(self, task_id, dag_id, pre_merge_command: str = None, **kwargs):
        super().__init__(**kwargs)
        self.task_id = task_id
        self.dag_id = dag_id
        self.pre_merge_command = pre_merge_command if pre_merge_command else None

    @property
    def base_table_meta_cols(self) -> List[Column]:
        base_table_meta_cols = [
            Column('meta_row_hash', 'INT'),
            Column('meta_create_datetime', 'TIMESTAMP_LTZ(3)'),
            Column('meta_update_datetime', 'TIMESTAMP_LTZ(3)'),
        ]
        return base_table_meta_cols

    @property
    def merge_update_names_str(self) -> str:
        if self.stg_cols_as_source:
            update_names_str = f',\n{" " * 16}'.join(
                [f"t.{x.name} = s.{x.source_name}" for x in self.column_list]
            )
        else:
            update_names_str = f',\n{" " * 16}'.join([f"t.{x} = s.{x}" for x in self.column_names])
        return update_names_str

    @cached_property
    def delta_columns(self):
        delta_col_list = [x for x in self.column_list if x.delta_column is not False]
        return sorted(delta_col_list, key=lambda x: x.delta_column)  # type: ignore

    @property
    def stg_delta_col_names(self):
        return [x.source_name if self.stg_cols_as_source else x.name for x in self.delta_columns]

    @property
    def base_delta_col_names(self):
        return [x.name for x in self.delta_columns]

    def _merge_order_by(self, stg_table, alias=None) -> str:
        if not self.delta_columns:
            return ''
        full_alias = alias + '.' if alias else ''
        first_dc = self.delta_columns[0]
        null_repl = '1900-01-01' if self._is_timestamp_col(first_dc.type) else '0'
        delta_names = self.stg_delta_col_names if stg_table else self.base_delta_col_names
        sort_col_list = ', '.join(f"{full_alias}{x}" for x in delta_names)
        return f"coalesce({sort_col_list}, '{null_repl}')"

    @property
    def merge_rn_order_by(self):
        cmd = self._merge_order_by(stg_table=True)
        return cmd + ' DESC' if cmd else '( SELECT NULL )'

    @property
    def merge_update_condition(self):
        source_coalesce = self._merge_order_by(stg_table=True, alias='s')
        if not source_coalesce:
            return ''
        else:
            target_coalesce = self._merge_order_by(stg_table=False, alias='t')
            return f"\n    AND {source_coalesce} > {target_coalesce}"

    @property
    def dml_merge(self) -> str:
        uniqueness_join = '\n    AND '.join(
            [f"equal_null(t.{x}, s.{y})" for x, y in zip(self.pk_col_names, self.pk_stg_col_names)]
        )
        cmd = f"""
            MERGE INTO {self.base_table} t
            USING (
                SELECT
                    a.*
                FROM (
                    SELECT
                        *,
                        hash(*) AS meta_row_hash,
                        current_timestamp AS meta_create_datetime,
                        current_timestamp AS meta_update_datetime,
                        row_number() OVER ( PARTITION BY {self.pk_stg_names_str} ORDER BY {self.merge_rn_order_by} ) AS rn
                    FROM {self.stg_table}
                 ) a
                WHERE a.rn = 1
            ) s ON {uniqueness_join}
            WHEN NOT MATCHED THEN INSERT (
                {self.insert_names_str}, meta_row_hash, meta_create_datetime, meta_update_datetime
            )
            VALUES (
                {self.insert_stg_names_str}, meta_row_hash, meta_create_datetime, meta_update_datetime
            )
            WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash{self.merge_update_condition} THEN UPDATE
            SET {self.merge_update_names_str},
                t.meta_row_hash = s.meta_row_hash,
                t.meta_update_datetime = s.meta_update_datetime;
            """  # noqa: E501
        return self._unindent(cmd, 12)

    @property
    def dml_append_to_archive(self) -> Optional[str]:
        if not self.archive_table:
            return None
        cmd = f"""
            INSERT INTO {self.archive_table} ({self.insert_names_str})
            SELECT {self.insert_names_str}
            FROM {self.stg_table} s;
            """
        return self._unindent(cmd, 12)

    def execute(self, *, initial_load: bool = False, dryrun: bool = False):
        query_tag_cmd = generate_query_tag_cmd(dag_id=self.dag_id, task_id=self.task_id)
        self.run_cmd(cmd=query_tag_cmd, dryrun=dryrun)
        if initial_load:
            self.run_ddls(dryrun=dryrun)
        self.run_cmd(self.dml_begin, dryrun=dryrun)
        self.run_cmd(self.dml_clear_stage, dryrun=dryrun)
        self.run_copy_cmd(self.dml_copy_into_stg, dryrun=dryrun)
        self.run_cmd(self.pre_merge_command, dryrun=dryrun)
        self.run_cmd(self.dml_merge, dryrun=dryrun)
        self.run_cmd(self.dml_append_to_archive, dryrun=dryrun)
        self.run_cmd(self.dml_commit, dryrun=dryrun)
        self.raise_for_status()


class LoadJobInsertUpdateDelete(LoadJobInsertUpdate):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def ddl_view(self):
        if not self.view_name:
            return None
        meta_list = ['meta_create_datetime', 'meta_update_datetime']
        select_list = ',\n    '.join([x.name for x in self.column_list] + meta_list)
        select = f"SELECT\n    {select_list}\n"
        select += f'FROM {self.base_table}'
        cmd = (
            f"CREATE VIEW IF NOT EXISTS {self.view_name} AS\n{select}\nWHERE NOT meta_is_deleted;\n"
        )
        return cmd

    @property
    def base_table_meta_cols(self) -> List[Column]:
        base_table_meta_cols: List[Column] = [
            Column('meta_row_hash', 'INT'),
            Column('meta_create_datetime', 'TIMESTAMP_LTZ(3)'),
            Column('meta_update_datetime', 'TIMESTAMP_LTZ(3)'),
            Column('meta_is_deleted', 'BOOLEAN DEFAULT FALSE'),
        ]
        return base_table_meta_cols

    @property
    def dml_merge(self) -> str:
        uniqueness_join = '\n    AND '.join(
            [f"equal_null(t.{x}, s.{y})" for x, y in zip(self.pk_col_names, self.pk_stg_col_names)]
        )
        partial_update_cond = self.merge_update_condition
        update_cond = "t.meta_row_hash != s.meta_row_hash OR t.meta_is_deleted"
        if partial_update_cond:
            update_cond = f"({update_cond}){partial_update_cond}"
        cmd = f"""
            MERGE INTO {self.base_table} t
            USING (
                SELECT
                    a.*
                FROM (
                    SELECT
                        *,
                        hash(*) AS meta_row_hash,
                        current_timestamp AS meta_create_datetime,
                        current_timestamp AS meta_update_datetime,
                        row_number() OVER ( PARTITION BY {self.pk_names_str} ORDER BY {self.merge_rn_order_by} ) AS rn
                    FROM {self.stg_table}
                 ) a
                WHERE a.rn = 1
            ) s ON {uniqueness_join}
            WHEN NOT MATCHED THEN INSERT (
                {self.insert_names_str}, meta_row_hash, meta_create_datetime, meta_update_datetime
            )
            VALUES (
                {self.insert_names_str}, meta_row_hash, meta_create_datetime, meta_update_datetime
            )
            WHEN MATCHED AND (
                {update_cond}
                ) THEN UPDATE
            SET {self.merge_update_names_str},
                t.meta_row_hash = s.meta_row_hash,
                t.meta_update_datetime = s.meta_update_datetime,
                t.meta_is_deleted = FALSE
            """
        return self._unindent(cmd, 12)

    @property
    def dml_deletes(self) -> str:
        uniqueness_join = '\n    AND '.join(
            [f"equal_null(t.{x}, s.{x})" for x in self.pk_col_names]
        )
        cmd = f"""
            UPDATE {self.base_table} t
            SET
                t.meta_is_deleted = TRUE,
                t.meta_update_datetime = current_timestamp
            WHERE NOT EXISTS (
                SELECT 1
                FROM {self.stg_table} s
                WHERE {uniqueness_join}
            )
            AND (SELECT count(1) FROM {self.stg_table}) > 1;
            """
        return self._unindent(cmd, 12)

    def execute(self, *, initial_load: bool = False, dryrun: bool = False):
        query_tag_cmd = generate_query_tag_cmd(dag_id=self.dag_id, task_id=self.task_id)
        self.run_cmd(cmd=query_tag_cmd, dryrun=dryrun)
        if initial_load:
            self.run_ddls(dryrun=dryrun)
        self.run_cmd(self.dml_begin, dryrun=dryrun)
        self.run_cmd(self.dml_clear_stage, dryrun=dryrun)
        self.run_copy_cmd(self.dml_copy_into_stg, dryrun=dryrun)
        self.run_cmd(self.dml_merge, dryrun=dryrun)
        self.run_cmd(self.dml_deletes, dryrun=dryrun)
        self.run_cmd(self.dml_append_to_archive, dryrun=dryrun)
        self.run_cmd(self.dml_commit, dryrun=dryrun)
        self.raise_for_status()


class LoadJobTruncate(LoadJobInsertUpdate):
    def execute(self, *, initial_load: bool = False, dryrun: bool = False):
        query_tag_cmd = generate_query_tag_cmd(dag_id=self.dag_id, task_id=self.task_id)
        self.run_cmd(cmd=query_tag_cmd, dryrun=dryrun)
        if initial_load:
            self.run_ddls(dryrun=dryrun)
        self.run_cmd(self.dml_begin, dryrun=dryrun)
        self.run_cmd(self.dml_clear_stage, dryrun=dryrun)
        self.run_copy_cmd(self.dml_copy_into_stg, dryrun=dryrun)
        self.run_cmd(self.dml_truncate_base_table, dryrun=dryrun)
        self.run_cmd(self.dml_merge, dryrun=dryrun)
        self.run_cmd(self.dml_commit, dryrun=dryrun)
        self.raise_for_status()


class LoadJobScd(BaseLoadJob):
    initial_from_timestamp = "convert_timezone('UTC', to_timestamp_ltz(0))"
    initial_to_timestamp = "convert_timezone('UTC', to_timestamp_ltz(3e9))"

    def __init__(self, task_id, dag_id, min_type_1_updates: bool = True, **kwargs):
        super().__init__(**kwargs)
        self.min_type_1_updates = min_type_1_updates
        self.delta_table = '_delta'
        self.task_id = task_id
        self.dag_id = dag_id

    @property
    def type_1_col_names(self):
        type_1_col_names = [x.name for x in self.column_list if x.type_1 is True]
        if type_1_col_names:
            return type_1_col_names

    @property
    def type_2_col_names(self):
        type_2_col_names = [x.name for x in self.column_list if x.type_2 is True]
        if not type_2_col_names:
            raise ValueError('At least 1 type 2 column is required for SCD.')
        return type_2_col_names

    @property
    def type_2_timestamp_col(self):
        delta_cols = [x for x in self.column_list if x.delta_column is not False]
        num_cols = len(delta_cols)
        if num_cols > 1:
            raise ValueError('For SCD you may pass at most one delta column')
        elif num_cols == 1:
            col = delta_cols[0]
            if not self._is_timestamp_col(col.type):
                raise ValueError('For SCD, delta column must have data type TIMESTAMP_*')
            return col
        else:
            return Column('current_timestamp', 'TIMESTAMP_LTZ', delta_column=True)

    @property
    def surrogate_key_name(self):
        base_table_name = self.base_table.split('.').pop()
        surr_key_name = f"{base_table_name}_key"
        return surr_key_name

    @property
    def base_table_meta_cols(self) -> List[Column]:
        base_table_meta_cols = [
            Column('meta_type_1_hash', 'INT'),
            Column('meta_type_2_hash', 'INT'),
            Column('meta_from_datetime', self.type_2_timestamp_col.type),
            Column('meta_to_datetime', self.type_2_timestamp_col.type),
            Column('meta_is_current', 'BOOLEAN'),
            Column('meta_create_datetime', 'TIMESTAMP_LTZ'),
            Column('meta_update_datetime', 'TIMESTAMP_LTZ'),
        ]
        return base_table_meta_cols

    @property
    def pk_names_str(self) -> str:
        return ', '.join(self.pk_col_names)

    @property
    def primary_key_col_ddl(self):
        return f",\n    PRIMARY KEY ({self.pk_names_str}, meta_from_datetime)"

    @property
    def meta_type_1_hash(self):
        return f"hash({','.join(self.type_1_col_names) if self.type_1_col_names else 'NULL'})"

    @property
    def meta_type_2_hash(self):
        return f"hash({','.join(self.type_2_col_names)})"

    @property
    def ddl_delta_table(self):
        delta_meta_col_list = [
            ('meta_from_datetime', 'TIMESTAMP_LTZ'),
            ('meta_type_1_hash', 'INT'),
            ('meta_type_2_hash', 'INT'),
            ('rn', 'INT'),
            ('meta_record_status', 'VARCHAR(15)'),
        ]
        delta_meta_col_lines = [f"{x[0]} {x[1]}" for x in delta_meta_col_list]
        delta_meta_ddls = ',\n            '.join(delta_meta_col_lines)
        cmd = f"""
        CREATE TEMP TABLE {self.delta_table} (
            {self.source_col_ddls},
            {delta_meta_ddls}
        );
        """
        return self._unindent(cmd)

    @property
    def dml_load_delta(self):
        uniqueness_join = '\n    AND '.join(
            [f"equal_null(t.{x}, s.{x})" for x in self.pk_col_names]
        )

        cmd = f"""
        INSERT INTO {self.delta_table}
        SELECT s.*,
            CASE
            WHEN t.{self.surrogate_key_name} IS NULL THEN NULL -- null means new record
            WHEN s.meta_from_datetime < t.meta_from_datetime THEN 'ignore'
            WHEN s.meta_type_1_hash != t.meta_type_1_hash
                AND s.meta_type_2_hash != t.meta_type_2_hash THEN 'both'
            WHEN s.meta_type_1_hash != t.meta_type_1_hash THEN 'type 1'
            WHEN s.meta_type_2_hash != t.meta_type_2_hash THEN 'type 2'
            ELSE 'ignore' END :: VARCHAR(15) AS meta_record_status
        FROM (
            SELECT *,
                {self.type_2_timestamp_col.name} AS meta_from_datetime,
                {self.meta_type_1_hash} :: INT AS meta_type_1_hash,
                {self.meta_type_2_hash} :: INT AS meta_type_2_hash,
                row_number() OVER (PARTITION BY {self.pk_names_str} ORDER BY meta_from_datetime DESC) as rn
            FROM {self.stg_table} s
        ) s
        LEFT JOIN {self.base_table} t on {uniqueness_join}
            AND t.meta_is_current
        WHERE rn = 1
        ;
        """
        cmd = self._unindent(cmd)
        return cmd

    @property
    def dml_type_1_update(self):
        if self.type_1_col_names:
            uniqueness_join = '\n    AND '.join(
                [f"equal_null(t.{x}, s.{x})" for x in self.pk_col_names]
            )

            cnt = ',\n\t'
            type_1_update_list = [f"t.{x} = s.{x}" for x in self.type_1_col_names]
            cmd = f"""
            -- type 1 updates
            UPDATE {self.base_table} t
            SET
                {cnt.join(type_1_update_list)}
            FROM {self.delta_table} s
            WHERE {'t.meta_is_current' if self.min_type_1_updates else '1 = 1'}
                AND {uniqueness_join}
                AND s.meta_record_status IN ('both', 'type 1');
            """
            cmd = self._unindent(cmd)
            return cmd

    @property
    def dml_type_2_update(self):
        uniqueness_join = '\n    AND '.join(
            [f"equal_null(t.{x}, s.{x})" for x in self.pk_col_names]
        )

        cmd = f"""
        -- close off type 2
        UPDATE {self.base_table} t
        SET t.meta_to_datetime = s.meta_from_datetime,
            t.meta_is_current = FALSE,
            t.meta_update_datetime = current_timestamp
        FROM {self.delta_table} s
        WHERE t.meta_is_current
            AND {uniqueness_join}
            AND s.meta_record_status IN ('both', 'type 2');
        """
        cmd = self._unindent(cmd)
        return cmd

    @property
    def dml_inserts(self):
        cnt = ',\n\t'
        full_col_list = self.column_names + [x.name for x in self.base_table_meta_cols]
        full_col_select_list = ', '.join(full_col_list)
        cmd = f"""
        -- insert new records
        INSERT INTO {self.base_table} ({full_col_select_list})
        SELECT
            {cnt.join(self.column_names)},
            meta_type_1_hash,
            meta_type_2_hash,
            nvl2(
                s.meta_record_status,
                s.meta_from_datetime,
                {self.initial_from_timestamp} :: {self.type_2_timestamp_col.type}
            ) AS meta_from_datetime,
            {self.initial_to_timestamp} :: {self.type_2_timestamp_col.type} AS meta_to_datetime,
            TRUE AS meta_is_current,
            current_timestamp AS meta_create_datetime,
            current_timestamp AS meta_update_datetime
        FROM {self.delta_table} s
        WHERE nvl(s.meta_record_status, 'new') IN ('new', 'type 2', 'both');
        """
        cmd = self._unindent(cmd)
        return cmd

    def execute(self, *, initial_load: bool = False, dryrun: bool = False):
        query_tag_cmd = generate_query_tag_cmd(dag_id=self.dag_id, task_id=self.task_id)
        self.run_cmd(cmd=query_tag_cmd, dryrun=dryrun)
        if initial_load:
            self.run_ddls(dryrun=dryrun)
        self.run_cmd(self.ddl_delta_table, dryrun=dryrun)
        self.run_cmd(self.dml_begin, dryrun=dryrun)
        self.run_cmd(self.dml_clear_stage, dryrun=dryrun)
        self.run_copy_cmd(self.dml_copy_into_stg, dryrun=dryrun)
        self.run_cmd(self.dml_load_delta, dryrun=dryrun)
        self.run_cmd(self.dml_type_1_update, dryrun=dryrun)
        self.run_cmd(self.dml_type_2_update, dryrun=dryrun)
        self.run_cmd(self.dml_inserts, dryrun=dryrun)
        self.run_cmd(self.dml_commit, dryrun=dryrun)
        self.raise_for_status()


class LoadJobInsertIfNotExists(LoadJobInsertUpdate):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def dml_insert(self) -> str:
        cmd = f"""
                INSERT INTO {self.base_table}
                (
                    {self.insert_names_str}, meta_row_hash, meta_create_datetime, meta_update_datetime
                )

                SELECT
                    {self.insert_names_str}, meta_row_hash, meta_create_datetime, meta_update_datetime
                FROM
                    (
                        SELECT
                            a.*
                        FROM (
                            SELECT
                                *,
                                hash(*) AS meta_row_hash,
                                current_timestamp AS meta_create_datetime,
                                current_timestamp AS meta_update_datetime,
                                row_number() OVER ( PARTITION BY meta_row_hash ORDER BY NULL) AS rn
                            FROM {self.stg_table}
                         ) a
                        LEFT JOIN {self.base_table} b
                            ON a.meta_row_hash = b.meta_row_hash
                        WHERE b.meta_row_hash IS NULL
                        AND a.rn = 1
                ) s;
                """  # noqa: E501
        return self._unindent(cmd, 16)

    def execute(self, *, initial_load: bool = False, dryrun: bool = False):
        query_tag_cmd = generate_query_tag_cmd(dag_id=self.dag_id, task_id=self.task_id)
        self.run_cmd(cmd=query_tag_cmd, dryrun=dryrun)
        if initial_load:
            self.run_ddls(dryrun=dryrun)
        self.run_cmd(self.dml_begin, dryrun=dryrun)
        self.run_cmd(self.dml_clear_stage, dryrun=dryrun)
        self.run_copy_cmd(self.dml_copy_into_stg, dryrun=dryrun)
        self.run_cmd(self.dml_insert, dryrun=dryrun)
        self.run_cmd(self.dml_commit, dryrun=dryrun)
        self.raise_for_status()


class LoadJobInsertUpdateBrand(BaseLoadJob):
    def __init__(
        self,
        task_id: str,
        dag_id: str,
        brand: str,
        pre_merge_command: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.task_id = task_id
        self.dag_id = dag_id
        self.brand = brand.lower()
        self.pre_merge_command = pre_merge_command

    @property
    def base_table(self) -> str:
        if self.table.lower() == "order":
            return f'{self.database}.{self.schema}."{self.table.upper()}"'
        else:
            return f"{self.database}.{self.schema}.{self.table}"

    @property
    def base_table_meta_cols(self) -> List[Column]:
        base_table_meta_cols = [
            Column('hvr_is_deleted', 'INT'),
            Column('hvr_change_time', 'TIMESTAMP_TZ(3)'),
            Column('meta_row_source', 'VARCHAR(40)'),
        ]
        return base_table_meta_cols

    @property
    def merge_update_names_str(self) -> str:
        if self.stg_cols_as_source:
            update_names_str = f',\n{" " * 16}'.join(
                [f"t.{x.name} = s.{x.source_name}" for x in self.column_list]
            )
        else:
            update_names_str = f',\n{" " * 16}'.join([f"t.{x} = s.{x}" for x in self.column_names])
        return update_names_str

    @property
    def stg_table(self):
        stg_database = self.staging_database or self.database
        stg_schema = self.staging_schema or self.schema
        return f"{stg_database}.{stg_schema}.{self.unquote_and_lower(self.table)}_stg_{self.brand}"

    @property
    def dml_merge(self) -> str:
        uniqueness_join = '\n    AND '.join(
            [f"equal_null(t.{x}, s.{y})" for x, y in zip(self.pk_col_names, self.pk_stg_col_names)]
        )
        cmd = f"""
            MERGE INTO {self.base_table} t
            USING (
                SELECT
                    a.*
                FROM (
                    SELECT
                        *,
                        0 as hvr_is_deleted,
                        current_timestamp AS hvr_change_time,
                        'hvr_airflow' as meta_row_source,
                        row_number() OVER ( PARTITION BY {self.pk_stg_names_str} ORDER BY (SELECT NULL) ) AS rn
                    FROM {self.stg_table}
                 ) a
                WHERE a.rn = 1
            ) s ON {uniqueness_join}
            WHEN NOT MATCHED THEN INSERT (
                {self.insert_names_str}, hvr_is_deleted, hvr_change_time, meta_row_source
            )
            VALUES (
                {self.insert_stg_names_str}, hvr_is_deleted, hvr_change_time, meta_row_source
            )
            WHEN MATCHED THEN UPDATE
            SET {self.merge_update_names_str},
                t.hvr_is_deleted = s.hvr_is_deleted,
                t.hvr_change_time = s.hvr_change_time,
                t.meta_row_source = s.meta_row_source;
        """
        return self._unindent(cmd, 12)

    @property
    def dml_append_to_archive(self) -> Optional[str]:
        if not self.archive_table:
            return None
        cmd = f"""
            INSERT INTO {self.archive_table} ({self.insert_names_str})
            SELECT {self.insert_names_str}
            FROM {self.stg_table} s;
            """
        return self._unindent(cmd, 12)

    def execute(self, *, initial_load: bool = False, dryrun: bool = False):
        query_tag_cmd = generate_query_tag_cmd(dag_id=self.dag_id, task_id=self.task_id)
        self.run_cmd(cmd=query_tag_cmd, dryrun=dryrun)
        if initial_load:
            self.run_ddls(dryrun=dryrun)
        self.run_cmd(self.dml_begin, dryrun=dryrun)
        self.run_cmd(self.dml_clear_stage, dryrun=dryrun)
        self.run_copy_cmd(self.dml_copy_into_stg, dryrun=dryrun)
        self.run_cmd(self.pre_merge_command, dryrun=dryrun)
        self.run_cmd(self.dml_merge, dryrun=dryrun)
        self.run_cmd(self.dml_commit, dryrun=dryrun)
        self.run_cmd(self.dml_append_to_archive, dryrun=dryrun)
        self.raise_for_status()
