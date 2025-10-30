import logging
import re
from functools import cached_property
from pathlib import Path
from typing import Any, List, Optional, Union

from include import SQL_DIR
from include.airflow.hooks.snowflake import SnowflakeHook
from include.airflow.operators.snowflake import BaseSnowflakeOperator, get_effective_database
from include.config import conn_ids, snowflake_roles
from include.utils.snowflake import ForeignKey, generate_query_tag_cmd, get_snowflake_warehouse
from include.utils.string import camel_to_snake, indent, unindent_auto
from snowflake.connector import DictCursor

MART_VIEW_DATABASE = 'edw_prod'
MART_VIEW_SCHEMA = 'data_model'
MART_DATABASE = 'edw_prod'
MART_SCHEMA = 'stg'
MART_EXCEPTION_SCHEMA = 'excp'


class MissingViewException(Exception):
    """when view is missing from source control, this exception is raised"""

    def __init__(self, table_name):
        message = f"view for table {table_name} is missing from source control.  please add."
        super().__init__(message)


def create_unknown_record(hook, full_table_name, database, dryrun=True):
    if hook is None:
        return
    type_map = {
        'BOOLEAN': 'NULL',
        'TIMESTAMP_TZ': "'1900-01-01'",
        'TEXT': "'Unknown'",
        'TIMESTAMP_NTZ': "'1900-01-01'",
        'TIMESTAMP_LTZ': "'1900-01-01'",
        'DATE': "'1900-01-01'",
        'NUMBER': '-1',
    }

    def get_unknown_val(col_name, dtype):
        if dtype not in type_map:
            raise Exception(f"dtype '{dtype}' is unmapped")
        elif col_name.lower().endswith('is_current'):
            return 'TRUE'
        elif col_name.lower() == 'effective_end_datetime':
            return "'9999-12-31'"
        else:
            return type_map[dtype]

    insert_template = (
        "INSERT INTO {table_name} ({col_list})\n"
        "SELECT {values_list}\n"
        "WHERE NOT EXISTS (SELECT 1 FROM {table_name} WHERE {first_col} = -1);\n"
    )

    def get_insert_cmd(column_list, table):
        first_col = column_list[0]['COLUMN_NAME']
        col_list = ', '.join([x['COLUMN_NAME'].lower() for x in column_list])
        values_list = ', '.join(
            [get_unknown_val(x['COLUMN_NAME'], x['DATA_TYPE']) for x in column_list]
        )
        return insert_template.format(
            first_col=first_col,
            col_list=col_list,
            values_list=values_list,
            table_name=table,
        )

    with hook.get_conn() as cnx:
        cur = cnx.cursor(DictCursor)
        db, schema, table = full_table_name.split('.')
        cur.execute(
            f"""
        select
            l.column_name,
            l.data_type,
            l.ordinal_position
        from {database}.information_schema.columns l
        where table_name = '{table.upper()}'
        and table_schema = 'STG'
        order by l.ordinal_position
        """
        )

        columns = cur.fetchall()

        cmd = get_insert_cmd(columns, full_table_name)
        print(cmd)
        if not dryrun:
            cur.execute(cmd)
            print(cur.fetchall())


class ColumnList(list):
    @property
    def has_uniqueness(self):
        for col in self:
            if col.uniqueness is True:
                return True
        return False

    @property
    def is_type_2_load(self):
        return any([x.type_2 for x in self])

    @property
    def unique_col_names(self):
        return [x.name for x in self if x.uniqueness]


class KeyLookupJoin:
    """
    Used by mart operator to generate key look joins.

    Args:
        stg_column: column name from stg table used in lookup key join condition
        stg_column_type: Is the data type of stg_column and used to generate stg table ddl
        lookup_column: column name from look up table in lookup key join condition
    """

    def __init__(self, stg_column: str, stg_column_type: str, lookup_column: str = None):
        self.stg_column = stg_column
        self.stg_column_type = stg_column_type
        self.lookup_column = lookup_column or stg_column


class Column:
    """
    Config class for columns in facts and dims.

    Args:
        name: target column name
        type: datatype of the column in the target table
        uniqueness: ``True`` if this column is part of primary key
        type_2: ``True`` if it is a type 2 column
        delta_column: this will be used for incremental load into fact table
        source_name: if the stage table column name must be different from the target table column name
        allow_nulls: Not null constraint for target table
        default_value: if the column should have a default, provide here
        stg_only: ``True`` if the column should not be created in target table, but exist only in the stg table
        lookup_table: the dimension table name, if target column is a key lookup value
        lookup_table_key: the key column as it is named in the lookup table
        key_lookup_list: list of :class:`~.KeyLookupJoin` config classes
        comment: Description of the column

    E.g. suppose in fact order we want to do a key lookup for order_date.

    .. code-block:: python

        Column(
            name='order_date_key',  # this column will not exist in the stg table
            type='INT',  # order_date_key will have type INT in the fact
            lookup_table='dim_date',  # table we join to, to retrieve the key
            lookup_table_key='date_key',  # the column we retrieve in the key lookup
            key_lookup_list=[
                KeyLookup(
                    lookup_column='full_date',  # the column to join to in the dim
                    stg_column='order_date',  # name for column in the stage table
                    stg_column_type='DATE'  # data type for column in stage table
                ),
            ]
        )

    To illustrate a multi-column lookup, here is a more contrived example.  Let's stage three column:

        * order_day (1-31)
        * order_month (1-12)
        * order_year (0000-9999)

    Here's how we could do that:

    .. code-block:: python

        Column(
            name='order_date_key',
            type='INT',
            lookup_table='dim_date',
            lookup_table_key='date_key',
            key_lookup_list=[
                KeyLookup(
                    lookup_column='day_of_month',
                    stg_column='order_day',
                    stg_column_type='INT'
                ),
                KeyLookup(
                    lookup_column='month',
                    stg_column='order_month',
                    stg_column_type='INT'
                ),
                KeyLookup(
                    lookup_column='year',
                    stg_column='order_year',
                    stg_column_type='INT'
                ),
            ]
        )

    In this case we'd still only have ``order_date_key`` in the fact, but the three ``stg_column``
    columns would be created in the stg table.
    """

    def __init__(
        self,
        name: str,
        type: str,
        uniqueness: bool = False,
        type_2: bool = False,
        delta_column: Union[bool, int] = False,
        source_name: str = None,
        allow_nulls: bool = True,
        default_value: Any = None,
        stg_only: bool = False,
        lookup_table: str = None,
        lookup_table_key: str = None,
        key_lookup_list: List[KeyLookupJoin] = None,
        comment: str = None,
        foreign_key: ForeignKey = None,
    ):
        self.name = name
        self.type = type
        self.uniqueness = uniqueness
        self.type_2 = type_2
        self.delta_column = delta_column
        self._source_name = source_name
        self.allow_nulls = allow_nulls
        self._default_value = default_value
        self.stg_only = stg_only
        self.lookup_table_name = lookup_table
        self.key_lookup_list = key_lookup_list
        self.lookup_table_key = lookup_table_key
        self.comment = comment
        self.foreign_key = foreign_key

    @property
    def source_name(self):
        """If staging table column name is different from mart table then use this"""
        return self._source_name or self.name

    @property
    def nullability(self):
        """Null constraint for target table columns"""
        return "" if self.allow_nulls else " NOT NULL"

    @property
    def default_value(self):
        """If we need a defalut value for a column"""
        return f" DEFAULT {self._default_value}" if self._default_value else ""

    @property
    def lookup_table_alias(self) -> str:
        """Alias for lookup table"""
        return f"{self.lookup_table_name}_{self.name}"

    @property
    def type_1(self) -> bool:
        """To identify the type 1 columns"""
        return False if self.type_2 or self.uniqueness else True


class BaseSnowflakeMartOperator(BaseSnowflakeOperator):
    """
    Base class for dim and fact etl.

    Args:
        table: Mart table name.
            E.g. ``'dim_customer'``, or ``'fact_order'``
        column_list: The names given here are for the Mart tables.
        transform_proc_list: list of transform sproc scripts in the order of execution.
            E.g. ``['transform_dim_sku.sql', 'transform_dim_sku2.sql']``
        watermark_tables: List of watermark dependency tables.
            E.g. ``['lake.ultra_merchant.address', 'lake.ultra_merchant.store']``
        initial_load_value: on first run, what will be ``low_watermark``.
        use_surrogate_key: Surrogate key will be created in target table if ``True``
        skip_param_validation: if you want to skip parameter validation checks
        cluster_by: for target snowflake table, if we want to add a clustering key
        database: database for the stg / base tables.  if omitted, it will be grabbed from hook

    """

    DATABASE_CONFIG_VARIABLE_KEY = 'database_config'
    ROLE = snowflake_roles.etl_service_account
    EFF_START_TIMESTAMP_COL_TYPE = 'TIMESTAMP_LTZ'
    DEFAULT_TIMEZONE = 'America/Los_Angeles'
    STR_INDENT = ''
    NAMESPACE = 'edw_load'
    VIEWS_FOLDER = SQL_DIR / MART_VIEW_DATABASE / 'views'

    def __init__(
        self,
        table: str,
        column_list: List[Column],
        watermark_tables: List[str],
        transform_proc_list: list,
        initial_load_value: str = '1900-01-01',
        use_surrogate_key: bool = True,
        skip_param_validation: bool = False,
        cluster_by: str = None,
        initial_load_proc_list: Optional[List[str]] = None,
        warehouse: str = None,
        **kwargs,
    ):
        self.table = table
        self.column_list = ColumnList(column_list)
        self.initial_load_value = initial_load_value
        self.use_surrogate_key = use_surrogate_key
        self.transform_proc_list = transform_proc_list
        self.initial_load_proc_list = initial_load_proc_list
        self.warehouse = warehouse
        self.skip_param_validation = skip_param_validation
        self.watermark_tables = watermark_tables
        self.cluster_by = cluster_by
        self._command_debug_log = ''
        if not self.skip_param_validation:
            self.validate_column_list()
        if 'database' in kwargs:
            raise Exception(
                f"key `database` found in kwargs.  target database should be configured"
                f" using variable named `{self.DATABASE_CONFIG_VARIABLE_KEY}`"
            )
        super().__init__(
            snowflake_conn_id=conn_ids.Snowflake.default,
            warehouse=self.warehouse,
            schema=MART_SCHEMA,
            role=self.ROLE,
            timezone=self.DEFAULT_TIMEZONE,
            autocommit=False,
            task_id=self.table,
            **kwargs,
        )

    @cached_property
    def stg_database(self):
        """
        If database is given in operator init, we use that.  Otherwise, we grab from variable
        configuration.  And if there is no variable, default to ``edw_dev``.
        Set ``database`` attributed to the value used for ``stg_database``.
        """
        if self.is_test:
            self.database = MART_DATABASE
        elif self.database is None:
            self.database = get_effective_database(MART_DATABASE, self)
        return self.database

    @property
    def target_database(self):
        return self.stg_database

    @property
    def surrogate_key_name(self):
        return re.sub('.*(fact|dim)_', '', self.table_name) + '_key'

    @property
    def unique_cols_str(self) -> str:
        unique_cols_str = ', '.join(self.column_list.unique_col_names)
        return unique_cols_str

    @property
    def table_name(self) -> str:
        table = camel_to_snake(self.table.strip('[').strip(']').strip())
        return table

    @property
    def target_full_table_name(self):
        return self.target_database + '.' + MART_SCHEMA + '.' + self.table_name

    @property
    def view_full_table_name(self):
        return self.target_database + '.' + MART_VIEW_SCHEMA + '.' + self.table_name

    @property
    def watermark_param(self):
        return f"{MART_SCHEMA}.{self.table_name}"

    def _get_temp_table_name(self, suffix=''):
        """
        To get delta and excp  temp table names by providing suffix value

        Args:
            suffix: suffix value for table. e.g. _stg or _excp
        """
        return f"_{self.table_name}_{suffix}"

    @property
    def stg_table_name(self):
        return self.stg_database + '.' + MART_SCHEMA + '.' + self.table_name + '_stg'

    @property
    def excp_table_name(self):
        return self.stg_database + '.' + MART_EXCEPTION_SCHEMA + '.' + self.table_name

    @property
    def delta_table_name(self):
        return self._get_temp_table_name('delta')

    @property
    def excp_tmp_table_name(self):
        return self._get_temp_table_name('excp')

    @property
    def base_table_meta_cols(self) -> List[Column]:
        base_table_meta_cols: List[Column] = []
        return base_table_meta_cols

    @property
    def stg_table_meta_cols(self) -> List[Column]:
        stg_table_meta_cols: List[Column] = []
        return stg_table_meta_cols

    @property
    def insert_names_list(self) -> List[str]:
        return [x.name for x in self.column_list if not x.stg_only]

    @property
    def stg_col_name_list(self) -> List[str]:
        cols = []
        column_list = self.column_list + self.stg_table_meta_cols
        for x in column_list:
            if x.lookup_table_name:
                for lk in x.key_lookup_list:
                    cols.append(lk.stg_column)
            else:
                cols.append(x.source_name)
        return cols

    @property
    def delta_table_col_str(self) -> str:
        """
        Select col list in delta table creation script.
        Is combination of columns from stg and look up tables.
        """
        delta_table_col_list = [
            (
                f"nvl({x.lookup_table_alias}.{x.lookup_table_key}, -1) AS {x.name}"
                if x.lookup_table_name
                else f"s.{x.source_name} AS {x.name}"
            )
            for x in self.column_list
            if not x.stg_only
        ]
        return f",\n{self.STR_INDENT}".join(delta_table_col_list)

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

    @staticmethod
    def get_hash_column(column_name, data_type):
        if data_type.lower()[0:11] in ('timestamp_l', 'timestamp_t'):
            return f"{column_name}::timestamp_ntz"
        else:
            return column_name

    @staticmethod
    def key_lookup_join_cond(key_lookup_list, lookup_table_alias) -> str:
        """
        Join condition to look up dimension keys. E.g:

        .. code-block:: sql

            ON s.is_activating = dim_order_detail_order_detail_key.is_activating
            AND s.is_ecomm = dim_order_detail_order_detail_key.is_ecomm
        """
        return '\nAND '.join(
            [f"s.{x.stg_column} = {lookup_table_alias}.{x.lookup_column}" for x in key_lookup_list]
        )

    @staticmethod
    def key_lookup_meta_datetime_join_cond(
        lookup_table_name,
        key_lookup_join_datetime_column,
        lookup_table_alias,
        str_indent,
    ) -> Optional[str]:
        """
        To get key_lookup_join_datetime_column join condition for lookup keys. E.g:

        .. code-block:: sql

            AND nvl(s.order_datetime, current_timestamp)
            BETWEEN dim_store_store_key.effective_start_datetime
            AND dim_store_store_key.effective_end_datetime
        """
        meta_datetime_join_cond = None
        if lookup_table_name.lower() != 'dim_date':
            _key_lookup_join_datetime_column = (
                f"s.{key_lookup_join_datetime_column}"
                if key_lookup_join_datetime_column
                else "NULL"
            )
            meta_datetime_join_cond = (
                f"AND nvl({_key_lookup_join_datetime_column}, current_timestamp)"
                f"\n{str_indent}BETWEEN {lookup_table_alias}.effective_start_datetime "
                f"AND {lookup_table_alias}.effective_end_datetime"
            )
        return meta_datetime_join_cond

    @property
    def key_lookup_join_datetime_column(self):
        raise NotImplementedError

    @property
    def key_lookup_join_str(self) -> str:
        """
        Final look up join condition str used in delta table. Concatenation of key_lookup_join_cnd
        and key_lookup_meta_datetime_join_cond. E.g:

        .. code:: SQL

            LEFT JOIN dbo.dim_store AS dim_store_store_key
                ON s.store_id = dim_store_store_key.store_id
                AND nvl(s.order_datetime, current_timestamp)
                BETWEEN dim_store_store_key.effective_start_datetime
                AND dim_store_store_key.effective_end_datetime
        """
        return """\n\t""".join(
            [
                f"LEFT JOIN {self.target_database}.{MART_SCHEMA}.{x.lookup_table_name} AS {x.lookup_table_alias}"
                f"\n{self.STR_INDENT}ON {self.key_lookup_join_cond(x.key_lookup_list, x.lookup_table_alias)}"
                f"\n{self.STR_INDENT}{self.key_lookup_meta_datetime_join_cond(x.lookup_table_name, self.key_lookup_join_datetime_column, x.lookup_table_alias, self.STR_INDENT)}"  # noqa: E501
                for x in self.column_list
                if x.key_lookup_list
            ]
        )

    @staticmethod
    def _get_base_col_ddls(column_list) -> str:
        return ',\n            '.join(
            [f"{x.name} {x.type}{x.nullability}" for x in column_list if not x.stg_only]
        )

    @staticmethod
    def _get_stg_col_ddls(column_list) -> str:
        cols = []
        for x in column_list:
            if x.lookup_table_name:
                for lk in x.key_lookup_list:
                    cols.append(f"{lk.stg_column} {lk.stg_column_type}")
            else:
                cols.append(f"{x.source_name} {x.type}{x.default_value}")

        return ',\n            '.join(cols)

    @property
    def primary_key_col_ddl(self):
        return f",\n    PRIMARY KEY ({self.unique_cols_str})"

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
        base_table_meta_col_ddls = self._get_base_col_ddls(self.base_table_meta_cols)
        if base_table_meta_col_ddls:
            base_table_meta_col_ddls = ',\n            ' + base_table_meta_col_ddls
        return base_table_meta_col_ddls or ''

    @property
    def stg_table_meta_col_ddls(self):
        stg_table_meta_col_ddls = self._get_stg_col_ddls(self.stg_table_meta_cols)
        if stg_table_meta_col_ddls:
            stg_table_meta_col_ddls = ',\n            ' + stg_table_meta_col_ddls
        return stg_table_meta_col_ddls or ''

    @property
    def transform_proc_file_list(self):
        """This will return list of transform procedure's file path"""
        transform_proc_filepath = []
        for proc in self.transform_proc_list:
            schema, name, ext = proc.split('.')
            transform_proc_filepath.append(
                Path(SQL_DIR, 'edw_prod', 'procedures', f"{schema}.{name}.{ext}")
            )
        return transform_proc_filepath

    def validate_column_list(self):
        if not self.column_list.has_uniqueness:
            raise Exception("Column list must have at least one uniqueness column")

        for x in self.column_list:
            if x.uniqueness and x.stg_only:
                raise Exception("uniqueness column should not be stg only column")

            if x.key_lookup_list and (not x.lookup_table_name or not x.lookup_table_key):
                raise Exception("Lookup table or lookup table key is missing for key lookup")

    def is_initial_load(self, dryrun=False) -> bool:
        cmd = f"""
SELECT
    1
FROM {self.stg_database}.INFORMATION_SCHEMA.TABLES l
WHERE UPPER(table_name) = '{self.table.upper()}'
    AND UPPER(table_schema) = '{self.schema.upper()}'
LIMIT 1;
"""

        if dryrun:
            self.print(cmd)
            return True
        logging.info(cmd)

        with self.get_snowflake_cnx() as cnx:
            cur = cnx.cursor(DictCursor)
            query_tag = generate_query_tag_cmd(dag_id=self.dag_id, task_id=self.task_id)
            cur.execute(query_tag)
            cur.execute(cmd)
            result = cur.fetchone()

        return result is None

    @property
    def not_null_cols_str(self) -> str:
        """
        Not null columns validation script for not null exception validation. E.g:

        .. code-block:: sql

            order_datetime IS NULL
            OR customer_id IS NULL
            OR store_id IS NULL
        """
        return '\n    OR '.join(
            [
                f"{x.source_name} IS NULL"
                for x in self.column_list
                if x.allow_nulls is False and not x.stg_only and not x.key_lookup_list
            ]
        )

    def print(self, val):
        self._command_debug_log += val + '\n'
        if not self.is_test:
            print(val)

    @property
    def full_cmd(self):
        return self._command_debug_log[:-1]

    def stg_dups_validation(self):
        raise NotImplementedError

    def stg_nulls_validation(self) -> str:
        """
        Check null values in stg_table for not null columns in dims and facts.
        This is only for non key lookup columns
        """
        cmd = f"""
            -- Null values validation for non key lookup columns
            INSERT INTO {self.excp_tmp_table_name}
            SELECT
                s.id,
                'warning' AS meta_data_quality,
                'Null values present in not null columns' AS excp_message
            FROM {self.stg_table_name} s
            WHERE {self.not_null_cols_str};
            """
        return unindent_auto(cmd)

    def stg_update_with_errors(self) -> str:
        """Updates stg table with errors after stg table data validation"""
        cmd = f"""
            UPDATE {self.stg_table_name} s
                SET s.meta_data_quality = e.meta_data_quality
            FROM {self.excp_tmp_table_name} e
            WHERE s.id = e.id
                AND e.meta_data_quality = 'error';
            """
        return unindent_auto(cmd)

    def create_base_table(self) -> str:
        cluster_by = f"\nCLUSTER BY ({self.cluster_by})\n" if self.cluster_by else ''
        surr_key_name = self.surrogate_key_name if self.use_surrogate_key else ''
        surr_key_ddl = f"{surr_key_name} INT IDENTITY UNIQUE,\n    " if surr_key_name else ''
        cmd = f"""
        CREATE TABLE IF NOT EXISTS {self.target_full_table_name} (
            {surr_key_ddl}{self._get_base_col_ddls(self.column_list)}{self.base_table_meta_col_ddls}{self.primary_key_col_ddl}{self.foreign_key_ref_ddl}
        ){cluster_by};
        """
        return unindent_auto(cmd)

    def create_stg_table(self) -> str:
        create_command = 'CREATE TABLE IF NOT EXISTS'
        cmd = f"""
        {create_command} {self.stg_table_name} (
            id INT IDENTITY,
            {self._get_stg_col_ddls(self.column_list)}{self.stg_table_meta_col_ddls}
        );
        """
        return unindent_auto(cmd)

    def create_excp_table(self) -> str:
        cmd = f"""
        CREATE TABLE IF NOT EXISTS {self.excp_table_name} (
            id INT,
            {self._get_stg_col_ddls(self.column_list)}{self.stg_table_meta_col_ddls},
            excp_message VARCHAR,
            meta_is_current_excp BOOLEAN
        );
        """
        return unindent_auto(cmd)

    def default_view_ddl(self) -> str:
        view_name = MART_VIEW_SCHEMA + '.' + self.table_name
        type_2_meta_cols_list = (
            ['effective_start_datetime', 'effective_end_datetime', 'is_current']
            if self.column_list.is_type_2_load
            else []
        )
        meta_cols_list = type_2_meta_cols_list + ['meta_create_datetime', 'meta_update_datetime']
        meta_cols = ',\n\t\t\t' + ',\n\t\t\t'.join(meta_cols_list)
        view_cols = ',\n\t\t\t'.join(
            [
                f"{x.name} COMMENT '{x.comment}'" if x.comment else x.name
                for x in self.column_list
                if not x.stg_only
            ]
        )
        full_col_select_list = ',\n\t\t\t'.join(self.insert_names_list)
        surr_key = f"{self.surrogate_key_name},\n\t\t\t" if self.use_surrogate_key else ''
        cmd = f"""
        CREATE VIEW IF NOT EXISTS {view_name}
        (
            {surr_key}{view_cols}{meta_cols}
        ) AS
        SELECT
            {surr_key}{full_col_select_list}{meta_cols}
        FROM {MART_SCHEMA}.{self.table_name};
        """
        return unindent_auto(cmd)

    def create_temp_excp_table(self) -> str:
        cmd = f"""
        CREATE OR REPLACE TEMPORARY TABLE {self.excp_tmp_table_name} (
            id INT,
            meta_data_quality VARCHAR(10),
            excp_message VARCHAR
        );
        """
        return unindent_auto(cmd, 2)

    def load_excp_table(self) -> str:
        """To load data from temp excp table to persistent excp table after all excp validations"""
        stg_col_list = [
            x for x in self.stg_col_name_list if x != 'meta_data_quality' and x != 'excp_message'
        ]
        insert_list_str = ',\n\t'.join(stg_col_list)
        select_col_str = ',\n\t'.join([f"s.{x}" for x in stg_col_list])

        cmd = f"""
        INSERT INTO {self.excp_table_name}
        (
            id,
            {insert_list_str},
            meta_data_quality,
            excp_message,
            meta_is_current_excp
        )
        SELECT
            s.id,
            {select_col_str},
            e.meta_data_quality,
            e.excp_message,
            TRUE AS meta_is_current_excp
        FROM {self.stg_table_name} s
        JOIN {self.excp_tmp_table_name} e
            ON s.id = e.id;
        """
        return unindent_auto(cmd)

    def update_excp_table(self) -> str:
        cmd = f"""
        UPDATE {self.excp_table_name} e
            SET e.meta_is_current_excp = FALSE
        WHERE e.meta_is_current_excp;
        """
        return unindent_auto(cmd)

    def clear_stg_table(self) -> str:
        cmd = f"\nDELETE FROM {self.stg_table_name};"
        return cmd

    def execute_cmd(self, cmd: str, dryrun=False):
        if dryrun:
            self.print(cmd)
            return
        logging.info(cmd)
        with self.cnx.cursor(cursor_class=DictCursor) as cur:
            cur.execute(cmd)
            result_list = cur.fetchall()
            logging.info(result_list)
            messages = cur.messages
            logging.info(messages)

    @cached_property
    def is_test(self):
        return self.table_name.startswith('test')

    def get_view_ddl(self):
        if self.is_test:
            return self.default_view_ddl()
        view_file = self.VIEWS_FOLDER / f"{MART_VIEW_SCHEMA}.{self.table_name}.sql"
        if not view_file.exists():
            view_file.write_text(self.default_view_ddl())
            raise MissingViewException(self.table_name)
        else:
            return view_file.read_text()

    def create_objects(self, dryrun):
        cmd = self.create_stg_table()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

        cmd = self.create_excp_table()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

        cmd = self.create_base_table()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

        cmd = self.get_view_ddl()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

    def pre_stg_table_load(self, dryrun):

        cmd = self.clear_stg_table()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

    def post_stg_table_load(self, dryrun):
        pass

    def load_stg_table(self, hook, dryrun, initial_load=False):
        proc_list = []
        if initial_load and self.initial_load_proc_list:
            proc_list.extend(self.initial_load_proc_list)
        proc_list.extend(self.transform_proc_list)
        for proc_name in proc_list:
            sql_path = Path(SQL_DIR, 'edw_prod', 'procedures', proc_name)
            cmd = self.get_sql_cmd(sql_or_path=sql_path)
            if dryrun:
                self.print(cmd)
            else:
                hook.execute_multiple_with_cnx(cnx=self.cnx, sql=cmd, parameters=self.parameters)

    def validate_stg_table(self, dryrun):
        cmd = self.stg_dups_validation()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

        if self.not_null_cols_str:
            cmd = self.stg_nulls_validation()
            self.execute_cmd(cmd=cmd, dryrun=dryrun)

        cmd = self.update_excp_table()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

        cmd = self.load_excp_table()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

        cmd = self.stg_update_with_errors()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

    def load_base_table(self, dryrun):
        pass

    def etl_execute(self, initial_load, dryrun, hook=None):
        query_tag = generate_query_tag_cmd(dag_id=self.dag_id, task_id=self.task_id)
        self.execute_cmd(cmd=query_tag, dryrun=dryrun)
        if initial_load:
            self.create_objects(dryrun=dryrun)
            create_unknown_record(
                hook=hook,
                full_table_name=self.target_full_table_name,
                database=self.stg_database,
                dryrun=dryrun,
            )
        self.pre_stg_table_load(dryrun=dryrun)
        self.load_stg_table(hook=hook, initial_load=initial_load, dryrun=dryrun)
        self.post_stg_table_load(dryrun=dryrun)
        self.validate_stg_table(dryrun=dryrun)
        self.load_base_table(dryrun=dryrun)

    def ensure_correct_database(self, cnx):
        """
        We pull target database from airflow Variable lazily (i.e. not in operator init).
        If someone calls snowflake_hook before updating database, then the connection may not be
        using the correct database.
        As a result, we double check that target database is current database.
        """
        if cnx.database.lower() != self.target_database.lower():
            cnx.execute_string(f"use database {self.target_database}")

    @cached_property
    def snowflake_hook(self):
        """Ensure target database is set from config before instantiating hook"""
        return SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            warehouse=self.warehouse,
            database=self.target_database,
            role=self.role,
            schema=self.schema,
        )

    def get_snowflake_cnx(self):
        """Ensure warehouse and database are set before returning connection"""
        self.cnx = self.snowflake_hook.get_conn()

        with self.cnx.cursor() as cur:
            cur.execute("USE WAREHOUSE COMPUTE_WH")
            cur.execute(f"USE DATABASE {self.target_database}")  # ⚠️ 注意这里去掉了参数化
            cur.execute(f"USE SCHEMA {self.schema}")  # ⚠️ 这里也一样

        print(f"Snowflake SessionId: {self.cnx.session_id}")
        return self.cnx

    def watermark_execute(self, context=None):
        initial_load = self.is_initial_load()
        with self.get_snowflake_cnx():
            self.etl_execute(initial_load=initial_load, dryrun=False, hook=self.snowflake_hook)

    def dry_run(self):
        self.is_initial_load(dryrun=True)
        self.get_high_watermark(initial_load=True, dryrun=True)
        self.etl_execute(initial_load=True, dryrun=True)
        self.update_high_watermark(dryrun=True)

    def get_high_watermark_cmd(self, table_name):
        database, schema, table = table_name.split('.')
        if 'lake.ultra_warehouse.inventory_rollup' in table_name.lower():
            delta_column = 'meta_update_datetime'
        elif 'lake.ultra_warehouse' in table_name.lower():
            delta_column = 'hvr_change_time'
        elif 'lake_jfb.ultra_partner' in table_name.lower():
            delta_column = 'hvr_change_time'
        else:
            delta_column = 'meta_update_datetime'
        hwm_command = f"""
        SELECT
            '{table_name}' AS dependent_table_name,
            CAST(max({delta_column}) AS TIMESTAMP_LTZ(3)) AS high_watermark_datetime
        FROM {get_effective_database(database, self)}.{schema}.{table}"""

        return unindent_auto(hwm_command)

    def get_table_high_watermark_cmd(self, initial_load):
        command_list = [self.get_high_watermark_cmd(dep) for dep in self.watermark_tables]
        if initial_load:
            self_table_cmd = f"""
            SELECT
                '{self.target_full_table_name}' AS dependent_table_name,
                '1900-01-01'::TIMESTAMP_LTZ AS high_watermark_datetime"""

            command_list.append(unindent_auto(self_table_cmd))
        else:
            full_table_name = MART_DATABASE + '.' + MART_SCHEMA + '.' + self.table_name
            command_list.append(self.get_high_watermark_cmd(full_table_name))

        union = "\n\t\t\t\t\tUNION\n".join([indent(x, 20) for x in command_list])
        cmd = f"""
        MERGE INTO stg.meta_table_dependency_watermark AS t
        USING
        (
            SELECT
                '{self.watermark_param}' AS table_name,
                NULLIF(dependent_table_name,'{self.target_full_table_name}') AS dependent_table_name,
                high_watermark_datetime AS new_high_watermark_datetime
            FROM({union}
                ) h
        ) AS s
        ON t.table_name = s.table_name
            AND equal_null(t.dependent_table_name, s.dependent_table_name)
        WHEN MATCHED
            AND not equal_null(t.new_high_watermark_datetime, s.new_high_watermark_datetime)
        THEN
            UPDATE
                SET t.new_high_watermark_datetime = s.new_high_watermark_datetime,
                    t.meta_update_datetime = current_timestamp::timestamp_ltz(3)
        WHEN NOT MATCHED
        THEN
            INSERT
            (
                table_name,
                dependent_table_name,
                high_watermark_datetime,
                new_high_watermark_datetime
            )
            VALUES
            (
                s.table_name,
                s.dependent_table_name,
                '{self.initial_load_value}'::timestamp_ltz,
                s.new_high_watermark_datetime
            );
        """

        return unindent_auto(cmd)

    def get_high_watermark(self, initial_load, dryrun=False):
        query_tag = generate_query_tag_cmd(dag_id=self.dag_id, task_id=self.task_id)
        self.execute_cmd(cmd=query_tag, dryrun=dryrun)
        cmd = self.get_table_high_watermark_cmd(initial_load)
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

    def get_table_high_watermark_update_cmd(self):
        cmd = f"""
        UPDATE stg.meta_table_dependency_watermark
        SET
            high_watermark_datetime = IFF(
                                            dependent_table_name IS NOT NULL,
                                            new_high_watermark_datetime,
                                            (
                                                SELECT
                                                    MAX(meta_update_datetime)
                                                FROM {self.target_full_table_name}
                                            )
                                        ),
            meta_update_datetime = current_timestamp::timestamp_ltz(3)
        WHERE table_name = '{self.watermark_param}';
        """

        return unindent_auto(cmd)

    def update_high_watermark(self, dryrun=False):
        query_tag = generate_query_tag_cmd(dag_id=self.dag_id, task_id=self.task_id)
        self.execute_cmd(cmd=query_tag, dryrun=dryrun)
        cmd = self.get_table_high_watermark_update_cmd()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

    def watermark_pre_execute(self):
        initial_load = self.is_initial_load()
        with self.get_snowflake_cnx():
            self.get_high_watermark(initial_load)

    def watermark_post_execute(self):
        with self.get_snowflake_cnx():
            self.update_high_watermark()

    def execute(self, context=None):
        self.warehouse = get_snowflake_warehouse(context)
        self.watermark_pre_execute()

        self.watermark_execute(context=context)

        self.watermark_post_execute()


class BaseSnowflakeMartUpsertOperator(BaseSnowflakeMartOperator):
    """
    Base class for insert update fact and dim processes (as distinguished from SCD processes).

    Args:
        key_lookup_join_datetime_column: Used to get dimension key where this column between meta
            eff start and end dates.

    """

    STR_INDENT = "\t"

    def __init__(
        self,
        key_lookup_join_datetime_column: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._key_lookup_join_datetime_column = key_lookup_join_datetime_column

    @property
    def key_lookup_join_datetime_column(self):
        return self._key_lookup_join_datetime_column

    def stg_dups_validation(self):
        """Query to identify duplicates business key in stg table"""
        unique_col_names = []
        for x in self.column_list:
            if x.uniqueness:
                if x.key_lookup_list:
                    for k in x.key_lookup_list:
                        unique_col_names.append(k.stg_column)
                else:
                    unique_col_names.append(x.source_name)

        unique_cols_str = ', '.join(unique_col_names)
        uniqueness_join = '\n    AND '.join([f"equal_null(t.{x}, s.{x})" for x in unique_col_names])
        cmd = f"""
            -- Identifying duplicate business keys
            INSERT INTO {self.excp_tmp_table_name}
            WITH dups_cte
            AS
            (
                SELECT
                    {unique_cols_str},
                    'error' AS meta_data_quality,
                    'Duplicate business key values are present' AS excp_message
                FROM {self.stg_table_name}
                GROUP BY {unique_cols_str} HAVING COUNT(*) > 1
            )
            SELECT s.id, t.meta_data_quality, t.excp_message
            FROM {self.stg_table_name} s
            JOIN dups_cte t
                ON {uniqueness_join};
            """
        return unindent_auto(cmd)

    @property
    def stg_table_meta_cols(self) -> List[Column]:
        stg_table_meta_cols = [
            Column('meta_data_quality', 'VARCHAR(10)'),
            Column(
                'meta_create_datetime',
                'TIMESTAMP_LTZ',
                default_value='current_timestamp',
            ),
            Column(
                'meta_update_datetime',
                'TIMESTAMP_LTZ',
                default_value='current_timestamp',
            ),
        ]
        return stg_table_meta_cols

    @property
    def base_table_meta_cols(self) -> List[Column]:
        raise NotImplementedError

    @property
    def meta_row_hash(self) -> str:
        """
        meta_row_hash for type 1 dimensions and Fact loads.
        This will be used in delta table creation
        """
        meta_hash_cols = [
            (
                f"nvl({x.lookup_table_alias}.{x.lookup_table_key}, -1)".lower()
                if x.lookup_table_name
                else f"s.{self.get_hash_column(x.source_name, x.type)}"
            )
            for x in self.column_list
            if not x.stg_only
        ]
        return f"hash({', '.join(meta_hash_cols) if meta_hash_cols else 'NULL'})"

    def load_delta_table(self):
        raise NotImplementedError

    @staticmethod
    def is_timestamp_col(data_type) -> bool:
        return data_type.lower()[0:4] in ('date', 'time')

    @property
    def merge_update_names_str(self) -> str:
        update_names_str = ',\n\t'.join([f"t.{x} = s.{x}" for x in self.insert_names_list])
        return update_names_str

    def _merge_order_by(self, col_list, alias=None) -> str:
        curr_order_by = ''
        delta_col_list = sorted(col_list, key=lambda x: x.delta_column)  # type: ignore
        full_alias = alias + '.' if alias else ''
        if delta_col_list:
            first_dc = delta_col_list[0]
            if self.is_timestamp_col(first_dc.type):
                null_repl = '1900-01-01'
            else:
                null_repl = '0'
            col_list_str = ', '.join(f"{full_alias}{x.source_name}" for x in delta_col_list)
            curr_order_by = f"coalesce({col_list_str}, '{null_repl}')"
        return curr_order_by

    @property
    def merge_update_condition(self):
        """
        We will use this to check whether data in stg table is latest data compared to final table.
        This will be used only if delta column is provided in config table.
        """
        col_list = [x for x in self.column_list if x.delta_column is not False and not x.stg_only]
        source_coalesce = self._merge_order_by(col_list, 's')
        if not source_coalesce:
            return ''
        else:
            target_coalesce = self._merge_order_by(col_list, 't')
            return f"\n    AND {source_coalesce} > {target_coalesce}"

    def merge_into_base_table(self) -> str:
        uniqueness_join = '\n    AND '.join(
            [f"equal_null(t.{x}, s.{x})" for x in self.column_list.unique_col_names]
        )
        full_col_select_list = ', '.join(
            self.insert_names_list + [x.name for x in self.base_table_meta_cols]
        )
        cmd = f"""
            MERGE INTO {self.target_full_table_name} t
            USING {self.delta_table_name} s
                ON {uniqueness_join}
            WHEN NOT MATCHED THEN INSERT (
                {full_col_select_list}
            )
            VALUES (
                {full_col_select_list}
            )
            WHEN MATCHED AND t.meta_row_hash != s.meta_row_hash{self.merge_update_condition}
            THEN
            UPDATE SET
                {self.merge_update_names_str},
                t.meta_row_hash = s.meta_row_hash,
                t.meta_update_datetime = s.meta_update_datetime;
            """
        return unindent_auto(cmd)

    def create_delta_table(self) -> str:
        cmd = f"""
        CREATE OR REPLACE TEMPORARY TABLE {self.delta_table_name} (
            {self._get_base_col_ddls(self.column_list)}{self.base_table_meta_col_ddls}
        );
        """
        return unindent_auto(cmd)

    def post_stg_table_load(self, dryrun):

        cmd = self.create_temp_excp_table()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

        cmd = self.create_delta_table()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

    def load_base_table(self, dryrun):
        cmd = self.load_delta_table()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

        cmd = self.merge_into_base_table()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)
