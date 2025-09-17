import datetime
import decimal
from typing import Optional
from urllib.parse import urlencode

import pyodbc
from airflow.hooks.dbapi import DbApiHook

from include.utils.snowflake import Column
from include.utils.string import camel_to_snake
from include.config import conn_ids


class MsSqlOdbcHook(DbApiHook):
    """
    Interact with Microsoft SQL Server.
    """

    conn_name_attr = 'mssql_conn_id'
    default_conn_name = 'mssql_default'
    supports_autocommit = True

    def __init__(
        self,
        mssql_conn_id=conn_ids.MsSql.default,
        database=None,
        schema=None,
        trx_isolation=pyodbc.SQL_TXN_READ_UNCOMMITTED,
        *args,
        **kwargs,
    ):
        super(MsSqlOdbcHook, self).__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self._database = database
        self.schema = schema
        self._conn_extra = None
        self._conn = None
        self.trx_isolation = trx_isolation

    @property
    def conn(self):
        if not self._conn:
            self._conn = self.get_connection(self.mssql_conn_id)
        return self._conn

    @property
    def database(self):
        return self._database or self.conn.schema

    @property
    def conn_extra(self):
        if not self._conn_extra:
            self._conn_extra = self.conn.extra_dejson
        return self._conn_extra

    @property
    def conn_str(self):
        extra = self.conn_extra.copy()
        driver = extra.pop('driver').strip()
        if driver[0] != '{':
            driver = f"{{{driver}}}"
        conn_str = f"DRIVER={driver};"
        if self.conn.host:
            conn_str += f"SERVER={self.conn.host};"
        database = self.database or self.conn.schema
        if database:
            conn_str += f"DATABASE={database};"
        if self.conn.login:
            conn_str += f"UID={self.conn.login};"
        if self.conn.password:
            conn_str += f"PWD={self.conn.password};"
        if self.schema:
            conn_str += f"SCHEMA={self.schema};"
        if self.conn.port:
            f"PORT={self.conn.port};"
        for k, v in extra.items():
            conn_str += f"{k}={v};"
        return conn_str

    def get_conn(self) -> pyodbc.Connection:
        """
        Returns a mssql connection object
        """
        conn = pyodbc.connect(self.conn_str, autocommit=True)
        conn.set_attr(pyodbc.SQL_ATTR_TXN_ISOLATION, self.trx_isolation)
        return conn

    def get_uri(self):
        params = urlencode(self.conn_extra)
        user_block = ""
        if self.conn.login:
            user_block = self.conn.login
            if self.conn.password:
                user_block += f":{self.conn.password}"
            user_block += "@"
        host_block = self.conn.host
        if self.conn.port:
            host_block += f":{self.conn.port}"
        uri = f"mssql+pyodbc://{user_block}{host_block}/{self.database or ''}?{params or ''}"
        return uri

    def get_sqlalchemy_connection(self, connect_kwargs=None, engine_kwargs=None):
        effective_engine_kwargs = dict(isolation_level='AUTOCOMMIT')
        if engine_kwargs:
            effective_engine_kwargs.update(engine_kwargs)
        engine = self.get_sqlalchemy_engine(engine_kwargs=effective_engine_kwargs)
        cnx = engine.connect(**(connect_kwargs or {}))
        return cnx

    def get_turbodbc_connection(self, turbodbc_options: Optional[dict] = None):
        # imported locally so turbodbc can be optional
        import turbodbc
        from turbodbc import make_options
        from turbodbc_intern import Megabytes

        default_options_kwargs = dict(
            read_buffer_size=Megabytes(300),
            large_decimals_as_64_bit_types=True,
            prefer_unicode=True,
            autocommit=True,
        )
        turbodbc_options = make_options(**{**default_options_kwargs, **(turbodbc_options or {})})
        return turbodbc.connect(connection_string=self.conn_str, turbodbc_options=turbodbc_options)

    def set_autocommit(self, conn, autocommit):
        conn.autocommit = autocommit

    def get_autocommit(self, conn):
        return conn.autocommit

    @staticmethod
    def get_snowflake_types_from_query(description, column_subset=None):
        def result_type_mapping(row_meta):
            col_type = row_meta[1]
            column_size = row_meta[3]
            num_prec_radix = row_meta[4]
            decimal_digits = row_meta[5]
            if col_type == str:
                dest_type = f"VARCHAR({column_size})"
            elif col_type == datetime.datetime:
                dest_type = f"TIMESTAMP_NTZ({decimal_digits})"
            elif col_type == int:
                dest_type = "INT"
            elif col_type == decimal.Decimal:
                dest_type = f"DECIMAL({num_prec_radix}, {decimal_digits})"
            elif col_type == bytearray:
                dest_type = f"BINARY({column_size})"
            elif col_type == bool:
                dest_type = "BOOLEAN"
            else:
                raise ValueError(f"type {col_type} is unmapped")
            return dest_type

        column_subset = set(column_subset) if column_subset else None
        result = []
        for row in description:
            column_name = row[0]
            if column_subset is None or column_name in column_subset:
                result.append((column_name, result_type_mapping(row)))
        return result


def get_snowflake_types_from_schema(cur, table, database, schema, column_subset=None):
    """
    Gets snowflake column type information from ms sql server database using information_schema.

    Args:
        cur: pyodbc cursor object
        table: name of table
        database:
        schema:
        column_subset:

    Examples:
        >>> hook = MsSqlOdbcHook('mssql_edw01_app_airflow')
        >>> cnx = hook.get_conn()
        >>> cur = cnx.cursor()
        >>> get_snowflake_types_from_schema(
        >>>     cur=cur, database='ultramerchant', schema='dbo', table='shipping_option'
        >>> )
    """
    col_list = cur.columns(catalog=database, schema=schema, table=table).fetchall()
    col_names = [x[0] for x in cur.description]
    col_list = [dict(zip(col_names, row)) for row in col_list]

    def source_type_mapping(type_name, column_size, decimal_digits):
        if type_name in ('char', 'nvarchar', 'uniqueidentifier', 'varchar', 'text'):
            if 0 < column_size < 16777216:
                dest_type = f"VARCHAR({column_size})"
            else:
                dest_type = 'VARCHAR'
        elif type_name in ('datetime', 'datetime2', 'smalldatetime'):
            dest_type = f"TIMESTAMP_NTZ({decimal_digits})"
        elif type_name in ('datetimeoffset',):
            dest_type = f"TIMESTAMP_LTZ({decimal_digits})"
        elif type_name in (
            'int',
            'smallint',
            'tinyint',
            'bigint',
            "int identity",
            "bigint identity",
        ):

            dest_type = "INT"
        elif type_name in ('bit',):
            dest_type = "BOOLEAN"
        elif type_name in ('date',):
            dest_type = "DATE"
        elif type_name in ('sql_variant',):
            dest_type = "VARIANT"
        elif type_name in ('time',):
            dest_type = "TIME"
        elif type_name in ('money', 'decimal', 'numeric'):
            dest_type = f"NUMBER({column_size}, {decimal_digits})"
        elif type_name in ('double', 'float', 'real'):
            dest_type = "DOUBLE"
        elif type_name in ('binary', 'image', 'varbinary', 'timestamp', 'rowversion'):
            if 0 < column_size < 8388608:
                dest_type = f"BINARY({column_size})"
            else:
                dest_type = "BINARY"
        else:
            raise ValueError(f"type {type_name} is unmapped")
        return dest_type

    column_subset = set(column_subset) if column_subset else None
    result = []
    for row in col_list:
        column_name = row['column_name']
        type_name = row['type_name']
        column_size = row['column_size']
        decimal_digits = row['decimal_digits']
        if column_subset is None or column_name in column_subset:
            result.append(
                (column_name, source_type_mapping(type_name, column_size, decimal_digits))
            )
    return result


def get_primary_key_list(cur, database, schema, table):
    query = f"""
        SELECT
            cu.column_name
        FROM {database}.information_schema.table_constraints AS tc
        JOIN {database}.information_schema.constraint_column_usage AS cu ON cu.constraint_name = tc.constraint_name
            AND cu.table_name = tc.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = '{schema}'
          AND cu.table_name = '{table}'
    """
    cur.execute(query)
    rows = cur.fetchall()
    columns = [x[0] for x in rows]
    return columns


def get_column_list_definition(cur, database, schema, table):
    """
    Given SQL server database / schema / table, generates list of :class:`~.utils.snowflake.Column`.

    Infers uniqueness and guesses delta column.

    Args:
        cur: pyodbc cursor
        database: source database name
        schema: source schema name
        table: source table name

    """
    reserved_word_map = {'order': '"ORDER"', 'group': '"GROUP"'}
    primary_key_list = {
        camel_to_snake(x)
        for x in get_primary_key_list(cur=cur, database=database, schema=schema, table=table)
    }

    cols = get_snowflake_types_from_schema(cur, table=table, database=database, schema=schema)
    column_list = []
    for name, type_ in cols:
        target_name = camel_to_snake(name)
        source_name = name if name != target_name else None
        if 'TIMESTAMP_LTZ' in type_:
            source_name = f"convert(VARCHAR, {(source_name or target_name)}, 127)"
        if target_name in reserved_word_map:
            target_name = reserved_word_map.get(target_name)
            source_name = f"[{name}]"
        column_list.append(Column(target_name, type_, source_name=source_name))

    delta_candidate_list = [
        'meta_update_datetime',
        'meta_create_datetime',
        'datetime_modified',
        'datetime_added',
        'rollup_datetime_modified',
        'date_update',
        'date_create',
        'modified_on',
        'created_on',
    ]
    delta_candidate_sort_mapping = {x[1]: x[0] for x in enumerate(delta_candidate_list)}
    delta_cols = [col.name for col in column_list if col.name in delta_candidate_list]
    delta_cols_sorted = sorted(delta_cols, key=lambda x: delta_candidate_sort_mapping.get(x))
    delta_cols_mapping = {x[1]: x[0] for x in enumerate(delta_cols_sorted)}
    for c in column_list:
        if c.name in delta_cols:
            c.delta_column = delta_cols_mapping.get(c.name)
        if c.name in primary_key_list:
            c.uniqueness = True
        yield c
