import csv
from functools import cached_property
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, List, Optional, TypedDict

import pandas as pd
from airflow.models import BaseOperator
from include.airflow.hooks.bash import BashHook
from include.airflow.hooks.mssql import MsSqlOdbcHook
from include.airflow.hooks.snowflake import SnowflakeHook
from include.airflow.operators.snowflake import SnowflakeSqlOperator
from include.airflow.operators.watermark import BaseTaskWatermarkOperator
from include.config import conn_ids
from include.utils.context_managers import ConnClosing
from include.utils.snowflake import Column, generate_query_tag_cmd
from include.utils.sql import build_bcp_in_command
from include.utils.string import unindent_auto
from sqlalchemy import types


class SnowflakeSqlToMSSqlOperator(SnowflakeSqlOperator):
    def __init__(
        self,
        tgt_database: str,
        tgt_schema: str,
        tgt_table: str,
        warehouse: str = 'DA_WH_ETL_LIGHT',
        snowflake_conn_id=conn_ids.Snowflake.default,
        mssql_conn_id=conn_ids.MsSql.default,
        if_exists="replace",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.warehouse = warehouse
        self.mssql_conn_id = mssql_conn_id
        self.tgt_database = tgt_database
        self.tgt_schema = tgt_schema
        self.tgt_table = tgt_table
        self.if_exists = if_exists

    @cached_property
    def mssql_hook(self):
        return MsSqlOdbcHook(
            mssql_conn_id=self.mssql_conn_id, database=self.tgt_database, schema=self.tgt_schema
        )

    def execute(self, context=None):
        with self.snowflake_hook.get_conn() as cnx:
            sql = self.get_sql_cmd(self.sql_or_path)
            query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
            cur = cnx.cursor()
            cur.execute(query_tag)
            df = pd.read_sql_query(sql=sql, con=cnx, index_col=None)
            df.columns = [x.lower() for x in df.columns]
        with self.mssql_hook.get_sqlalchemy_connection() as conn_mssql:
            df.to_sql(
                name=self.tgt_table,
                schema=self.tgt_schema,
                con=conn_mssql,
                index=False,
                if_exists=self.if_exists,
            )


class SnowflakeSqlToMSSqlOperatorTruncateAndLoad(SnowflakeSqlToMSSqlOperator):
    @property
    def sql_truncate(self):
        return f"DELETE FROM {self.tgt_database}.{self.tgt_schema}.{self.tgt_table}"

    def execute(self, context=None):
        with self.snowflake_hook.get_conn() as cnx:
            sql = self.get_sql_cmd(self.sql_or_path)
            query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
            cur = cnx.cursor()
            cur.execute(query_tag)
            df = pd.read_sql_query(sql=sql, con=cnx, index_col=None)
            df.columns = [x.lower() for x in df.columns]
        with self.mssql_hook.get_sqlalchemy_connection() as conn_mssql:
            conn_mssql.execute(self.sql_truncate)
            df.to_sql(
                name=self.tgt_table,
                schema=self.tgt_schema,
                con=conn_mssql,
                index=False,
                if_exists=self.if_exists,
            )


class SnowflakeSqlToMSSqlWatermarkOperator(SnowflakeSqlToMSSqlOperator, BaseTaskWatermarkOperator):
    """
    The operator loads Snowflake data to MSSQL incrementally.
    If custom_sql is passed,
    Note:
        - The column_list should be in the same order as that of the columns in custom_sql if provided
    Args:
        column_list: List of columns that are to be created in MSSQL
        watermark_column: Column used for incremental load watermarking
        tgt_database: Target MSSQL database
        tgt_schema: Target MSSQL schema
        tgt_table: Target MSSQL table
        src_database: Source Snowflake database
        src_schema: (Optional) Source Snowflake schema.
            By default, picks tgt_schema if not specified
        src_table: (Optional) Source Snowflake table. By default, picks tgt_table if not specified
        custom_sql: (Optional) Snowflake SQL query to execute
            which overrides the default SELECT * FROM tbl_name WHERE watermark_column > watermark
            condition statement.
        strict_inequality: (Optional) Controls whether where clause is ``>`` or ``>=``
        initial_load: (Optional) If you want the target table to be created at runtime
        initial_load_value: (Optional) On first run, the value that should be used for low watermark
            Default is '1900-01-01'
    """

    def __init__(
        self,
        column_list,
        watermark_column,
        src_database,
        src_schema=None,
        src_table=None,
        custom_sql=None,
        strict_inequality=None,
        initial_load=True,
        initial_load_value='1900-01-01',
        **kwargs,
    ):
        super().__init__(sql_or_path='', **kwargs)
        self.column_list = column_list
        self.watermark_column = watermark_column
        self.custom_sql = custom_sql
        self.src_database = src_database
        self.src_schema = src_schema or self.tgt_schema
        self.src_table = src_table or self.tgt_table
        self.strict_inequality = strict_inequality
        self.initial_load = initial_load
        self.initial_load_value = initial_load_value
        self.sql_or_path = None

    def build_query(self):
        inequality = '>' if self.strict_inequality else '>='
        select_list = (
            ', '.join([f"{x.source_name}" for x in self.column_list]) if self.column_list else '*'
        )
        where_clause = ''
        if self.watermark_column:
            where_clause = (
                f"WHERE s.{self.watermark_column} {inequality} '{self.get_low_watermark()}'"
            )
        full_table_name = f"{self.src_database}.{self.src_schema}.{self.src_table}"
        query = f"""
        SELECT DISTINCT {select_list}
        FROM {full_table_name} s
        {where_clause}"""
        query = self.custom_sql if self.custom_sql else query
        return unindent_auto(query)

    def build_high_watermark_command(self):
        table_name = f"{self.src_database}.{self.src_schema}.{self.src_table}"
        cmd = f"""
            SELECT max({self.watermark_column})
            FROM {table_name}
        """
        return unindent_auto(cmd)

    def get_high_watermark(self):
        cmd = self.build_high_watermark_command()
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        with snowflake_hook.get_conn() as cnx:
            self.log.info("Executing: %s", cmd)
            cur = cnx.cursor()
            query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
            cur.execute(query_tag)
            cur.execute(cmd)
            k = cur.fetchone()
            val = k[0]
            if hasattr(val, 'isoformat'):
                return val.isoformat()
            else:
                return val

    def return_sql_alchemy_datatype(self, datatype):
        if 'VARCHAR' in datatype:
            return types.VARCHAR
        elif 'DATETIME' in datatype:
            return types.DateTime
        elif datatype == 'BIGINT':
            return types.BIGINT
        elif 'INT' in datatype:
            return types.INTEGER
        elif datatype == 'DATE':
            return types.DATE
        elif 'DECIMAL' in datatype:
            return types.NUMERIC
        elif datatype in ('BIT', 'BOOLEAN'):
            return types.Boolean
        else:
            return datatype

    @property
    def base_table_meta_cols(self) -> List[Column]:
        base_table_meta_cols = [
            Column('meta_create_datetime', 'DATETIME'),
            Column('meta_update_datetime', 'DATETIME'),
        ]
        return base_table_meta_cols

    @property
    def merge_update_names_str(self) -> str:
        update_names_str = ',\n\t'.join([f"t.{x.name} = s.{x.name}" for x in self.column_list])
        return update_names_str

    def get_tbl_ddl(self, column_list) -> str:
        newline_char = '\n'
        cmd = f"""
        IF OBJECT_ID('{self.tgt_database}.{self.tgt_schema}.{self.tgt_table}') IS NULL
        BEGIN
        CREATE TABLE {self.tgt_database}.{self.tgt_schema}.{self.tgt_table}(
            {f',{newline_char}    '.join([f"{x.name} {x.type}" for x in column_list]
                                         + [f"{x.name} {x.type}" for x in self.base_table_meta_cols])}
        )
        END
        """
        return unindent_auto(cmd)

    def merge_into_base_table(self) -> str:
        uniqueness_join = '\n    AND '.join(
            [
                f"COALESCE(t.{x.name},'') = COALESCE(s.{x.name},'')"
                for x in self.column_list
                if x.uniqueness
            ]
        )
        col_select_list = ', '.join([x.name for x in self.column_list])
        full_col_select_list = ', '.join(
            [x.name for x in self.column_list] + [x.name for x in self.base_table_meta_cols]
        )
        cmd = f"""
            MERGE INTO {self.tgt_database}.{self.tgt_schema}.{self.tgt_table} t
            USING {self.tgt_database}.{self.tgt_schema}.{self.tgt_table}_stg s
                ON {uniqueness_join}
            WHEN NOT MATCHED THEN INSERT (
                {full_col_select_list}
            )
            VALUES (
                {col_select_list}, GETDATE(), GETDATE()
            )
            WHEN MATCHED THEN
            UPDATE SET
                {self.merge_update_names_str},
                t.meta_update_datetime = GETDATE();
            """
        return unindent_auto(cmd)

    def watermark_execute(self, context=None):
        with self.snowflake_hook.get_conn() as cnx:
            sql = self.get_sql_cmd(self.sql_or_path)
            query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
            cur = cnx.cursor()
            cur.execute(query_tag)
            row_count = 0
            batch_number = 1
            for rows in pd.read_sql_query(
                sql=sql, con=cnx, index_col=None, params=self.parameters, chunksize=100000
            ):
                rows.columns = [x.name.lower() for x in self.column_list]
                with self.mssql_hook.get_sqlalchemy_connection(
                    engine_kwargs={'fast_executemany': True}
                ) as conn_mssql:
                    if batch_number == 1:
                        stg_delete_cmd = f"""IF OBJECT_ID('{self.tgt_database}.{self.tgt_schema}.{self.tgt_table}_stg')
                        IS NOT NULL
                        BEGIN
                        DELETE FROM {self.tgt_database}.{self.tgt_schema}.{self.tgt_table}_stg
                        END"""
                        self.log.info(f"Executing: {stg_delete_cmd}")
                        conn_mssql.execute(unindent_auto(stg_delete_cmd))
                    rows.to_sql(
                        name=self.tgt_table + '_stg',
                        schema=f"{self.tgt_database}.{self.tgt_schema}",
                        con=conn_mssql,
                        index=False,
                        if_exists=self.if_exists,
                        dtype={
                            x.name: self.return_sql_alchemy_datatype(x.type)
                            for x in self.column_list
                        },
                    )
                    self.log.info(f"Batch {batch_number} rows extracted: {rows.shape[0]}")
                    row_count += rows.shape[0]
                    batch_number += 1
            self.log.info(f"Stage table rows extracted: {row_count}")

    def execute(self, context=None):
        self.sql_or_path = self.build_query()
        self.watermark_pre_execute()
        self.parameters = {"low_watermark": self.low_watermark}
        self.watermark_execute()
        with ConnClosing(self.mssql_hook.get_conn()) as con:
            cur = con.cursor()
            if self.initial_load:
                cur.execute(self.get_tbl_ddl(self.column_list))
            merge_cmd = self.merge_into_base_table()
            self.log.info(f"Executing merge command: {merge_cmd}")
            cur.execute(merge_cmd)
        self.watermark_post_execute()


class SnowflakeToMSSqlFastMany(SnowflakeSqlToMSSqlOperatorTruncateAndLoad):
    def __init__(
        self,
        chunk_size=100000,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.chunk_size = chunk_size

    def execute(self, context=None):
        with ConnClosing(self.snowflake_hook.get_conn()) as conn:
            query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
            cur = conn.cursor()
            cur.execute(query_tag)
            sql = self.get_sql_cmd(self.sql_or_path)

            with self.mssql_hook.get_sqlalchemy_connection(
                engine_kwargs={'fast_executemany': True}
            ) as conn_mssql:
                conn_mssql.execute(self.sql_truncate)

                cnt = 1
                for rows in pd.read_sql_query(sql=sql, con=conn, chunksize=self.chunk_size):
                    rows.to_sql(
                        name=self.tgt_table,
                        schema=self.tgt_schema,
                        con=conn_mssql,
                        chunksize=self.chunk_size,
                        if_exists=self.if_exists,
                        index=False,
                    )
                    self.log.info(f"Batch {cnt} completed.....")
                    print(f"Batch {cnt} completed.....")
                    cnt += 1


class ColumnMetaData(TypedDict):
    colname: str
    data_type: str
    precision: Optional[int]
    scale: Optional[int]
    select_expr: str


class SnowflakeToMsSqlBCPOperator(BaseOperator):
    def __init__(
        self,
        snowflake_database: str,
        snowflake_schema: str,
        snowflake_table: str,
        mssql_conn_id: str,
        mssql_target_database: str,
        mssql_target_schema: str,
        mssql_target_table: str,
        unique_columns: List[str],
        watermark_column: str,
        snowflake_conn_id: str = conn_ids.Snowflake.default,
        **kwargs,
    ) -> None:
        self.snowflake_conn_id = snowflake_conn_id
        self.mssql_conn_id = mssql_conn_id
        self.mssql_target_database = mssql_target_database
        self.mssql_target_schema = mssql_target_schema
        self.mssql_target_table = mssql_target_table
        self.snowflake_database = snowflake_database
        self.snowflake_schema = snowflake_schema
        self.snowflake_table = snowflake_table
        self.unique_columns = unique_columns
        self.watermark_column = watermark_column
        super().__init__(**kwargs)

    def return_sql_alchemy_datatype(
        self, datatype: str, precision: Optional[int] = None, scale: Optional[int] = None
    ) -> Any:
        if 'VARCHAR' in datatype or 'TEXT' in datatype:
            if precision:
                return types.VARCHAR(length=precision)
            return types.VARCHAR
        elif 'DATETIME' in datatype or 'TIMESTAMP' in datatype:
            return types.DateTime
        elif datatype == 'BIGINT':
            return types.BIGINT
        elif 'NUMBER' in datatype or 'DECIMAL' in datatype:
            if precision and scale:
                return types.NUMERIC(precision=precision, scale=scale)
            return types.NUMERIC
        elif 'INT' in datatype:
            return types.INTEGER
        elif datatype == 'FLOAT':
            return types.FLOAT
        elif datatype == 'DATE':
            return types.DATE
        elif datatype in ('BIT', 'BOOLEAN'):
            return types.Boolean
        else:
            return datatype

    @cached_property
    def snowflake_hook(self) -> SnowflakeHook:
        return SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)

    @cached_property
    def mssql_hook(self) -> MsSqlOdbcHook:
        return MsSqlOdbcHook(
            mssql_conn_id=self.mssql_conn_id,
            database=self.mssql_target_database,
            schema=self.mssql_target_schema,
        )

    @cached_property
    def bash_hook(self) -> BashHook:
        return BashHook()

    @property
    def snowflake_full_table_name(self) -> str:
        return f"{self.snowflake_database}.{self.snowflake_schema}.{self.snowflake_table}"

    @cached_property
    def column_list(self) -> List[ColumnMetaData]:
        with self.snowflake_hook.get_conn() as con:
            cur = con.cursor()
            query_tag_sql = generate_query_tag_cmd(self.dag_id, self.task_id)
            cur.execute(query_tag_sql)
            cur.execute(
                f"""
                SELECT
                    COLUMN_NAME,
                    DATA_TYPE,
                    CASE DATA_TYPE
                        WHEN 'VARCHAR' THEN CHARACTER_MAXIMUM_LENGTH
                        WHEN 'TEXT' THEN CHARACTER_MAXIMUM_LENGTH
                        WHEN 'NUMBER' THEN NUMERIC_PRECISION
                    END AS PRECISION,
                    CASE DATA_TYPE
                        WHEN 'NUMBER' THEN NUMERIC_SCALE
                    END AS SCALE
                FROM {self.snowflake_database}.INFORMATION_SCHEMA.COLUMNS
                WHERE lower(TABLE_SCHEMA) = '{self.snowflake_schema.lower()}'
                AND lower(TABLE_NAME) = '{self.snowflake_table.lower()}'
                ORDER BY ORDINAL_POSITION
            """
            )
            rows = cur.fetchall()
            col_list: List[ColumnMetaData] = [
                {
                    "colname": row[0],
                    "data_type": row[1],
                    "precision": row[2],
                    "scale": row[3],
                    "select_expr": (
                        f"cast({row[0]} as timestamp_ntz) as {row[0]}"
                        if row[1].upper() == "TIMESTAMP_LTZ"
                        else (
                            f"nvl(cast({row[0]} as int), 0) as {row[0]}"
                            if row[1].upper() == "BOOLEAN"
                            else row[0]
                        )
                    ),
                }
                for row in rows
            ]
            return col_list

    @cached_property
    def filter_condition(self) -> str:
        filter_condition = ""
        with ConnClosing(self.mssql_hook.get_conn()) as con:
            cur = con.cursor()
            cur.execute(
                f""" \
                IF OBJECT_ID('{self.mssql_target_schema}.{self.mssql_target_table}') IS NOT NULL
                    BEGIN
                        SELECT MAX({self.watermark_column}) as LOW_WATERMARK
                        FROM {self.mssql_target_schema}.{self.mssql_target_table}
                    END
                ELSE
                    BEGIN
                        SELECT NULL AS LOW_WATERMARK
                    END
            """
            )
            rows = cur.fetchall()
            max_date: Optional[str] = None
            if rows:
                max_date = rows[0][0]
                if max_date and max_date != "None":
                    filter_condition = f"{self.watermark_column} > '{max_date}'"
        return filter_condition

    @cached_property
    def snowflake_cmd(self) -> str:
        snowflake_cmd = unindent_auto(
            f""" \
            SELECT
                {', '.join(i["select_expr"] for i in self.column_list)}
            FROM
                {self.snowflake_full_table_name}
        """
        )
        if self.filter_condition:
            snowflake_cmd += f"WHERE {self.filter_condition}"
        return unindent_auto(snowflake_cmd)

    @property
    def pre_sql_cmd(self) -> str:
        return f"""IF OBJECT_ID('{self.mssql_target_schema}.{self.mssql_target_table}_stg') IS NOT NULL
        BEGIN
        DELETE FROM {self.mssql_target_schema}.{self.mssql_target_table}_stg
        END"""

    @property
    def uniqueness_join(self) -> str:
        return " AND ".join(f"t.{col} = s.{col}" for col in self.unique_columns)

    @property
    def merge_update_names_str(self) -> str:
        return ",".join(f"t.{i['colname']} = s.{i['colname']}" for i in self.column_list)

    @property
    def mssql_merge_cmd(self) -> str:
        cmd = unindent_auto(
            f""" \
            MERGE INTO {self.mssql_target_schema}.{self.mssql_target_table} t
            USING {self.mssql_target_schema}.{self.mssql_target_table}_stg s
                ON {self.uniqueness_join}
            WHEN NOT MATCHED THEN INSERT (
                {', '.join(i["colname"] for i in self.column_list)}
            )
            VALUES (
                {', '.join(i["colname"] for i in self.column_list)}
            )
            WHEN MATCHED THEN
            UPDATE SET
                {self.merge_update_names_str};
        """
        )
        return cmd

    def dry_run(self) -> None:
        print(self.pre_sql_cmd)
        print(self.snowflake_cmd)
        print(self.mssql_merge_cmd)

    def execute(self, context=None):
        with ConnClosing(self.mssql_hook.get_conn()) as con:
            cur = con.cursor()
            print(self.pre_sql_cmd)
            cur.execute(self.pre_sql_cmd)
        with ConnClosing(self.snowflake_hook.get_conn()) as conn:
            query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
            cur = conn.cursor()
            cur.execute(query_tag)
            print(self.snowflake_cmd)
            cur.execute(self.snowflake_cmd)
            batch_count = 0
            with TemporaryDirectory() as td:
                while rows := cur.fetchmany(20000):
                    temp_file = Path(td, "tmpfile").as_posix()
                    with open(temp_file, "w+") as f:
                        writer = csv.writer(
                            f,
                            dialect="unix",
                            delimiter="\x01",
                            lineterminator="\x02",
                            quoting=csv.QUOTE_MINIMAL,
                            escapechar='\\',
                        )
                        writer.writerows(rows)
                        batch_count += 1
                        print(f"fetching more rows: batch {batch_count}")
                    bcp_command = build_bcp_in_command(
                        database=self.mssql_target_database,
                        schema=self.mssql_target_schema,
                        table=f"{self.mssql_target_table}_stg",
                        path=temp_file,
                        server=self.mssql_hook.conn.host,
                        login=self.mssql_hook.conn.login,
                        password=self.mssql_hook.conn.password,
                        field_delimiter="0x01",
                        record_delimiter="0x02",
                    )
                    self.bash_hook.run_command(bash_command=bcp_command)
        with ConnClosing(self.mssql_hook.get_conn()) as con:
            cur = con.cursor()
            print(self.mssql_merge_cmd)
            cur.execute(self.mssql_merge_cmd)
