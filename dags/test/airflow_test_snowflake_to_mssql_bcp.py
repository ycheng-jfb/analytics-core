import csv
from functools import cached_property
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List

import pendulum
from airflow import DAG
from airflow.models import BaseOperator
from snowflake.connector import DictCursor

from include.airflow.hooks.bash import BashHook
from include.airflow.hooks.mssql import MsSqlOdbcHook
from include.airflow.hooks.snowflake import SnowflakeHook
from include.config import conn_ids, owners
from include.config.email_lists import data_integration_support
from include.utils.context_managers import ConnClosing
from include.utils.snowflake import generate_query_tag_cmd
from include.utils.sql import build_bcp_in_command
from include.utils.string import unindent_auto


class SnowflakeToMsSql(BaseOperator):
    def __init__(
        self,
        snowflake_database: str,
        snowflake_schema: str,
        snowflake_table: str,
        mssql_conn_id: str,
        mssql_target_database: str,
        mssql_target_schema: str,
        mssql_target_table: str,
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
        super().__init__(**kwargs)

    @cached_property
    def bash_hook(self) -> BashHook:
        return BashHook()

    @cached_property
    def snowflake_hook(self) -> SnowflakeHook:
        return SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)

    @cached_property
    def mssql_hook(self) -> MsSqlOdbcHook:
        return MsSqlOdbcHook(mssql_conn_id=self.mssql_conn_id, database=self.mssql_target_database)

    @property
    def mssql_full_table_name(self) -> str:
        return f"{self.mssql_target_database}.{self.mssql_target_schema}.{self.mssql_target_table}"

    @property
    def snowflake_full_table_name(self) -> str:
        return f"{self.snowflake_database}.{self.snowflake_schema}.{self.snowflake_table}"

    @cached_property
    def column_list(self) -> List[str]:
        with self.snowflake_hook.get_conn() as con:
            cur = con.cursor()
            cur.execute(
                f"""
                SELECT COLUMN_NAME FROM {self.snowflake_database}.INFORMATION_SCHEMA.COLUMNS
                WHERE lower(TABLE_SCHEMA) = '{self.snowflake_schema.lower()}'
                AND lower(TABLE_NAME) = '{self.snowflake_table.lower()}'
                ORDER BY ORDINAL_POSITION
            """
            )
            rows = cur.fetchall()
            return [row[0] for row in rows]

    @cached_property
    def snowflake_cmd(self) -> str:
        snowflake_cmd = unindent_auto(
            f"""
            SELECT {', '.join(self.column_list)}
            FROM {self.snowflake_full_table_name}
        """
        )
        return unindent_auto(snowflake_cmd)

    @property
    def pre_sql_cmd(self) -> str:
        return f"DELETE FROM {self.mssql_full_table_name}"

    def delete_mssql(self, dry_run: bool = False) -> None:
        if dry_run:
            print(self.pre_sql_cmd)
            return
        with ConnClosing(self.mssql_hook.get_conn()) as con:
            cur = con.cursor()
            cur.execute(self.pre_sql_cmd)

    def snowflake_to_mssql(self, dry_run: bool = False) -> None:
        if dry_run:
            print(self.snowflake_cmd)
            return
        with ConnClosing(self.snowflake_hook.get_conn()) as conn:
            query_tag = generate_query_tag_cmd(self.dag_id, self.task_id)
            cur = conn.cursor(cursor_class=DictCursor)
            cur.execute(query_tag)
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
                            quoting=csv.QUOTE_NONE,
                            escapechar='\\',
                        )
                        writer.writerows(rows)
                        batch_count += 1
                        print(f"fetching more rows: batch {batch_count}")
                    bcp_command = build_bcp_in_command(
                        database=self.mssql_target_database,
                        schema=self.mssql_target_schema,
                        table=self.mssql_target_table,
                        path=temp_file,
                        server=self.mssql_hook.conn.host,
                        login=self.mssql_hook.conn.login,
                        password=self.mssql_hook.conn.password,
                        field_delimiter='0x01',
                        record_delimiter='0x02',
                    )
                    self.bash_hook.run_command(bash_command=bcp_command)

    def dry_run(self) -> None:
        self.delete_mssql(dry_run=True)
        self.snowflake_to_mssql(dry_run=True)

    def execute(self, context=None):
        self.delete_mssql()
        self.snowflake_to_mssql()


default_args = {
    'start_date': pendulum.datetime(2019, 11, 19, tz='America/Los_Angeles'),
    'retries': 0,
    'owner': owners.data_integrations,
    'email': data_integration_support,
}

dag = DAG(
    dag_id='airflow_test_snowflake_to_mssql_bcp',
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
)

with dag:
    oocl_shipment_to_mssql = SnowflakeToMsSql(
        task_id='oocl_shipment_dataset_to_mssql',
        snowflake_database="reporting_base",
        snowflake_schema="gsc",
        snowflake_table="lc_oocl_shipment_dataset",
        mssql_target_table='oocl_shipment_dataset',
        mssql_target_database='ultrawarehouse',
        mssql_target_schema='rpt',
        mssql_conn_id=conn_ids.MsSql.dbp40_app_airflow_rw,
    )
