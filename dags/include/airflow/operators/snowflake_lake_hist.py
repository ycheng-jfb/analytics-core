import logging
from pathlib import Path
from typing import List, Optional, Union

from functools import cached_property
from snowflake.connector import DictCursor

from edm.acquisition.configs import get_lake_consolidated_table_config, get_table_config
from include import DAGS_DIR
from include.airflow.hooks.snowflake import SnowflakeHook
from include.airflow.operators.snowflake import (
    BaseSnowflakeOperator,
    get_effective_database,
)
from include.config import snowflake_roles
from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig,
)
from include.utils.acquisition.table_config import Column
from include.utils.string import camel_to_snake, indent, unindent_auto
from include.utils.snowflake import generate_query_tag_cmd


class SnowflakeLakeHistoryOperator(BaseSnowflakeOperator):
    DATABASE_CONFIG_VARIABLE_KEY = "database_config"
    ROLE = snowflake_roles.etl_service_account
    EFF_START_TIMESTAMP_COL_TYPE = "TIMESTAMP_LTZ"
    DEFAULT_TIMEZONE = "America/Los_Angeles"
    STR_INDENT = ""
    NAMESPACE = "lake_history"
    LAKE_HISTORY_DATABASE = "lake_history"

    def __init__(self, table, warehouse=None, **kwargs):
        self.table_config = get_table_config(table_name=table)
        self.warehouse = warehouse
        self.table = self.table_config.target_table
        self.schema = self.table_config.target_schema
        self.database = self.table_config.target_database
        self.column_list = self.table_config.column_list
        self.column_name_list = self.table_config.column_list.column_name_list
        self.cluster_by = self.table_config.cluster_by
        self.archive_database = self.table_config.archive_database
        self.source_table = camel_to_snake(
            self.table_config.table.strip("[").strip("]")
        )
        self.watermark_column = self.table_config.watermark_column
        self.snowflake_conn_id = "snowflake_default"
        if "." in self.source_table:
            raise ValueError("table cannot have a '.'")
        self._command_debug_log = ""

        if self.is_delete_table_exists:
            delete_log_table_config = get_table_config(self.delete_table_path)
            self.delete_log_table = delete_log_table_config.target_table
            self.delete_log_schema = delete_log_table_config.target_schema
            self.delete_log_database = delete_log_table_config.target_database
            self.delete_log_column_list = delete_log_table_config.column_list
            self.delete_log_column_name_list = (
                self.delete_log_column_list.column_name_list
            )
            self.delete_log_watermark_column = delete_log_table_config.watermark_column
        else:
            self.delete_log_table = None

        super().__init__(
            warehouse=self.warehouse,
            schema=self.schema,
            role=self.ROLE,
            timezone=self.DEFAULT_TIMEZONE,
            autocommit=False,
            task_id=f"{self.LAKE_HISTORY_DATABASE}.{self.schema}.{self.history_table}",
            **kwargs,
        )

    @cached_property
    def delete_table_path(self):
        if Path(
            DAGS_DIR,
            "edm",
            "acquisition",
            "configs",
            "lake",
            f"{self.schema}_cdc",
            f"{self.source_table}__all.py",
        ).exists():
            delete_config_file = f"lake.{self.schema}_cdc.{self.source_table}__all"
        elif Path(
            DAGS_DIR,
            "edm",
            "acquisition",
            "configs",
            "lake",
            f"{self.schema}_cdc",
            f"{self.source_table}__del.py",
        ).exists():
            delete_config_file = f"lake.{self.schema}_cdc.{self.source_table}__del"
        elif Path(
            DAGS_DIR,
            "edm",
            "acquisition",
            "configs",
            "lake",
            self.schema,
            f"{self.source_table}_delete_log.py",
        ).exists():
            delete_config_file = f"lake.{self.schema}.{self.source_table}_delete_log"
        else:
            delete_config_file = None
        return delete_config_file

    @cached_property
    def target_database(self):
        self.database = get_effective_database(self.LAKE_HISTORY_DATABASE, self)
        return self.database

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
        """Verify correct database is used before returning connection"""
        self.cnx = self.snowflake_hook.get_conn()
        self.ensure_correct_database(self.cnx)
        return self.cnx

    def _get_table_name(self, table_type=None):
        if table_type:
            return self.table.replace('"', "").lower() + f"_{table_type}"
        else:
            return self.table.replace('"', "").lower()

    @property
    def watermark_param(self):
        return f"{self.schema}.{self.table}"

    @property
    def history_table_full_name(self):
        return f"{self.target_database}.{self.schema}.{self.table}"

    @property
    def history_table(self):
        return self._get_table_name()

    @cached_property
    def is_delete_table_exists(self):
        return True if self.delete_table_path else False

    @staticmethod
    def _get_col_ddls(column_list) -> str:
        return f',\n{" " * 16}'.join(
            [f"{x.name} {x.type}{x.default_value}" for x in column_list]
        )

    @property
    def column_ddls(self):
        return self._get_col_ddls(self.column_list)

    @staticmethod
    def _is_timestamp_col(data_type) -> bool:
        return data_type.lower()[0:4] in ("date", "time")

    @property
    def delta_col_list(self):
        delta_cols_list = [x for x in self.column_list if x.delta_column is not False]
        num_cols = len(delta_cols_list)
        if num_cols < 1:
            raise ValueError("For lake hist you must have one delta column")
        if num_cols < 2:
            delta_cols_list.append(delta_cols_list[0])
        return sorted(delta_cols_list, key=lambda x: x.delta_column)

    @property
    def delete_log_delta_col_list(self):
        if self.is_delete_table_exists:
            delta_cols_list = [
                x for x in self.delete_log_column_list if x.delta_column is not False
            ]
            return sorted(delta_cols_list, key=lambda x: x.delta_column)
        else:
            return "datetime_modified"

    @property
    def type_2_timestamp_col(self):
        col = self.delta_col_list[0]
        if not self._is_timestamp_col(col.type):
            raise ValueError(
                "For lake hist, delta column must have data type TIMESTAMP_*"
            )
        return col

    @property
    def hist_table_meta_cols(self) -> List[Column]:
        hist_table_meta_cols = [
            Column("effective_start_datetime", "TIMESTAMP_LTZ(9)", uniqueness=True),
            Column("effective_end_datetime", "TIMESTAMP_LTZ(9)"),
            Column("is_current", "BOOLEAN"),
            Column("meta_row_is_deleted", "BOOLEAN"),
            Column("meta_row_source", "VARCHAR"),
            Column("meta_row_hash", "INT"),
            Column(
                "meta_create_datetime",
                "TIMESTAMP_LTZ(9)",
                default_value="CURRENT_TIMESTAMP",
            ),
            Column(
                "meta_update_datetime",
                "TIMESTAMP_LTZ(9)",
                default_value="CURRENT_TIMESTAMP",
            ),
        ]
        return hist_table_meta_cols

    @property
    def delta_table_meta_cols(self) -> List[Column]:
        delta_table_meta_cols = [
            Column("meta_row_is_deleted", "BOOLEAN"),
            Column("meta_row_source", "VARCHAR"),
            Column("meta_row_hash", "INT"),
            Column("prev_meta_row_hash", "INT"),
            Column("rno", "INT"),
            Column("effective_start_datetime", "TIMESTAMP_LTZ(9)"),
            Column("effective_end_datetime", "TIMESTAMP_LTZ(9)"),
        ]
        return delta_table_meta_cols

    @property
    def pk_col_names(self):
        pk_col_names = [x.name for x in self.column_list if x.uniqueness is True]
        if not pk_col_names:
            pk_col_names = [x.name for x in self.column_list]
        return pk_col_names

    @property
    def pk_names_str(self) -> str:
        pk_names_str = ", ".join(self.pk_col_names)
        return pk_names_str

    def pk_names_str_with_alias(self, alias="s") -> str:
        pk_names_str = ", ".join([f"{alias}.{x}" for x in self.pk_col_names])
        return pk_names_str

    @property
    def primary_key_col_ddl(self):
        return f",\n\t\t\t\tPRIMARY KEY ({self.pk_names_str}, effective_start_datetime)"

    @property
    def hash_col_names_str(self):
        hash_col_list = [
            x for x in self.column_name_list if x != f"{self.type_2_timestamp_col.name}"
        ]
        hash_col_list.extend(["meta_row_is_deleted"])
        return ", ".join(hash_col_list)

    def select_list(self, tab_space=1):
        str_indent = "\t" * tab_space
        return f",\n{str_indent}".join(self.column_name_list)

    @property
    def uniqueness_join(self) -> str:
        unique_col_names = [x.name for x in self.column_list if x.uniqueness]
        return "\n    AND ".join(
            [f"equal_null(t.{x}, s.{x})" for x in unique_col_names]
        )

    @property
    def delete_log_table_type(self):
        if self.is_delete_table_exists:
            delete_table_name = self.delete_table_path.rsplit(".", 1)[1]
            if delete_table_name == f"{self.source_table}__all":
                return f"{self.schema}_cdc__all"
            elif delete_table_name == f"{self.source_table}__del":
                return f"{self.schema}_cdc__del"
            else:
                return f"{self.schema}_delete_log"
        else:
            return None

    @property
    def meta_row_delete_flag(self):
        if self.delete_log_table_type:
            if self.delete_log_table_type == f"{self.schema}_cdc__all":
                return "iff(s.repl_action = 'D', TRUE, FALSE)"
            else:
                return "TRUE"
        else:
            return None

    @property
    def delete_update_names_str(self) -> str:
        if self.delete_log_table_type == f"{self.schema}_cdc__all":
            update_names_str = ",\n\t".join(
                [
                    f"t.{x.name} = s.{x.name}"
                    for x in self.column_list
                    if not x.uniqueness and x.name != self.type_2_timestamp_col.name
                ]
            )
        else:
            column_list = [
                x.name
                for x in self.column_list
                if x.name not in self.delete_log_column_name_list
            ]
            update_names_str = ",\n\t".join([f"t.{x} = s.{x}" for x in column_list])
        return update_names_str

    @property
    def delete_update_lag_str(self) -> str:
        update_names_str = ""
        if self.delete_log_table_type == f"{self.schema}_cdc__all":
            update_names_str = ",\n\t\t\t\t\t\t".join(
                [
                    f"LAG(s.{x.name}) over(partition by {self.pk_names_str_with_alias('t')} order by s.effective_start_datetime) AS {x.name}"  # noqa: E501
                    for x in self.column_list
                    if not x.uniqueness and x.name != self.type_2_timestamp_col.name
                ]
            )
        else:
            column_list = [
                x.name
                for x in self.column_list
                if x.name not in self.delete_log_column_name_list
            ]
            if len(column_list) > 0:
                update_names_str = ",\n\t\t\t\t\t\t".join(
                    [
                        f"LAG(s.{x}) over(partition by {self.pk_names_str_with_alias('t')} order by s.effective_start_datetime) AS {x}"  # noqa: E501
                        for x in column_list
                    ]
                )
        return update_names_str

    @property
    def delete_log_select_list(self):
        str_indent = "\t" * 8
        if self.delete_log_table:
            delta_cols_list = [
                x for x in self.delete_log_column_list if x.delta_column is not False
            ]
            delta_cols = sorted(delta_cols_list, key=lambda x: x.delta_column)
            delete_log_type_2_timestamp_col = delta_cols[0].name
            delete_log_column_name_list = self.delete_log_column_name_list
            if self.type_2_timestamp_col.name != delete_log_type_2_timestamp_col:
                delete_log_column_name_list.remove(delete_log_type_2_timestamp_col)
            delete_log_select_list = []
            for col in self.column_name_list:
                if col == self.type_2_timestamp_col.name:
                    delete_log_select_list.append(
                        f"s.{delete_log_type_2_timestamp_col} AS {self.type_2_timestamp_col.name}"
                    )
                elif col in delete_log_column_name_list:
                    if self.delete_log_table_type == f"{self.schema}_cdc__all":
                        delete_log_select_list.append(
                            f"iff(s.repl_action = 'D', nvl(s.{col}, t.{col}), s.{col}) AS {col}"
                        )
                    else:
                        delete_log_select_list.append(f"s.{col}")
                else:
                    delete_log_select_list.append(f"t.{col}")
            return f",\n{str_indent}".join(delete_log_select_list)
        else:
            return None

    @property
    def hist_table_surrogate_key_name(self):
        return f"{self.history_table}_hist_id"

    def get_table_meta_col_ddls(self, table_type="hist") -> str:
        if table_type == "hist":
            meta_cols_ddls = self.hist_table_meta_cols
        else:
            meta_cols_ddls = self.delta_table_meta_cols
        meta_col_ddls = self._get_col_ddls(meta_cols_ddls)
        if meta_col_ddls:
            meta_col_ddls = ",\n\t\t\t\t" + meta_col_ddls
        return meta_col_ddls or ""

    @property
    def ddl_hist_table(self) -> str:
        cluster_by = f"\nCLUSTER BY ({self.cluster_by})\n" if self.cluster_by else ""
        surr_key_ddl = (
            f"{self.hist_table_surrogate_key_name} INT IDENTITY,\n    "
            if self.hist_table_surrogate_key_name
            else ""
        )
        cmd = f"""
            CREATE TABLE IF NOT EXISTS {self.history_table_full_name} (
                {surr_key_ddl}{self.column_ddls}{self.get_table_meta_col_ddls()}{self.primary_key_col_ddl}
            ){cluster_by};
            """
        return unindent_auto(cmd)

    @property
    def ddl_stg_table(self) -> str:
        cluster_by = f"\nCLUSTER BY ({self.cluster_by})\n" if self.cluster_by else ""
        cmd = f"""
            CREATE OR REPLACE TEMP TABLE _{self.history_table}_stg (
                {self.column_ddls},
                meta_row_is_deleted BOOLEAN,
                meta_row_source VARCHAR,
                meta_row_hash INT
            ){cluster_by};
            """
        return unindent_auto(cmd)

    @property
    def ddl_delta_table(self) -> str:
        cluster_by = f"\nCLUSTER BY ({self.cluster_by})\n" if self.cluster_by else ""
        cmd = f"""
            CREATE OR REPLACE TEMP TABLE _{self.history_table}_delta (
                {self.column_ddls}{self.get_table_meta_col_ddls('delta')}
            ){cluster_by};
            """
        return unindent_auto(cmd)

    @property
    def dml_stg_table(self) -> str:
        cmd = f"""
            INSERT INTO _{self.history_table}_stg
            SELECT\n\t{self.select_list()},
                meta_row_is_deleted,
                meta_row_source,
                hash({self.hash_col_names_str}) AS meta_row_hash
            FROM
                (
                    SELECT
                        {self.select_list(tab_space=6)},
                        meta_row_is_deleted,
                        meta_row_source,
                        ROW_NUMBER() OVER (PARTITION BY {self.pk_names_str},{self.type_2_timestamp_col.name} ORDER BY meta_row_is_deleted DESC) AS dup_num
                    FROM
                        (
                            SELECT
                                {self.select_list(tab_space=8)},
                                FALSE AS meta_row_is_deleted,
                                '{self.archive_database}_{self.schema}' AS meta_row_source
                            FROM {self.archive_database}.{self.schema}.{self.table}
                            WHERE {self.delta_col_list[0].name} > stg.udf_get_watermark('{self.watermark_param}', '{self.archive_database}.{self.schema}.{self.table}')
            """  # noqa: E501

        if self.is_delete_table_exists:
            delete_select = f"""
                            UNION ALL

                            SELECT
                                {self.delete_log_select_list},
                                {self.meta_row_delete_flag} AS meta_row_is_deleted,
                                '{self.delete_log_table_type}' AS meta_row_source
                            FROM {self.delete_log_database}.{self.delete_log_schema}.{self.delete_log_table} s
                            LEFT JOIN {self.history_table_full_name} AS t
                                ON {self.uniqueness_join}
                                AND t.is_current
                            WHERE s.{self.delete_log_delta_col_list[0].name} > stg.udf_get_watermark('{self.watermark_param}', '{self.delete_log_database}.{self.delete_log_schema}.{self.delete_log_table}')
                        """  # noqa: E501
        else:
            delete_select = ""

        where_clause = f"""
                        ) AS src
                ) AS stg
            WHERE stg.dup_num = 1;"""  # noqa: F541

        return unindent_auto(cmd + delete_select + where_clause)

    @property
    def dml_delta_table(self):
        cmd = f"""
            INSERT INTO _{self.history_table}_delta
            SELECT
                *,
                row_number() OVER(PARTITION BY {self.pk_names_str} ORDER BY {self.type_2_timestamp_col.name}) as rno,
                iff(prev_meta_row_hash is null and rno = 1 and {self.delta_col_list[1].name} != {self.type_2_timestamp_col.name}, {self.delta_col_list[1].name}, {self.type_2_timestamp_col.name}) AS effective_start_datetime,
                coalesce(dateadd(ms, -1, lead({self.type_2_timestamp_col.name}) OVER (PARTITION BY {self.pk_names_str} ORDER BY {self.type_2_timestamp_col.name})), '9999-12-31') AS effective_end_datetime
            FROM
                (
                    SELECT
                        s.*,
                        lag(s.meta_row_hash, 1, t.meta_row_hash) OVER (PARTITION BY {self.pk_names_str_with_alias()} ORDER BY s.{self.type_2_timestamp_col.name}) AS prev_meta_row_hash
                    FROM _{self.history_table}_stg AS s
                    LEFT JOIN {self.history_table_full_name} AS t
                        ON {self.uniqueness_join}
                        AND t.is_current
                    WHERE t.{self.type_2_timestamp_col.name} IS NULL
                        OR s.{self.type_2_timestamp_col.name} >= t.{self.type_2_timestamp_col.name}
                ) AS src
            WHERE NOT equal_null(meta_row_hash, prev_meta_row_hash);
            """  # noqa: E501
        return unindent_auto(cmd)

    @property
    def dml_update_delete_data(self):
        cmd = f"""
            UPDATE _{self.history_table}_delta t
            SET {self.delete_update_names_str}
            FROM
                (
                    SELECT
                        {self.pk_names_str_with_alias('t')},
                        t.effective_start_datetime,
                        {self.delete_update_lag_str}
                    FROM _{self.history_table}_delta t
                    JOIN _{self.history_table}_delta s
                        ON {self.uniqueness_join}
                        AND t.effective_start_datetime > s.effective_start_datetime
                    WHERE t.meta_row_is_deleted
                    AND (NOT s.meta_row_is_deleted OR s.rno = 1)
                ) AS s
            WHERE {self.uniqueness_join}
                AND t.effective_start_datetime = s.effective_start_datetime;
            """

        return unindent_auto(cmd)

    @property
    def dml_update_hist_close_current_record(self):
        cmd = f"""
            UPDATE {self.history_table_full_name} AS t
            SET t.effective_end_datetime = dateadd(MILLISECOND, -1, s.effective_start_datetime),
                t.is_current = FALSE,
                t.meta_update_datetime = current_timestamp::TIMESTAMP_LTZ(9)
            FROM _{self.history_table}_delta AS s
            WHERE t.is_current = TRUE
                AND {self.uniqueness_join}
                AND s.rno = 1;
            """
        return unindent_auto(cmd)

    @property
    def insert_into_hist_table(self) -> str:
        cmd = f"""
            INSERT INTO {self.history_table_full_name}
            (
                {self.select_list()},
                effective_start_datetime,
                effective_end_datetime,
                is_current,
                meta_row_is_deleted,
                meta_row_source,
                meta_row_hash,
                meta_create_datetime,
                meta_update_datetime
            )
            SELECT
                {self.select_list()},
                effective_start_datetime,
                effective_end_datetime,
                iff(effective_end_datetime = '9999-12-31', TRUE, FALSE) AS is_current,
                meta_row_is_deleted,
                meta_row_source,
                meta_row_hash,
                current_timestamp,
                current_timestamp
            FROM _{self.history_table}_delta
            ORDER BY {self.pk_names_str}, effective_start_datetime;
            """
        return unindent_auto(cmd)

    @cached_property
    def is_test(self):
        return self.table.startswith("test")

    def print(self, val):
        self._command_debug_log += val + "\n"
        if not self.is_test:
            print(val)

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
    def is_initial_load(self) -> bool:
        cmd = f"""
            select
                1
            from {self.LAKE_HISTORY_DATABASE}.information_schema.tables l
            where upper(table_name) = '{self.history_table.upper()}'
                and upper(table_schema) = '{self.schema.upper()}'
            limit 1;
            """
        query_tag_cmd = generate_query_tag_cmd(dag_id=self.dag_id, task_id=self.task_id)
        with self.get_snowflake_cnx() as cnx:
            cur = cnx.cursor(DictCursor)
            self.execute_cmd(cmd=query_tag_cmd)
            cur.execute(cmd)
            result = cur.fetchone()

        return result is None

    def watermark_execute(self, context=None):
        initial_load = self.is_initial_load
        with self.get_snowflake_cnx():
            self.etl_execute(initial_load=initial_load, dryrun=False)

    def dry_run(self):
        self.get_high_watermark(initial_load=True, dryrun=True)
        self.etl_execute(initial_load=True, dryrun=True)
        self.update_high_watermark(dryrun=True)

    def get_high_watermark_cmd(self, table_name, watermark_column):
        database, schema, table = table_name.split(".")
        hwm_command = f"""
        SELECT
            '{table_name}' AS dependent_table_name,
            max({watermark_column}) AS high_watermark_datetime
        FROM {get_effective_database(database, self)}.{schema}.{table}"""

        return unindent_auto(hwm_command)

    @property
    def watermark_tables(self):
        watermark = {
            f"{self.archive_database}.{self.schema}.{self.table}": f"{self.delta_col_list[0].name}"
        }
        if self.is_delete_table_exists:
            watermark[
                f"{self.delete_log_database}.{self.delete_log_schema}.{self.delete_log_table}"
            ] = f"{self.delete_log_delta_col_list[0].name}"

        return watermark

    def get_table_high_watermark_cmd(self, initial_load):
        command_list = [
            self.get_high_watermark_cmd(dep, wc)
            for dep, wc in self.watermark_tables.items()
        ]
        if initial_load:
            self_table_cmd = f"""
            SELECT
                '{self.history_table_full_name}' AS dependent_table_name,
                '1900-01-01'::TIMESTAMP_LTZ AS high_watermark_datetime"""

            command_list.append(unindent_auto(self_table_cmd))
        else:
            command_list.append(
                self.get_high_watermark_cmd(
                    self.history_table_full_name, self.delta_col_list[0].name
                )
            )
        union = "\n\t\t\t\t\tUNION\n".join([indent(x, 20) for x in command_list])
        cmd = f"""
        MERGE INTO stg.meta_table_dependency_watermark AS t
        USING
        (
            SELECT
                '{self.watermark_param}' AS table_name,
                nullif(dependent_table_name,'{self.history_table_full_name}') AS dependent_table_name,
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
                '1900-01-01'::timestamp_ltz,
                s.new_high_watermark_datetime
            );
        """

        return unindent_auto(cmd)

    def get_high_watermark(self, initial_load, dryrun=False):
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
                                                    MAX({self.delta_col_list[0].name})
                                                FROM {self.history_table_full_name}
                                            )
                                        ),
            meta_update_datetime = current_timestamp::timestamp_ltz(3)
        WHERE table_name = '{self.watermark_param}';
        """

        return unindent_auto(cmd)

    def update_high_watermark(self, dryrun=False):
        cmd = self.get_table_high_watermark_update_cmd()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

    def watermark_pre_execute(self):
        initial_load = self.is_initial_load
        with self.get_snowflake_cnx():
            self.get_high_watermark(initial_load)

    def watermark_post_execute(self):
        with self.get_snowflake_cnx():
            self.update_high_watermark()

    def create_objects(self, dryrun):
        self.execute_cmd(cmd=self.ddl_hist_table, dryrun=dryrun)

    def create_temp_objects(self, dryrun):
        self.execute_cmd(cmd=self.ddl_stg_table, dryrun=dryrun)
        self.execute_cmd(cmd=self.ddl_delta_table, dryrun=dryrun)

    def load_hist_table(self, dryrun):
        self.execute_cmd(cmd="BEGIN;", dryrun=dryrun)
        self.execute_cmd(cmd=self.dml_stg_table, dryrun=dryrun)
        self.execute_cmd(cmd=self.dml_delta_table, dryrun=dryrun)
        if self.is_delete_table_exists and len(self.delete_update_lag_str) > 0:
            self.execute_cmd(cmd=self.dml_update_delete_data, dryrun=dryrun)
        self.execute_cmd(cmd=self.dml_update_hist_close_current_record, dryrun=dryrun)
        self.execute_cmd(cmd=self.insert_into_hist_table, dryrun=dryrun)
        self.execute_cmd(cmd="COMMIT;", dryrun=dryrun)

    def etl_execute(self, initial_load, dryrun):
        if initial_load:
            self.create_objects(dryrun=dryrun)
        self.create_temp_objects(dryrun=dryrun)
        self.load_hist_table(dryrun=dryrun)

    def execute(self, context=None):
        self.watermark_pre_execute()
        self.watermark_execute(context=context)
        self.watermark_post_execute()


class SnowflakeLakeHistorytoLakeBrandHistoryOperator(BaseSnowflakeOperator):
    SOURCE_DATABASE = "lake_history"
    BRAND_SOURCE_DATABASE = "lake_fl"

    def __init__(
        self,
        table: str,
        target_database: str,
        target_schema: str,
        company_id: list,
        warehouse: Optional[str] = "DA_WH_ETL",
        **kwargs,
    ):
        self.table = table
        self.schema = target_schema
        self.source_schema = self.schema.replace("_history", "")
        self.database = target_database
        self.warehouse = warehouse
        self.company_id = company_id
        super().__init__(
            warehouse=self.warehouse,
            schema=self.schema,
            database=self.database,
            role=snowflake_roles.etl_service_account,
            task_id=f"{self.SOURCE_DATABASE}.{self.source_schema}.{self.table}.to_{self.database}",
            **kwargs,
        )

    @property
    def table_config(self) -> LakeConsolidatedTableConfig:
        config: LakeConsolidatedTableConfig = get_lake_consolidated_table_config(
            table_name=f"lake_consolidated.{self.source_schema}.{self.table}"
        )
        return config

    @property
    def ddl_hist_table(self) -> str:
        return unindent_auto(
            f"""
        CREATE TABLE IF NOT EXISTS {self.database}.{self.schema}.{self.table} AS
        SELECT * FROM {self.BRAND_SOURCE_DATABASE}.{self.schema}.{self.table}
        ORDER BY META_SOURCE_CHANGE_DATETIME ASC;"""
        )

    @property
    def meta_columns(self) -> dict[str, Optional[Union[int, str]]]:
        return {
            "hvr_change_op": -1,
            "hvr_change_sequence": -1,
            "hvr_change_time": "effective_start_datetime",
            "meta_source_change_datetime": "effective_start_datetime",
            "meta_row_source": None,
        }

    @property
    def column_list(self) -> list[str]:
        return [i.name for i in self.table_config.column_list]

    @property
    def lake_history_column_list(self) -> str:
        return ",\n\t".join(self.column_list + list(self.meta_columns.keys()))

    @property
    def lake_brand_history_column_select(self) -> str:
        meta_column_select = [
            f"{v} as {k}" if v else k for k, v in self.meta_columns.items()
        ]
        return ",\n\t".join([f"A.{v}" for v in self.column_list] + meta_column_select)

    # @property
    def pkey_str(self, alias1, alias2) -> str:
        pkeys = [i.name for i in self.table_config.column_list if i.uniqueness]
        if not len(pkeys):
            return "1 = 1"
        return ",\n\t".join(f"{alias1}.{i} = {alias2}.{i}" for i in pkeys)

    @property
    def dml_insert_query(self) -> str:
        return unindent_auto(
            f"""
        INSERT INTO {self.database}.{self.schema}.{self.table} (
            {self.lake_history_column_list}
        ) SELECT
            {self.lake_brand_history_column_select}
        FROM {self.SOURCE_DATABASE}.{self.source_schema}.{self.table} as A
        {'' if not self.table_config.company_join_sql
            else 'JOIN (' + self.table_config.company_join_sql.format(
                    database='lake',
                    source_schema=self.schema.replace('_history', ''),
                    schema=self.schema.replace('_history', ''))
            + ') C ON ' + self.pkey_str('A', 'C') + ' AND c.company_id IN (' + ', '.join(self.company_id) + ')'}
        WHERE NOT EXISTS (
            SELECT 1
            FROM {self.database}.{self.schema}.{self.table} as B
            WHERE {self.pkey_str('A', 'B')}
            AND a.effective_start_datetime = b.meta_source_change_datetime
        )
        ORDER BY META_SOURCE_CHANGE_DATETIME ASC;
    """
        )

    def dry_run(self) -> None:
        print(self.ddl_hist_table)
        print(self.dml_insert_query)

    def execute(self, context=None) -> None:
        self.run_sql_or_path(sql_or_path=self.ddl_hist_table)
        self.run_sql_or_path(sql_or_path=self.dml_insert_query)
