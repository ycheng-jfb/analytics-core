from typing import List

from include.airflow.operators.snowflake_mart_base import (
    BaseSnowflakeMartOperator,
    Column,
)
from include.utils.string import unindent_auto


class SnowflakeMartSCDOperator(BaseSnowflakeMartOperator):
    META_EVENT_DATETIME_COLUMN = "meta_event_datetime"
    META_EVENT_DATETIME_TYPE = "TIMESTAMP_LTZ"
    INITIAL_FROM_TIMESTAMP = "to_timestamp_ltz('1900-01-01')"
    INITIAL_TO_TIMESTAMP = "to_timestamp_ltz('9999-12-31')"
    STR_INDENT = "\t" * 9

    @property
    def key_lookup_join_datetime_column(self):
        return self.META_EVENT_DATETIME_COLUMN

    def stg_dups_validation(self):
        """
        returns a table with list of duplicate business keys in stg table
        """
        _unique_col_names = []
        for x in self.column_list:
            if x.uniqueness:
                if x.key_lookup_list:
                    for k in x.key_lookup_list:
                        _unique_col_names.append(k.stg_column)
                else:
                    _unique_col_names.append(x.source_name)

        _unique_col_names.append(self.META_EVENT_DATETIME_COLUMN)

        _unique_cols_str = ", ".join(_unique_col_names)
        _uniqueness_join = "\n    AND ".join(
            [f"equal_null(t.{x}, s.{x})" for x in _unique_col_names]
        )
        cmd = f"""
            -- Identifying duplicate business keys
            INSERT INTO {self.excp_tmp_table_name}
            WITH dups_cte
            AS
            (
                SELECT
                    {_unique_cols_str},
                    'error' AS meta_data_quality,
                    'Duplicate business key values are present' AS excp_message
                FROM {self.stg_table_name}
                GROUP BY {_unique_cols_str} HAVING COUNT(*) > 1
            )
            SELECT s.id, t.meta_data_quality, t.excp_message
            FROM {self.stg_table_name} s
            JOIN dups_cte t
                ON {_uniqueness_join};
            """
        return unindent_auto(cmd)

    @property
    def type_1_col_names(self) -> list:
        type_1_col_name = [
            (
                f"nvl({x.lookup_table_alias}.{x.lookup_table_key},-1)"
                if x.lookup_table_name
                else f"s.{x.source_name}"
            )
            for x in self.column_list
            if not x.stg_only
            if x.type_1
        ]
        return type_1_col_name

    @property
    def type_2_col_names(self) -> list:
        type_2_col_name = [
            (
                f"nvl({x.lookup_table_alias}.{x.lookup_table_key},-1)"
                if x.lookup_table_name
                else f"s.{x.source_name}"
            )
            for x in self.column_list
            if not x.stg_only
            if x.type_2
        ]
        return type_2_col_name

    @property
    def type_1_hash_col_names(self) -> list:
        type_1_hash_col_name = [
            (
                f"nvl({x.lookup_table_alias}.{x.lookup_table_key},-1)"
                if x.lookup_table_name
                else f"s.{self.get_hash_column(x.source_name, x.type)}"
            )
            for x in self.column_list
            if not x.stg_only
            if x.type_1
        ]
        return type_1_hash_col_name

    @property
    def type_2_hash_col_names(self) -> list:
        type_2_hash_col_name = [
            (
                f"nvl({x.lookup_table_alias}.{x.lookup_table_key},-1)"
                if x.lookup_table_name
                else f"s.{self.get_hash_column(x.source_name, x.type)}"
            )
            for x in self.column_list
            if not x.stg_only
            if x.type_2
        ]
        return type_2_hash_col_name

    @property
    def type_1_upd_col_names(self) -> list:
        type_1_upd_col_names = [
            f"{x.name}" for x in self.column_list if not x.stg_only if x.type_1
        ]
        return type_1_upd_col_names

    @property
    def meta_type_1_hash(self):
        return f"hash({','.join(self.type_1_hash_col_names) if self.type_1_hash_col_names else 'NULL'})"

    @property
    def meta_type_2_hash(self):
        return f"hash({','.join(self.type_2_hash_col_names) if self.type_2_hash_col_names else 'NULL'})"

    @property
    def stg_table_meta_cols(self) -> List[Column]:
        stg_table_meta_cols = [
            Column(
                f"{self.META_EVENT_DATETIME_COLUMN}",
                f"{self.META_EVENT_DATETIME_TYPE}",
                default_value="current_timestamp",
            ),
            Column("meta_data_quality", "VARCHAR(10)"),
            Column(
                "meta_create_datetime",
                "TIMESTAMP_LTZ",
                default_value="current_timestamp",
            ),
            Column(
                "meta_update_datetime",
                "TIMESTAMP_LTZ",
                default_value="current_timestamp",
            ),
        ]
        return stg_table_meta_cols

    @property
    def base_table_meta_cols(self) -> List[Column]:
        base_table_meta_cols = [
            Column("effective_start_datetime", f"{self.META_EVENT_DATETIME_TYPE}"),
            Column("effective_end_datetime", f"{self.META_EVENT_DATETIME_TYPE}"),
            Column("is_current", "BOOLEAN"),
            Column(
                f"{self.META_EVENT_DATETIME_COLUMN}", f"{self.META_EVENT_DATETIME_TYPE}"
            ),
            Column("meta_type_1_hash", "INT"),
            Column("meta_type_2_hash", "INT"),
            Column("meta_create_datetime", "TIMESTAMP_LTZ"),
            Column("meta_update_datetime", "TIMESTAMP_LTZ"),
        ]
        return base_table_meta_cols

    @property
    def delta_table_meta_cols(self) -> List[Column]:
        delta_table_meta_cols = [
            Column("effective_start_datetime", f"{self.META_EVENT_DATETIME_TYPE}"),
            Column(
                f"{self.META_EVENT_DATETIME_COLUMN}", f"{self.META_EVENT_DATETIME_TYPE}"
            ),
            Column("meta_type_1_hash", "INT"),
            Column("meta_type_2_hash", "INT"),
            Column("meta_create_datetime", "TIMESTAMP_LTZ"),
            Column("meta_update_datetime", "TIMESTAMP_LTZ"),
            Column("lag_meta_type_1_hash", "INT"),
            Column("lag_meta_type_2_hash", "INT"),
            Column("lag_effective_start_datetime", f"{self.META_EVENT_DATETIME_TYPE}"),
            Column("is_new_data", "BOOLEAN"),
            Column("first_record_rn", "INT"),
            Column("meta_record_status", "VARCHAR"),
        ]
        return delta_table_meta_cols

    @property
    def uniqueness_join(self) -> str:
        return "\n    AND ".join(
            [f"equal_null(t.{x}, s.{x})" for x in self.column_list.unique_col_names]
        )

    def load_base_delta_table(self):
        unique_cols_str = ", ".join(
            [f"s.{x}" for x in self.column_list.unique_col_names]
        )
        cmd = f"""
                INSERT INTO {self.delta_table_name}
                SELECT s.*,
                    CASE
                        WHEN s.is_new_data = TRUE AND first_record_rn = 1 THEN 'new'
                        WHEN s.effective_start_datetime <= s.lag_effective_start_datetime THEN 'ignore'
                        WHEN s.meta_type_1_hash != s.lag_meta_type_1_hash
                            AND s.meta_type_2_hash != s.lag_meta_type_2_hash THEN 'both'
                        WHEN s.meta_type_1_hash != s.lag_meta_type_1_hash THEN 'type 1'
                        WHEN s.meta_type_2_hash != s.lag_meta_type_2_hash THEN 'type 2'
                        ELSE 'ignore'
                    END :: VARCHAR(10) AS meta_record_status
                FROM (
                        SELECT
                            s.*,
                            nvl(lag(s.meta_type_1_hash) OVER (PARTITION BY {unique_cols_str} ORDER BY s.effective_start_datetime), t.meta_type_1_hash) AS lag_meta_type_1_hash,
                            nvl(lag(s.meta_type_2_hash) OVER (PARTITION BY {unique_cols_str} ORDER BY s.effective_start_datetime), t.meta_type_2_hash) AS lag_meta_type_2_hash,
                            nvl(lag(s.effective_start_datetime) OVER (PARTITION BY {unique_cols_str} ORDER BY s.effective_start_datetime), t.effective_start_datetime) AS lag_effective_start_datetime,
                            nvl2(t.is_current, FALSE, TRUE) AS is_new_data,
                            row_number() OVER (PARTITION BY {unique_cols_str} ORDER BY s.effective_start_datetime) AS first_record_rn
                        FROM
                            (
                                SELECT
                                    {self.delta_table_col_str},
                                    s.{self.META_EVENT_DATETIME_COLUMN} AS effective_start_datetime,
                                    s.{self.META_EVENT_DATETIME_COLUMN},
                                    {self.meta_type_1_hash} AS meta_type_1_hash,
                                    {self.meta_type_2_hash} AS meta_type_2_hash,
                                    s.meta_create_datetime,
                                    s.meta_update_datetime
                                FROM {self.stg_table_name} s
                                {self.key_lookup_join_str}
                                WHERE s.meta_data_quality IS DISTINCT FROM 'error'
                            )s
                        LEFT JOIN {self.target_full_table_name} t ON {self.uniqueness_join}
                            AND t.is_current
                        WHERE
                            t.is_current IS NULL
                            OR t.effective_start_datetime < s.effective_start_datetime
                    )s;"""  # noqa: E501

        return unindent_auto(cmd)

    def load_type_2_delta_table(self):
        cmd = f"""
        INSERT INTO _type_2_delta
        SELECT
            *,
            dateadd(ms,-1,LEAD(effective_start_datetime) OVER (PARTITION BY {self.unique_cols_str} ORDER BY effective_start_datetime)) AS effective_end_datetime,
            row_number() OVER (PARTITION BY {self.unique_cols_str} ORDER BY effective_start_datetime) AS event_seq_num
        FROM {self.delta_table_name}
        WHERE meta_record_status IN ('type 2', 'both', 'new');
        """  # noqa: E501
        return unindent_auto(cmd)

    def load_type_1_delta_table(self):
        type_1_cols_names = ",\n\t".join(
            [x.name for x in self.column_list if not x.stg_only and x.type_1]
        )
        cmd = f"""
        INSERT INTO _type_1_delta
        SELECT
            {self.unique_cols_str},
            {type_1_cols_names},
            meta_update_datetime,
            meta_type_1_hash,
            row_number() OVER (PARTITION BY {self.unique_cols_str} ORDER BY effective_start_datetime DESC) AS event_seq_num
        FROM {self.delta_table_name}
        WHERE meta_record_status IN ('type 1', 'both');
        """  # noqa: E501
        return unindent_auto(cmd)

    def get_type_1_update_command(self):
        cnt = ",\n\t"
        type_1_update_list = [f"t.{x} = s.{x}" for x in self.type_1_upd_col_names]
        cmd = f"""
            -- type 1 updates
            UPDATE {self.target_full_table_name} t
            SET
                {cnt.join(type_1_update_list)},
                t.meta_update_datetime = s.meta_update_datetime,
                t.meta_type_1_hash = s.meta_type_1_hash
            FROM _type_1_delta s
            WHERE {self.uniqueness_join}
                AND s.event_seq_num = 1;
            """
        return unindent_auto(cmd)

    def get_type_2_update_command(self):
        cmd = f"""
            -- close off type 2
            UPDATE {self.target_full_table_name} t
            SET t.effective_end_datetime = dateadd(ms,-1,s.effective_start_datetime),
                t.is_current = FALSE,
                t.meta_update_datetime = s.meta_update_datetime
            FROM _type_2_delta s
            WHERE t.is_current
                AND s.event_seq_num = 1
                AND {self.uniqueness_join}
                AND s.meta_record_status IN ('both', 'type 2');
            """
        return unindent_auto(cmd)

    def get_insert_command(self):
        cnt = ",\n\t"
        full_col_list = self.insert_names_list + [
            x.name for x in self.base_table_meta_cols
        ]
        full_col_select_list = ",\n\t".join(full_col_list)
        cmd = f"""
            -- insert new records
            INSERT INTO {self.target_full_table_name}
            (
                {full_col_select_list}
            )
            SELECT
                {cnt.join(self.insert_names_list)},
                CASE
                    WHEN s.meta_record_status = 'new'
                    THEN {self.INITIAL_FROM_TIMESTAMP}
                    ELSE s.effective_start_datetime
                END AS effective_start_datetime,
                nvl(s.effective_end_datetime, {self.INITIAL_TO_TIMESTAMP}) AS effective_end_datetime,
                nvl2(s.effective_end_datetime, FALSE, TRUE) AS is_current,
                {self.META_EVENT_DATETIME_COLUMN},
                meta_type_1_hash,
                meta_type_2_hash,
                meta_create_datetime,
                meta_update_datetime
            FROM _type_2_delta s;
            """
        cmd = unindent_auto(cmd)
        return cmd

    def create_base_delta_table(self) -> str:
        cmd = f"""
        CREATE OR REPLACE TEMPORARY TABLE {self.delta_table_name} (
            {self._get_base_col_ddls(self.column_list)},
            {self._get_base_col_ddls(self.delta_table_meta_cols)}
        );
        """
        return unindent_auto(cmd)

    def create_type_1_delta_table(self) -> str:
        type_1_cols = [x for x in self.column_list if not x.stg_only and x.type_1]
        uniqueness_cols = [x for x in self.column_list if x.uniqueness]
        type_1_meta_cols = [
            Column("meta_update_datetime", "TIMESTAMP_LTZ"),
            Column("meta_type_1_hash", "INT"),
            Column("event_seq_num", "INT"),
        ]
        cmd = f"""
        CREATE OR REPLACE TEMPORARY TABLE _type_1_delta (
            {self._get_base_col_ddls(uniqueness_cols)},
            {self._get_base_col_ddls(type_1_cols)},
            {self._get_base_col_ddls(type_1_meta_cols)}
        );
        """
        return unindent_auto(cmd)

    def create_type_2_delta_table(self) -> str:
        cmd = f"""
        CREATE OR REPLACE TEMPORARY TABLE _type_2_delta (
            {self._get_base_col_ddls(self.column_list)},
            {self._get_base_col_ddls(self.delta_table_meta_cols)},
            effective_end_datetime {self.META_EVENT_DATETIME_TYPE},
            event_seq_num INT
        );
        """
        return unindent_auto(cmd)

    def post_stg_table_load(self, dryrun):
        cmd = self.create_temp_excp_table()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

        cmd = self.create_base_delta_table()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

        if self.type_1_col_names:
            cmd = self.create_type_1_delta_table()
            self.execute_cmd(cmd=cmd, dryrun=dryrun)

        cmd = self.create_type_2_delta_table()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

    def load_base_table(self, dryrun):
        cmd = self.load_base_delta_table()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

        if self.type_1_col_names:
            cmd = self.load_type_1_delta_table()
            self.execute_cmd(cmd=cmd, dryrun=dryrun)

        cmd = self.load_type_2_delta_table()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

        cmd = self.get_type_2_update_command()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

        cmd = self.get_insert_command()
        self.execute_cmd(cmd=cmd, dryrun=dryrun)

        if self.type_1_col_names:
            cmd = self.get_type_1_update_command()
            self.execute_cmd(cmd=cmd, dryrun=dryrun)
