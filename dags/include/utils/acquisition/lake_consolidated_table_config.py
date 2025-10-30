# from datetime import timedelta
from enum import Enum, unique
from typing import List, Optional, Type

from include.airflow.operators.snowflake import SnowflakeSqlOperator
from include.config import snowflake_roles
from include.config.pools import Pool
from include.utils.snowflake import Column
from include.utils.string import unindent_auto


@unique
class TableType(str, Enum):
    NSYNC = "nsync"
    VALUE_COLUMN = "has_value_column"
    NAME_VALUE_COLUMN = "has_name_value_column"
    OBJECT_COLUMN = "has_object_column"
    OBJECT_COLUMN_NULL = "has_object_column_with_null"
    OBJECT_COLUMN_NULL_AND_NAME_VALUE = "has_object_column_with_null_and_name_value_column"
    REGULAR = "regular"
    REGULAR_GLOBAL = "regular_multiple_company_ids"

    @property
    def include_datasource_id(self):
        exclude_list = [TableType.NSYNC, TableType.VALUE_COLUMN, TableType.OBJECT_COLUMN]
        return self not in exclude_list

    @property
    def include_company_id(self):
        exclude_list = [
            TableType.NSYNC,
            # TableType.VALUE_COLUMN,
            TableType.OBJECT_COLUMN,
            TableType.OBJECT_COLUMN_NULL,
            TableType.OBJECT_COLUMN_NULL_AND_NAME_VALUE,
        ]
        return self not in exclude_list


lake_databases_dict = {
    'lake_jfb': '10',
    'lake_fl': '20',
    'lake_sxf': '30',
}

lake_databases_companies_dict = {
    'lake_jfb': '10',
    'lake_fl': '20',
    'lake_sxf': '30',
}

schema_database_mapping = {
    'ultra_cart': 'ultracart',
    'ultra_cms': 'ultracms',
    'ultra_identity': 'ultraidentity',
    'ultra_merchant': 'ultramerchant',
    'ultra_rollup': 'ultrarollup',
    'ultra_warehouse': 'ultrawarehouse',
}
"""
Map Snowflake Target Schema --> SQL Server Source Database
"""


class ColumnList(list):
    @property
    def has_uniqueness(self):
        for col in self:
            if col.uniqueness is True:
                return True
        return False

    @property
    def column_name_list(self):
        return [x.name for x in self]

    @property
    def uniqueness_cols_str(self) -> List[str]:
        return [x.name for x in self if x.uniqueness is True]

    @property
    def key_cols(self) -> List[str]:
        return [x.name for x in self if x.key is True]

    @property
    def keep_original_column(self) -> List[str]:
        return [
            x.name
            for x in self
            if x.key is True and (x.uniqueness is True or x.keep_original is True)
        ]

    @property
    def data_types(self) -> List[str]:
        return [(str(x.name) + ' ' + str(x.type)) for x in self]


def remove_all_quoting(table_name):
    return table_name.replace('"', '').replace('[', '').replace(']', '').lower()


class LakeConsolidatedTableConfig:
    """
    Config class to manage consolidating various Lake databases into the "Lake_Consolidated" database

    Args:
        database: database of target table
        schema: target schema of target table
        table: table to be merged into
        table_type: Is Table "NSYNC", "OBJECT", "REGULAR" etc
        column_list: List of Column objects. The names given here are for the target, to prevent Schema Drift from
            Source
        company_join_sql: SQL Query to identify Company_ID
            The SQL Query may contain the following formatting parameters:
                {database} - snowflake brand database
                {schema} - snowflake schema; example: ultra_merchant
                {source_schema} - snowflake schema for driver table.
                     This is formatted based on the operator we're using; example:
                         to_lake_consolidated_operator - source_schema = ultra_merchant
                         to_lake_consolidated_history_operator - source_schema = ultra_merchant_history
        company_join_columns: Optional, use if Primary Key of Table can't be used to JOIN on "company_join_sql"
        warehouse: Optional, defines Snowflake Warehouse, use if Default Warehouse does not meet SLA needs
        post_sql: Optional, SQL Query to be executed after the merge
            post_sql may contain the following formatting parameters:
                {database} - snowflake brand database
                {schema} - snowflake schema; example: ultra_merchant
    """

    UTCNOW_TEMPLATE = '{{ macros.tfgdt.utcnow_nodash() }}'

    def __init__(
        self,
        table: str,
        column_list: List[Column],
        company_join_sql: Optional[str] = None,
        database: str = 'lake_consolidated',
        schema: str = 'ultra_merchant',
        table_type: TableType = TableType.REGULAR,
        company_join_columns: Optional[List[str]] = None,
        warehouse: str = 'DA_WH_ETL_LIGHT',
        cluster_by: Optional[str] = None,
        partition_cols: Optional[List[str]] = None,
        watermark_column: Optional[str] = None,
        post_sql: Optional[str] = None,
    ):
        # Used to assist legacy migration, DO NOT REMOVE
        if database == 'ultra_merchant' and schema == 'dbo':
            database = 'LAKE_CONSOLIDATED'
            schema = 'ULTRA_MERCHANT'
        self.database = database
        self.schema = schema
        self.table = table
        self.table_type = TableType(table_type)
        self.column_list = ColumnList(column_list)
        self.company_join_sql = company_join_sql
        self.company_join_columns = company_join_columns
        self.warehouse = warehouse
        self.watermark_column = watermark_column
        self.post_sql = post_sql

    @property
    def base_table_name(self) -> str:
        if self.table.lower() == 'order':
            return f'"{self.table.upper()}"'
        else:
            return self.table.upper()

    @property
    def full_target_table_name(self):
        return f'{self.database}.{self.schema}.{self.base_table_name}'

    @property
    def base_task_id(self):
        return self.full_target_table_name.replace('"', '').lower()

    @property
    def to_lake_consolidated_task_id(self):
        return f"{self.base_task_id}.to_lake_consolidated"

    @property
    def to_lake_consolidated_history_task_id(self):
        return f"{self.base_task_id}.to_lake_consolidated_history"

    @property
    def load_operator(
        self,
    ) -> Type[SnowflakeSqlOperator]:
        return SnowflakeSqlOperator

    @property
    def source_column_name_list(self):
        return [x.source_name for x in self.column_list]

    @property
    def to_lake_consolidated_operator(
        self,
    ) -> SnowflakeSqlOperator:
        # TODO: add a reasonable execution_timeout here
        params = dict(
            task_id=self.to_lake_consolidated_task_id,
            role=snowflake_roles.etl_service_account,
            database=self.database,
            schema=self.schema,
            warehouse=self.warehouse,
            # execution_timeout=timedelta(hours=1),
            pool=Pool.snowflake_lake_load,
            sql_or_path=self.merge_into_lake_consolidated,
        )
        return self.load_operator(**params)

    @property
    def to_lake_consolidated_history_operator(
        self,
    ) -> SnowflakeSqlOperator:
        params = dict(
            task_id=self.to_lake_consolidated_history_task_id,
            role=snowflake_roles.etl_service_account,
            database=self.database,
            schema=self.schema,
            warehouse=self.warehouse,
            # execution_timeout=timedelta(hours=1),
            pool=Pool.snowflake_lake_load,
            sql_or_path=self.merge_into_lake_consolidated_history,
        )
        return self.load_operator(**params)

    @property
    def merge_into_lake_consolidated(self):
        """
        Helper function to create SQL commands to merge lake records into lake_consolidated

        """
        lake_consolidated_table = self.full_target_table_name
        source_temp_delta = ''
        uniqueness_str = ','.join(self.column_list.uniqueness_cols_str)
        raw_column_str = ',\n        '.join(self.column_list.column_name_list)
        source_column_str = ',\n            '.join(self.column_list.column_name_list)
        if self.table_type.include_company_id is True:
            full_column_str = raw_column_str + ''.join(
                [f",\n        meta_original_{x}" for x in self.column_list.keep_original_column]
            )
        else:
            full_column_str = raw_column_str
        if self.table_type == TableType.NSYNC:
            lake_db_dict = {'lake_fl': '20'}
        else:
            lake_db_dict = lake_databases_dict
            lake_db_company_dict = lake_databases_companies_dict

        create_table_cmd = f"CREATE TRANSIENT TABLE IF NOT EXISTS {lake_consolidated_table} ("

        if self.table_type.include_datasource_id is True:
            create_table_cmd += "\n    data_source_id INT,"
        if self.table_type.include_company_id is True:
            create_table_cmd += "\n    meta_company_id INT,"

        meta_rows = (
            "\n    hvr_is_deleted INT,"
            "\n    meta_create_datetime TIMESTAMP_LTZ(3),"
            "\n    meta_update_datetime TIMESTAMP_LTZ(3)\n);"
        )
        for col in self.column_list.data_types:
            create_table_cmd += f"""\n    {col}, """
        for key_col in self.column_list.key_cols:
            for col_datatype in self.column_list.data_types:
                if (
                    str(key_col) in str(col_datatype)
                    and str(key_col) in [*self.column_list.keep_original_column]
                    and self.table_type.include_company_id is True
                ):
                    create_table_cmd += f"\n    meta_original_{col_datatype},"
        create_table_cmd += meta_rows

        watermarks = ''
        for db in lake_db_dict:
            lake_db_table = f'{db}.{self.schema}.{self.base_table_name}'
            watermarks += (
                f"SET {db}_watermark = ( \n"
                f"SELECT MIN(last_update) \n"
                f"FROM ( \n"
                f"    SELECT \n"
                f"        COALESCE( \n"
                f"        DATEADD(minutes,  \n"
                f"            CASE \n"
                f"            WHEN DATEDIFF(minutes, MAX(META_UPDATE_DATETIME), CURRENT_TIMESTAMP) < 60 \n"
                f"            THEN -180 \n"
                f"            WHEN DATEDIFF(minutes, MAX(META_UPDATE_DATETIME), CURRENT_TIMESTAMP) >= 360 \n"
                f"            THEN (DATEDIFF(minutes, MAX(META_UPDATE_DATETIME), CURRENT_TIMESTAMP) + 1080) * -1 \n"
                f"            ELSE DATEDIFF(minutes, MAX(META_UPDATE_DATETIME), CURRENT_TIMESTAMP) * -3 \n"
                f"            END, \n"
                f"        MAX(META_UPDATE_DATETIME) \n"
                f"        ), '1900-01-01' \n"
                f"    ) as last_update \n"
                f"FROM {lake_consolidated_table} \n"
            )
            if self.table_type.include_datasource_id is True:
                watermarks += f"    WHERE data_source_id = {lake_db_dict[db]}\n"
            watermarks += (
                f"    UNION \n"
                f"    SELECT CAST(MAX(HVR_CHANGE_TIME) AS TIMESTAMP_LTZ(3)) AS last_update "
                f"FROM {lake_db_table} \n"
                f"    ) AS A \n"
                f");  \n"
            )

        merge_cond = ''
        if self.column_list.key_cols is not None and self.table_type.include_datasource_id is True:
            merge_cond += 's.data_source_id = t.data_source_id \n        AND'
        if self.table_type == TableType.REGULAR_GLOBAL:
            merge_cond += ' s.meta_company_id = t.meta_company_id \n        AND'

        for idx, col in enumerate(self.column_list.uniqueness_cols_str):
            if idx > 0:
                merge_cond += '\n        AND'
            if (
                col in self.column_list.keep_original_column
                and self.table_type.include_company_id is True
            ):
                merge_cond += f" s.{col} = t.meta_original_{col}"
            else:
                merge_cond += f" s.{col} = t.{col}"

        # Build the PreMerge statements
        company_join_temp_tables = ''
        source_union = ''
        for lake_db_loop, db in enumerate(lake_db_dict):
            lake_db_table = f'{db}.{self.schema}.{self.base_table_name}'
            lake_db_temp_table = f"_{db}_{self.schema}_{self.table}_delta"

            # Build the Source temp tables
            source_temp_delta += (
                f"CREATE OR REPLACE TEMP TABLE {lake_db_temp_table} AS \n"
                f"SELECT "
                f"    {source_column_str}, \n"
                f"    {lake_db_dict[db]} AS data_source_id, \n"
                f"    CAST(l.HVR_CHANGE_TIME AS TIMESTAMP_LTZ(3)) AS meta_update_datetime, \n"
                f"    l.hvr_is_deleted, \n"
                f"    row_number() over(PARTITION BY {uniqueness_str} \n"
                f"        ORDER BY CAST(l.HVR_CHANGE_TIME AS TIMESTAMP_LTZ(3)) DESC) AS rn \n"
                f"FROM {lake_db_table} l \n"
                f"WHERE l.HVR_CHANGE_TIME >= DATEADD(MINUTE, -5, ${db}_watermark) \n"
                f"QUALIFY row_number() over(PARTITION BY {uniqueness_str} \n"
                f"    ORDER BY CAST(l.HVR_CHANGE_TIME AS TIMESTAMP_LTZ(3)) DESC) = 1; \n"
            )

            # Build the Source Unions
            if lake_db_loop > 0:
                source_union += '\n\n       UNION ALL\n'
            source_union += "    SELECT \n        A.* \n"

            # Build the Company_ID temp tables

            if self.company_join_sql is not None and self.table_type.include_company_id is True:
                company_join_subquery = self.company_join_sql.format(
                    database=db,
                    source_schema=self.schema,
                    schema=self.schema,
                )
                company_join_temp_table = f"_{db}_company_{self.table}"
                company_join_temp_tables += (
                    f"CREATE OR REPLACE TEMP TABLE {company_join_temp_table} AS \n"
                    f"SELECT {uniqueness_str}, company_id as meta_company_id \n"
                    f"FROM ({company_join_subquery} \n"
                    f"    WHERE l.HVR_CHANGE_TIME >= DATEADD(MINUTE, -5, ${db}_watermark) \n"
                    f"    ) AS l \n"
                )
                if self.table_type != TableType.REGULAR_GLOBAL:
                    company_join_temp_tables += (
                        f"QUALIFY ROW_NUMBER() OVER(PARTITION BY {uniqueness_str} "
                        f"ORDER BY company_id ASC) = 1"
                    )

                company_join_temp_tables += "; \n"

                source_union += (
                    f"        ,COALESCE(CJ.meta_company_id, 40) AS meta_company_id \n"
                    f"    FROM {lake_db_temp_table} AS A \n"
                    f"    LEFT JOIN {company_join_temp_table} AS CJ \n"
                )

                company_join_cond = ''
                if self.company_join_columns is not None:
                    for idx, col in enumerate(self.company_join_columns):
                        if idx == 0:
                            company_join_cond += '\n        ON '
                        else:
                            company_join_cond += '\n        AND '
                        company_join_cond += f" cj.{col} = a.{col}"
                else:
                    for idx, col in enumerate(self.column_list.uniqueness_cols_str):
                        if idx == 0:
                            company_join_cond += '\n        ON '
                        else:
                            company_join_cond += '\n        AND '
                        company_join_cond += f" cj.{col} = a.{col}"

                company_join_cond += f"""
                WHERE COALESCE(CJ.meta_company_id, 40) IN ({lake_db_company_dict[db]})
                """

                source_union += company_join_cond
            else:
                source_union += f"    FROM {lake_db_temp_table} AS A \n"

        consolidated_column_merge_update = ''
        for col in self.column_list.column_name_list:
            if col in self.column_list.key_cols and self.table_type.include_company_id is True:
                consolidated_column_merge_update += (
                    f"\n        t.{col} = CASE WHEN NULLIF(s.{col}, '0') IS NOT NULL "
                    f"THEN CONCAT(s.{col}, s.meta_company_id) "
                    f"ELSE NULL END, "
                )
            else:
                consolidated_column_merge_update += f"\n        t.{col} = s.{col},"

        for col in self.column_list.key_cols:
            if (
                col in self.column_list.keep_original_column
                and self.table_type.include_company_id is True
            ):
                consolidated_column_merge_update += (
                    f"""\n        t.meta_original_{col} = s.{col},"""
                )

        consolidated_column_merge_insert = ''
        for col in self.column_list.column_name_list:
            if col in self.column_list.key_cols and self.table_type.include_company_id is True:
                consolidated_column_merge_insert += (
                    f"\n        CASE WHEN NULLIF(s.{col}, '0') IS NOT NULL THEN CONCAT(s.{col}, s.meta_company_id) "
                    f"ELSE NULL END,"
                )
            else:
                consolidated_column_merge_insert += f"""\n        s.{col},"""
        for col in self.column_list.key_cols:
            if (
                col in self.column_list.keep_original_column
                and self.table_type.include_company_id is True
            ):
                consolidated_column_merge_insert += f"""\n        s.{col},"""

        # Build the Final SQL statement
        cmd = (
            f"{create_table_cmd} "
            f"\n\n"
            f"{watermarks}"
            f"\n\n"
            f"{source_temp_delta}"
            f"\n\n"
            f"{company_join_temp_tables}"
            f"\n\n"
            f"MERGE INTO {lake_consolidated_table} t \n"
            f"    USING ( \n"
            f"    {source_union} \n"
            f"    ) s \n"
            f"    ON {merge_cond} \n"
            f"    WHEN MATCHED \n"
            f"        AND s.meta_update_datetime > t.meta_update_datetime \n"
            f"    THEN UPDATE SET \n"
            f"{'        t.meta_company_id = s.meta_company_id, ' * (self.table_type.include_company_id is True)}\n"
            f"{consolidated_column_merge_update} \n"
            f"        t.meta_update_datetime = s.meta_update_datetime, \n"
            f"        t.hvr_is_deleted = s.hvr_is_deleted \n"
            f"    WHEN NOT MATCHED THEN INSERT ( \n"
            f"        {'data_source_id,' * (self.table_type.include_datasource_id is True)} \n"
            f"        {'meta_company_id,' * (self.table_type.include_company_id is True)} \n"
            f"        {full_column_str}, \n"
            f"        meta_create_datetime, \n"
            f"        meta_update_datetime, \n"
            f"        hvr_is_deleted \n"
            f"    ) \n"
            f"    VALUES ( \n"
            f"        {'s.data_source_id,' * (self.table_type.include_datasource_id is True)} \n"
            f"        {'s.meta_company_id,' * (self.table_type.include_company_id is True)} \n"
            f"        {consolidated_column_merge_insert} \n"
            f"        s.meta_update_datetime, \n"
            f"        s.meta_update_datetime, \n"
            f"        s.hvr_is_deleted \n"
            f"    );\n"
        )
        if self.post_sql:
            cmd += unindent_auto(
                self.post_sql.format(
                    database=self.database,
                    schema=self.schema,
                )
            )
        return cmd

    @property
    def merge_into_lake_consolidated_history(self):
        """
        Helper function to create SQL commands to merge lake_consolidated records into lake_consolidated_history

        """

        lake_consolidated_history_table = (
            f"lake_consolidated.{self.schema}_history.{self.base_table_name}"
        )
        source_temp_delta = ''
        source_union = ''
        company_join_temp_tables = ''
        uniqueness_str = ','.join(self.column_list.uniqueness_cols_str)
        column_list = self.column_list.column_name_list
        raw_column_str = ',\n    '.join(self.column_list.column_name_list)
        if self.table_type.include_company_id is True:
            full_column_str = raw_column_str + ''.join(
                [f",\n    meta_original_{x}" for x in self.column_list.keep_original_column]
            )
        else:
            full_column_str = raw_column_str
        if self.table_type == TableType.NSYNC:
            lake_db_dict = {'lake_fl': '20'}
        else:
            lake_db_dict = lake_databases_dict
            lake_db_company_dict = lake_databases_companies_dict

        full_final_column_str = ''
        for col in self.column_list.column_name_list:
            if col in self.column_list.key_cols and self.table_type.include_company_id is True:
                full_final_column_str += (
                    f"\n    CASE WHEN NULLIF(s.{col}, '0') IS NOT NULL THEN CONCAT(s.{col}, s.meta_company_id) "
                    f"ELSE NULL END as {col}, "
                )
            else:
                full_final_column_str += f"\n    s.{col},"

        for col in self.column_list.key_cols:
            if (
                col in self.column_list.keep_original_column
                and self.table_type.include_company_id is True
            ):
                full_final_column_str += f"""\n    s.{col} as meta_original_{col},"""

        create_history_table_cmd = (
            f"CREATE TRANSIENT TABLE IF NOT EXISTS {lake_consolidated_history_table} ("
        )

        if self.table_type.include_datasource_id is True:
            create_history_table_cmd += "\n    data_source_id INT,"
        if self.table_type.include_company_id is True:
            create_history_table_cmd += "\n    meta_company_id INT,"

        meta_rows = (
            "\n    meta_row_source VARCHAR(40),"
            "\n    hvr_change_op INT,"
            "\n    effective_start_datetime TIMESTAMP_LTZ(3),"
            "\n    effective_end_datetime TIMESTAMP_LTZ(3),"
            "\n    meta_update_datetime TIMESTAMP_LTZ(3)\n);"
        )
        for col in self.column_list.data_types:
            create_history_table_cmd += f"""\n    {col}, """

        for key_col in self.column_list.keep_original_column:
            for col_datatype in self.column_list.data_types:
                if (
                    str(key_col) in str(col_datatype)
                    and str(key_col) in [*self.column_list.uniqueness_cols_str]
                    and self.table_type.include_company_id is True
                ):
                    create_history_table_cmd += f"\n    meta_original_{col_datatype},"
        create_history_table_cmd += meta_rows

        watermarks = ''
        for db in lake_db_dict:
            lake_history_db_table = f"{db}.{self.schema}_history.{self.base_table_name}"
            watermarks += (
                f"SET {db}_watermark = ( \n"
                f"    SELECT MIN(last_update) \n"
                f"    FROM ( \n"
                f"        SELECT \n"
                f"            COALESCE( \n"
                f"            DATEADD(minutes,  \n"
                f"                CASE \n"
                f"                WHEN DATEDIFF(minutes, MAX(META_UPDATE_DATETIME), CURRENT_TIMESTAMP) < 60 \n"
                f"                THEN -180 \n"
                f"                WHEN DATEDIFF(minutes, MAX(META_UPDATE_DATETIME), CURRENT_TIMESTAMP) >= 360 \n"
                f"                THEN (DATEDIFF(minutes, MAX(META_UPDATE_DATETIME), CURRENT_TIMESTAMP) + 1080) * -1 \n"
                f"                ELSE DATEDIFF(minutes, MAX(META_UPDATE_DATETIME), CURRENT_TIMESTAMP) * -3 \n"
                f"                END, \n"
                f"            MAX(META_UPDATE_DATETIME) \n"
                f"            ), '1900-01-01' \n"
                f"        ) as last_update \n"
                f"        FROM {lake_consolidated_history_table} "
            )
            if self.table_type.include_datasource_id is True:
                watermarks += f"\n        WHERE data_source_id = {lake_db_dict[db]}"
            watermarks += (
                f" \n"
                f"        UNION \n"
                f"        SELECT CAST(MAX(META_SOURCE_CHANGE_DATETIME) AS TIMESTAMP_LTZ(3)) AS last_update \n"
                f"        FROM {lake_history_db_table} \n"
                f"    ) AS A \n"
                f"); \n"
            )
        for lake_db_loop, db in enumerate(lake_db_dict):
            lake_history_db_table = f"{db}.{self.schema}_history.{self.base_table_name}"

            lake_history_db_temp_table = f"_{db}_{self.table}_history"

            source_temp_delta += (
                f"CREATE OR REPLACE TEMP TABLE {lake_history_db_temp_table} AS \n"
                f"SELECT DISTINCT \n"
            )
            # Add columns
            for col in column_list:
                source_temp_delta += f"\n    {col},"
            source_temp_delta += (
                f"\n    meta_row_source, "
                f"\n    hvr_change_op, "
                f"\n    CAST(META_SOURCE_CHANGE_DATETIME AS TIMESTAMP_LTZ(3))  as effective_start_datetime, "
                f"\n    {lake_db_dict[db]} as data_source_id "
                f"\nFROM {lake_history_db_table} "
                f"\nWHERE META_SOURCE_CHANGE_DATETIME >= DATEADD(MINUTE, -5, ${db}_watermark)"
                f"\nQUALIFY ROW_NUMBER() OVER(PARTITION BY {uniqueness_str}, META_SOURCE_CHANGE_DATETIME"
                f"\n    ORDER BY HVR_CHANGE_SEQUENCE DESC) = 1; "
            )

            # Build the Company_ID temp tables
            if self.company_join_sql and self.table_type.include_company_id is True:
                company_join_subquery = self.company_join_sql.format(
                    database=db, source_schema=f"{self.schema}_history", schema=self.schema
                )
                company_join_temp_table = f"_{db}_company_{self.table}"
                company_join_temp_tables += (
                    f"CREATE OR REPLACE TEMP TABLE {company_join_temp_table} AS \n"
                    f"SELECT {uniqueness_str}, company_id as meta_company_id \n"
                    f"FROM ({company_join_subquery} \n"
                    f"    WHERE l.META_SOURCE_CHANGE_DATETIME >= DATEADD(MINUTE, -5, ${db}_watermark) \n"
                    f"    ) AS l \n"
                )
                if self.table_type != TableType.REGULAR_GLOBAL:
                    company_join_temp_tables += (
                        f"QUALIFY ROW_NUMBER() OVER(PARTITION BY {uniqueness_str} "
                        f"ORDER BY company_id ASC) = 1"
                    )
                company_join_temp_tables += "; \n"

                # Build the Source Unions
                if lake_db_loop > 0:
                    source_union += '\n\n    UNION ALL\n'
                source_union += (
                    f"    SELECT "
                    f"\n        A.*"
                    f",\n        COALESCE(CJ.meta_company_id, 40) AS meta_company_id "
                    f"\n    FROM {lake_history_db_temp_table} AS A "
                    f"\n    LEFT JOIN {company_join_temp_table} AS CJ "
                )

                company_join_cond = ''
                if self.company_join_columns is not None:
                    for idx, col in enumerate(self.company_join_columns):
                        if idx == 0:
                            company_join_cond += '\n        ON'
                        else:
                            company_join_cond += '\n        AND'
                        company_join_cond += f" cj.{col} = a.{col}"
                else:
                    for idx, col in enumerate(self.column_list.uniqueness_cols_str):
                        if idx == 0:
                            company_join_cond += '\n        ON'
                        else:
                            company_join_cond += '\n        AND'
                        company_join_cond += f" cj.{col} = a.{col}"

                company_join_cond += (
                    f"\n    WHERE COALESCE(CJ.meta_company_id, 40)"
                    f" IN ({lake_db_company_dict[db]})"
                )
                source_union += company_join_cond
            else:
                source_union += f"    SELECT A.* FROM {lake_history_db_temp_table} AS A \n"

        merge_cond = ''
        unique_cols = ''
        effective_join_cols = ''
        effective_unique_cols = ''
        if self.table_type.include_datasource_id is True:
            merge_cond = 's.data_source_id = t.data_source_id \n        AND '
            unique_cols = 'data_source_id, \n'
            effective_join_cols = 's.data_source_id = t.data_source_id \n    AND '
            effective_unique_cols = 's.data_source_id, \n'
        if self.table_type == TableType.REGULAR_GLOBAL:
            merge_cond += ' s.meta_company_id = t.meta_company_id \n        AND '
            unique_cols += 'meta_company_id, \n'
            effective_join_cols += 's.meta_company_id = t.meta_company_id \n    AND '
            effective_unique_cols += 's.meta_company_id, \n'

        for idx, col in enumerate(self.column_list.uniqueness_cols_str):
            if idx == 0:
                merge_cond += '        '
                unique_cols += '\n    '
                effective_join_cols += ''
                effective_unique_cols += '\n    '
            else:
                merge_cond += '\n        AND'
                unique_cols += ',\n    '
                effective_join_cols += '\n    AND'
                effective_unique_cols += ',\n    '
            if col in self.column_list.keep_original_column:
                merge_cond += f" s.{col} = t.meta_original_{col}"
                unique_cols += f"    meta_original_{col}"
                effective_join_cols += f"    s.meta_original_{col} = t.meta_original_{col}"
                effective_unique_cols += f"    s.meta_original_{col}"
            else:
                merge_cond += f" s.{col} = t.{col}"
                unique_cols += f"    {col}"
                effective_join_cols += f"    s.{col} = t.{col}"
                effective_unique_cols += f"    s.{col}"

        consolidated_column_merge_insert = ''
        for col in self.column_list.column_name_list:
            if col in self.column_list.key_cols and self.table_type.include_company_id is True:
                consolidated_column_merge_insert += (
                    f"\n        CASE WHEN NULLIF(s.{col}, '0') IS NOT NULL THEN CONCAT(s.{col}, s.meta_company_id) "
                    f"ELSE NULL END,"
                )
            else:
                consolidated_column_merge_insert += f"""\n        s.{col},"""
        for col in self.column_list.key_cols:
            if col in self.column_list.keep_original_column:
                consolidated_column_merge_insert += f"""\n        s.{col},"""

        final_insert = (
            f"INSERT INTO {lake_consolidated_history_table} (\n"
            f"    {'data_source_id,' * (self.table_type.include_datasource_id is True)} \n"
            f"    {'meta_company_id,' * (self.table_type.include_company_id is True)} \n"
            f"    {full_column_str}, \n"
            f"    meta_row_source, \n"
            f"    hvr_change_op, \n"
            f"    effective_start_datetime, \n"
            f"    effective_end_datetime, \n"
            f"    meta_update_datetime \n"
            f") \n"
            f"SELECT DISTINCT \n"
            f"    {'s.data_source_id,' * (self.table_type.include_datasource_id is True)} \n"
            f"    {'s.meta_company_id,' * (self.table_type.include_company_id is True)} \n"
            f"    {full_final_column_str} \n"
            f"    s.meta_row_source, \n"
            f"    s.hvr_change_op, \n"
            f"    s.effective_start_datetime, \n"
            f"    NULL AS effective_end_datetime, \n"
            f"    CURRENT_TIMESTAMP() AS meta_update_datetime \n"
            f"FROM ( \n"
            f"{source_union} \n"
            f"    ) AS s \n"
            f"WHERE NOT EXISTS ( \n"
            f"    SELECT \n"
            f"        1 \n"
            f"    FROM {lake_consolidated_history_table} AS t\n"
            f"    WHERE \n"
            f"        {merge_cond} \n"
            f"        AND s.effective_start_datetime = t.effective_start_datetime \n"
            f"    )\n"
            f"ORDER BY s.effective_start_datetime ASC;\n"
        )

        effective_timestamps_update = ''
        effective_timestamps_update += (
            f"CREATE OR REPLACE temp table _{self.table}_updates as \n"
            "SELECT s.*,\n"
            f"    row_number() over(partition by {effective_unique_cols}\n"
            "ORDER BY s.effective_start_datetime ASC) AS rnk \n"
            "FROM (\n"
            f"SELECT DISTINCT\n"
            f"    {effective_unique_cols}, \n"
            f"    s.effective_start_datetime, \n"
            f"    s.effective_end_datetime \n"
            f"FROM {lake_consolidated_history_table} as s \n"
            f"INNER JOIN ( \n"
            f"    SELECT \n"
            f"        {unique_cols}, \n"
            f"        effective_start_datetime \n"
            f"    FROM {lake_consolidated_history_table} \n"
            f"    WHERE effective_end_datetime is NULL \n"
            f"    ) AS t \n"
            f"    ON {effective_join_cols} \n"
            f"WHERE ( \n"
            f"    s.effective_start_datetime >= t.effective_start_datetime \n"
            f"    OR s.effective_end_datetime = '9999-12-31 00:00:00.000 -0800' \n"
            f")) s;\n\n"
            f"CREATE OR REPLACE TEMP TABLE _{self.table}_delta AS \n"
            f"SELECT \n"
            f"    {effective_unique_cols}, \n"
            f"    s.effective_start_datetime, \n"
            f"    COALESCE(dateadd(MILLISECOND, -1, t.effective_start_datetime), '9999-12-31 00:00:00.000 -0800') "
            f"AS new_effective_end_datetime \n"
            f"FROM _{self.table}_updates AS s \n"
            f"    LEFT JOIN _{self.table}_updates AS t \n"
            f"    ON {effective_join_cols} \n"
            f"    AND S.rnk = T.rnk - 1 \n"
            f"WHERE (S.effective_end_datetime <> dateadd(MILLISECOND, -1, T.effective_start_datetime) \n"
            f"    OR S.effective_end_datetime IS NULL) \n"
            f";\n\n"
            f"UPDATE {lake_consolidated_history_table} AS t \n"
            f"SET t.effective_end_datetime = s.new_effective_end_datetime, \n"
            f"    META_UPDATE_DATETIME = current_timestamp() \n"
            f"FROM _{self.table}_delta AS s \n"
            f"WHERE {effective_join_cols} \n"
            f"    AND s.effective_start_datetime = t.effective_start_datetime \n"
            f"    AND COALESCE(t.effective_end_datetime, '1900-01-01 00:00:00 -0800') "
            f"<> s.new_effective_end_datetime; \n"
        )

        cmd = (
            f"\n{create_history_table_cmd} \n"
            f"\n"
            f"\n{watermarks} \n"
            f"\n"
            f"{source_temp_delta} \n"
            f"\n"
            f"{company_join_temp_tables} \n"
            f"\n"
            f"{final_insert} \n"
            f"\n"
            f"{effective_timestamps_update}\n"
        )

        return cmd
