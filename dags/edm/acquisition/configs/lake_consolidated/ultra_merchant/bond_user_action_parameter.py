from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
    TableType,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="bond_user_action_parameter",
    company_join_sql="""
    SELECT DISTINCT
         L.bond_user_action_parameter_id,
         DS.company_id
     FROM {database}.reference.dim_store AS DS
     INNER JOIN {database}.{source_schema}.case AS C
        ON DS.store_group_id = C.store_group_id
     INNER JOIN {database}.{source_schema}.bond_user_action AS B
        ON C.case_id = B.case_id
     INNER JOIN {database}.{source_schema}.bond_user_action_parameter AS L
        ON B.bond_user_action_id = L.bond_user_action_id
    """,
    table_type=TableType.NAME_VALUE_COLUMN,
    column_list=[
        Column("bond_user_action_parameter_id", "INT", uniqueness=True),
        Column("bond_user_action_id", "INT", key=True),
        Column("parameter_name", "VARCHAR(255)"),
        Column("parameter_value", "VARCHAR"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)"),
    ],
    watermark_column="datetime_modified",
)
