from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="hdyh_global_list",
    company_join_sql="""
    SELECT l.hdyh_global_list_id, ds.company_id
    FROM {database}.reference.dim_store AS ds
    JOIN {database}.{schema}.hdyh_global_list AS l
        ON ds.store_group_id = l.store_group_id
    """,
    column_list=[
        Column("hdyh_global_list_id", "INT", uniqueness=True, key=True),
        Column("group_name", "VARCHAR(250)"),
        Column("global_code", "VARCHAR(250)"),
        Column("store_group_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)"),
        Column("administrator_id", "INT", key=True),
        Column("membership_brand_id_list", "VARCHAR(250)"),
        Column("is_active", "BOOLEAN"),
        Column("list_data", "VARCHAR(100)"),
    ],
    watermark_column="datetime_modified",
)
