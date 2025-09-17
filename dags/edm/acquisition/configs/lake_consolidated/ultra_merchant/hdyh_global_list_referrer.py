from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="hdyh_global_list_referrer",
    company_join_sql="""
    SELECT l.hdyh_global_list_referrer_id, ds.company_id
    FROM {database}.{schema}.hdyh_global_list_referrer l
    JOIN {database}.reference.dim_store ds
        ON l.store_group_id = ds.store_group_id """,
    column_list=[
        Column("hdyh_global_list_referrer_id", "INT", uniqueness=True, key=True),
        Column("store_group_id", "INT"),
        Column("hdyh_global_list_id", "INT", key=True),
        Column("customer_referrer_id", "INT", key=True),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)"),
        Column("administrator_id", "INT", key=True),
        Column("is_active", "BOOLEAN"),
    ],
    watermark_column="datetime_modified",
)
