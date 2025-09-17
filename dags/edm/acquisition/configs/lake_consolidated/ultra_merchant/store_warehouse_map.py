from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="store_warehouse_map",
    company_join_sql="""
        SELECT DISTINCT
            L.STORE_ID,
            L.COUNTRY_CODE,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{source_schema}.store_warehouse_map AS L
            ON DS.STORE_ID = L.STORE_ID """,
    column_list=[
        Column("store_id", "INT", uniqueness=True),
        Column("country_code", "VARCHAR(2)", uniqueness=True),
        Column("warehouse_id", "INT"),
        Column("shipper_item_number", "VARCHAR(30)"),
    ],
)
