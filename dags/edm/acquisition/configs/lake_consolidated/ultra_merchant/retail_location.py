from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="retail_location",
    company_join_sql="""
    SELECT DISTINCT
        L.retail_location_id,
        DS.company_id
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{source_schema}.retail_location AS L
        ON L.STORE_ID = DS.STORE_ID""",
    column_list=[
        Column("retail_location_id", "INT", uniqueness=True, key=True),
        Column("retailer_id", "INT", key=True),
        Column("label", "VARCHAR(100)"),
        Column("address", "VARCHAR(100)"),
        Column("city", "VARCHAR(35)"),
        Column("state", "VARCHAR(2)"),
        Column("zip", "VARCHAR(10)"),
        Column("phone", "VARCHAR(25)"),
        Column("store_id", "INT"),
        Column("hours", "VARCHAR(200)"),
        Column("latitude", "VARCHAR(50)"),
        Column("longitude", "VARCHAR(50)"),
        Column("statuscode", "INT"),
        Column("data", "VARCHAR(1000)"),
    ],
)
