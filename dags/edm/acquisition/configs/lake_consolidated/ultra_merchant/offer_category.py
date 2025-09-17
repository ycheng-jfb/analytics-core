from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table="offer_category",
    table_type=TableType.REGULAR_GLOBAL,
    company_join_sql="""
       SELECT DISTINCT
           L.OFFER_CATEGORY_ID,
           DS.COMPANY_ID
       FROM {database}.{source_schema}.offer_category AS L
       JOIN (
            SELECT DISTINCT company_id
            FROM {database}.REFERENCE.DIM_STORE
            WHERE company_id IS NOT NULL
            ) AS DS """,
    column_list=[
        Column("offer_category_id", "INT", uniqueness=True, key=True),
        Column("store_group_id", "INT"),
        Column("parent_offer_category_id", "INT"),
        Column("label", "VARCHAR(50)"),
        Column("sort", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_added",
)
