from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NAME_VALUE_COLUMN,
    table="order_detail",
    company_join_sql="""
        SELECT DISTINCT
            L.ORDER_DETAIL_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}."ORDER" AS O
            ON DS.STORE_ID = O.STORE_ID
        INNER JOIN {database}.{source_schema}.order_detail AS L
            ON L.ORDER_ID = O.ORDER_ID""",
    column_list=[
        Column("order_detail_id", "INT", uniqueness=True, key=True),
        Column("order_id", "INT", key=True),
        Column("name", "VARCHAR(50)"),
        Column("value", "VARCHAR(255)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_modified",
)
