from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="order_shipment_line",
    company_join_sql="""
        SELECT DISTINCT
            L.ORDER_SHIPMENT_ID,
            L.ORDER_LINE_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}."ORDER" AS O
            ON DS.STORE_ID = O.STORE_ID
        INNER JOIN {database}.{schema}.ORDER_LINE AS OL
            ON OL.ORDER_ID = O.ORDER_ID
        INNER JOIN {database}.{source_schema}.order_shipment_line AS L
            ON L.ORDER_LINE_ID = OL.ORDER_LINE_ID """,
    column_list=[
        Column("order_shipment_id", "INT", uniqueness=True, key=True),
        Column("order_line_id", "INT", uniqueness=True, key=True),
        Column("item_id", "INT"),
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
