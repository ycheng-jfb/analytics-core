from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="order_line_warehouse_mapping_log_detail",
    company_join_sql="""
        SELECT DISTINCT
            L.ORDER_LINE_WAREHOUSE_MAPPING_LOG_DETAIL_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}."ORDER" AS O
            ON DS.STORE_ID = O.STORE_ID
        INNER JOIN {database}.{source_schema}.order_line_warehouse_mapping_log_detail AS L
            ON L.ORDER_ID = O.ORDER_ID """,
    column_list=[
        Column(
            "order_line_warehouse_mapping_log_detail_id",
            "INT",
            uniqueness=True,
            key=True,
        ),
        Column("order_line_warehouse_mapping_log_id", "INT", key=True),
        Column("order_id", "INT", key=True),
        Column("order_line_id", "INT", key=True),
        Column("item_id", "INT"),
        Column("quantity", "INT"),
        Column("selected_warehouse_id", "INT"),
        Column("available_warehouse_id", "INT"),
        Column("available_quantity", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("preallocated_quantity", "INT"),
    ],
    watermark_column="datetime_added",
)
