from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="membership_product_wait_list_order_log",
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_PRODUCT_WAIT_LIST_ORDER_LOG_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}."ORDER" AS O
            ON DS.STORE_ID = O.STORE_ID
        INNER JOIN {database}.{source_schema}.membership_product_wait_list_order_log AS L
            ON L.ORDER_ID = O.ORDER_ID """,
    column_list=[
        Column(
            "membership_product_wait_list_order_log_id",
            "INT",
            uniqueness=True,
            key=True,
        ),
        Column("membership_product_wait_list_id", "INT", key=True),
        Column("order_id", "INT", key=True),
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
