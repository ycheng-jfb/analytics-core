from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="order_chargeback_log",
    company_join_sql="""
        SELECT DISTINCT
            L.ORDER_CHARGEBACK_LOG_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}."ORDER" AS O
            ON DS.STORE_ID = O.STORE_ID
        INNER JOIN {database}.{schema}.ORDER_CHARGEBACK AS OC
            ON O.ORDER_ID = OC.ORDER_ID
        INNER JOIN {database}.{source_schema}.order_chargeback_log AS L
            ON OC.ORDER_CHARGEBACK_ID = L.ORDER_CHARGEBACK_ID""",
    column_list=[
        Column("order_chargeback_log_id", "INT", uniqueness=True, key=True),
        Column("order_chargeback_id", "INT", key=True),
        Column("administrator_id", "INT"),
        Column("orignal_statuscode", "INT"),
        Column("modified_statuscode", "INT"),
        Column("comments", "VARCHAR(200)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_added",
)
