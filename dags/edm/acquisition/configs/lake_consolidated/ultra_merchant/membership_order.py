from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="membership_order",
    company_join_sql="""
        SELECT DISTINCT
            L.MEMBERSHIP_ORDER_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}."ORDER" AS O
            ON DS.STORE_ID = O.STORE_ID
        INNER JOIN {database}.{source_schema}.membership_order AS L
            ON L.ORDER_ID = O.ORDER_ID """,
    column_list=[
        Column("membership_order_id", "INT", uniqueness=True, key=True),
        Column("membership_id", "INT", key=True),
        Column("order_id", "INT", key=True),
        Column("period_id", "INT"),
        Column("is_credit_order", "BOOLEAN"),
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
