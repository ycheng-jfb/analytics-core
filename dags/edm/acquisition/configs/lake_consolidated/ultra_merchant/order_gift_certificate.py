from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="order_gift_certificate",
    company_join_sql="""
        SELECT DISTINCT
            L.ORDER_GIFT_CERTIFICATE_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.GIFT_CERTIFICATE AS GC
            ON DS.STORE_GROUP_ID = GC.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.order_gift_certificate AS L
            ON L.GIFT_CERTIFICATE_ID = GC.GIFT_CERTIFICATE_ID """,
    column_list=[
        Column(
            "order_gift_certificate_id",
            "INT",
            uniqueness=True,
            key=True,
        ),
        Column("order_line_id", "INT", key=True),
        Column("gift_certificate_id", "INT", key=True),
    ],
    watermark_column="order_gift_certificate_id",
)
