from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="order_classification",
    company_join_sql="""
        SELECT DISTINCT
            L.ORDER_CLASSIFICATION_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}."ORDER" AS O
            ON O.STORE_ID = DS.STORE_ID
        INNER JOIN {database}.{source_schema}.order_classification AS L
            ON O.ORDER_ID = L.ORDER_ID""",
    column_list=[
        Column("order_classification_id", "INT", uniqueness=True, key=True),
        Column("order_id", "INT", key=True),
        Column("order_type_id", "INT"),
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
