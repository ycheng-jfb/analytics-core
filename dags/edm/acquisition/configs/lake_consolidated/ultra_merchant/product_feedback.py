from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="product_feedback",
    company_join_sql="""
        SELECT DISTINCT
            L.PRODUCT_FEEDBACK_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{source_schema}.product_feedback AS L
            ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column("product_feedback_id", "INT", uniqueness=True, key=True),
        Column("store_group_id", "INT"),
        Column("label", "VARCHAR(50)"),
        Column("is_positive", "BOOLEAN"),
        Column("sequence_id", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("hidden", "BOOLEAN"),
        Column("auto_remove_from_cart", "BOOLEAN"),
    ],
    watermark_column="datetime_modified",
)
