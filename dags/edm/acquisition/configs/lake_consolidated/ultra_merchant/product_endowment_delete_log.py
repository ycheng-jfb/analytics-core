from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="product_endowment_delete_log",
    company_join_sql="""
            SELECT DISTINCT
                L.product_endowment_delete_log_id,
                DS.company_id
            FROM {database}.REFERENCE.DIM_STORE AS DS
            INNER JOIN {database}.{source_schema}.product_endowment_delete_log AS L
                ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID
    """,
    column_list=[
        Column("product_endowment_delete_log_id", "INT", uniqueness=True, key=True),
        Column("product_endowment_id", "INT"),
        Column("store_group_id", "INT"),
        Column("product_id", "INT"),
        Column("endowment_amount", "DECIMAL(19,4)"),
        Column("is_endowment_eligible", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
    ],
    watermark_column="datetime_added",
)
