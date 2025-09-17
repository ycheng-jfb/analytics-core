from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    company_join_sql="""
     SELECT DISTINCT
         L.customer_referrer_id,
         DS.company_id
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{source_schema}.customer_referrer AS L
     ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    table="customer_referrer",
    column_list=[
        Column("customer_referrer_id", "INT", uniqueness=True, key=True),
        Column("store_group_id", "INT"),
        Column("object", "VARCHAR(50)"),
        Column("object_id", "INT"),
        Column("label", "VARCHAR(50)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("sort", "INT"),
        Column("active", "INT"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)"),
        Column("referrer_ref_id", "INT"),
        Column("customer_referrer_type_id", "INT"),
        Column("group_name", "VARCHAR(100)"),
        Column("is_influencer", "BOOLEAN"),
    ],
    watermark_column="datetime_modified",
)
