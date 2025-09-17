from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="CUSTOMER_LINK",
    company_join_sql="""
       SELECT DISTINCT
           L.CUSTOMER_LINK_ID,
           DS.COMPANY_ID
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}.CUSTOMER AS C
           ON DS.STORE_GROUP_ID = C.STORE_GROUP_ID
       INNER JOIN {database}.{source_schema}.customer_link AS L
           ON L.ORIGINAL_CUSTOMER_ID = C.CUSTOMER_ID """,
    column_list=[
        Column("customer_link_id", "INT", uniqueness=True, key=True),
        Column("customer_link_type_id", "INT"),
        Column("original_customer_id", "INT", key=True),
        Column("current_customer_id", "INT", key=True),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_added",
)
