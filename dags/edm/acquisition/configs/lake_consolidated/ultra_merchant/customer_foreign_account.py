from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="customer_foreign_account",
    company_join_sql="""
       SELECT DISTINCT
           L.customer_foreign_account_id,
           DS.company_id
       FROM {database}.REFERENCE.DIM_STORE AS DS
       INNER JOIN {database}.{schema}.CUSTOMER AS C
           ON DS.STORE_GROUP_ID = C.STORE_GROUP_ID
       INNER JOIN {database}.{source_schema}.customer_foreign_account AS L
           ON L.CUSTOMER_ID = C.CUSTOMER_ID """,
    column_list=[
        Column("customer_foreign_account_id", "INT", uniqueness=True, key=True),
        Column("customer_foreign_account_type_id", "INT"),
        Column("customer_id", "INT", key=True),
        Column("foreign_account_id", "VARCHAR(50)"),
        Column("datetime_relationship_updated", "TIMESTAMP_NTZ(3)"),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("active", "INT"),
    ],
    watermark_column="datetime_modified",
)
