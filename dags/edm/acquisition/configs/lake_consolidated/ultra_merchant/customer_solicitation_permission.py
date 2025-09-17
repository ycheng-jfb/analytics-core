from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.OBJECT_COLUMN_NULL,
    table="customer_solicitation_permission",
    company_join_sql="""
     SELECT DISTINCT
         L.customer_solicitation_permission_id,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{schema}.CUSTOMER  AS C
     ON DS.STORE_GROUP_ID = C.STORE_GROUP_ID
     INNER JOIN {database}.{source_schema}.customer_solicitation_permission AS L
     ON C.CUSTOMER_ID = L.CUSTOMER_ID """,
    column_list=[
        Column("customer_solicitation_permission_id", "INT", uniqueness=True, key=True),
        Column("customer_id", "INT", key=True),
        Column("type", "VARCHAR(25)"),
        Column("object", "VARCHAR(50)"),
        Column("object_id", "INT"),
        Column("ip", "VARCHAR(15)"),
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
