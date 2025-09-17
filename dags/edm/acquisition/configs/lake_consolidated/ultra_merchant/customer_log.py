from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NAME_VALUE_COLUMN,
    table="customer_log",
    company_join_sql="""
        SELECT DISTINCT
            L.customer_log_id,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.CUSTOMER AS C
            ON DS.STORE_GROUP_ID = C.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.customer_log AS L
            ON L.CUSTOMER_ID = C.CUSTOMER_ID """,
    column_list=[
        Column("customer_log_id", "INT", uniqueness=True, key=True),
        Column("customer_id", "INT", key=True),
        Column("customer_log_source_id", "INT"),
        Column("administrator_id", "INT"),
        Column("object", "VARCHAR(50)"),
        Column("object_id", "INT"),
        Column("comment", "VARCHAR"),
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
