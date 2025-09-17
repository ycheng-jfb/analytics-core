from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NAME_VALUE_COLUMN,
    table="customer_detail",
    company_join_sql="""
        SELECT DISTINCT
            L.CUSTOMER_DETAIL_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.CUSTOMER AS C
            ON DS.STORE_GROUP_ID = C.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.customer_detail AS L
            ON L.CUSTOMER_ID = C.CUSTOMER_ID """,
    column_list=[
        Column("customer_detail_id", "INT", uniqueness=True, key=True),
        Column("customer_id", "INT", key=True),
        Column("name", "VARCHAR(50)"),
        Column("value", "VARCHAR(255)"),
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
