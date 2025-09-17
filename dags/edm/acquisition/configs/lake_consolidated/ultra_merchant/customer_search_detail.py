from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NAME_VALUE_COLUMN,
    table="customer_search_detail",
    company_join_sql="""
     SELECT DISTINCT
         L.CUSTOMER_SEARCH_DETAIL_ID,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{schema}.CUSTOMER_SEARCH AS CS
     ON DS.STORE_GROUP_ID = CS.STORE_GROUP_ID
     INNER JOIN {database}.{source_schema}.customer_search_detail AS L
     ON L.customer_search_id=CS.customer_search_id """,
    column_list=[
        Column("customer_search_detail_id", "INT", uniqueness=True, key=True),
        Column("customer_search_id", "INT", key=True),
        Column("name", "VARCHAR(50)"),
        Column("value", "VARCHAR(3500)"),
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
