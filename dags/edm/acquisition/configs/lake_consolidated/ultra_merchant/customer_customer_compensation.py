from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.OBJECT_COLUMN_NULL,
    table="customer_customer_compensation",
    company_join_sql="""
        SELECT DISTINCT
            L.customer_customer_compensation_id,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.CUSTOMER AS C
            ON DS.STORE_GROUP_ID = C.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.customer_customer_compensation AS L
            ON L.CUSTOMER_ID = C.CUSTOMER_ID """,
    column_list=[
        Column("customer_customer_compensation_id", "INT", uniqueness=True, key=True),
        Column("customer_id", "INT", key=True),
        Column("customer_compensation_id", "INT", key=True),
        Column("administrator_id", "INT"),
        Column("approved_administrator_id", "INT", key=True),
        Column("object", "VARCHAR(50)"),
        Column("object_id", "INT"),
        Column("previous_balance", "DOUBLE"),
        Column("balance", "DOUBLE"),
        Column("customer_log_id", "INT", key=True),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("customer_compensation_vip_reason_id", "INT"),
    ],
    watermark_column="datetime_modified",
)
