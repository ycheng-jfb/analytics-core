from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="address_validation_request_log",
    company_join_sql="""
        SELECT DISTINCT
            L.address_validation_request_log_id,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.CUSTOMER AS C
            ON DS.STORE_GROUP_ID = C.STORE_GROUP_ID
        INNER JOIN {database}.{source_schema}.address_validation_request_log AS L
            ON L.CUSTOMER_ID = C.CUSTOMER_ID """,
    column_list=[
        Column("address_validation_request_log_id", "INT", uniqueness=True, key=True),
        Column("customer_id", "INT", key=True),
        Column("session_id", "INT", key=True),
        Column("latency", "INT"),
        Column("address1", "VARCHAR(50)"),
        Column("address2", "VARCHAR(25)"),
        Column("city", "VARCHAR(35)"),
        Column("state", "VARCHAR(25)"),
        Column("zip", "VARCHAR(25)"),
        Column("statuscode", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_added",
)
