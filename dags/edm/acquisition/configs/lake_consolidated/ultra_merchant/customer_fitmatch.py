from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="customer_fitmatch",
    company_join_sql="""
     SELECT DISTINCT
         L.customer_fitmatch_id,
         DS.COMPANY_ID
     FROM {database}.REFERENCE.DIM_STORE AS DS
     INNER JOIN {database}.{source_schema}.customer_fitmatch AS L
     ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column("customer_fitmatch_id", "INT", uniqueness=True, key=True),
        Column("customer_id", "INT", key=True),
        Column("store_id", "INT"),
        Column("store_group_id", "INT"),
        Column("scan_id", "VARCHAR(255)"),
        Column("fitmatch_user_id", "VARCHAR(255)"),
        Column("bust_size", "VARCHAR(50)"),
        Column("band_size", "VARCHAR(50)"),
        Column("active", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_registered",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_scan_completed",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_scan_started",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_modified",
)
