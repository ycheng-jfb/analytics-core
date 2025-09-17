from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="test_metadata",
    company_join_sql="""
        SELECT DISTINCT
            L.TEST_METADATA_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{source_schema}.test_metadata AS L
            ON DS.STORE_GROUP_ID = L.STORE_GROUP_ID """,
    column_list=[
        Column("test_metadata_id", "INT", uniqueness=True, key=True),
        Column("name", "VARCHAR(255)"),
        Column("label", "VARCHAR(255)"),
        Column("store_group_id", "INT"),
        Column("control_pct", "INT"),
        Column("variant_count", "INT"),
        Column("statuscode", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("datetime_activated", "TIMESTAMP_NTZ(3)"),
    ],
    watermark_column="datetime_modified",
)
