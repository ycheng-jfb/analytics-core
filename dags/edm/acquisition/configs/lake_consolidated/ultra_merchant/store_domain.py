from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="store_domain",
    company_join_sql="""
        SELECT DISTINCT
            L.STORE_DOMAIN_ID,
            DS.COMPANY_ID
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{source_schema}.store_domain AS L
            ON DS.STORE_ID = L.STORE_ID """,
    column_list=[
        Column("store_domain_id", "INT", uniqueness=True, key=True),
        Column("store_id", "INT"),
        Column("store_domain_type_id", "INT"),
        Column("domain_name", "VARCHAR(64)"),
        Column("base_domain", "VARCHAR(64)"),
        Column("http_base_url", "VARCHAR(128)"),
        Column("https_base_url", "VARCHAR(128)"),
        Column("environment", "VARCHAR(15)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("http_media_url", "VARCHAR(128)"),
        Column("https_media_url", "VARCHAR(128)"),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_modified",
)
