from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="marin_tracking",
    company_join_sql="""
        SELECT DISTINCT
            L.MARIN_TRACKING_ID,
            DS.company_id
        FROM {database}.REFERENCE.DIM_STORE AS DS
        INNER JOIN {database}.{schema}.SESSION AS S
            ON DS.STORE_ID = S.STORE_ID
        INNER JOIN {database}.{source_schema}.marin_tracking AS L
            ON S.SESSION_ID = L.SESSION_ID """,
    column_list=[
        Column("marin_tracking_id", "INT", uniqueness=True, key=True),
        Column("session_id", "INT", key=True),
        Column("mkwid", "VARCHAR(25)"),
        Column("pcrid", "VARCHAR(25)"),
        Column("pkw", "VARCHAR(500)"),
        Column("pmt", "VARCHAR(10)"),
        Column("pdv", "VARCHAR(10)"),
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
