from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="retail_store_address",
    company_join_sql="""
    SELECT DISTINCT
        L.address_id,
        DS.company_id
    FROM {database}.REFERENCE.DIM_STORE AS DS
    INNER JOIN {database}.{source_schema}.retail_store_address AS L
        ON L.STORE_ID = DS.STORE_ID""",
    column_list=[
        Column("store_id", "INT"),
        Column("address_id", "INT", uniqueness=True, key=True),
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
