from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.REGULAR_GLOBAL,
    table="asset",
    company_join_sql="""
        SELECT DISTINCT
            L.ASSET_ID,
            COALESCE(ds.company_id, sg.company_id) as company_id
        FROM {database}.{source_schema}.asset AS L
        LEFT JOIN {database}.{source_schema}.product AS p
            ON p.product_id = L.object_id
            AND L.object = 'product'
        LEFT JOIN {database}.REFERENCE.DIM_STORE AS ds
            ON ds.store_id = p.default_store_id
        LEFT JOIN {database}.REFERENCE.DIM_STORE AS sg
            ON sg.store_group_id = L.object_id
            AND L.object = 'store_group'""",
    column_list=[
        Column("asset_id", "INT", uniqueness=True, key=True),
        Column("object", "VARCHAR(50)"),
        Column("object_id", "INT"),
        Column("asset_type_id", "INT"),
        Column("asset_category_id", "INT"),
        Column("filename", "VARCHAR(255)"),
        Column("path", "VARCHAR(255)"),
        Column("width", "INT"),
        Column("height", "INT"),
        Column("sort", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("statuscode", "INT"),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("ISMAINIMAGE", "BOOLEAN"),
        Column("ISHOVERIMAGE", "BOOLEAN"),
    ],
    watermark_column="datetime_modified",
)
