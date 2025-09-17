from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="suggested_product",
    company_join_sql="""
            SELECT DISTINCT
                L.suggested_product_id,
                ds.company_id
            FROM {database}.reference.dim_store AS ds
            INNER  JOIN {database}.{schema}.product AS p
            ON ds.store_group_id = p.store_group_id
            INNER JOIN {database}.{source_schema}.suggested_product AS L
            ON L.product_id=p.product_id""",
    column_list=[
        Column("suggested_product_id", "INT", uniqueness=True, key=True),
        Column("suggested_product_type_id", "INT"),
        Column("product_id", "INT", key=True),
        Column("related_product_id", "INT", key=True),
        Column("sort", "INT"),
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
