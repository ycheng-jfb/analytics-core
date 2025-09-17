from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="retail_return",
    company_join_sql="""SELECT DISTINCT l.retail_return_id, ds.company_id
                    from {database}.REFERENCE.dim_store ds
                      join {database}.{schema}."ORDER" o
                          on ds.store_id=o.store_id
                       join {database}.{source_schema}.retail_return l
                       on l.order_id=o.order_id""",
    column_list=[
        Column("retail_return_id", "INT", uniqueness=True, key=True),
        Column("retail_return_code", "VARCHAR(255)"),
        Column("order_id", "INT", key=True),
        Column("administrator_id", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("statuscode", "INT"),
        Column("store_id", "INT"),
    ],
    watermark_column="datetime_modified",
)
