from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="return_line",
    company_join_sql="""SELECT DISTINCT l.return_line_id, DS.company_id
              from {database}.REFERENCE.dim_store ds
               join {database}.{schema}."ORDER" o
                   on ds.store_id=o.store_id
               join {database}.{schema}.return r
                  on r.order_id=o.order_id
                    join {database}.{source_schema}.return_line l
                  on l.return_id=r.return_id""",
    column_list=[
        Column("return_line_id", "INT", uniqueness=True, key=True),
        Column("return_id", "INT", key=True),
        Column("order_line_id", "INT", key=True),
        Column("product_id", "INT", key=True),
        Column("condition", "VARCHAR(25)"),
        Column("quantity", "INT"),
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
