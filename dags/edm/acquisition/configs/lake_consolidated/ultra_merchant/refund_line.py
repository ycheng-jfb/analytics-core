from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="refund_line",
    company_join_sql="""SELECT DISTINCT l.refund_line_id, ds.company_id
      from {database}.REFERENCE.dim_store ds
         join {database}.{schema}."ORDER" r
              on ds.STORE_ID=r.STORE_ID
         join {database}.{schema}.refund t
           on t.order_id=r.order_id
         join {database}.{source_schema}.refund_line l
          on l.refund_id=t.refund_id""",
    column_list=[
        Column("refund_line_id", "INT", uniqueness=True, key=True),
        Column("refund_id", "INT", key=True),
        Column("order_line_id", "INT", key=True),
        Column("refund_amount", "NUMBER(19, 4)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)"),
    ],
    watermark_column="datetime_modified",
)
