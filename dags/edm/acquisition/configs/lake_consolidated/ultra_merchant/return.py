from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="return",
    company_join_sql="""SELECT DISTINCT l.return_id, ds.company_id
              from {database}.REFERENCE.dim_store ds
              join {database}.{schema}."ORDER" o
                   on ds.store_id=o.store_id
                join {database}.{source_schema}.return l
                   on l.order_id=o.order_id""",
    column_list=[
        Column("return_id", "INT", uniqueness=True, key=True),
        Column("order_id", "INT", key=True),
        Column("administrator_id", "INT"),
        Column("rma_id", "INT", key=True),
        Column("refund_id", "INT", key=True),
        Column("exchange_order_id", "INT", key=True),
        Column("return_action_request_id", "INT"),
        Column("return_category_id", "INT"),
        Column("return_batch_id", "INT", key=True),
        Column("return_reason_id", "INT"),
        Column("reason_comment", "VARCHAR(255)"),
        Column("contains_unidentified_items", "INT"),
        Column("process_mode", "VARCHAR(10)"),
        Column("review_by_administrator_id", "INT"),
        Column("date_added", "TIMESTAMP_NTZ(0)"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("date_received", "TIMESTAMP_NTZ(0)"),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("datetime_resolved", "TIMESTAMP_NTZ(3)"),
        Column("statuscode", "INT"),
        Column("datetime_transaction", "TIMESTAMP_NTZ(3)"),
        Column("datetime_local_transaction", "TIMESTAMP_NTZ(3)"),
    ],
    watermark_column="datetime_modified",
)
