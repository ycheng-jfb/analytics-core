from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table="credit_transfer_transaction_type",
    column_list=[
        Column("credit_transfer_transaction_type_id", "INT", uniqueness=True),
        Column("type", "VARCHAR(25)"),
        Column("label", "VARCHAR(100)"),
        Column("object", "VARCHAR(50)"),
        Column("foreign_source_code", "VARCHAR(50)"),
        Column("debit_credit_transfer_transaction_type_id", "INT"),
        Column("store_credit_reason_id", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_added",
)
