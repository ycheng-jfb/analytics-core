from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table="product_wait_list_type",
    column_list=[
        Column("product_wait_list_type_id", "INT", uniqueness=True),
        Column("store_group_id", "INT"),
        Column("label", "VARCHAR(100)"),
        Column("code", "VARCHAR(25)"),
        Column("is_active", "BOOLEAN"),
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
