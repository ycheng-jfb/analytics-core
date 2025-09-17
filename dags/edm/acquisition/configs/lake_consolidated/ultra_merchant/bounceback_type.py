from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
    TableType,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="bounceback_type",
    table_type=TableType.NSYNC,
    column_list=[
        Column("bounceback_type_id", "INT", uniqueness=True),
        Column("label", "VARCHAR(50)"),
        Column("object", "VARCHAR(50)"),
        Column("gift_certificate_type_id", "INT"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
    ],
    watermark_column="datetime_added",
)
