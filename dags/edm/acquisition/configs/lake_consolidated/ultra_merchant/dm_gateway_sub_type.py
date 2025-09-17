from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table="dm_gateway_sub_type",
    column_list=[
        Column("dm_gateway_sub_type_id", "INT", uniqueness=True),
        Column("dm_gateway_type_id", "INT"),
        Column("label", "VARCHAR(50)"),
        Column("sort", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("active", "BOOLEAN"),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
    ],
    watermark_column="datetime_modified",
)
