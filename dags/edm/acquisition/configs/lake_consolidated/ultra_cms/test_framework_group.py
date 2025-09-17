from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    schema="ultra_cms",
    table="test_framework_group",
    column_list=[
        Column("test_framework_group_id", "INT", uniqueness=True),
        Column("group_name", "VARCHAR(20)"),
        Column("activated_group", "INT"),
        Column("device_type", "VARCHAR(50)"),
        Column("number_of_groups", "INT"),
        Column("active", "BOOLEAN"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)"),
    ],
    watermark_column="datetime_modified",
)
