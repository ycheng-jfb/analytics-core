from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table="zipcode_range_warehouse_map",
    column_list=[
        Column("zipcode_range_warehouse_map_id", "INT", uniqueness=True),
        Column("zipcode_start", "VARCHAR(25)"),
        Column("zipcode_end", "VARCHAR(25)"),
        Column("warehouse_id", "INT"),
        Column(
            "datetime_added",
            "TIMESTAMP_NTZ(3)",
        ),
        Column(
            "datetime_modified",
            "TIMESTAMP_NTZ(3)",
        ),
        Column("store_group_id", "INT"),
        Column("is_rush_shipping", "INT"),
        Column("allocation", "NUMBER(19,2)"),
    ],
    watermark_column="datetime_modified",
)
