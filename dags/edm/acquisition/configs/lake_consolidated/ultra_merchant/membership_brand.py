from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
)
from include.utils.acquisition.lake_consolidated_table_config import TableType
from include.utils.snowflake import Column

table_config = TableConfig(
    table_type=TableType.NSYNC,
    table="membership_brand",
    column_list=[
        Column("membership_brand_id", "INT", uniqueness=True),
        Column("store_group_id", "INT"),
        Column("code", "VARCHAR(25)"),
        Column("label", "VARCHAR(50)"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)"),
        Column("product_enabled", "BOOLEAN"),
        Column("membership_enabled", "BOOLEAN"),
    ],
    watermark_column="datetime_added",
)
