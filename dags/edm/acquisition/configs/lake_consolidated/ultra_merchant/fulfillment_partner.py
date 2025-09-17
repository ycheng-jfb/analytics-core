from include.utils.acquisition.lake_consolidated_table_config import (
    LakeConsolidatedTableConfig as TableConfig,
    TableType,
)
from include.utils.snowflake import Column

table_config = TableConfig(
    table="fulfillment_partner",
    table_type=TableType.NSYNC,
    watermark_column="datetime_modified",
    column_list=[
        Column("fulfillment_partner_id", "INT", uniqueness=True),
        Column("warehouse_id", "INT"),
        Column("label", "VARCHAR(50)"),
        Column("code", "VARCHAR(25)"),
        Column("active", "BOOLEAN"),
        Column("datetime_added", "TIMESTAMP_NTZ(3)", delta_column=1),
        Column("datetime_modified", "TIMESTAMP_NTZ(3)", delta_column=0),
    ],
)
